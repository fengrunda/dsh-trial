#!/usr/bin/env python3
"""In-session retry for transient upstream/environment failures (G8 hot path).

dsh-acp-ask.py runs per ticket, so retry logic lives there to take effect
without restarting the broker. Timeouts/early exits/ordinary 4xx are not
retried; exhausted transient failures surface as ``env_blocked(...)``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent  # .../broker

spec = importlib.util.spec_from_file_location(
    "dsh_acp_ask_env_retry_mod", ROOT / "dsh-acp-ask.py"
)
ask = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ask)

OBSERVED_DEVELOPER_400 = (
    "ACP session/prompt failed: {'jsonrpc': '2.0', 'id': 4, 'error': "
    "{'code': -32603, 'message': 'Internal error: turn failed: 400: "
    '{"message":"Upstream request failed: [invalid_request_error] chat '
    "message role `developer` is not supported by the router gRPC frontend "
    "(request_id: gw-f283724097e-3f1b06)\",...}'}}"
)


class FakeClient:
    """Yields scripted outcomes: str results or exceptions."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, str]] = []

    def session_prompt(self, sid: str, prompt: str) -> str:
        self.calls.append((sid, prompt))
        item = self.outcomes.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


# --- classify_transient -----------------------------------------------------


@pytest.mark.parametrize(
    "err,reason",
    [
        (OBSERVED_DEVELOPER_400, "developer_role_400"),
        ("role `developer` is not supported", "developer_role_400"),
        ("ROLE 'DEVELOPER' IS NOT SUPPORTED", "developer_role_400"),
        ("turn failed: 429 Too Many Requests", "rate_limit"),
        ("turn failed: rate limit exceeded", "rate_limit"),
        ("turn failed: 500 internal", "upstream_5xx"),
        ("turn failed: 503", "upstream_5xx"),
        ("upstream said Bad Gateway", "upstream_5xx"),
        ("Service Unavailable", "upstream_5xx"),
        ("Gateway Timeout", "upstream_5xx"),
        ("TRANSPORT broken pipe", "transport"),
        ("Connection error while writing", "transport"),
        ("read ECONNRESET", "transport"),
        ("socket hang up", "transport"),
    ],
)
def test_classify_transient_hits(err, reason):
    assert ask.classify_transient(err) == reason


@pytest.mark.parametrize(
    "err",
    [
        "ACP session/prompt failed: timeout after 3600s hard cap",
        "ACP session/prompt failed: timeout after 900s idle",
        "ACP session/prompt failed: dsh-acp process exited early before replying",
        "ACP session/prompt failed: turn failed: 400 invalid params",
        "ACP session/prompt failed: turn failed: 404 not found",
        "something entirely different",
    ],
)
def test_classify_transient_misses(err):
    assert ask.classify_transient(err) is None


# --- prompt_with_env_retry --------------------------------------------------


def test_retry_then_success_reuses_session_with_continue_prompt():
    client = FakeClient(
        [RuntimeError(OBSERVED_DEVELOPER_400), "ticket done"]
    )
    sleeps: list[float] = []

    out = ask.prompt_with_env_retry(
        client,
        "sid-1",
        "original prompt",
        retries=2,
        base_backoff=20,
        sleep=sleeps.append,
    )

    assert out == "ticket done"
    assert sleeps == [20]
    assert [c[0] for c in client.calls] == ["sid-1", "sid-1"]
    assert client.calls[0][1] == "original prompt"
    assert client.calls[1][1] == ask.ENV_RETRY_CONTINUE_PROMPT


def test_exhausted_retries_prefix_env_blocked_and_back_off_exponentially():
    client = FakeClient(
        [RuntimeError(OBSERVED_DEVELOPER_400) for _ in range(3)]
    )
    sleeps: list[float] = []

    with pytest.raises(RuntimeError) as ei:
        ask.prompt_with_env_retry(
            client,
            "sid-1",
            "p",
            retries=2,
            base_backoff=20,
            sleep=sleeps.append,
        )

    assert str(ei.value).startswith(
        "env_blocked(developer_role_400, retries=2): "
    )
    assert sleeps == [20, 40]
    assert len(client.calls) == 3


def test_retries_zero_does_not_retry():
    client = FakeClient([RuntimeError("turn failed: 503")])
    sleeps: list[float] = []

    with pytest.raises(RuntimeError) as ei:
        ask.prompt_with_env_retry(
            client, "sid-1", "p", retries=0, base_backoff=20, sleep=sleeps.append
        )

    assert str(ei.value).startswith("env_blocked(upstream_5xx, retries=0): ")
    assert sleeps == []
    assert len(client.calls) == 1


def test_non_transient_error_is_reraised_without_retry():
    client = FakeClient([RuntimeError("ACP session/prompt failed: timeout after 900s idle")])
    sleeps: list[float] = []

    with pytest.raises(RuntimeError, match="timeout after 900s idle"):
        ask.prompt_with_env_retry(
            client, "sid-1", "p", retries=2, base_backoff=20, sleep=sleeps.append
        )

    assert sleeps == []
    assert len(client.calls) == 1


def test_env_defaults_control_retries_and_backoff(monkeypatch):
    monkeypatch.setenv("DSH_ACP_ENV_RETRY", "0")
    monkeypatch.setenv("DSH_ACP_ENV_RETRY_BACKOFF", "3")
    client = FakeClient([RuntimeError("socket hang up")])
    sleeps: list[float] = []

    with pytest.raises(RuntimeError) as ei:
        ask.prompt_with_env_retry(client, "sid-1", "p", sleep=sleeps.append)

    assert str(ei.value).startswith("env_blocked(transport, retries=0): ")
    assert sleeps == []
    assert len(client.calls) == 1
