from pathlib import Path

import pytest


@pytest.fixture
def budget():
    """Cap of 1 + clean counts, restored on teardown.

    Sets the module global directly rather than reload()+setenv so the cap does
    not leak into later tests (try_consume reads the global at call time).
    """
    import src.call_budget as cb
    original = cb.MAX_LLM_CALLS_PER_DEVICE
    cb.MAX_LLM_CALLS_PER_DEVICE = 1
    cb.reset_all()
    yield cb
    cb.MAX_LLM_CALLS_PER_DEVICE = original
    cb.reset_all()


@pytest.mark.asyncio
async def test_run_kimi_refuses_when_over_budget(budget, monkeypatch):
    import src.kimi_runner as kr

    spawned = {"count": 0}

    async def fake_spawn(*args, **kwargs):  # should never be reached on 2nd call
        spawned["count"] += 1
        raise AssertionError("subprocess should not be spawned when over budget")

    # First call consumes the only budget slot (cap=1). Stub the spawn so the
    # first call returns cleanly without a real subprocess.
    monkeypatch.setattr(kr.shutil, "which", lambda _: "/usr/bin/kimi")
    budget.try_consume("dev-x")  # exhaust the cap directly

    monkeypatch.setattr(kr.asyncio, "create_subprocess_exec", fake_spawn)
    result = await kr.run_kimi(
        "prompt", skills_dir=Path("/tmp"), work_dir=Path("/tmp"),
        device_id="dev-x",
    )
    assert result is None
    assert spawned["count"] == 0


@pytest.mark.asyncio
async def test_run_claude_refuses_when_over_budget(budget, monkeypatch):
    import src.claude_runner as cr

    spawned = {"count": 0}

    async def fake_spawn(*args, **kwargs):
        spawned["count"] += 1
        raise AssertionError("subprocess should not be spawned when over budget")

    monkeypatch.setattr(cr.shutil, "which", lambda _: "/usr/bin/claude")
    budget.try_consume("dev-y")  # cap=1, exhaust it

    monkeypatch.setattr(cr.asyncio, "create_subprocess_exec", fake_spawn)
    result = await cr._run_claude(
        "prompt", skills_dir=Path("/tmp"), work_dir=Path("/tmp"),
        timeout=10, device_id="dev-y",
    )
    assert result is None
    assert spawned["count"] == 0


@pytest.mark.asyncio
async def test_chat_completion_refuses_when_over_budget(budget, monkeypatch):
    from src.call_budget import CallBudgetExceeded
    import src.moonshot_client as mc

    client = mc.MoonshotClient()
    budget.try_consume("dev-z")  # cap=1, exhaust it

    # Patch the underlying OpenAI SDK call so a hypothetical un-gated request
    # would be detectable; the gate should prevent it from being reached.
    called = {"http": 0}

    async def fake_create(*a, **k):
        called["http"] += 1
        raise AssertionError("HTTP request must not fire when over budget")

    monkeypatch.setattr(client._client.chat.completions, "create", fake_create)

    with pytest.raises(CallBudgetExceeded):
        await client.chat_completion("hi", device_id="dev-z")
    assert called["http"] == 0
