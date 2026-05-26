import importlib
from pathlib import Path

import pytest


@pytest.fixture
def budget(monkeypatch):
    monkeypatch.setenv("MAX_LLM_CALLS_PER_DEVICE", "1")
    import src.call_budget as cb
    importlib.reload(cb)
    yield cb
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
