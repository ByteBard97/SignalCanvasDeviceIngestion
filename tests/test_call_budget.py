import importlib
import pytest


@pytest.fixture
def budget(monkeypatch):
    """Fresh module state + known cap for each test."""
    monkeypatch.setenv("MAX_LLM_CALLS_PER_DEVICE", "3")
    import src.call_budget as cb
    importlib.reload(cb)
    yield cb
    cb.reset_all()


def test_allows_up_to_cap_then_refuses(budget):
    assert budget.try_consume("dev-a") is True   # 1
    assert budget.try_consume("dev-a") is True   # 2
    assert budget.try_consume("dev-a") is True   # 3
    assert budget.try_consume("dev-a") is False  # 4 -> over cap
    assert budget.get_count("dev-a") == 3        # refused call not counted


def test_none_device_id_always_allowed_and_uncounted(budget):
    for _ in range(10):
        assert budget.try_consume(None) is True
    assert budget.get_count("dev-a") == 0


def test_counts_are_independent_per_device(budget):
    assert budget.try_consume("dev-a") is True
    assert budget.try_consume("dev-b") is True
    assert budget.get_count("dev-a") == 1
    assert budget.get_count("dev-b") == 1


def test_reset_all_clears_counts(budget):
    budget.try_consume("dev-a")
    budget.reset_all()
    assert budget.get_count("dev-a") == 0
