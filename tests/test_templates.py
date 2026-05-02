from src.generate.templates import TASK_TYPES
from src.generate.difficulty import score_difficulty


def test_all_task_types_have_templates():
    for name, config in TASK_TYPES.items():
        assert "templates" in config, f"{name} missing templates"
        assert len(config["templates"]) > 0, f"{name} has empty templates"
        assert "tier" in config, f"{name} missing tier"
        assert config["tier"] in (1, 2, 3), f"{name} has invalid tier"


def test_difficulty_scoring_tiers():
    r = score_difficulty(num_sources=1, num_reasoning_steps=1)
    assert r["tier"] == 1
    assert r["label"] == "retrieval"

    r = score_difficulty(num_sources=2, num_reasoning_steps=3)
    assert r["tier"] == 2
    assert r["label"] == "integration"

    r = score_difficulty(num_sources=3, num_reasoning_steps=4, has_adversarial_element=True)
    assert r["tier"] == 3
    assert r["label"] == "adversarial_causal"
