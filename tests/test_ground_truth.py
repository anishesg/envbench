import pytest


def test_threshold_exceedance_answer():
    from src.generate.ground_truth import compute_threshold_exceedance
    result = compute_threshold_exceedance(measured=12.5, threshold=10.0)
    assert result["exceeds"] is True
    assert result["exceedance_pct"] == pytest.approx(25.0)


def test_threshold_exceedance_compliant():
    from src.generate.ground_truth import compute_threshold_exceedance
    result = compute_threshold_exceedance(measured=8.0, threshold=10.0)
    assert result["exceeds"] is False


def test_disparity_ratio():
    from src.generate.ground_truth import compute_disparity_ratio
    result = compute_disparity_ratio(
        group_a_exposure=15.2,
        group_b_exposure=8.1,
    )
    assert result["ratio"] == pytest.approx(1.877, rel=0.01)
    assert result["disparity_pct"] == pytest.approx(87.7, rel=0.1)


def test_counterfactual_threshold():
    from src.generate.ground_truth import compute_counterfactual_threshold
    values = [5.0, 8.0, 11.0, 14.0, 7.0, 3.0]
    populations = [1000, 2000, 3000, 4000, 1500, 500]
    result = compute_counterfactual_threshold(
        values=values,
        populations=populations,
        old_threshold=15.0,
        new_threshold=10.0,
    )
    assert result["newly_exceeding_count"] == 2
    assert result["newly_affected_population"] == 7000
