import numpy as np


def compute_threshold_exceedance(measured: float, threshold: float) -> dict:
    exceeds = measured > threshold
    exceedance_pct = ((measured - threshold) / threshold) * 100 if exceeds else 0.0
    return {
        "exceeds": exceeds,
        "measured": measured,
        "threshold": threshold,
        "exceedance_pct": round(exceedance_pct, 1),
    }


def compute_disparity_ratio(group_a_exposure: float, group_b_exposure: float) -> dict:
    ratio = group_a_exposure / group_b_exposure if group_b_exposure != 0 else float("inf")
    disparity_pct = (ratio - 1) * 100
    return {
        "ratio": round(ratio, 3),
        "disparity_pct": round(disparity_pct, 1),
        "group_a_exposure": group_a_exposure,
        "group_b_exposure": group_b_exposure,
    }


def compute_counterfactual_threshold(
    values: list[float],
    populations: list[int],
    old_threshold: float,
    new_threshold: float,
) -> dict:
    newly_exceeding = [
        (v, p) for v, p in zip(values, populations)
        if v > new_threshold and v <= old_threshold
    ]
    return {
        "newly_exceeding_count": len(newly_exceeding),
        "newly_affected_population": sum(p for _, p in newly_exceeding),
        "old_threshold": old_threshold,
        "new_threshold": new_threshold,
    }


def compute_cumulative_burden_score(
    pollution_indicators: dict[str, float],
    demographic_indicators: dict[str, float],
    weights: dict[str, float] | None = None,
) -> dict:
    if weights is None:
        weights = {k: 1.0 for k in pollution_indicators}
    pollution_score = sum(
        pollution_indicators.get(k, 0) * weights.get(k, 1.0)
        for k in pollution_indicators
    )
    demo_multiplier = 1.0
    if "pct_poverty" in demographic_indicators:
        demo_multiplier += demographic_indicators["pct_poverty"] / 100
    if "pct_minority" in demographic_indicators:
        demo_multiplier += demographic_indicators["pct_minority"] / 200
    combined = pollution_score * demo_multiplier
    return {
        "pollution_score": round(pollution_score, 2),
        "demographic_multiplier": round(demo_multiplier, 3),
        "combined_burden_score": round(combined, 2),
    }


def compute_partial_correlation(
    x: list[float], y: list[float], z: list[float]
) -> dict:
    x, y, z = np.array(x), np.array(y), np.array(z)
    mask = ~(np.isnan(x) | np.isnan(y) | np.isnan(z))
    x, y, z = x[mask], y[mask], z[mask]
    if len(x) < 10:
        return {"partial_r": None, "n": len(x), "sufficient_data": False}
    from scipy import stats
    r_xy = np.corrcoef(x, y)[0, 1]
    r_xz = np.corrcoef(x, z)[0, 1]
    r_yz = np.corrcoef(y, z)[0, 1]
    denom = np.sqrt((1 - r_xz**2) * (1 - r_yz**2))
    partial_r = (r_xy - r_xz * r_yz) / denom if denom > 0 else 0.0
    n = len(x)
    t_stat = partial_r * np.sqrt((n - 3) / (1 - partial_r**2)) if abs(partial_r) < 1 else float("inf")
    p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df=n - 3)) if np.isfinite(t_stat) else 0.0
    return {
        "partial_r": round(float(partial_r), 4),
        "raw_r": round(float(r_xy), 4),
        "p_value": round(float(p_value), 6),
        "significant_at_05": p_value < 0.05,
        "n": n,
        "sufficient_data": True,
    }
