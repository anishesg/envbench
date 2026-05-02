def score_difficulty(
    num_sources: int,
    num_reasoning_steps: int,
    has_adversarial_element: bool = False,
    requires_statistical_computation: bool = False,
    correct_answer_is_insufficient_data: bool = False,
) -> dict:
    base = num_sources + num_reasoning_steps
    if has_adversarial_element:
        base += 3
    if requires_statistical_computation:
        base += 2
    if correct_answer_is_insufficient_data:
        base += 2
    if base <= 3:
        tier = 1
        label = "retrieval"
    elif base <= 6:
        tier = 2
        label = "integration"
    else:
        tier = 3
        label = "adversarial_causal"
    return {"score": base, "tier": tier, "label": label}
