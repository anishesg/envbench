TASK_TYPES = {
    "threshold_exceedance": {
        "tier": 1,
        "description": "Does a measurement exceed a regulatory threshold?",
        "templates": [
            "Water system {system_name} (PWSID: {pwsid}) in {county}, {state} had a most recent {contaminant} reading of {measured} {unit}. The EPA Maximum Contaminant Level (MCL) for {contaminant} is {threshold} {unit}. Is this system in violation?",
            "The EPA sets the MCL for {contaminant} at {threshold} {unit}. Water system {system_name} serving {population:,} people in {county}, {state} recorded {measured} {unit}. Does this exceed the federal standard, and by what percentage?",
        ],
    },
    "counterfactual_threshold": {
        "tier": 2,
        "description": "If a regulatory threshold changed, how many more entities would be in violation?",
        "templates": [
            "The current EPA MCL for {contaminant} is {old_threshold} {unit}. If it were lowered to {new_threshold} {unit}, how many additional water systems in {state} would exceed the new standard? What is the total population served by those newly noncompliant systems?",
        ],
    },
    "disparity_analysis": {
        "tier": 2,
        "description": "Compare environmental burden across demographic groups.",
        "templates": [
            "In {county} County, {state}, compare the average {indicator} for census tracts where the majority of residents are {group_a} versus tracts where the majority are {group_b}. What is the ratio of {group_a} exposure to {group_b} exposure?",
            "Across all census tracts in {state}, compute the average number of TRI facilities within 3 miles for tracts in the top income quintile versus the bottom income quintile. What is the disparity ratio?",
        ],
    },
    "cumulative_burden": {
        "tier": 3,
        "description": "Multi-source integration to compute environmental burden.",
        "templates": [
            "For census tract {tract_fips} in {county}, {state}: (1) How many TRI facilities are within the tract boundaries? (2) What is the total reported toxic release in pounds? (3) What is the tract's asthma prevalence from CDC PLACES? (4) What is the tract's median household income? (5) Compute a cumulative burden score as (total_releases_lbs / 1000) * (asthma_prevalence / state_avg_asthma) * (1 + poverty_rate).",
        ],
    },
    "confounded_correlation": {
        "tier": 3,
        "description": "Apparent correlation that disappears after controlling for a confounder.",
        "templates": [
            "In {county} County, the Pearson correlation between TRI facility density (facilities per sq mile) and adult asthma prevalence across census tracts is r={raw_r}. However, median household income is also correlated with both variables. Compute the partial correlation between TRI density and asthma prevalence, controlling for income. Does the relationship remain statistically significant (p < 0.05)?",
        ],
    },
    "missing_data_detection": {
        "tier": 3,
        "description": "Question where correct answer is 'insufficient data'.",
        "templates": [
            "What is the drinking water quality for residents using private wells in census tract {tract_fips} ({county}, {state})? The tract has {population:,} residents, of whom approximately {pct_private_well}% rely on private wells.",
            "What was the air quality in census tract {tract_fips} on {specific_date}? The nearest EPA AQS monitor is {monitor_distance_miles:.1f} miles away, located in {monitor_county} County.",
        ],
    },
}
