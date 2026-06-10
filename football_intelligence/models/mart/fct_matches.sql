-- fct_matches.sql
-- Fact table: one row per match

select
    match_id,
    match_timestamp,
    matchday,
    status,

    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,

    home_score,
    away_score,
    home_score_ht,
    away_score_ht,
    total_goals,
    home_goal_diff,

    winner,
    home_result,
    home_points,
    away_points

from {{ ref('int_match_results') }}
