-- dim_teams.sql
-- Dimension table: one row per team with aggregated stats

with home_stats as (
    select
        home_team_id        as team_id,
        home_team_name      as team_name,
        count(*)            as home_matches,
        sum(home_points)    as home_points,
        sum(home_score)     as home_goals_scored,
        sum(away_score)     as home_goals_conceded
    from {{ ref('int_match_results') }}
    group by 1, 2
),

away_stats as (
    select
        away_team_id        as team_id,
        away_team_name      as team_name,
        count(*)            as away_matches,
        sum(away_points)    as away_points,
        sum(away_score)     as away_goals_scored,
        sum(home_score)     as away_goals_conceded
    from {{ ref('int_match_results') }}
    group by 1, 2
),

combined as (
    select
        h.team_id,
        h.team_name,

        -- Matches
        h.home_matches,
        a.away_matches,
        h.home_matches + a.away_matches     as total_matches,

        -- Points
        h.home_points + a.away_points       as total_points,

        -- Goals
        h.home_goals_scored + a.away_goals_scored       as total_goals_scored,
        h.home_goals_conceded + a.away_goals_conceded   as total_goals_conceded,
        (h.home_goals_scored + a.away_goals_scored) -
        (h.home_goals_conceded + a.away_goals_conceded) as goal_difference

    from home_stats h
    join away_stats a on h.team_id = a.team_id
)

select
    *,
    -- League position helper
    rank() over (order by total_points desc, goal_difference desc) as league_position
from combined
order by league_position
