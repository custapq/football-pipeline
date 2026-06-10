-- int_match_results.sql
-- Intermediate layer: business logic, calculations

with matches as (
    select * from {{ ref('stg_matches') }}
    where status = 'FINISHED'  -- เฉพาะแมตช์ที่จบแล้ว
),

with_points as (
    select
        *,
        -- Home team points
        case
            when home_result = 'WIN'  then 3
            when home_result = 'DRAW' then 1
            when home_result = 'LOSS' then 0
        end as home_points,

        -- Away team points
        case
            when home_result = 'LOSS' then 3
            when home_result = 'DRAW' then 1
            when home_result = 'WIN'  then 0
        end as away_points,

        -- Goal difference
        home_score - away_score as home_goal_diff

    from matches
)

select * from with_points
