with source as (
    select * from `bangkok-football.football_staging.raw_matches`
),

deduped as (
    select *,
        row_number() over (
            partition by match_id
            order by cast(ingested_at as TIMESTAMP) desc
        ) as rn
    from source
),

renamed as (
    select
        cast(match_id as INT64)         as match_id,
        cast(home_team_id as INT64)     as home_team_id,
        cast(away_team_id as INT64)     as away_team_id,
        match_timestamp,
        cast(matchday as INT64)         as matchday,
        status,
        stage,
        home_team_name,
        home_team_short,
        away_team_name,
        away_team_short,
        cast(home_score as INT64)       as home_score,
        cast(away_score as INT64)       as away_score,
        cast(home_score_ht as INT64)    as home_score_ht,
        cast(away_score_ht as INT64)    as away_score_ht,
        cast(total_goals as INT64)      as total_goals,
        winner,
        home_result,
        cast(ingested_at as TIMESTAMP)  as ingested_at,
        execution_date

    from deduped
    where match_id is not null
      and rn = 1
)

select * from renamed
