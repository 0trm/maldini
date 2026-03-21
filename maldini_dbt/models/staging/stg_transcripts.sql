-- Deduplicated, validated transcripts.
-- Keeps the most recently ingested row per video_id.

with source as (
    select * from {{ source('raw', 'transcripts') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by video_id order by ingested_at desc) = 1
)

select
    video_id,
    video_url,
    publish_date,
    transcript_text,
    ingested_at

from deduped
where video_id is not null
