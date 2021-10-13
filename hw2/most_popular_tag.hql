WITH
tags AS (
    SELECT explode(split(tags_lastfm, ";")) as tag FROM artists
),
tags_clean AS (
    SELECT trim(tag) as tag FROM tags
    WHERE tag != ""
),
tags_counts AS (
    SELECT tag, count(tag) as tag_count FROM tags_clean
    GROUP BY tag
)
SELECT tag FROM tags_counts
WHERE tag_count in (SELECT MAX(tag_count) FROM tags_counts)
