WITH
tags AS (SELECT explode(split(tags_mb, ";")) as tag FROM artists),
tags_counts AS (
    SELECT tag, count(tag) as tag_count FROM tags
    WHERE tag != ""
    GROUP BY tag
)
SELECT tag FROM tags_counts
WHERE tag_count in (SELECT MAX(tag_count) FROM tags_counts)
