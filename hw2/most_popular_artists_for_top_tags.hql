WITH
tags AS (SELECT artist_lastfm, listeners_lastfm, tag FROM artists LATERAL VIEW explode(split(tags_lastfm, ";")) tag_table AS tag),
tags_counts AS (
    SELECT tag, count(tag) as tag_count FROM tags
    WHERE tag != ""
    GROUP BY tag
    ORDER BY tag_count DESC
    LIMIT 10
),
artists_popularity AS (
    SELECT DISTINCT artist_lastfm, listeners_lastfm FROM tags
    WHERE tag in (SELECT tag FROM tags_counts)
    ORDER BY listeners_lastfm DESC
    LIMIT 10
)
SELECT artist_lastfm FROM artists_popularity
