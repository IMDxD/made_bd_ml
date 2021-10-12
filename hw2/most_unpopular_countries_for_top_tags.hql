WITH
tags AS (SELECT explode(split(tags_mb, ";")) as tag FROM artists),
tags_counts AS (
    SELECT tag, count(tag) as tag_count FROM tags
    WHERE tag != ""
    GROUP BY tag
),
tag_countries AS (
SELECT listeners_lastfm, country, tag FROM artists
LATERAL VIEW explode(split(tags_lastfm, ";")) tag_table AS tag
LATERAL VIEW explode(split(country_lastfm, ";")) country_table AS country
WHERE country != ""
),
country_popularity AS (
    SELECT DISTINCT country, listeners_lastfm FROM tag_countries
    WHERE tag in (SELECT tag FROM tags_counts)
    ORDER BY listeners_lastfm ASC
    LIMIT 10
)
SELECT country FROM country_popularity
