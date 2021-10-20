WITH
tag_countries AS (
    SELECT artist_lastfm, listeners_lastfm, country, tag FROM artists
    LATERAL VIEW explode(split(tags_lastfm, ";")) tag_table AS tag
    LATERAL VIEW explode(split(country_lastfm, ";")) country_table AS country
    WHERE country != "" and tag != ""
),
tag_countries_clean AS (
SELECT listeners_lastfm, trim(country) as country, trim(tag) as tag FROM tag_countries
),
tags_counts AS (
    SELECT tag, count(tag) as tag_count FROM tag_countries_clean
    GROUP BY tag
),
countries_in_tags AS (
    SELECT DISTINCT country, artist_lastfm, listeners_lastfm FROM tag_countries_clean
    WHERE tag in (SELECT tag FROM tags_counts)
),
country_popularity AS (
    SELECT country, SUM(listeners_lastfm) as total_listen FROM tag_countries_clean
    GROUP BY country
    ORDER BY total_listen DESC
    LIMIT 10
)
SELECT country FROM country_popularity
