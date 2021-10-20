DROP TABLE IF EXISTS artists;

CREATE EXTERNAL TABLE artists (
    mbid STRING,
    artist_mb STRING,
    artist_lastfm STRING,
    country_mb STRING,
    country_lastfm STRING,
    tags_mb STRING,
    tags_lastfm STRING,
    listeners_lastfm INT,
    scrobbles_lastfm INT,
    ambiguous_artist BOOLEAN
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
LOCATION '/data'
TBLPROPERTIES("skip.header.line.count"="1");
