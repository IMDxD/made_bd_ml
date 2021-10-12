SELECT artist_mb FROM artists
WHERE scrobbles_lastfm in (SELECT MAX(scrobbles_lastfm) FROM artists)
