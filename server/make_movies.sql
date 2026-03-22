.headers on
.mode column

.print "===== START REBUILD ====="

PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

-- --------------------------------------------------
-- Drop tables you no longer want
-- --------------------------------------------------
DROP TABLE IF EXISTS datasets;
DROP TABLE IF EXISTS name_basics;

-- --------------------------------------------------
-- Drop old movies indexes first
-- --------------------------------------------------
DROP INDEX IF EXISTS ux_movies_tconst;
DROP INDEX IF EXISTS idx_movies_primaryTitle;
DROP INDEX IF EXISTS idx_movies_titleType;
DROP INDEX IF EXISTS idx_movies_startYear;
DROP INDEX IF EXISTS idx_movies_averageRating;
DROP INDEX IF EXISTS idx_movies_numVotes;

-- --------------------------------------------------
-- Remove old movies table
-- --------------------------------------------------
DROP TABLE IF EXISTS movies;

-- --------------------------------------------------
-- Rebuild movies as:
-- movies = title_basics + title_ratings
-- LEFT JOIN keeps all title_basics rows
-- --------------------------------------------------
CREATE TABLE movies AS
SELECT
    tb.tconst          AS tconst,
    tb.titleType       AS titleType,
    tb.primaryTitle    AS primaryTitle,
    tb.originalTitle   AS originalTitle,
    tb.isAdult         AS isAdult,
    tb.startYear       AS startYear,
    tb.endYear         AS endYear,
    tb.runtimeMinutes  AS runtimeMinutes,
    tb.genres          AS genres,
    tr.averageRating   AS averageRating,
    tr.numVotes        AS numVotes
FROM title_basics tb
LEFT JOIN title_ratings tr
    ON tb.tconst = tr.tconst;

-- --------------------------------------------------
-- Recreate indexes
-- --------------------------------------------------
CREATE UNIQUE INDEX ux_movies_tconst
ON movies(tconst);

CREATE INDEX idx_movies_primaryTitle
ON movies(primaryTitle);

CREATE INDEX idx_movies_titleType
ON movies(titleType);

CREATE INDEX idx_movies_startYear
ON movies(startYear);

CREATE INDEX idx_movies_averageRating
ON movies(averageRating);

CREATE INDEX idx_movies_numVotes
ON movies(numVotes);

COMMIT;

-- --------------------------------------------------
-- Update planner stats
-- --------------------------------------------------
ANALYZE;
PRAGMA optimize;

.print "===== DONE ====="

.print "===== MOVIES ROW COUNT ====="
SELECT COUNT(*) AS movies_count
FROM movies;

.print "===== MOVIES SAMPLE ====="
SELECT *
FROM movies
LIMIT 5;