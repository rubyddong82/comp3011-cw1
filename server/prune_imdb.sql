CREATE TABLE movies_small AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY tconst) AS rn
    FROM movies
)
WHERE rn % 20 = 0;
