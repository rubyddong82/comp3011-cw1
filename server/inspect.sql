.headers on
.mode column

.print '===== SQLITE VERSION ====='
SELECT sqlite_version();

.print ''
.print '===== TABLE LIST ====='
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;

.print ''
.print '===== FULL SCHEMA ====='
SELECT name, sql FROM sqlite_master WHERE type='table';

.print ''
.print '===== COLUMN DETAILS ====='
.print '--- name_basics ---'
PRAGMA table_info(name_basics);

.print ''
.print '--- title_basics ---'
PRAGMA table_info(title_basics);

.print ''
.print '--- title_ratings ---'
PRAGMA table_info(title_ratings);

.print ''
.print '===== ROW COUNTS ====='
SELECT 'name_basics' AS table_name, COUNT(*) FROM name_basics;
SELECT 'title_basics' AS table_name, COUNT(*) FROM title_basics;
SELECT 'title_ratings' AS table_name, COUNT(*) FROM title_ratings;

.print ''
.print '===== SAMPLE: name_basics ====='
SELECT * FROM name_basics LIMIT 3;

.print ''
.print '===== SAMPLE: title_basics ====='
SELECT * FROM title_basics LIMIT 3;

.print ''
.print '===== SAMPLE: title_ratings ====='
SELECT * FROM title_ratings LIMIT 3;

.print ''
.print '===== INDEXES ====='
SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index';
