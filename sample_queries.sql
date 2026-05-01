-- 20 example statements for the mini engine (CSV mode unless noted).

-- DDL
CREATE TABLE students (id INT, name TEXT, subject TEXT, score REAL);
CREATE TABLE courses (id INT, title TEXT);
ALTER TABLE students ADD COLUMN year INT;

-- DML
INSERT INTO students (id,name,subject,score,year) VALUES (1,'Alice','math',88,1);
INSERT INTO students VALUES (2,'Bob','cs',74,1);
UPDATE students SET score=90 WHERE id=1;
DELETE FROM students WHERE score < 75;

-- Single-table SELECT
SELECT * FROM students;
SELECT id, name FROM students WHERE score >= 80 ORDER BY score DESC LIMIT 5;
SELECT name FROM students WHERE subject = 'math' AND score > 70;

-- JOIN
SELECT students.id, courses.title FROM students INNER JOIN courses ON students.id=courses.id;

-- Aggregates
SELECT subject, COUNT(*), AVG(score) FROM students GROUP BY subject;
SELECT subject, SUM(score), MIN(score), MAX(score) FROM students GROUP BY subject;

-- SQLite passthrough mode examples (run with: python main.py --mode sqlite --sql "...")
-- SELECT name, AVG(score) FROM students JOIN grades ON students.id = grades.student_id GROUP BY students.id;

-- Cleanup
DROP TABLE students;
DROP TABLE courses;
