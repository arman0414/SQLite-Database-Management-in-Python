DROP TABLE IF EXISTS grades;
DROP TABLE IF EXISTS enrollments;
DROP TABLE IF EXISTS students;

CREATE TABLE students (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  gpa REAL
);

CREATE TABLE enrollments (
  id INTEGER PRIMARY KEY,
  student_id INTEGER NOT NULL,
  course TEXT NOT NULL,
  FOREIGN KEY(student_id) REFERENCES students(id)
);

CREATE TABLE grades (
  id INTEGER PRIMARY KEY,
  student_id INTEGER NOT NULL,
  subject TEXT NOT NULL,
  score REAL NOT NULL,
  FOREIGN KEY(student_id) REFERENCES students(id)
);

INSERT INTO students (id, name, gpa) VALUES
 (1, 'Alice', 3.6), (2, 'Bob', 3.1), (3, 'Carol', 3.9), (4, 'Dave', 2.8), (5, 'Erin', 3.4),
 (6, 'Frank', 3.0), (7, 'Grace', 3.7), (8, 'Heidi', 3.2), (9, 'Ivan', 2.9), (10, 'Judy', 3.5);

INSERT INTO enrollments (id, student_id, course) VALUES
 (1, 1, 'CS101'), (2, 1, 'MA201'), (3, 2, 'CS101'), (4, 3, 'PH150'), (5, 4, 'CS101'),
 (6, 5, 'MA201'), (7, 6, 'CS101'), (8, 7, 'PH150'), (9, 8, 'MA201'), (10, 9, 'CS101');

INSERT INTO grades (id, student_id, subject, score) VALUES
 (1, 1, 'math', 88), (2, 1, 'cs', 92), (3, 2, 'math', 74), (4, 2, 'cs', 81), (5, 3, 'math', 95),
 (6, 3, 'cs', 90), (7, 4, 'math', 60), (8, 4, 'cs', 66), (9, 5, 'math', 78), (10, 5, 'cs', 84),
 (11, 6, 'math', 70), (12, 6, 'cs', 72), (13, 7, 'math', 91), (14, 7, 'cs', 89), (15, 8, 'math', 76),
 (16, 8, 'cs', 80), (17, 9, 'math', 68), (18, 9, 'cs', 74), (19, 10, 'math', 82), (20, 10, 'cs', 86),
 (21, 1, 'physics', 84), (22, 2, 'physics', 70), (23, 3, 'physics', 93), (24, 4, 'physics', 62),
 (25, 5, 'physics', 77), (26, 6, 'physics', 71), (27, 7, 'physics', 88), (28, 8, 'physics', 73),
 (29, 9, 'physics', 69), (30, 10, 'physics', 81);
