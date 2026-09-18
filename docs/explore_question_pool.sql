-- Explore nudges.question_pool from DBeaver. Database dev, schema nudges.
-- The pool is the POC's stand-in for a question bank. Every row says so in the source column.

-- 1. Shape of the table
select column_name, data_type, character_maximum_length
from information_schema.columns
where table_schema = 'nudges' and table_name = 'question_pool'
order by ordinal_position;

-- 2. How many questions per topic and difficulty
select topic, difficulty, count(*) as questions, sum(points) as points
from nudges.question_pool
where course_id = 1
group by 1, 2
order by 1, 2;

-- 3. The questions themselves, answers rendered as text
select question_id, topic, difficulty, question_type, points,
       question_text,
       json_serialize(answers) as answers
from nudges.question_pool
where course_id = 1
order by topic, question_id;

-- 4. One row per answer, with the correct one flagged (SUPER unnest)
select q.question_id, q.topic, a.text::varchar as answer, a.correct::boolean as is_correct
from nudges.question_pool q, q.answers as a
where q.course_id = 1
order by q.question_id, is_correct desc;

-- 5. Exactly what rule R2 selects for a topic: first five by question_id
select question_id, difficulty, question_text
from nudges.question_pool
where course_id = 1 and topic = 'Heart of Algebra'
order by question_id
limit 5;

-- 6. Which generated quizzes the pool has produced (content_items rows tagged practice)
select module_item_id, item_type, title, url, topics, is_practice, computed_at
from nudges.content_items
where course_id = 1 and item_type = 'Quiz'
order by module_item_id;

-- 7. Quiz proposals from rule R2 and their state
select id, rule, user_id, status, next_title, next_url, decided_at, pushed_at
from nudges.recommendations
where rule = 'R2'
order by id;

-- 8. Provenance check. Every row should say qbank-stand-in until a real bank lands.
select source, count(*) from nudges.question_pool group by 1;
