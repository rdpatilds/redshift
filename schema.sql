CREATE SCHEMA IF NOT EXISTS nudges;

CREATE TABLE IF NOT EXISTS nudges.student_course_status (
  course_id BIGINT NOT NULL, user_id BIGINT NOT NULL, name VARCHAR(255),
  enrollment_id BIGINT, enrollment_state VARCHAR(32),
  last_activity_at TIMESTAMP, last_submission_at TIMESTAMP, last_event_at TIMESTAMP,
  days_inactive INT,
  assignments_total INT, assignments_submitted INT, assignments_graded INT,
  assignments_late INT, assignments_missing INT, assignments_excused INT,
  assignments_due_3d_unsubmitted INT,
  quizzes_total INT, quizzes_complete INT, quizzes_in_progress INT, quizzes_untaken INT,
  quiz_avg_percent FLOAT, module_requirement_count INT, module_requirement_completed INT,
  current_score FLOAT, logins_7d INT, risk_score FLOAT, risk_level VARCHAR(8),
  computed_at TIMESTAMP DEFAULT GETDATE()
) DISTKEY (user_id) SORTKEY (course_id, user_id);

CREATE TABLE IF NOT EXISTS nudges.assignment_status (
  course_id BIGINT NOT NULL, user_id BIGINT NOT NULL, assignment_id BIGINT NOT NULL,
  title VARCHAR(255), due_at TIMESTAMP, status VARCHAR(16),
  submitted_at TIMESTAMP, score FLOAT, points_possible FLOAT,
  computed_at TIMESTAMP DEFAULT GETDATE()
) DISTKEY (user_id) SORTKEY (course_id, due_at);

CREATE TABLE IF NOT EXISTS nudges.recommendations (
  id BIGINT IDENTITY(1,1), rule VARCHAR(64) NOT NULL,
  course_id BIGINT NOT NULL, user_id BIGINT NOT NULL,
  surface VARCHAR(16) NOT NULL, priority SMALLINT,
  text VARCHAR(1024) NOT NULL, context VARCHAR(64), reason SUPER,
  dedupe_key VARCHAR(160) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'proposed',
  decided_by VARCHAR(64), decided_at TIMESTAMP, decision_note VARCHAR(512),
  created_at TIMESTAMP DEFAULT GETDATE(),
  pushed_at TIMESTAMP, push_error VARCHAR(512)
) DISTKEY (user_id) SORTKEY (created_at);
