CREATE TABLE IF NOT EXISTS viewing_sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    content_id VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    platform VARCHAR(50),
    watch_duration_minutes INT 
);

CREATE TABLE IF NOT EXISTS app_feedbacks (
    feedback_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    feedback_type VARCHAR(50),
    status VARCHAR(20), 
    created_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    resolution_time_hours FLOAT 
);