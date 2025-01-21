DROP TABLE IF EXISTS
    user_activity_logs,
    login_attempts,
    messages,
    likes,
    friendships,
    user_hobby_preferences,
    user_hobbies,
    user_photos,
    hobbies,
    users,
    addresses
CASCADE;




CREATE TABLE addresses (
    address_id  SERIAL PRIMARY KEY,
    street      VARCHAR(100),
    house_no    VARCHAR(50),
    zip_code    VARCHAR(20),
    city        VARCHAR(100)
);

CREATE TABLE users (
    user_id          SERIAL PRIMARY KEY,
    first_name       VARCHAR(65) NOT NULL,
    last_name        VARCHAR(65) NOT NULL,
    gender           VARCHAR(10),
    birth_date       DATE,
    email            VARCHAR(100) NOT NULL UNIQUE,
    phone            VARCHAR(50),
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    address_id       INT REFERENCES addresses(address_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    interested_in    VARCHAR(10),
    registration_ip  INET, 
    last_login_ip    INET, 
    last_login_at    TIMESTAMP
);


CREATE TABLE hobbies (
    hobby_id  SERIAL PRIMARY KEY,
    name      VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE user_hobbies (
    user_id   INT NOT NULL,
    hobby_id  INT NOT NULL,
    priority  INT,
    PRIMARY KEY (user_id, hobby_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (hobby_id) REFERENCES hobbies(hobby_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE user_hobby_preferences (
    user_id    INT NOT NULL,
    hobby_id   INT NOT NULL,
    preference INT,
    PRIMARY KEY (user_id, hobby_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (hobby_id) REFERENCES hobbies(hobby_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE friendships (
    user_id1 INT NOT NULL,
    user_id2 INT NOT NULL,
    status   VARCHAR(50),    
    PRIMARY KEY (user_id1, user_id2),
    FOREIGN KEY (user_id1) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (user_id2) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE likes (
    liker_id   INT NOT NULL,
    likee_id   INT NOT NULL,
    status     VARCHAR(50),
    like_time  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    liker_ip   INET,
    PRIMARY KEY (liker_id, likee_id),
    FOREIGN KEY (liker_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (likee_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE messages (
    message_id       SERIAL PRIMARY KEY,
    conversation_id  INT,             
    sender_id        INT NOT NULL,
    receiver_id      INT NOT NULL,
    message_text     TEXT NOT NULL,
    send_time        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sender_ip        INET,
    FOREIGN KEY (sender_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (receiver_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE user_photos (
    photo_id     SERIAL PRIMARY KEY,
    user_id      INT NOT NULL,
    photo_data   BYTEA,       
    photo_url    VARCHAR(255), 
    description  VARCHAR(255),
    is_profile   BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE user_activity_logs (
    log_id        SERIAL PRIMARY KEY,
    user_id       INT NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    activity_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ip_address    INET, 
    details       TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE login_attempts (
    attempt_id    SERIAL PRIMARY KEY,
    user_id       INT,
    attempt_time  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ip_address    INET,
    success       BOOLEAN NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);