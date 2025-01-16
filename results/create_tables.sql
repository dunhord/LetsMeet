DROP TABLE IF EXISTS addresses CASCADE;
DROP TABLE IF EXISTS user_photos CASCADE;
DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS likes CASCADE;
DROP TABLE IF EXISTS friendships CASCADE;
DROP TABLE IF EXISTS user_hobby_preferences CASCADE;
DROP TABLE IF EXISTS user_hobbies CASCADE;
DROP TABLE IF EXISTS hobbies CASCADE;
DROP TABLE IF EXISTS users CASCADE;



CREATE TABLE addresses (
    address_id  SERIAL PRIMARY KEY,
    street      VARCHAR(255),
    house_no    VARCHAR(50),
    zip_code    VARCHAR(20),
    city        VARCHAR(100)
);

CREATE TABLE users (
    user_id       SERIAL PRIMARY KEY,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    gender        VARCHAR(20),
    birth_date    DATE,
    email         VARCHAR(255) NOT NULL UNIQUE,
    phone         VARCHAR(50),
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    profile_image BYTEA,
    address_id    INT REFERENCES addresses(address_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    interested_in VARCHAR(20)
);

CREATE TABLE hobbies (
    hobby_id  SERIAL PRIMARY KEY,
    name      VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE user_hobbies (
    user_id   INT NOT NULL,
    hobby_id  INT NOT NULL,
    priority  INT,           -- z.B. 0-100
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
    preference INT,    -- z.B. -100 bis +100
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
    status   VARCHAR(50),     -- z.B. 'confirmed', 'requested'...
    PRIMARY KEY (user_id1, user_id2),
    FOREIGN KEY (user_id1) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (user_id2) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE likes (
    liker_id  INT NOT NULL,
    likee_id  INT NOT NULL,
    status    VARCHAR(50),    -- 'pending', 'mutual'...
    like_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
    conversation_id  INT,               -- optional
    sender_id        INT NOT NULL,
    receiver_id      INT NOT NULL,
    message_text     TEXT NOT NULL,
    send_time        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sender_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (receiver_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE user_photos (
    photo_id    SERIAL PRIMARY KEY,
    user_id     INT NOT NULL,
    photo_data  BYTEA,        -- BLOB für binäre Daten (optional)
    photo_url   VARCHAR(255), -- externer Link, falls nicht in DB gespeichert
    description VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
