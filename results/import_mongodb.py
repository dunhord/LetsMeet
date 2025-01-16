"""
import_mongodb.py
-----------------
Importiert Dokumente aus MongoDB in eine PostgreSQL-Datenbank.
 - Telefon wird bereinigt (nur Ziffern und '+')
 - Vor-/Nachname: erster Buchstabe groß, Rest klein

"""

from pymongo import MongoClient
import psycopg2
from datetime import datetime
import re  # Für Regex

# MongoDB-Verbindungsdaten
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "LetsMeet"
MONGO_COLLECTION = "users"

# PostgreSQL-Verbindungsdaten
POSTGRES_HOST = "localhost"
POSTGRES_DB = "lf8_lets_meet_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "secret"
POSTGRES_PORT = 5432


def main():
    # 1) Verbindung zur MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_database = mongo_client[MONGO_DB]
    user_collection = mongo_database[MONGO_COLLECTION]

    # 2) Verbindung zu PostgreSQL
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )
    # UTF-8-Client-Encoding
    pg_conn.set_client_encoding('UTF8')
    pg_cursor = pg_conn.cursor()

    # 3) Alle Dokumente aus Mongo einlesen
    documents = user_collection.find({})

    for doc in documents:
        email_mongo = doc.get("_id", "")
        name_mongo = doc.get("name", "")
        phone_mongo = doc.get("phone", "")
        friends_mongo = doc.get("friends", [])
        likes_mongo = doc.get("likes", [])
        messages_mongo = doc.get("messages", [])

        # 3.1) Telefon bereinigen
        # Entfernt alle Zeichen außer Ziffern und "+"
        if phone_mongo:
            phone_mongo = re.sub(r"[^0-9+]", "", phone_mongo)

        # 3.2) Namen splitten (Fallback: E-Mail)
        first_name, last_name = split_name(name_mongo, email_mongo)

        # 3.3) Vor-/Nachname: erster Buchstabe groß, Rest klein
        first_name = fix_capitalization(first_name)
        last_name = fix_capitalization(last_name)

        # 3.4) User anlegen/finden
        user_id = get_or_create_user_id(pg_cursor, first_name, last_name, email_mongo, phone_mongo)
        if not user_id:
            continue

        # 3.5) FRIENDS -> friendships
        for friend_email in friends_mongo:
            f_first, f_last = split_name("", friend_email)
            f_first = fix_capitalization(f_first)
            f_last = fix_capitalization(f_last)

            friend_id = get_or_create_user_id(pg_cursor, f_first, f_last, friend_email, "")
            if friend_id and friend_id != user_id:
                uid1, uid2 = sorted([user_id, friend_id])
                pg_cursor.execute(
                    """
                    INSERT INTO friendships (user_id1, user_id2, status)
                    VALUES (%s, %s, 'friends')
                    ON CONFLICT DO NOTHING
                    """,
                    (uid1, uid2)
                )

        # 3.6) LIKES -> likes
        for like_obj in likes_mongo:
            liked_email = like_obj.get("liked_email", "")
            status_str = like_obj.get("status", "")
            like_ts_str = like_obj.get("timestamp", "")
            like_ts = parse_datetime_str(like_ts_str, "%Y-%m-%d %H:%M:%S")

            lf_first, lf_last = split_name("", liked_email)
            lf_first = fix_capitalization(lf_first)
            lf_last = fix_capitalization(lf_last)

            likee_id = get_or_create_user_id(pg_cursor, lf_first, lf_last, liked_email, "")
            if likee_id and likee_id != user_id:
                pg_cursor.execute(
                    """
                    INSERT INTO likes (liker_id, likee_id, status, like_time)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (user_id, likee_id, status_str, like_ts)
                )

        # 3.7) MESSAGES -> messages
        for msg_obj in messages_mongo:
            conv_id = msg_obj.get("conversation_id", None)
            recv_email = msg_obj.get("receiver_email", "")
            msg_text = msg_obj.get("message", "")
            msg_ts_str = msg_obj.get("timestamp", "")
            msg_ts = parse_datetime_str(msg_ts_str, "%Y-%m-%d %H:%M:%S")

            mf_first, mf_last = split_name("", recv_email)
            mf_first = fix_capitalization(mf_first)
            mf_last = fix_capitalization(mf_last)

            recv_id = get_or_create_user_id(pg_cursor, mf_first, mf_last, recv_email, "")
            if recv_id and recv_id != user_id:
                pg_cursor.execute(
                    """
                    INSERT INTO messages (conversation_id, sender_id, receiver_id, message_text, send_time)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (conv_id, user_id, recv_id, msg_text, msg_ts)
                )

    # 4) Commit
    pg_conn.commit()

    # 5) Schließen
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()

    print("MongoDB-Daten wurden erfolgreich importiert.")


# -------------------------------------------------
# Hilfsfunktionen
# -------------------------------------------------

def split_name(full_name, email):
    """
    Versucht 'full_name' per Komma zu parsen ("Nachname, Vorname").
    Wenn kein Komma, leitet Vor- und Nachname aus der E-Mail ab.
    Beispiel: "ellen.wickern@..." -> (ellen, wickern)
    """
    if full_name and "," in full_name:
        parts = full_name.split(",", 1)
        last_n = parts[0].strip()
        first_n = parts[1].strip()
        return (first_n, last_n)
    else:
        return split_name_from_email(email)

def split_name_from_email(email):
    """
    Nimmt den Teil vor dem '@', teilt am ersten '.' => (vorher, nachher)
    'ellen.wickern@...' -> ('ellen','wickern')
    """
    if not email or '@' not in email:
        return ("-", "-")
    local_part = email.split('@')[0]
    if '.' in local_part:
        idx = local_part.find('.')
        return (local_part[:idx], local_part[idx+1:])
    else:
        return ("-", local_part)

def fix_capitalization(s):
    """
    Setzt den ersten Buchstaben in Groß, den Rest in Kleinbuchstaben.
    Beispiel: "kARl-hEINz" -> "Karl-heinz".
    Wenn du Bindestriche & Co. original erhalten willst, nutze str.title() oder 
    setze eine ausgefeilte Logik ein.
    """
    if not s:
        return ""
    return s[0].upper() + s[1:].lower()

def get_or_create_user_id(cursor, first_name, last_name, email, phone):
    """
    Prüft, ob der user (per email) existiert. Wenn nicht, Insert.
    created_at / updated_at -> DB default.
    """
    if not email:
        return None

    cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
    row = cursor.fetchone()
    if row:
        return row[0]

    print(f"INSERT user -> first_name='{first_name}' last_name='{last_name}' email='{email}' phone='{phone}'")

    insert_sql = """
        INSERT INTO users (first_name, last_name, email, phone)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING user_id
    """
    cursor.execute(insert_sql, (first_name, last_name, email, phone))
    new_row = cursor.fetchone()
    if new_row:
        return new_row[0]
    else:
        # ON CONFLICT -> user existierte
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        row2 = cursor.fetchone()
        return row2[0] if row2 else None

def parse_datetime_str(ts_str, fmt):
    """Parst z.B. '2024-03-17 07:39:29' -> datetime"""
    if not ts_str:
        return None
    try:
        return datetime.strptime(ts_str, fmt)
    except:
        return None


if __name__ == "__main__":
    main()
