"""
import_letsmeet.py
------------------
Ein einzelnes Skript, das Daten aus:
 1) Excel-Datei
 2) MongoDB
 3) XML-Datei
in die "Let's Meet"-Datenbank importiert.

Gleichzeitig werden Dubletten (z.B. dieselbe E-Mail in verschiedenen Quellen)
verhindert, indem wir:
 - E-Mail in 'users' unique halten -> ON CONFLICT DO NOTHING
 - Adressen per get_or_create_address -> identische Adresse wird nur einmal angelegt
 - Hobbies per get_or_create_hobby -> name UNIQUE
 - Sonstige Tabellen wie user_hobbies, friendships, likes, messages -> PK/Unique

Benötigte Pakete:
 - pandas + openpyxl (für Excel)
 - pymongo (für MongoDB)
 - psycopg2 (für PostgreSQL)
 - xml.etree.ElementTree (Standard in Python)
"""

import pandas as pd
import psycopg2
import re
from datetime import datetime
import xml.etree.ElementTree as ET
from pymongo import MongoClient

# =========== KONFIGURATION ANPASSEN ===========

## Pfade
EXCEL_FILE = "Lets Meet DB Dump.xlsx"
XML_FILE   = "Lets_Meet_Hobbies.xml"

## MongoDB
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "LetsMeet"
MONGO_COLLECTION = "users"

## PostgreSQL
POSTGRES_HOST = "localhost"
POSTGRES_DB   = "lf8_lets_meet_db"
POSTGRES_USER = "user"
POSTGRES_PWD  = "secret"
POSTGRES_PORT = 5432


def main():
    """
    Hauptprogramm:
     1) Verbindung zur Postgres-DB herstellen
     2) Excel importieren
     3) MongoDB importieren
     4) XML importieren
     5) Verbindung schließen
    """
    # 1) PostgreSQL-Verbindung
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD,
        port=POSTGRES_PORT
    )
    conn.set_client_encoding('UTF8')
    cursor = conn.cursor()

    # 2) Excel-Daten importieren
    import_from_excel(cursor, conn)

    # 3) MongoDB-Daten importieren
    import_from_mongo(cursor, conn)

    # 4) XML-Daten importieren
    import_from_xml(cursor, conn)

    # 5) Verbindung schließen
    cursor.close()
    conn.close()
    print("Alle Importe (Excel, MongoDB, XML) erfolgreich abgeschlossen.")


# ----------------------------------------------------------------------------
# 1) EXCEL-IMPORT
# ----------------------------------------------------------------------------

def import_from_excel(cursor, conn):
    """
    Liest eine Excel-Datei mit 8 Spalten:
     1) Nachname, Vorname
     2) Straße Nr, PLZ Ort
     3) Telefon
     4) Hobbies (z.B. "Kochen %80%; Joggen %20%; ...")
     5) E-Mail
     6) Geschlecht (m / w / nicht binär / ...)
     7) Interessiert an (wird hier ignoriert)
     8) Geburtsdatum (z.B. 07.03.1959)

    und speichert direkt in addresses, users, hobbies, user_hobbies.
    Doppelte E-Mails werden verhindert ("ON CONFLICT").
    Gleiche Hobbies -> "name UNIQUE" + ON CONFLICT.
    Gleiche Addressen -> get_or_create_address(...).
    """
    print("Starte Excel-Import...")
    df = pd.read_excel(EXCEL_FILE, sheet_name=0)

    # Spalten umbenennen
    df.columns = [
        "nachname_vorname",
        "strasse_plz_ort",
        "telefon",
        "hobbies_raw",
        "email",
        "geschlecht",
        "interessiert_an",
        "geburtsdatum"
    ]

    for _, row in df.iterrows():
        # 1) Name
        name_str = str(row["nachname_vorname"]) if pd.notnull(row["nachname_vorname"]) else ""
        first_name, last_name = split_name_simple(name_str)

        # 2) Adresse parsen
        addr_str = str(row["strasse_plz_ort"]) if pd.notnull(row["strasse_plz_ort"]) else ""
        street, house_no, zip_code, city = parse_address(addr_str)

        # -> get_or_create_address
        address_id = get_or_create_address(cursor, street, house_no, zip_code, city)

        # 3) Telefon bereinigen
        row_telefon = str(row["telefon"]) if pd.notnull(row["telefon"]) else None
        if row_telefon:
            row_telefon = re.sub(r"[^0-9+]", "", row_telefon)

        # 4) Geschlecht + Geburtsdatum
        gender = str(row["geschlecht"]) if pd.notnull(row["geschlecht"]) else None
        birth_date = parse_date_ddmmYYYY(str(row["geburtsdatum"]))

        # 5) E-Mail
        email = str(row["email"]) if pd.notnull(row["email"]) else None

        interested_in_value = str(row["interessiert_an"]) if pd.notnull(row["interessiert_an"]) else None

        # 6) Hobbys
        hobbies_str = str(row["hobbies_raw"]) if pd.notnull(row["hobbies_raw"]) else ""
        hobby_entries = [h.strip() for h in hobbies_str.split(";") if h.strip()]
        

        # 7) user anlegen
        user_id = get_or_create_user(
            cursor=cursor,
            first_name=first_name,
            last_name=last_name,
            phone=row_telefon,
            email=email,
            gender=gender,
            birth_date=birth_date,
            address_id=address_id,
            interested_in=interested_in_value
        )
        if not user_id:
            # z.B. E-Mail leer oder bereits existierend -> next
            continue

        # 8) user_hobbies
        for hpart in hobby_entries:
            match = re.search(r"(.*?)%(\d+)%", hpart)
            if match:
                hobby_name = match.group(1).strip()
                priority_val = int(match.group(2))
                hobby_id = get_or_create_hobby(cursor, hobby_name)
                # user_hobbies
                insert_user_hobbies = """
                    INSERT INTO user_hobbies (user_id, hobby_id, priority)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """
                cursor.execute(insert_user_hobbies, (user_id, hobby_id, priority_val))
            else:
                # Falls kein %NN% => priority=0
                hobby_name = hpart
                if hobby_name:
                    hobby_id = get_or_create_hobby(cursor, hobby_name)
                    insert_user_hobbies = """
                        INSERT INTO user_hobbies (user_id, hobby_id, priority)
                        VALUES (%s, %s, 0)
                        ON CONFLICT DO NOTHING
                    """
                    cursor.execute(insert_user_hobbies, (user_id, hobby_id))

    conn.commit()
    print("Excel-Import abgeschlossen.")


# ----------------------------------------------------------------------------
# 2) MONGO-IMPORT
# ----------------------------------------------------------------------------

def import_from_mongo(cursor, conn):
    """
    Liest aus MongoDB (users-Collection) die Felder:
     - _id (email), name, phone, friends, likes, messages
    und schreibt in:
     - users (ggf. ON CONFLICT DO NOTHING)
     - friendships
     - likes
     - messages

    Telefon wird bereinigt, Vorname/Nachname über split_name(...).
    Doppelte E-Mails -> nur 1x angelegt.
    """
    print("Starte MongoDB-Import...")

    mongo_client = MongoClient(MONGO_URI)
    mdb = mongo_client[MONGO_DB]
    user_coll = mdb[MONGO_COLLECTION]

    documents = user_coll.find({})

    for doc in documents:
        email_mongo = doc.get("_id", "")
        name_mongo = doc.get("name", "")
        phone_mongo = doc.get("phone", "")
        friends_mongo = doc.get("friends", [])
        likes_mongo = doc.get("likes", [])
        messages_mongo = doc.get("messages", [])

        # phone bereinigen
        if phone_mongo:
            phone_mongo = re.sub(r"[^0-9+]", "", phone_mongo)

        # name splitten (z.B. "Nachname, Vorname" => (Vorname, Nachname))
        # Falls kein Komma => E-Mail fallback
        first_name, last_name = split_name(name_mongo, email_mongo)

        # Capitalization
        first_name = fix_capitalization(first_name)
        last_name  = fix_capitalization(last_name)

        # user anlegen
        user_id = get_or_create_user_mongo(
            cursor=cursor,
            first_name=first_name,
            last_name=last_name,
            email=email_mongo,
            phone=phone_mongo
        )
        if not user_id:
            continue

        # friends -> friendships
        for friend_email in friends_mongo:
            ff_first, ff_last = split_name("", friend_email)
            ff_first = fix_capitalization(ff_first)
            ff_last  = fix_capitalization(ff_last)
            friend_id = get_or_create_user_mongo(cursor, ff_first, ff_last, friend_email, "")
            if friend_id and friend_id != user_id:
                uid1, uid2 = sorted([user_id, friend_id])
                cursor.execute("""
                    INSERT INTO friendships (user_id1, user_id2, status)
                    VALUES (%s, %s, 'friends')
                    ON CONFLICT DO NOTHING
                """, (uid1, uid2))

        # likes -> likes
        for like_obj in likes_mongo:
            liked_email = like_obj.get("liked_email", "")
            status_str  = like_obj.get("status", "")
            like_ts_str = like_obj.get("timestamp", "")
            like_ts = parse_datetime_str(like_ts_str, "%Y-%m-%d %H:%M:%S")

            lf_first, lf_last = split_name("", liked_email)
            lf_first = fix_capitalization(lf_first)
            lf_last  = fix_capitalization(lf_last)
            likee_id = get_or_create_user_mongo(cursor, lf_first, lf_last, liked_email, "")
            if likee_id and likee_id != user_id:
                cursor.execute("""
                    INSERT INTO likes (liker_id, likee_id, status, like_time)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (user_id, likee_id, status_str, like_ts))

        # messages -> messages
        for msg_obj in messages_mongo:
            conv_id   = msg_obj.get("conversation_id", None)
            recv_email= msg_obj.get("receiver_email", "")
            msg_text  = msg_obj.get("message", "")
            msg_ts_str= msg_obj.get("timestamp", "")
            msg_ts = parse_datetime_str(msg_ts_str, "%Y-%m-%d %H:%M:%S")

            mf_first, mf_last = split_name("", recv_email)
            mf_first = fix_capitalization(mf_first)
            mf_last  = fix_capitalization(mf_last)
            recv_id = get_or_create_user_mongo(cursor, mf_first, mf_last, recv_email, "")
            if recv_id and recv_id != user_id:
                cursor.execute("""
                    INSERT INTO messages (conversation_id, sender_id, receiver_id, message_text, send_time)
                    VALUES (%s, %s, %s, %s, %s)
                """, (conv_id, user_id, recv_id, msg_text, msg_ts))

    conn.commit()
    mongo_client.close()
    print("MongoDB-Import abgeschlossen.")


# ----------------------------------------------------------------------------
# 3) XML-IMPORT
# ----------------------------------------------------------------------------

def import_from_xml(cursor, conn):
    """
    Liest eine XML-Datei mit Struktur:
    <users>
      <user>
        <email>...</email>
        <name>Nachname, Vorname</name>
        <hobbies>
          <hobby>Schreiben</hobby>
          ...
        </hobbies>
      </user>
      ...
    </users>

    und legt in 'users', 'hobbies', 'user_hobbies' Einträge an.
    Keine Dubletten dank ON CONFLICT.
    """
    print("Starte XML-Import...")

    tree = ET.parse(XML_FILE)
    root = tree.getroot()  # <users>

    user_elems = root.findall("user")
    for user_elem in user_elems:
        email_elem = user_elem.find("email")
        name_elem  = user_elem.find("name")
        hobbies_elem = user_elem.find("hobbies")

        if email_elem is None or name_elem is None:
            continue

        email = email_elem.text.strip() if email_elem.text else ""
        full_name = name_elem.text.strip() if name_elem.text else ""

        # split name
        first_name, last_name = split_name(full_name, email)
        first_name = fix_capitalization(first_name)
        last_name  = fix_capitalization(last_name)

        # XML hat kein phone => ""
        phone = ""

        # user
        user_id = get_or_create_user_mongo(cursor, first_name, last_name, email, phone)
        if not user_id:
            continue

        # hobbies
        if hobbies_elem is not None:
            hobby_nodes = hobbies_elem.findall("hobby")
            for hnode in hobby_nodes:
                hobby_name = hnode.text.strip() if hnode.text else ""
                hobby_name = fix_capitalization(hobby_name)
                if hobby_name:
                    hobby_id = get_or_create_hobby(cursor, hobby_name)
                    cursor.execute("""
                        INSERT INTO user_hobbies (user_id, hobby_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (user_id, hobby_id))

    conn.commit()
    print("XML-Import abgeschlossen.")


# ----------------------------------------------------------------------------
# HILFSFUNKTIONEN
# ----------------------------------------------------------------------------

def split_name_simple(name_str):
    """
    Für Excel: "Nachname, Vorname" -> (Vorname, Nachname)
    Falls kein Komma, parken alles in last_name.
    """
    if not name_str:
        return ("-", "-")
    parts = name_str.split(",", 1)
    if len(parts) == 2:
        last_n = parts[0].strip()
        first_n= parts[1].strip()
        return (first_n, last_n)
    else:
        return ("-", name_str.strip())

def parse_address(addr_str):
    """
    Für Excel-Spalte "Minslebener Str. 0, 46286, Dorsten"
    -> (street="Minslebener Str.", house_no="0", zip_code="46286", city="Dorsten")
    """
    street = None
    house_no = None
    zip_code = None
    city = None

    parts = [p.strip() for p in addr_str.split(",")]
    if len(parts) >= 3:
        # street + houseNo
        sh = parts[0]
        sh_split = sh.rsplit(" ", 1)
        if len(sh_split) == 2:
            street = sh_split[0]
            house_no= sh_split[1]
        else:
            street = sh
            house_no= None

        zip_code = parts[1]
        city     = parts[2]
    return (street, house_no, zip_code, city)

def parse_date_ddmmYYYY(date_str):
    """
    Versucht "07.03.1959" -> datetime.date
    Falls Fehler -> None
    """
    if not date_str or date_str.strip() == "":
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d.%m.%Y")
        return dt.date()
    except:
        return None


def get_or_create_address(cursor, street, house_no, zip_code, city):
    """
    Sucht in 'addresses' die gleiche (street, house_no, zip_code, city).
    Falls existiert -> address_id
    Sonst Insert -> address_id
    """
    if not street and not city:
        # Falls leer
        return None

    # CHECK
    cursor.execute("""
        SELECT address_id
        FROM addresses
        WHERE street = %s AND house_no = %s AND zip_code = %s AND city = %s
    """, (street, house_no, zip_code, city))
    row = cursor.fetchone()
    if row:
        return row[0]

    # Insert
    insert_sql = """
        INSERT INTO addresses (street, house_no, zip_code, city)
        VALUES (%s, %s, %s, %s)
        RETURNING address_id
    """
    cursor.execute(insert_sql, (street, house_no, zip_code, city))
    new_id = cursor.fetchone()[0]
    return new_id

def get_or_create_user(
    cursor,
    first_name,
    last_name,
    phone,
    email,
    gender,
    birth_date,
    address_id,
    interested_in=None  # default None
):
    """
    Für Excel-Import: Legt User an (oder findet existierenden).
    Achtung: 'interested_in' ist neu.
    """
    if not email:
        return None

    # Check user
    cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
    row = cursor.fetchone()
    if row:
        return row[0]

    print(f"INSERT user (Excel) -> {first_name} {last_name} / {email} / interested_in={interested_in}")

    insert_sql = """
        INSERT INTO users (
            first_name, last_name, phone, email,
            gender, birth_date, address_id, interested_in
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING user_id
    """
    cursor.execute(insert_sql, (
        first_name, last_name, phone, email,
        gender, birth_date, address_id, interested_in
    ))
    new_row = cursor.fetchone()
    if new_row:
        return new_row[0]
    else:
        # user existierte
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        r2 = cursor.fetchone()
        return r2[0] if r2 else None


def get_or_create_user_mongo(cursor, first_name, last_name, email, phone):
    """
    Ähnlich wie get_or_create_user, nur ohne gender/birth_date/address.
    """
    if not email:
        return None

    cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
    row = cursor.fetchone()
    if row:
        return row[0]

    print(f"INSERT user (Mongo/XML) -> {first_name} {last_name} / {email}")

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
        # existiert
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        r2 = cursor.fetchone()
        return r2[0] if r2 else None

def get_or_create_hobby(cursor, hobby_name):
    """
    Legt Hobby an, falls nicht existiert. hobby_id -> zurück
    name UNIQUE => ON CONFLICT
    """
    if not hobby_name:
        return None
    cursor.execute("SELECT hobby_id FROM hobbies WHERE name = %s", (hobby_name,))
    row = cursor.fetchone()
    if row:
        return row[0]

    insert_sql = """
        INSERT INTO hobbies (name)
        VALUES (%s)
        ON CONFLICT (name) DO NOTHING
        RETURNING hobby_id
    """
    cursor.execute(insert_sql, (hobby_name,))
    new_row = cursor.fetchone()
    if new_row:
        return new_row[0]
    else:
        # existierte
        cursor.execute("SELECT hobby_id FROM hobbies WHERE name = %s", (hobby_name,))
        r2 = cursor.fetchone()
        return r2[0] if r2 else None

def split_name(full_name, email):
    """
    Für Mongo/XML:
    - Versuch Komma-Split ("Nachname, Vorname")
    - Fallback -> E-Mail local-part split am '.' => (vorher, nachher)
    """
    if full_name and "," in full_name:
        parts = full_name.split(",", 1)
        ln = parts[0].strip()
        fn = parts[1].strip()
        return (fix_capitalization(fn), fix_capitalization(ln))
    else:
        # fallback
        return split_name_from_email(email)

def split_name_from_email(email):
    """
    local_part vor '@' => split am ersten '.'
    """
    if not email or "@" not in email:
        return ("-", "-")
    local_part = email.split("@")[0]
    if "." in local_part:
        idx = local_part.find(".")
        return (local_part[:idx], local_part[idx+1:])
    else:
        return ("-", local_part)

def fix_capitalization(s):
    """Erster Buchstabe groß, Rest klein."""
    if not s:
        return ""
    return s[0].upper() + s[1:].lower()

def parse_datetime_str(ts_str, fmt):
    """
    Parst z.B. '2024-03-17 07:39:29' nach datetime.
    """
    if not ts_str:
        return None
    try:
        return datetime.strptime(ts_str, fmt)
    except:
        return None


# ----------------------------------------------------------------------------
# START
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
