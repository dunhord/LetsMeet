"""
import_xml.py
-------------
Liest eine XML-Datei mit Struktur wie:

<users>
  <user>
    <email>heinz.heinrichs@gmaiil.te</email>
    <name>Heinrichs, Heinz</name>
    <hobbies>
      <hobby>Schreiben</hobby>
      <hobby>Musik</hobby>
      <hobby>Bowling</hobby>
    </hobbies>
  </user>
  ...
</users>

Und importiert die Daten direkt in die Tabellen "users", "hobbies" und "user_hobbies".
Vor- und Nachname werden aufgesplittet (Komma oder Fallback aus E-Mail), 
erster Buchstabe groß, Rest klein. 
"""

import xml.etree.ElementTree as ET
import psycopg2
import re
from datetime import datetime

# Anpassen: Pfad zur XML-Datei
XML_FILE = "Lets_Meet_Hobbies.xml"

# PostgreSQL-Verbindungsdaten anpassen
POSTGRES_HOST = "localhost"
POSTGRES_DB = "lf8_lets_meet_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "secret"
POSTGRES_PORT = 5432

def main():
    # 1) XML-Datei parsen
    tree = ET.parse(XML_FILE)
    root = tree.getroot()  # sollte <users> sein

    # 2) Verbindung zur PostgreSQL-Datenbank
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )
    # UTF-8-Encoding sicherstellen
    conn.set_client_encoding('UTF8')
    cursor = conn.cursor()

    # 3) Für jeden <user>-Knoten: E-Mail, Name, Hobbys importieren
    user_elements = root.findall("user")
    for user_elem in user_elements:
        email_elem = user_elem.find("email")
        name_elem = user_elem.find("name")
        hobbies_elem = user_elem.find("hobbies")

        if email_elem is None or name_elem is None:
            # Daten unvollständig -> ggf. überspringen
            continue

        email = email_elem.text.strip() if email_elem.text else ""
        full_name = name_elem.text.strip() if name_elem.text else ""

        # 3.1) Vor- und Nachname ermitteln
        first_name, last_name = split_name(full_name, email)
        first_name = fix_capitalization(first_name)
        last_name = fix_capitalization(last_name)

        # Da kein <phone> in XML -> phone=""
        phone = ""

        # 3.2) User anlegen -> user_id
        user_id = get_or_create_user_id(cursor, first_name, last_name, email, phone)

        if user_id is None:
            # Evtl. E-Mail leer oder Insert fehlgeschlagen
            continue

        # 3.3) Hobbys: <hobbies><hobby>XYZ</hobby>...</hobbies>
        # Falls <hobbies> fehlt oder leer -> keine Hobbys
        if hobbies_elem is not None:
            hobby_elems = hobbies_elem.findall("hobby")
            for hobby_elem in hobby_elems:
                hobby_name = hobby_elem.text.strip() if hobby_elem.text else ""
                if hobby_name:
                    hobby_name = fix_capitalization(hobby_name)
                    hobby_id = get_or_create_hobby(cursor, hobby_name)
                    if hobby_id:
                        # Verknüpfung user_hobbies
                        cursor.execute(
                            """
                            INSERT INTO user_hobbies (user_id, hobby_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (user_id, hobby_id)
                        )

    # 4) Commit & Verbindung schließen
    conn.commit()
    cursor.close()
    conn.close()

    print("XML-Daten wurden erfolgreich importiert.")


# -------------------------------------------------
# Hilfsfunktionen
# -------------------------------------------------

def split_name(full_name, email):
    """
    Versucht "Nachname, Vorname" per Komma zu parsen.
    Fällt zurück auf E-Mail (lokaler Teil split am ersten '.').
    Beispiel: "Fleddermann, Heinz" -> (Heinz, Fleddermann)
              "heinz.fleddermann@..." -> (heinz, fleddermann)
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
    E-Mail -> local_part -> split am ersten '.' => (vorher, nachher)
    """
    if not email or "@" not in email:
        return ("-", "-")

    local_part = email.split('@')[0]
    if '.' in local_part:
        idx = local_part.find('.')
        return (local_part[:idx], local_part[idx + 1:])
    else:
        return ("-", local_part)

def fix_capitalization(s):
    """
    Erster Buchstabe groß, Rest klein. Beispiel: "heINz" -> "Heinz"
    """
    if not s:
        return ""
    return s[0].upper() + s[1:].lower()

def get_or_create_user_id(cursor, first_name, last_name, email, phone):
    """
    Sucht user per email. Wenn nicht existiert, Insert -> user_id zurück.
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
        # User existierte -> abfragen
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        row2 = cursor.fetchone()
        return row2[0] if row2 else None

def get_or_create_hobby(cursor, hobby_name):
    """
    Sucht hobby per name. Wenn nicht existiert, Insert -> hobby_id zurück.
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
        # already existed
        cursor.execute("SELECT hobby_id FROM hobbies WHERE name = %s", (hobby_name,))
        row2 = cursor.fetchone()
        return row2[0] if row2 else None


if __name__ == "__main__":
    main()
