"""
import_xml.py
-------------
Beispiel-Skript zum Einlesen einer XML-Datei mit z.B. Hobby-Daten
und Übernahme in eine PostgreSQL-Datenbank.
"""

import xml.etree.ElementTree as ET
import psycopg2

XML_FILE = "hobbies.xml"  # Beispiel-Dateiname
POSTGRES_HOST = "localhost"
POSTGRES_DB = "letsmeetdb"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_PORT = 5432

def main():
    # 1. XML-Datei parsen
    tree = ET.parse(XML_FILE)
    root = tree.getroot()
    # Beispiel: Wir erwarten ein Wurzelelement <Hobbies> und darin <User>...
    # <User email="..." >
    #    <Hobby name="Kochen" priority="70" />
    #    <Hobby name="Joggen" priority="30" />
    # </User>

    # 2. Verbindung zur PostgreSQL-Datenbank
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()

    # 3. Durch die XML-Struktur iterieren
    for user_elem in root.findall("User"):
        user_email = user_elem.get("email", "")  # Attribut "email"

        # Holen wir uns die Hobby-Knoten
        for hobby_elem in user_elem.findall("Hobby"):
            hobby_name = hobby_elem.get("name", "")
            hobby_priority = hobby_elem.get("priority", "0")

            # In einer Zwischentabelle ablegen
            # z.B. "xml_hobby_dump" mit Spalten (user_email, hobby_name, priority)
            insert_query = """
                INSERT INTO xml_hobby_dump (
                    user_email,
                    hobby_name,
                    priority
                ) VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (user_email, hobby_name, hobby_priority))

    # 4. Änderungen festschreiben
    conn.commit()

    # 5. Verbindung schließen
    cursor.close()
    conn.close()

    print("XML-Daten wurden erfolgreich importiert.")

if __name__ == "__main__":
    main()
