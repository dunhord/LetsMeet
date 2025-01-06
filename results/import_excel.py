"""
import_excel_direct.py
----------------------
Liest die Excel-Datei mit den 8 Spalten ein und schreibt die Daten direkt 
in die Zieltabellen: addresses, users, hobbies, user_hobbies.

Spalten in Excel (in dieser Reihenfolge):
1) Nachname, Vorname
2) Straße Nr, PLZ Ort
3) Telefon
4) Hobbies (z.B. "Kochen %80%; Joggen %20%; ...")
5) E-Mail
6) Geschlecht (m / w / nicht binär / ...)
7) Interessiert an (wird hier nicht verwendet)
8) Geburtsdatum (z.B. 07.03.1959)

Benötigte Python-Pakete:
 - pandas + openpyxl (für Excel)
 - psycopg2 (für PostgreSQL)

Achtung:
 - Es wird sehr vereinfacht geparst (z.B. nur Komma- und Leerzeichen-Splitting).
 - Bitte ggf. Robustheit, Fehlerbehandlung, Logging erweitern.
"""

import pandas as pd
import psycopg2
import re
from datetime import datetime

# 1) Anpassen: Excel-Datei
EXCEL_FILE = "Lets Meet DB Dump.xlsx"

# 2) Anpassen: PostgreSQL-Daten
POSTGRES_HOST = "localhost"
POSTGRES_DB = "lf8_lets_meet_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "secret"
POSTGRES_PORT = 5432

def main():
    # A) Excel einlesen
    df = pd.read_excel(EXCEL_FILE, sheet_name=0)
    
    # Spalten umbenennen, damit wir sie per Namen ansprechen können
    # (Muss zur Reihenfolge der Excel-Spalten passen!)
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
    
    # B) Verbindung aufbauen
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()
    
    # Durch jeden Datensatz der Excel-Datei iterieren
    for _, row in df.iterrows():
        # 1) Name parsen -> "Forster, Martin"
        name_str = str(row["nachname_vorname"]) if pd.notnull(row["nachname_vorname"]) else ""
        last_name = ""
        first_name = ""
        
        name_parts = name_str.split(",")
        if len(name_parts) == 2:
            last_name = name_parts[0].strip()
            first_name = name_parts[1].strip()
        else:
            # Falls kein Komma: 
            # Ggf. nur in "Vorname" oder "Nachname" stecken 
            # oder beides in last_name
            last_name = name_str.strip()
        
        # 2) Adresse parsen -> "Minslebener Str. 0, 46286, Dorsten"
        addr_str = str(row["strasse_plz_ort"]) if pd.notnull(row["strasse_plz_ort"]) else ""
        street = None
        house_no = None
        zip_code = None
        city = None
        
        # z.B. "Minslebener Str. 0, 46286, Dorsten" -> split by Komma
        addr_parts = [p.strip() for p in addr_str.split(",")]
        if len(addr_parts) >= 3:
            # addr_parts[0] = "Minslebener Str. 0"
            # addr_parts[1] = "46286"
            # addr_parts[2] = "Dorsten"
            # -> street + house_no aufsplitten
            street_house_str = addr_parts[0]
            # naive Variante: re.split() am Ende
            # z.B. "Minslebener Str. 0" -> ["Minslebener Str.", "0"]
            # Achtung, nur wenn 1 Leerzeichen am Ende
            sh_parts = street_house_str.rsplit(" ", 1)
            if len(sh_parts) == 2:
                street = sh_parts[0]
                house_no = sh_parts[1]
            else:
                street = street_house_str
                house_no = None
            
            zip_code = addr_parts[1]
            city = addr_parts[2]
        
        # 3) Telefon
        phone = str(row["telefon"]) if pd.notnull(row["telefon"]) else None
        
        # 4) Geschlecht
        gender = str(row["geschlecht"]) if pd.notnull(row["geschlecht"]) else None
        
        # 5) Geburtsdatum parsen
        birth_str = str(row["geburtsdatum"]) if pd.notnull(row["geburtsdatum"]) else ""
        birth_date = None
        try:
            # Annahme: "07.03.1959"
            birth_date = datetime.strptime(birth_str, "%d.%m.%Y").date()
        except:
            # Falls Format nicht passt -> None
            pass
        
        # 6) E-Mail
        email = str(row["email"]) if pd.notnull(row["email"]) else None
        
        # 7) Hobbys parsen -> "Kochen %80%; Joggen %20%; ..."
        hobbies_str = str(row["hobbies_raw"]) if pd.notnull(row["hobbies_raw"]) else ""
        hobby_entries = [h.strip() for h in hobbies_str.split(";") if h.strip()]
        
        # ============= EINTRAGEN IN DB =============
        
        # a) Adresse einfügen -> addresses
        #    RETURNING address_id
        insert_address = """
            INSERT INTO addresses (street, house_no, zip_code, city)
            VALUES (%s, %s, %s, %s)
            RETURNING address_id
        """
        cursor.execute(insert_address, (street, house_no, zip_code, city))
        address_id = cursor.fetchone()[0]
        
        # b) User einfügen -> users
        #    RETURNING user_id
        insert_user = """
            INSERT INTO users (
                first_name, last_name, phone, email, 
                gender, birth_date, address_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING user_id
        """
        cursor.execute(insert_user, (
            first_name, last_name, phone, email, 
            gender, birth_date, address_id
        ))
        user_id = cursor.fetchone()[0]
        
        # c) Hobbys einfügen (falls neu) + user_hobbies
        #    Bsp-Regex: (.*?)%(\d+)%   -> Name & Priority
        for hpart in hobby_entries:
            # z.B. "Fremdsprachenkenntnisse erweitern %78%"
            match = re.search(r"(.*?)%(\d+)%", hpart)
            if match:
                hobby_name = match.group(1).strip()
                priority_str = match.group(2)
                priority_val = int(priority_str)

                # 1) Insert Hobby
                insert_hobby = """
                    INSERT INTO hobbies (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING hobby_id
                """
                cursor.execute(insert_hobby, (hobby_name,))
                row_hobby_id = cursor.fetchone()
                
                if not row_hobby_id:
                    # Hobby existiert bereits -> ID abfragen
                    cursor.execute("SELECT hobby_id FROM hobbies WHERE name = %s", (hobby_name,))
                    row_hobby_id = cursor.fetchone()
                
                hobby_id = row_hobby_id[0]

                # 2) In user_hobbies eintragen
                insert_user_hobbies = """
                    INSERT INTO user_hobbies (user_id, hobby_id, priority)
                    VALUES (%s, %s, %s)
                """
                cursor.execute(insert_user_hobbies, (user_id, hobby_id, priority_val))
            else:
                # Falls wir keinen passenden %NN% finden, könntest du:
                # - nur den Hobby-Namen speichern und priority=0
                # - oder ignorieren
                pass
    
    # Alles am Ende committen
    conn.commit()
    cursor.close()
    conn.close()

    print("Excel-Daten wurden direkt ins finalisierte Schema importiert.")

if __name__ == "__main__":
    main()
