"""
import_mongodb.py
-----------------
Beispiel-Skript zum Einlesen von Nutzerdaten aus einer MongoDB 
und Übernahme in eine PostgreSQL-Datenbank.
"""

from pymongo import MongoClient
import psycopg2

# MongoDB-Variablen anpassen
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "letsmeet_mongo"
MONGO_COLLECTION = "users"

# PostgreSQL-Variablen anpassen
POSTGRES_HOST = "localhost"
POSTGRES_DB = "lf8_lets_meet_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "secret"
POSTGRES_PORT = 5432

def main():
    # 1. Verbindung zur MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_database = mongo_client[MONGO_DB]
    user_collection = mongo_database[MONGO_COLLECTION]

    # 2. Verbindung zur PostgreSQL-Datenbank
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )
    pg_cursor = pg_conn.cursor()

    # 3. Alle Mongo-Dokumente abrufen
    documents = user_collection.find({})

    # 4. Für jedes Dokument Daten extrahieren und in Zwischentabelle importieren
    insert_query = """
        INSERT INTO mongo_user_dump (
            _id,
            name,
            phone,
            friends,
            likes_json,
            messages_json,
            created_at,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for doc in documents:
        _id = doc.get("_id", "")
        name = doc.get("name", "")
        phone = doc.get("phone", "")
        friends = doc.get("friends", [])
        likes = doc.get("likes", [])
        messages = doc.get("messages", [])
        created_at = doc.get("createdAt", None)
        updated_at = doc.get("updatedAt", None)

        # Da wir in einer SQL-Spalte nur Strings oder JSONB speichern,
        # konvertieren wir listen/dicts zu JSON-Strings.
        # Einfache Variante via str(), besser: json.dumps(...) für korrektes JSON
        import json
        friends_str = json.dumps(friends, ensure_ascii=False)
        likes_str = json.dumps(likes, ensure_ascii=False)
        messages_str = json.dumps(messages, ensure_ascii=False)

        pg_cursor.execute(insert_query, (
            _id,
            name,
            phone,
            friends_str,
            likes_str,
            messages_str,
            created_at,
            updated_at
        ))

    # 5. Änderungen festschreiben
    pg_conn.commit()

    # 6. Verbindungen schließen
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()

    print("MongoDB-Daten wurden erfolgreich importiert.")

if __name__ == "__main__":
    main()
