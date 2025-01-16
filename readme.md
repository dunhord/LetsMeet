## Lernsituation: Migration "Let’s Meet"

### Ausgangslage:

Ihr Unternehmen hat eine neue Kundin gewonnen: die _Let’s Meet GmbH_, die eine Dating- und Meeting-Plattform betreibt.
Ihr Unternehmen soll die Dating-App der Kundin weiterentwickeln. Leider erfolgte die Trennung vom vorigen IT-Dienstleister im Streit und nicht reibungsfrei, sodass die Kundin keinen Zugriff auf die bestehenden Datenbanken hat. Es liegt lediglich ein Datenbank-Dump in Form einer Excel-Datei vor, die die Inhalte aller Tabellen erfasst. Außerdem gibt es einen Backup einer MongoDB sowie eine XML-Datei mit einigen Daten.

![Ausschnitt aus "Tea with friends, and one must wear one's finest hat!" (public domain)](./images/tea-with-friends.png)
Ihr Team ist für die Datenanalyse und den Entwurf der neuen Datenbank sowie die Migration in neue Tabellen zuständig. Die eigentliche App wird von einem anderen Team betreut.

Ausgangslage ist der vorliegende Export der Datenbank als Excel-Tabelle. Um Problemen beim Ändern, Löschen und Einfügen von Datensätzen vorzubeugen, legt die Kundin sehr viel Wert darauf, im Vorfeld der Migration ein ausgefeiltes konzeptuelles Datenmodell zu erhalten. Anhand des Datenmodells soll die Kundin im Anschluss hinsichtlich der Wahl eines logischen Datenmodells und schließlich hinsichtlich eines Datenbankmanagementsystems beraten werden. Um nicht ein weiteres Mal ohne aussagekräftige Dokumente dazustehen, wünscht sich die Kundin zudem eine geeignete Dokumentation der einzelnen Entwurfsphasen und der reproduzierbaren Zwischenschritte.

Daneben existieren auch Daten in einer NoSql-Datenbank (MongoDB). Hier sind Likes und Nachrichten hinterlegt, die aus einer anderen App stammen. Diese sollen ebenfalls in die neue Datenbank migriert werden.

Desweiteren sind Hobbydaten in einer XML-Datei vorhanden und sollen ebenfalls migriert werden.

Alle genutzten SQL-Befehle und der Code für den Import sollen mit git versioniert werden. Dokumentieren Sie die durchgeführten Schritte bzw. die erstellten Diagramme, etc. jeweils in einer kurzen Markdown-Datei.

Das Zielsystem soll ein Postgres in einem Docker-Container sein. Der Docker-Container wurde bereits in der [compose.yml](compose.yml) konfiguriert und kann mit *docker compose up* gestartet werden.

### Auszug der Daten - Excel

Alle Daten finden sich in der Excel-Datei let_s-meet-db-dump.xlsx (Im Projektordner). Der Aufbau der Daten lässt sich an folgendem Schema erkennen:

* Spalte 1: Name kommagetrennt: Vorname, Nachname

* Spalte 2: Adresse kommagetrennt (Straße / Nr. durch Leerzeichen)

* Spalte 3: Telefonnummern, ggf. kommagetrennt

* Spalte 4: fünf Hobbies, getrennt durch Semikolon. Die Priorität des jeweiligen Hobbys (0-100) wird zwischen zwei Prozentzeichen angegeben.

* Spalte 5: E-Mail-Adresse

* Spalte 6: Gelesenes Geschlecht  (m / w / nicht binär)

* Spalte 7: Interessiert an Geschlecht (m/w/nicht binär, Mehrfachnennung möglich)

* Spalte 8: Geburtsdatum

|Nachname, Vorname	|Straße Nr, PLZ, Ort	|Telefon	|Hobby1 %Prio1%; Hobby2 %Prio2%; Hobby3 %Prio3%; Hobby4 %Prio4%; Hobby5 %Prio5%;	|E-Mail	|Geschlecht	|Interessiert an| Geburtsdatum|
|---|---|---|---|---|---|---|---|
|Gehlen, Ursula	|Papendamm 27, 52062, Aachen	|(0251) 340233	|Gäste einladen %91%; Geschenke machen %75%; Für sich selbst Dinge einkaufen %75%; Eine Blume oder Pflanze sehen oder daran riechen %21%; Leger gekleidet sein %84%; |ursula.gehlen@d-ohnline.te	|w	|m| 13.05.1983|
|Gehrmann, Kai	|Parkstr 6, 52072, Aachen	|(0251) 376775	|Etwas neuartig und originell machen %45%; Ausgiebig frühstücken %40%; An technischen Dingen arbeiten (Fahrzeuge, Hausgeräte usw.) %49%; 	|kai.gehrmann@autluuk.te	|m	|w|11.06.1991|

### **Auszug der Daten - MongoDB**

Die MongoDB-Datenbank speichert Benutzerdaten in der Sammlung `users`. Hier ein Beispiel eines Dokuments:

```json
{
    "_id": "ansgar.kötter@web.ork",
    "name": "Kötter, Ansgar",
    "phone": "03935 / 75289",
    "friends": [],
    "likes": [
        {
            "liked_email": "helmut.bußmann@autluuk.kom",
            "status": "pending",
            "timestamp": "2024-03-17 07:39:29"
        }
    ],
    "messages": [
        {
            "conversation_id": 36,
            "receiver_email": "christian.pollack@ge-em-ix.te",
            "message": "Ich habe eine spannende Idee, die ich mit dir teilen möchte.",
            "timestamp": "2024-09-22 07:40:31"
        }
    ],
    "createdAt": "2023-03-08T00:00:00",
    "updatedAt": "2023-10-20T00:00:00"
}
```
### *Auszug der Daten - XML*
Die XML-Datei speichert Hobbys der Benutzerinnen. Ein Beispiel für einen Benutzer in der XML-Datei sieht wie folgt aus:
```xml
<users>
    <user>
        <email>ursula.gehlen@d-ohnline.te</email>
        <name>Gehlen, Ursula</name>
        <hobby>Kochen</hobby>
        <hobby>Gartenarbeit</hobby>
        <hobby>Fotografie</hobby>
    </user>
</users>
```


### Erweiterung der Datenbank

Neben dem Import soll die Datenbank für folgende Anwendungsfälle eine Struktur bieten.

- Benutzer*innen können die Hobbys priorisieren, die sie an anderen interessieren (0-100) bzw. die sie ausgesprochen nicht mögen (-100 - 0 )

- Benutzer*innen können andere Benutzer*innen auf ihre „Freundesliste“ setzen.

- Benutzer*innen haben ein Profilbild. Dieses wird direkt in der Datenbank gespeichert (`BLOB`).

- Neben einem Profilbild können Benutzer*innen weitere Fotos hochladen oder verlinken.

Es wurde das unten abgedruckte Anwendungsfalldiagramm erstellt. Diese Anwendungsfälle sollen bereits im Datenmodell vorgesehen werden. Für jeden Use-Case soll ein passendes SQL-Query beispielhaft erstellt werden.

![Anwendungsfalldiagramm für die Let's Meet-DB](images/use-case.png)

## Hinweise und Anforderungen an die Realisierung

Kleingruppen sollen die Anpassungen planen, durchführen, jedes Zwischenergebnis testen und alle zugrundeliegenden Schritte dokumentieren. 

Die folgenden Schritte sollen ausgeführt und im _git_-Repository versioniert und dokumentiert werden:

* **Konzeptuelles Datemmodell**: Analyse der bestehenden Daten in der Excel-Tabelle und MongoDB und Extrahierung eines konzeptuellen Datenmodells (z.B. ER-Diagramm) der Zieldatenbank.

* **Logisches Datenmodell**: Anwenden der Transformationsregeln, um ein konzeptuellen Modell (Entity Relationship Model) in ein logisches Modell (Relationenmodell) umzuwandeln. Berücksichtigen Sie dabei die Regeln der [*Datenbanknormalisierung* ](normalization.md) und stellen Sie sicher, dass die Datenbank in der dritten Normalform ist.

* **Datenschutz**: Was ist erforderlich, um die betreffenden Daten verarbeiten und speichern zur dürfen? Aus Sicht des Datenschutzes: welche unterschiedlichen Arten von Daten liegen hier vor und wie müssen sie demnach geschützt werden? Welche Maßnahmen müssen ergriffen werden?

* **Erstellung des physischen Datenmodells** für die Ursprungsdaten und die Zieldatenbank Postgres (SQL-DDL, `CREATE TABLE...`)

* **Erstellung eines Importskriptes I** für die Daten aus der Excel-Datei (Programmiersprache Ihrer Wahl) (SQL-DML `INSERT INTO ...`)

* **Erstellung eines Importskriptes II** für die Daten aus der MongoDB (Programmiersprache Ihrer Wahl) (SQL-DML `INSERT INTO ...`)

* **Erstellung eines Importskriptes III** für die Daten aus der XML-Datei (Programmiersprache Ihrer Wahl) (SQL-DML `INSERT INTO ...`)



# Technische Hinweise für die Realisierung

## Docker-Setup

Die gesamte Infrastruktur ist so konfiguriert, dass sie mit Docker Compose gestartet werden kann. Das System enthält zwei zentrale Dienste: **PostgreSQL** als Ziel-Datenbank und **MongoDB** als Quelle für bestimmte Daten.

### Compose-Datei: `compose.yml`

#### PostgreSQL-Dienst
- **Container-Name:** `lf8_lets_meet_postgres_container`
- **Image:** `postgres:16.4`
- **Volumes:** Persistente Speicherung der PostgreSQL-Daten unter `lf8_lets_meet_postgres_data:/var/lib/postgresql/data`
- **Umgebungsvariablen:**
    - `POSTGRES_DB`: Name der Datenbank (`lf8_lets_meet_db`)
    - `POSTGRES_USER`: Benutzername für den Zugriff (`user`)
    - `POSTGRES_PASSWORD`: Passwort für den Zugriff (`secret`)
- **Ports:** Der Dienst ist auf dem lokalen Port `5432` verfügbar.

#### MongoDB-Dienst
- **Container-Name:** `lf8_lets_meet_mongodb_container`
- **Image:** `berndheidemann/letsmeet-mongodb:latest`
- **Volumes:** Persistente Speicherung der MongoDB-Daten unter `lf8_lets_meet_mongodb_data:/data/db`
- **Umgebungsvariablen:**
    - `MONGO_INITDB_DATABASE`: Name der initialen Datenbank (`LetsMeet`)
- **Ports:** Der Dienst ist auf dem lokalen Port `27017` verfügbar.

### Starten der Dienste
Führen Sie den folgenden Befehl im Projektordner aus, um die Dienste zu starten:
```bash
docker compose up
```

- Nach dem Starten läuft die PostgreSQL-Datenbank auf `localhost:5432`.
- Die MongoDB-Datenbank ist auf `localhost:27017` erreichbar.

### Zugriff auf die Datenbanken
#### PostgreSQL
Sie können mit jedem SQL-Client (z. B. pgAdmin, DBeaver) oder direkt mit `psql` auf die Datenbank zugreifen:
```bash
psql -h localhost -U user -d lf8_lets_meet_db
```
**Passwort:** `secret`

#### MongoDB
Verwenden Sie `mongo`-Clients oder GUI-Tools wie MongoDB Compass. Standard-URI:
```
mongodb://localhost:27017/LetsMeet
```

---

## Zugriff auf MongoDB in VS Code

Um auf die MongoDB in VS Code zuzugreifen, benötigen Sie die MongoDB-Erweiterung:
1. Installieren Sie die Erweiterung **"MongoDB for VS Code"** aus dem VS Code Marketplace.
2. Öffnen Sie die "MongoDB"-Ansicht (Strg+Shift+P → `MongoDB: Open Overview`).
3. Fügen Sie eine neue Verbindung hinzu. Verwenden Sie die folgende Verbindungs-URI:
   ```
   mongodb://localhost:27017
   ```
4. Nach der Verbindung können Sie in der Ansicht die `LetsMeet`-Datenbank durchsuchen, Abfragen durchführen und Dokumente ansehen.

---

## Zugriff auf PostgreSQL in VS Code

1. Installieren Sie die Erweiterung **"SQLTools"** aus dem VS Code Marketplace.
2. Gehen Sie in die SQLTools-Verwaltung und erstellen Sie eine neue Verbindung:
    - **DB-Typ:** PostgreSQL
    - **Host:** `localhost`
    - **Port:** `5432`
    - **Benutzername:** `user`
    - **Passwort:** `secret`
    - **Datenbankname:** `lf8_lets_meet_db`
3. Nach der Einrichtung können Sie SQL-Abfragen direkt aus VS Code ausführen.

---

## Dokumentation und Versionierung

- Alle SQL-Skripte, Import-Skripte und Modelle sollen im `git`-Repository versioniert werden.
- Verwenden Sie für die Dokumentation Markdown-Dateien. Empfohlene Struktur:
  ```
  results/
    konzeptuelles_modell.md
    logisches_modell.md
    datenschutz.md
    scripts/
      create_tables.sql
      import_excel.py
      import_mongodb.py
      import_xml.py
  ```

---

## Testen

Nach der Migration der Daten sollten alle Zwischenergebnisse durch SQL-Abfragen überprüft werden. Testen Sie die Konsistenz und Integrität der Daten anhand des konzeptuellen und logischen Modells.

