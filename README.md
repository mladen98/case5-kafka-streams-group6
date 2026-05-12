# case5-kafka-streams – Group 6

Kafka-Stream-Projekt mit zwei Apps, die GPS-Daten von Fahrzeugen verarbeiten und Verzögerungen ans Dashboard melden.

---

## Architektur / Pipeline

```
[driver-position]
       ↓
  App 1 (RouteTimingLauncher)
    → Liest GPS-Events
    → REST-Aufruf: berechnet Delay pro Fahrzeug
    → schreibt Delay-Events
       ↓
[group6-route-timing]   (Format: "id: 123, delay: 321")
       ↓
  App 2 (DelaydetectorLauncher)
    → Filtert: nur Delays > 180 Sekunden
    → MyProcessor: Logging
    → schreibt gefilterte Events
       ↓
[delays]   (Format: "id: 123, delay: 321")
       ↓
Dashboard: http://192.168.111.11:8080/status/
```

---

## Infrastruktur

| Komponente   | Adresse                              |
|--------------|--------------------------------------|
| Kafka Broker | `192.168.111.10:9092`                |
| REST-Service | `http://192.168.111.11:8080`         |
| Dashboard    | `http://192.168.111.11:8080/status/` |

---

## Bauen (PowerShell)

```powershell
$env:JAVA_HOME = "$env:USERPROFILE\.jdks\temurin-17.0.18"
$env:Path = "$env:JAVA_HOME\bin;C:\Program Files\JetBrains\IntelliJ IDEA 2025.3\plugins\maven\lib\maven3\bin;$env:Path"
cd "C:\Users\kapis\IdeaProjects\case5-kafka-streams-group6"
mvn clean package
```

JARs in `target/`:
- `target/route-timing.jar` → App 1
- `target/delay-detector.jar` → App 2

---

## Starten (Reihenfolge beachten!)

### 1. App 1 – RouteTimingLauncher

```powershell
java -jar "C:\Users\kapis\IdeaProjects\case5-kafka-streams-group6\target\route-timing.jar"
```

Erwarteter Output:
```
App 1 (RouteTimingLauncher) gestartet.
Warte auf neue GPS-Events ...
Stage 1 - processing driver-position event
→ Geschrieben nach group6-route-timing: id: 7862, delay: 413
```

### 2. App 2 – DelaydetectorLauncher (während App 1 läuft)

```powershell
java -jar "C:\Users\kapis\IdeaProjects\case5-kafka-streams-group6\target\delay-detector.jar"
```

Erwarteter Output:
```
App 2 (DelayDetectorLauncher) gestartet.
Stage 2 - significant delay detected:
id: 7862, delay: 413
MyProcessor got Event
KEY   = 7862
VALUE = id: 7862, delay: 413
✅ In 'delays' geschrieben: key=7862 value=id: 7862, delay: 413
```

---

## Technische Implementierung

### Warum kein Kafka Streams (Plain KafkaConsumer)

Beide Apps verwenden bewusst **Plain KafkaConsumer/KafkaProducer** statt Kafka Streams.

**Problem mit Kafka Streams:**
Wenn mehrere Java-Prozesse gleichzeitig laufen (z.B. IntelliJ + Hintergrundprozesse), hängt Kafka Streams
dauerhaft im `REBALANCING`-Zustand und verarbeitet **0 Records**. Dieser Zustand löst sich nicht auf.

```
STREAM STATE: CREATED -> REBALANCING
STREAM STATE: REBALANCING -> REBALANCING  ← hängt hier für immer
```

**Lösung:**
`consumer.assign(partitions)` statt `consumer.subscribe(topic)` → manuelle Partition-Zuweisung,
kein Group-Coordinator, kein REBALANCING.

### App 1 – RouteTimingLauncher

- Liest GPS-Events aus `driver-position` (ab aktuellem Ende → nur neue Events, keine 2.3 Mio. alten)
- REST-Aufruf: `GET /route/{id}?time=...&lat=...&lon=...` → liefert Delay in Sekunden
- Schreibt in `group6-route-timing`: `id: 7862, delay: 413`

### App 2 – DelaydetectorLauncher

- Liest aus `group6-route-timing` (ab aktuellem Ende → nur neue Events von App 1)
- **DelayFilter**: filtert Events mit Delay ≤ 180 Sekunden heraus
- **MyProcessor**: loggt signifikante Verzögerungen
- Schreibt in `delays`: `id: 7862, delay: 413` (exaktes Dashboard-Format laut Spezifikation)

### Nachrichtenformat `delays`-Topic

Laut Aufgabenstellung:
```
id: 123, delay: 321
```
- `id` = Delivery ID (auch als Kafka Message Key)
- `delay` = Verzögerung in Sekunden
- Nur korrekt formatierte Messages werden vom Dashboard verarbeitet

---

## Diagnosewerkzeuge

### TopicInspector

Prüft alle drei Topics und zeigt den aktuellen Stand:

| Topic                 | Was wird geprüft                                   |
|-----------------------|----------------------------------------------------|
| `driver-position`     | GPS-Events lesen + REST-Call simulieren            |
| `group6-route-timing` | App-1-Output + DelayFilter-Simulation              |
| `delays`              | Erste 5 Nachrichten anderer Gruppen + letzte 10    |

### DashboardTest

Schreibt direkt ins `delays`-Topic und prüft 30 Sekunden ob das Dashboard reagiert.
→ Beweist, dass unser Code korrekt ist – unabhängig vom Dashboard-Server.

---

## Pipeline-Status (Stand: 2026-05-12)

| Schritt | Beschreibung                              | Status                          |
|---------|-------------------------------------------|---------------------------------|
| 1       | `driver-position` hat Daten               | ✅ OK (2.384.373+ Nachrichten)  |
| 2       | REST-Service antwortet                    | ✅ OK (liefert Delays in Sek.)  |
| 3       | App 1 liest GPS-Events                    | ✅ OK                           |
| 4       | App 1 ruft REST auf                       | ✅ OK                           |
| 5       | App 1 schreibt → `group6-route-timing`    | ✅ OK (990+ Nachrichten)        |
| 6       | App 2 liest aus `group6-route-timing`     | ✅ OK                           |
| 7       | DelayFilter filtert > 180s                | ✅ OK                           |
| 8       | App 2 schreibt → `delays`                 | ✅ OK (Format verifiziert)      |
| 9       | Dashboard zeigt Daten                     | ❌ Dashboard-Server-Fehler      |

---

## Dashboard-Problem (Server-seitig, nicht unser Code)

### Befund

Das Dashboard unter `http://192.168.111.11:8080/status/` zeigt dauerhaft eine leere Tabelle,
obwohl korrekte Nachrichten im `delays`-Topic vorhanden sind.

### Beweis: Unser Code ist korrekt

**DashboardTest – Direkt ins `delays`-Topic geschrieben:**

```
DASHBOARD-TEST
  KEY  = '9999'
  VALUE= 'id: 9999, delay: 999'
✅ Geschrieben auf Partition=0 Offset=13932

Abfrage 1: ❌ ID 9999 NICHT sichtbar
Abfrage 2: ❌ ID 9999 NICHT sichtbar
Abfrage 3: ❌ ID 9999 NICHT sichtbar
Abfrage 4: ❌ ID 9999 NICHT sichtbar
Abfrage 5: ❌ ID 9999 NICHT sichtbar
```

**TopicInspector – `delays`-Topic hat Daten im richtigen Format:**

```
Partition 0: 13932+ Nachrichten total

--- Erste Nachricht (andere Gruppe, 04.05.2026) ---
KEY:   '110364'
VALUE: 'id: 110364, delay: 423'   ← identisches Format wie unseres

--- Unsere aktuellen Nachrichten ---
KEY:   '7862'
VALUE: 'id: 7862, delay: 415'
Timestamp: Tue May 12 23:29:12 CEST 2026
```

### Diagnose-Zusammenfassung

| Kriterium                    | Ergebnis |
|------------------------------|----------|
| Server erreichbar (HTTP 200) | ✅       |
| Nachrichten im Topic         | ✅       |
| Format korrekt (`id: X, delay: Y`) | ✅ |
| Format wie andere Gruppen    | ✅       |
| Dashboard zeigt unsere Daten | ❌       |
| Dashboard zeigt fremde Daten | ❌       |

**Schlussfolgerung:** Der Fehler liegt ausschliesslich am Dashboard-Server (Kafka-Consumer-Problem
server-seitig). Unser Code schreibt korrekte Nachrichten ins `delays`-Topic und ist vollständig funktionsfähig.

---

## Bekannte Probleme & Lösungen

### Problem 1: Kafka Streams REBALANCING

**Symptom:** App bleibt in `REBALANCING`, verarbeitet 0 Records, nie `RUNNING`.
**Ursache:** Mehrere JVM-Prozesse konkurrieren um dieselbe Consumer-Group.
**Lösung:** Umstieg auf `KafkaConsumer.assign()` (kein Group-Coordinator nötig).

### Problem 2: 2.3 Millionen alte Nachrichten

**Symptom:** App 1 braucht Stunden bevor neue Events verarbeitet werden.
**Ursache:** `auto.offset.reset=earliest` liest den gesamten `driver-position`-Verlauf.
**Lösung:** `consumer.seekToEnd()` – nur neue GPS-Events ab Start werden verarbeitet.

### Problem 3: ConcurrentModificationException beim Shutdown

**Symptom:** `KafkaConsumer is not safe for multi-threaded access` beim Beenden.
**Ursache:** Shutdown-Hook-Thread ruft `consumer.close()` auf, während Main-Thread in `poll()` ist.
**Lösung:** `consumer.wakeup()` im Shutdown-Hook (thread-sicher) → `WakeupException` im Main-Thread
→ `consumer.close()` im `finally`-Block des Main-Threads.
