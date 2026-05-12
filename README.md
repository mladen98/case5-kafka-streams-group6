# case5-kafka-streams – Group 6

Kafka Streams Projekt mit zwei Apps zur Erkennung von Lieferverzögerungen.

---

## Pipeline

```
[driver-position]
       ↓
  App 1 – RouteTimingLauncher
    → liest GPS-Position pro Fahrzeug
    → ruft REST-Service auf → berechnet Delay
    → schreibt Ergebnis
       ↓
[group6-route-timing]   (Format: id: 123, delay: 321)
       ↓
  App 2 – DelaydetectorLauncher
    → filtert nur Delays > 180 Sekunden (DelayFilter)
    → loggt signifikante Verzögerungen (MyProcessor)
    → schreibt ins Dashboard-Topic
       ↓
[delays]   (Format: id: 123, delay: 321)
       ↓
http://192.168.111.11:8080/status/
```

---

## Infrastruktur

| Komponente   | Adresse                              |
|--------------|--------------------------------------|
| Kafka Broker | `192.168.111.10:9092`                |
| REST-Service | `http://192.168.111.11:8080`         |
| Dashboard    | `http://192.168.111.11:8080/status/` |

---

## Bauen

Da Maven und Java nicht im globalen PATH sind, müssen sie einmalig gesetzt werden:

```powershell
$env:JAVA_HOME = "$env:USERPROFILE\.jdks\temurin-17.0.18"
$env:Path = "$env:JAVA_HOME\bin;C:\Program Files\JetBrains\IntelliJ IDEA 2025.3\plugins\maven\lib\maven3\bin;$env:Path"
cd "C:\Users\kapis\IdeaProjects\case5-kafka-streams-group6"
mvn clean package
```

JARs in `target/`:
- `target/route-timing.jar`
- `target/delay-detector.jar`

---

## Starten

**Erst App 1, dann App 2 starten.**

### App 1 – RouteTimingLauncher

```powershell
$env:JAVA_HOME = "$env:USERPROFILE\.jdks\temurin-17.0.18"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -jar "C:\Users\kapis\IdeaProjects\case5-kafka-streams-group6\target\route-timing.jar"
```

### App 2 – DelaydetectorLauncher

```powershell
$env:JAVA_HOME = "$env:USERPROFILE\.jdks\temurin-17.0.18"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -jar "C:\Users\kapis\IdeaProjects\case5-kafka-streams-group6\target\delay-detector.jar"
```

---

## Implementierung

### App 1 – RouteTimingLauncher

Liest GPS-Events aus `driver-position` mit `auto.offset.reset=latest` (nur neue Nachrichten).
Für jedes Event wird der `RouteTimingMapper` aufgerufen:

1. GPS-Koordinaten und Delivery-ID aus dem Event extrahieren
2. REST-Aufruf: `GET /route/{id}?time=...&lat=...&lon=...` → liefert Delay in Sekunden
3. Erzeugt neues Event im Format `id: 123, delay: 321`
4. Events ohne gültiges Ergebnis werden herausgefiltert (`value != null`)

Die gefilterten Events werden nach `group6-route-timing` geschrieben.

### App 2 – DelaydetectorLauncher

Liest Events aus `group6-route-timing` und verarbeitet sie mit Kafka Streams DSL:

1. **DelayFilter**: verwirft Events mit Delay ≤ 180 Sekunden
2. **MyProcessor** (`peek`): loggt alle signifikanten Verzögerungen auf der Konsole
3. Schreibt verbleibende Events nach `delays`

### Nachrichtenformat

Alle Events zwischen den Topics und im Dashboard-Topic verwenden das Format:

```
id: 123, delay: 321
```

- `id` – Delivery ID des Fahrzeugs
- `delay` – Verspätung in Sekunden

---

## Stoppen

`Ctrl+C` in beiden Terminal-Fenstern.
