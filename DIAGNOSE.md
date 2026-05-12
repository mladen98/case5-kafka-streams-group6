# Stream-Diagnose – Wo bricht der Flow ab?

## Gesamter Datenfluss

```
[driver-position]
      |
      |  (1) App 1 liest
      v
[RouteTimingLauncher]  →  REST-Aufruf  →  berechnet delay
      |
      |  (2) App 1 schreibt
      v
[group2-route-timing]
      |
      |  (3) App 2 liest
      v
[DelaydetectorLauncher]  →  filtert delay > 180s
      |
      |  (4) App 2 schreibt
      v
[delays]
      |
      |  (5) Dashboard liest
      v
[http://192.168.111.11:8080/status/]
```

---

## Status jeder Stufe

| Stufe | Status | Beweis |
|-------|--------|--------|
| `driver-position` hat Daten | ✅ OK | TopicInspector: 2.375.876 Nachrichten |
| REST-Service antwortet | ✅ OK | TopicInspector: Returns z.B. 298s, 441s |
| App 1 läuft | ✅ OK | Startet, subscribes auf `driver-position` |
| App 1 schreibt nach `group2-route-timing` | ⚠️ UNKLAR | Nie bestätigt – kein Stage 1 Output gesehen |
| `group2-route-timing` hat Daten | ✅ OK | TopicInspector: 309 Nachrichten (aber IDs 1xxx, nicht 7xxx!) |
| App 2 erreicht RUNNING-State | ❌ PROBLEM | Bleibt in REBALANCING hängen |
| App 2 verarbeitet Nachrichten | ❌ PROBLEM | Kein einziges „Stage 2"-Log |
| `delays`-Topic hat unsere Daten | ❌ PROBLEM | Nie überprüft |
| Dashboard zeigt etwas | ❌ PROBLEM | Leer, aber flackert (andere Gruppen schreiben evtl.) |

---

## Identifiziertes Hauptproblem

**App 2 (`DelaydetectorLauncher`) bleibt im `REBALANCING`-State hängen.**

Das bedeutet: Sie liest KEINE Nachrichten aus `group2-route-timing`, schreibt
NICHTS nach `delays`, und das Dashboard zeigt deshalb nichts.

### Warum hängt App 2 im REBALANCING?

Wahrscheinlichste Ursachen:

1. **Alter State im Temp-Verzeichnis**
   - Pfad: `C:\Users\kapis\AppData\Local\Temp\kafka-streams`
   - Der Ordner `delay-detector-group2` enthält alten, inkonsistenten Zustand
   - → Lösung: Ordner löschen, App 2 neu starten

2. **App 1 und App 2 konkurrieren**
   - Beide laufen gleichzeitig und blockieren sich beim Topic-Rebalancing
   - → Lösung: App 1 stoppen, App 2 alleine starten (testen)

3. **`group2-route-timing` existiert bereits mit einer anderen Konfiguration**
   - Das Topic hatte 309 Nachrichten mit IDs 1xxx (von einer anderen Gruppe)
   - Unsere App 1 schreibt IDs 7xxx
   - Möglicher Partitions-Konflikt
   - → Lösung: Neues Topic-Name verwenden

---

## Sofortmassnahmen (in dieser Reihenfolge)

### Schritt 1: State-Directory löschen
In Windows Explorer oder PowerShell:
```
C:\Users\kapis\AppData\Local\Temp\kafka-streams\
```
→ Den Ordner `delay-detector-group2` löschen (oder den ganzen `kafka-streams`-Ordner)

### Schritt 2: Beide Apps stoppen

### Schritt 3: Nur App 2 starten (ohne App 1!)
Rechtsklick auf `DelaydetectorLauncher.java` → Run

Warten auf:
```
STREAM STATE: REBALANCING -> RUNNING
```

Falls das erscheint → App 1 auch starten → Dashboard laden.

### Schritt 4: Falls App 2 immer noch hängt
neues Application-ID in `DelaydetectorLauncher`:
```java
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "delay-detector-group2-v2");
```
→ Rebuild → App 2 neu starten

---

## Nebenbeobachtung: Dashboard flackert

Das Dashboard flackert, weil andere Gruppen ebenfalls in das `delays`-Topic
schreiben. Das Dashboard ist ein gemeinsames Dashboard für alle Gruppen.
Unsere Daten fehlen, weil App 2 nie etwas schreibt.

---

## Nächster Test nach dem Fix

1. App 2 Konsole zeigt:
   ```
   STREAM STATE: REBALANCING -> RUNNING
   Stage 2 - significant delay detected:
   id: 1863, delay: 441
   MyProcessor got Event
   KEY   = 1863
   VALUE = id: 1863, delay: 441
   ```

2. Dashboard unter `http://192.168.111.11:8080/status/` zeigt Einträge.

