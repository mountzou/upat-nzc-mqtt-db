# Shelly compact — production stage 1, 2026-09-16

**PASS: ολοκληρώθηκε το εγκεκριμένο πρώτο στάδιο.** Το API παραμένει σε legacy reads,
ο Shelly ingestor λειτουργεί σε atomic dual writes, και ο παλιός πίνακας διατηρείται.
Η PostgreSQL δεν επανεκκινήθηκε. Δεν ολοκληρώθηκε ακόμη το API cutover ή η κατάργηση
του παλιού σχήματος. Το αρχείο VALIDATION-20260916.md καταγράφει την προηγούμενη,
τοπική φάση· οι αναφορές του σε ανενεργό production schema ήταν αληθείς τότε.

## Ανιχνευσιμότητα

- Branch: `codex/shelly-compact-migration-20260916`.
- Application source commit: `586ce99272372fdc4b6107a561017d5c40ead562`.
- Production runner/Compose commit: `4efea7e2c4290bfdf241ab0ddf52d92b8067d8f0`.
- Active ingestor image: `sha256:f92a082e0fbbb5cbbd4f3ab922b589df1ca22b403d4f291bbd8f30ace3308d7b`.
- Ingestor container: `16b01f0f241b67bc354ded03d970188064230d73678ad9dea462e099e6f57325`.
- Production release: `/opt/upat-shelly-compact-stage1-20260916`.
- Canonical Compose: `/opt/upat-nzc-mqtt-db/docker-compose.prod.yml`, ακριβές αντίγραφο
  του committed `ops/shelly-compact/compose.stage1.prod.yml`. Μόνο image του Shelly
  ingestor και `SHELLY_MEASUREMENTS_WRITE_MODE=dual` αλλάζουν στο resolved model.
- `.env` και τα υπόλοιπα containers αμετάβλητα. Δεν αντικαταστάθηκε το ήδη dirty
  production source tree. Το release manifest επαλήθευσε 22 αρχεία.

## Backup πριν από την αλλαγή

- Νέο `pg_dump` από τη ζωντανή βάση πριν την ενεργοποίηση, χωρίς παύση PostgreSQL.
- Dump: 1,115,298,832 bytes, SHA-256
  `9a20338a0ee94e619585404d5b4da458be1c765fa47131eca8c1acff800a65e7`.
- Πλήρης τοπική επαναφορά με το ίδιο PostgreSQL 15.17 amd64 image: 29 tables,
  72,164,391 rows, 18 sequence checks, catalog/index checks και persistence μετά
  από restart **μόνο του τοπικού restore container**: PASS.
- Αντίγραφο στον Chris: 41 αρχεία, πλήρες readback με SHA-256: PASS.
- Mac: `/Users/mountzou/upat-nzc-mqtt-db/backups/shelly-compact-production-stage1-20260916/backup-before`.
- Chris: `/Volumes/Chris/VPS-Backups/shelly-stage1-before-20260916`.
- Περιλαμβάνονται οι ήδη εγκεκριμένες ιδιωτικές ρυθμίσεις επαναφοράς.

## Ενεργοποίηση και αντιγραφή

- Νέο dictionary `(device_id, metric, unit)` και μετρήσεις
  `(id, series_id, value, event_time)`, με view που ανασυνθέτει τα έξι αρχικά πεδία.
- **120 σειρές** στο dictionary. Ο μικρός `shelly_devices` δεν αλλάζει.
- Σύντομη ελεγχόμενη αντικατάσταση μόνο του Shelly ingestor στις 19:55 ώρα Ελλάδας.
  Stop request έως νέο container: **31.79 sec**. Αυτό είναι το χρονικό παράθυρο
  αντικατάστασης, όχι μέτρηση του ακριβούς κενού ή του αριθμού MQTT μηνυμάτων.
- Μετά την αποστράγγιση writer καταγράφηκε checkpoint ID **86926633**.
- Αντιγράφηκαν **64,302,600 ιστορικές εγγραφές**, σε περιορισμένες συναλλαγές.
  Cursor έφθασε στο checkpoint. Το dual write συνέχισε ταυτόχρονα τη ζωντανή ροή.
- Covering index `(series_id, event_time DESC) INCLUDE(value)` κατασκευάστηκε
  CONCURRENTLY. Όλα τα νέα indexes/constraints είναι έγκυρα.
- Κατά την αντιγραφή, έλεγχος πριν από κάθε batch για ελεύθερο χώρο ≥20 GiB και
  retained WAL ≤4 GiB. Το τελευταίο retained WAL ήταν 1 GiB.

## Πλήρης ισότητα

Σε κοινό READ ONLY REPEATABLE READ snapshot, ordered binary COPY όλων των πεδίων
`id, device_id, metric, value, unit, event_time` και ανεξάρτητα count/min/max:

- Εγγραφές σε κάθε σχήμα: **64,315,782**.
- Εύρος ID: **10938671–86939815**.
- Bytes ανά δυαδική ροή: **5,467,133,303**.
- Κοινό SHA-256: `135e3e7ec289b69103d15e920c0d0a05f26b88e732eb9907e57a5a72e2976f80`.
- Διάρκεια παραγωγικού ελέγχου: **286.42 sec**, αποτέλεσμα **PASS**.

Το πλήθος είναι μεγαλύτερο από την ιστορική αντιγραφή επειδή περιλαμβάνει και
τις νέες μετρήσεις μέχρι το snapshot. Η αρχική ακρίβεια float8, οι μονάδες, τα NULLs
και οι χρόνοι διατηρούνται. Η νέα πολιτική ενός δεκαδικού στους μέσους όρους
**δεν ενεργοποιήθηκε** στο production API σε αυτό το στάδιο.

## Τελική λειτουργία και χώρος

Έλεγχος στις **2026-09-16T20:42:31.344913+03:00**:

- PostgreSQL container/image/start και postmaster start αμετάβλητα, restart count 0.
  Postmaster: `2026-09-16T17:37:17.821965+03:00`.
- API, TTN ingestor, Caddy και Mosquitto με αμετάβλητα IDs/images/start/restarts.
- Νέος Shelly ingestor ενεργός με restart count 0.
- Ίδιο τελευταίο ID και στα δύο σχήματα: **86941732**.
- Φρέσκα UPAT δεδομένα: `2026-09-16T20:42:27.397315+03:00`.
- Φρέσκα energy counters: `2026-09-16T20:42:01.369277+03:00`.
- Υπάρχον endpoint ιστορικού Shelly: HTTP 200, 16 buckets, **0.0967 sec**.
- Από την ενεργοποίηση, 0 γραμμές logs που ταίριαξαν σε error/exception/fatal/traceback.
  Αυτό συμπληρώνει τον έλεγχο πραγματικών δεδομένων· δεν αποτελεί μόνο του απόδειξη.
- Παλιές σχέσεις/ορισμοί indexes/constraints, extensions και durability settings
  ελέγχθηκαν αμετάβλητα. Host Volume preflight και Compose ownership check PASS.

| Μέγεθος στο VPS | GiB |
|---|---:|
| Παλιός πίνακας, indexes και TOAST | 13.534 |
| Νέο schema, πίνακες και όλα τα indexes | 7.982 |
| Διαφορά των δύο διατάξεων | 5.552 |
| Ελεύθερο Volume | 47.938 |
| Ελεύθερος root δίσκος | 25.466 |

Το νέο σχήμα χρησιμοποιεί **41.02% λιγότερο χώρο**. Η διαφορά είναι
μικρότερη από το 47.81% της στατικής τοπικής πρόβας: η εισαγωγή παλιών IDs κάτω από
το ήδη ενεργό νεότερο άκρο δημιουργεί αραιότερο primary-key B-tree. Τοπικό ανεξάρτητο
fixture αναπαρήγαγε τη συμπεριφορά: 1M ascending rows, PK 22,487,040 bytes· με seed
10k μεγαλύτερων IDs πριν την αντιγραφή, PK 40,738,816 bytes. Δεν έγινε production
REINDEX. Το production PK καταλαμβάνει 2,604,417,024 bytes και το covering index
2,608,766,976 bytes. Η εξήγηση της διαφοράς είναι συμπέρασμα από το fixture και
τη σειρά εισαγωγής, όχι μέτρηση εσωτερικής πυκνότητας κάθε production σελίδας.

**Δεν ανακτήθηκε ακόμη χώρος από τον παλιό πίνακα**, καθώς παραμένουν και οι δύο
διατάξεις στην ίδια PostgreSQL, στο ίδιο Hetzner Volume. Το dual write είναι
προσωρινή φάση μετάβασης και δεν αποτελεί ανεξάρτητο backup.

## Επόμενο ξεχωριστό στάδιο

Αλλαγή API σε compact reads, με σύγκριση πραγματικών απαντήσεων και διατήρηση dual
writes για άμεση επιστροφή. Έπειτα, χωριστά, compact-only writes και τελική απόσυρση
παλιού πίνακα. Αυτά **δεν εκτελέστηκαν** σε αυτό το στάδιο.

Η κοινή ID sequence εξακολουθεί να ανήκει στον παλιό πίνακα. Πριν από μελλοντική
διαγραφή απαιτείται ρητή μεταφορά ownership, νέος dependency έλεγχος και backup.
Για επιστροφή στο τρέχον dual στάδιο αρκεί η επαναφορά του προηγούμενου ingestor
image/Compose· ο παλιός πίνακας συνεχίζει να ενημερώνεται. Το προηγούμενο image
`sha256:540f7d5f5d4e277593a3fea2e9bccd6dc96a3817483fd2fa5d2d63ce39f984dd`
και `receipts/compose.before.yml` διατηρούνται.

## Αποδεικτικά

Ιδιωτικός φάκελος Mac: `/Users/mountzou/upat-nzc-mqtt-db/backups/shelly-compact-production-stage1-20260916`.

- `BACKUP-GATE.json`, `backup-before/RESTORE-VERIFIED.json`, `backup-before/COPY-VERIFIED.json`.
- `ARTIFACT-MANIFEST.json`, `final-receipts/activated.json`, `final-receipts/index.json`.
- `final-receipts/verify.json`, `BACKFILL-COMPLETE.json`, 33 copy-window receipts.
- `production-data-after.json`, `production-runtime-after.json`, `production-catalog-after.json`.
- `production-compose-after.json`, `configuration-hashes-after.json`, `df-after.txt`.
- `live-monitor.jsonl`, `final-verification-monitor.jsonl`, `api-and-ingestor-final.json`.
- `local-index-pattern.json` για το ανεξάρτητο τοπικό fixture.

Η προηγούμενη τοπική πρόβα/δοκιμές τεκμηριώνεται στο `VALIDATION-20260916.md`.
Τα επτά πρόσθετα tests του production runner κάλυψαν τη μοναδική επιτρεπόμενη
Compose διαφορά, απόρριψη αλλαγής PG lifecycle και rollback μόνο του ingestor.
