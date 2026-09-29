# Shelly compact — production API read cutover, 2026-09-16

**PASS: το ενεργό API διαβάζει από `shelly_compact.readings`.**
Writer παραμένει `dual`. Ο παλιός πίνακας εξακολουθεί να ενημερώνεται και να
διατηρείται. Δεν έγινε PostgreSQL ή ingestor restart. Το API αντικαταστάθηκε μόνο
του, χωρίς αλλαγή σε frontend κώδικα ή URL συμβόλαια.

## Αμετάβλητη έκδοση και ρυθμίσεις

- Application source: `586ce99272372fdc4b6107a561017d5c40ead562` — το ήδη δοκιμασμένο image του πρώτου σταδίου.
- Final cutover runner: `daf8d8af5cb7f7682cd688884dc6b1851ec3fa5a`.
- Release: `/opt/upat-shelly-compact-stage2-20260916-r4`.
- API image: `sha256:81ba07c4a526cdd8dbc3b3c0c79457633efbc91cd869be9a28af4befb9796429`.
- Active API container: `bbc1dbcb5c856d7e3014dc5d8c24d33ce8a8e72afabbf0f26abe34deb18ff1a0`.
- API started UTC: `2026-09-16T18:12:55.025863702Z`.
- Canonical Compose: `/opt/upat-nzc-mqtt-db/docker-compose.prod.yml`, ακριβές
  committed αντίγραφο του `ops/shelly-compact/compose.stage2.prod.yml`.
- Resolved Compose διαφορά: μόνο API image και
  `SHELLY_MEASUREMENTS_READ_STORAGE=compact`, `SHELLY_MEASUREMENTS_ROUNDING=decimal_1`.
- Ίδιο `.env`, εξωτερικό δίκτυο και PostgreSQL ownership από systemd.
- Καμία αλλαγή production dependencies. Τα shadow previews είχαν πρόσθετα όρια
  πόρων/read-only σύνδεση που δεν μεταφέρθηκαν στο ενεργό API.

## Gates πριν από την ενεργοποίηση

Χρησιμοποιήθηκε το πρόσφατο επαληθευμένο backup του πρώτου σταδίου:
`backups/shelly-compact-production-stage1-20260916/backup-before` στον Mac και
`/Volumes/Chris/VPS-Backups/shelly-stage1-before-20260916` στον εξωτερικό δίσκο.
Πλήρες τοπικό restore είχε περάσει. Δεν δημιουργήθηκε δεύτερο νέο database dump
σε αυτό το στάδιο ανάγνωσης.

Η πλήρης ισότητα του πρώτου σταδίου κάλυπτε 64,315,782 μετρήσεις, με ίδιο binary
SHA-256. Νέος έλεγχος των μεταγενέστερων IDs, μετά το 86939815, βρήκε **0 διαφορές**
στα έξι αρχικά πεδία. Ελέγχθηκαν επίσης εγκυρότητα indexes, φρέσκες διπλές εγγραφές,
χώρος, ακριβείς ταυτότητες containers και χρόνος εκκίνησης PostgreSQL.

Πέρασαν 9 tests του νέου runner, 7 του προηγούμενου και 103 σχετικά API tests.
14 API tests χρειάζονταν ανεξάρτητα database fixtures και παραλείφθηκαν στη
συγκεκριμένη unit-test εκτέλεση. Οι πραγματικές API συγκρίσεις έγιναν επιπλέον
με το ακριβές candidate image και read-only συνδέσεις στο VPS.

## Read-only συγκρίσεις API

Δύο προσωρινά APIs έτρεξαν στο loopback, με 0.75 CPU / 384 MiB το καθένα,
read-only root, προσωρινή cache στη RAM, και επιβεβαιωμένο
`default_transaction_read_only=on`, statement timeout 60 sec, lock timeout 2 sec.
Και τα δύο χρησιμοποίησαν `decimal_1` ώστε η σύγκριση να ελέγξει τη διάταξη δεδομένων.
Τα προσωρινά containers αφαιρέθηκαν μετά τις δοκιμές.

**10/10 ίδιες HTTP απαντήσεις ιστορικού**, συμπεριλαμβανομένων φίλτρων, πολλών
μετρικών, λεπτών/ωρών/ημερών, timezone offset και κενών αποτελεσμάτων:

| Δοκιμή | Buckets | Legacy sec | Compact sec |
|---|---:|---:|---:|
| power_15m | 15 | 0.0325 | 0.0558 |
| all_1h | 60 | 13.1729 | 0.1230 |
| power_24h | 25 | 0.1552 | 0.0733 |
| multiple_24h | 95 | 0.1265 | 0.0816 |
| power_7d | 169 | 2.6797 | 0.1611 |
| calendar_days | 4 | 0.3172 | 0.1025 |
| athens_offset | 12 | 0.0376 | 0.0290 |
| dst_boundary | 0 | 0.0384 | 0.0298 |
| empty_device | 0 | 0.0468 | 0.0391 |
| empty_metric | 0 | 0.0264 | 0.0364 |

Οι χρόνοι είναι ενδεικτικές διαδοχικές μετρήσεις, όχι ελεγχόμενο benchmark.
Το DST παράθυρο επέστρεψε 0 buckets και επομένως η παραγωγική δοκιμή του δεν
αποδεικνύει μετατροπή μη κενών δεδομένων· οι κανόνες DST ελέγχθηκαν τοπικά στα tests.

Το παλιό unbounded `latest` ξεπέρασε το όριο των 60 sec στο περιορισμένο preview.
Η PostgreSQL κατέγραψε statement timeout στις 18:07:56 UTC· αυτό αφορούσε το
προσωρινό API και δεν ενεργοποίησε αλλαγή στο live API.
Το compact `latest` επαληθεύτηκε έναντι των αντίστοιχων ολοκληρωμένων λεπτών του
bounded legacy history: **2 ίδια buckets**, compact χρόνος
**4.0897 sec**, reference **0.0411 sec**.
Δεν ισχυριζόμαστε πλήρη σύγκριση των δύο unbounded latest endpoints ή βελτιστοποίηση
της υπάρχουσας λογικής latest για όλες τις συσκευές/μετρικές.

## Συντήρηση του νέου πίνακα πριν από το cutover

Το read-only EXPLAIN έδειξε χρήση του covering index, αλλά μόνο 330310/409729
σελίδες είχαν all-visible metadata μετά την αντιγραφή. Για να ολοκληρωθεί αυτή
η συνήθης συντήρηση εκτελέστηκε μόνο στον νέο πίνακα:

```sql
VACUUM (FULL FALSE, INDEX_CLEANUP OFF, TRUNCATE FALSE, PARALLEL 0, VERBOSE TRUE)
  shelly_compact.measurements;
```

Session όρια: lock timeout 1 sec, statement timeout 5 min, vacuum cost delay 5 ms,
cost limit 200. Διάρκεια εργαλείου **55.14 sec**. Τελικό visibility
**409792/409793 σελίδες**. Η αναφορά PostgreSQL καταγράφει **0 tuples removed,
0 pages removed, 0 index scans**, και περίπου 4.74 MB WAL.
Δεν έγινε αλλαγή στο παλιό table, rebuild index ή αλλαγή μόνιμων DB settings.

Η διαδικασία ακολουθεί τη δυνατότητα concurrent reads/writes του
[κανονικού VACUUM](https://www.postgresql.org/docs/15/sql-vacuum.html) και τη χρήση
visibility metadata για [index-only scans](https://www.postgresql.org/docs/15/indexes-index-only-scans.html).
Η πρώτη αργή επταήμερη μέτρηση compact (~17.93 sec) βελτιώθηκε μετά τη συντήρηση,
αλλά οι αριθμοί περιλαμβάνουν και επιδράσεις cache/φόρτου.

## Ενεργοποίηση και τελική επαλήθευση

- Targeted `docker compose up -d --no-deps --no-build --pull never api`.
- Ετοιμότητα API μετά από **23.38 sec**. Πρόκειται για τον χρόνο
  του εργαλείου μέχρι επιτυχή health check, όχι ακριβή μέτρηση όλων των αιτημάτων
  που πιθανόν επηρεάστηκαν στο σύντομο παράθυρο αλλαγής.
- Και οι 10 απαντήσεις ιστορικού του live API συμφώνησαν με τις επαληθευμένες
  preview απαντήσεις. Εύρος χρόνων περίπου 0.04–0.15 sec.
- Δημόσιο `https://telemetry.schoolheroz.com`: authenticated health PASS,
  σωστό service token δεκτό, απόν/λανθασμένο token απορρίφθηκε με 401.
- SchoolHeroZ CORS preflight PASS. Unauthenticated school route επέστρεψε 401.
  Δεν έγινε login ως πραγματικός χρήστης ή πλήρης browser/UI δοκιμή σχολείου.
- Active latest: `2026-09-16T21:13:00+03:00`, **4.0649 sec**.
- Hourly energy endpoint: **4 buckets**, **0.058 sec**.
- Runtime reader επέστρεψε `shelly_compact.readings` και
  `ROUND(AVG(value::numeric), 1)::double precision`.
- 0 error/exception/traceback/fatal matches στα νέα API logs κατά τον έλεγχο.
- PostgreSQL, Shelly ingestor, TTN ingestor, Caddy και Mosquitto διατήρησαν
  IDs/images/start/restart counts. PostgreSQL postmaster παραμένει
  `2026-09-16T17:37:17.821965+03:00`, restart count **0**.
- Ίδια νέα IDs στα δύο σχήματα: **86952391** στις `2026-09-16T21:13:46.786576+03:00`.
- Ελεύθερο Volume: **47.98 GiB**.

Η στρογγυλοποίηση αφορά τους API μέσους όρους. Οι αρχικές float8 μετρήσεις δεν
τροποποιήθηκαν. Και οι δύο πίνακες παραμένουν στην ίδια PostgreSQL στο Volume.

## Επιστροφή και επόμενα στάδια

Το rollback αρχείο βρίσκεται στο release `receipts/compose.before.yml` και το
παλιό API image `sha256:d27069f42d0f4cb647d8011a19dfbf81a7dcb71a140b493f033ed847eb788856`
παραμένει διαθέσιμο. Ο runner είχε αυτόματη επαναφορά αν αποτύγχανε οποιοδήποτε
activation check· **δεν χρειάστηκε rollback του production API**.
Η συντήρηση visibility δεν χρειάζεται αναίρεση.

Στην τρέχουσα φάση API-only rollback είναι επαρκές επειδή ο writer παραμένει dual.
Compact-only writes και διαγραφή του παλιού πίνακα **δεν εκτελέστηκαν**.
Πριν από μελλοντικό retirement απαιτείται χωριστό checkpoint, έλεγχος consumers,
μεταφορά ownership της κοινής sequence και backup.

Τρέχων read-only έλεγχος:
`python3 /opt/upat-shelly-compact-stage2-20260916-r4/ops/shelly-compact/stage2.py status`.
Ο παλιός stage1 guard έχει σκόπιμα αποθηκευμένο το παλιό API και δεν αποτελεί πλέον
τον κατάλληλο έλεγχο συνολικής ταυτότητας μετά το cutover.

Αποδεικτικά: `/Users/mountzou/upat-nzc-mqtt-db/backups/shelly-compact-production-stage2-20260916`.
Τα προηγούμενα immutable releases διατηρούνται για ανιχνευσιμότητα. Οι δύο πρώτες
διορθώσεις αφορούσαν μόνο το προσωρινό preview: Numba writable cache και την
υπάρχουσα παράμετρο `interval=day`. Καμία αποτυχημένη preview προσπάθεια δεν άλλαξε
το ενεργό API. Το τελικό release και οι πραγματικές ρυθμίσεις είναι versioned.

Τελική παρατήρηση στις `2026-09-16T21:17:34.242628+03:00`: ίδιοι ενεργοί containers, 0 restarts,
ίδιο νέο ID **86953708** και στα δύο σχήματα, φρέσκια μέτρηση
ηλικίας 1.956017 sec. Όλα τα προσωρινά previews έχουν αφαιρεθεί.
