# Shelly compact migration — τοπική υλοποίηση και πλήρης πρόβα

2026-09-16. **Προετοιμασία ολοκληρωμένη· παραγωγική ενεργοποίηση δεν έγινε.**
Ο production πίνακας παραμένει ο `public.shelly_measurements` και δεν υπάρχει ακόμη
το schema `shelly_compact` στο VPS. Δεν έγινε restart PostgreSQL ή εφαρμογής στο VPS.

## Υλοποίηση

- Ξεχωριστό branch `codex/shelly-compact-migration-20260916`.
- Κώδικας των δοκιμασμένων images: `586ce99272372fdc4b6107a561017d5c40ead562`.
- Διορθωμένο εργαλείο HTTP smoke test: `dc118b7eba930ed1aee464846d3c01c043e1e632`.
- Worktree: `/private/tmp/upat-shelly-compact-migration-20260916`.
- Νέο dictionary ανά `(device_id,metric,unit)` και συμπαγείς μετρήσεις με
  `(id,series_id,value,event_time)`. Ο μικρός πίνακας `shelly_devices` δεν αλλάζει.
- Αρχικές τιμές float8, IDs, NULLs, μονάδες και timestamps διατηρούνται. Το API
  υπολογίζει μόνο τους μέσους όρους με `ROUND(AVG(value::numeric),1)`.
- Επιλογές writer `legacy/dual/compact`, reader `legacy/compact` και ρητή επιλογή
  δεκαδικής στρογγυλοποίησης. Defaults παραμένουν `legacy`.
- Atomic dual writes στην ίδια συναλλαγή με raw MQTT, συσκευές και counters.
- Αντιγραφή σε μικρές επαναλήψιμες συναλλαγές, σταθερό checkpoint μετά από παύση
  και αποστράγγιση writers, έλεγχος συγκρουόμενων IDs και επαληθευμένο reverse copy.
- Το covering index κατασκευάζεται CONCURRENTLY μετά την ιστορική αντιγραφή.
- Το migration σταματά πριν από νέα παρτίδα αν ο ελεύθερος χώρος πέσει κάτω από
  20 GiB ή αν το retained WAL ξεπεράσει τα 4 GiB. Η δημιουργία index απαιτεί έλεγχο
  χώρου πριν και εξωτερική παρακολούθηση κατά τη διάρκειά της.

## Πλήρης επαλήθευση δεδομένων

Χρησιμοποιήθηκε ξεχωριστό τοπικό clone του επαληθευμένου post-Volume backup
`pg-volume-live-backup-20260916-1810`, με το **ίδιο PostgreSQL 15.17 amd64 image**
της παραγωγής. Το αρχικό backup/restore volume παρέμεινε αμετάβλητο.

- Εγγραφές σε κάθε σχήμα: **64,266,981**.
- Εύρος IDs: 10938671–86891014.
- Συγκρίθηκαν και τα έξι αρχικά πεδία, για όλες τις εγγραφές, σε μία κοινή
  READ ONLY REPEATABLE READ snapshot με ordered binary COPY.
- Bytes σε κάθε δυαδική ροή: **5,462,983,998**.
- SHA-256 και των δύο ροών: `cd2ddb60a2a2871c88fc2c63b5f6b2b1a705df2798a629ee22aadc1c9f498c96`.
- Αποτέλεσμα: **PASS**, σε 91.84 sec τοπικά.
- Όλα τα απαιτούμενα indexes/constraints ήταν έγκυρα.

Η πλήρης πρόβα αφορά σταθερό snapshot. Ταυτόχρονες εγγραφές, μερική αποτυχία,
διακοπή/συνέχιση, out-of-order commits και reverse recovery ελέγχθηκαν επιπλέον
σε ξεχωριστό πραγματικό PostgreSQL fixture. Δεν ισχυριζόμαστε πλήρη ιστορική πρόβα
με live production traffic ή αποτυχία λειτουργικού/δίσκου.

## Χώρος

| Αντικείμενο στο τοπικό snapshot | GiB |
|---|---:|
| Αρχικός πίνακας μαζί με indexes/TOAST | 13.214 |
| Νέο schema μαζί με measurements, series, indexes και progress | 6.896 |
| Διαφορά | 6.318 |

Μείωση **47.81%**. Δεν έχει απελευθερωθεί αυτός ο χώρος στο VPS:
ο παλιός πίνακας πρέπει αρχικά να παραμείνει για rollback και να διαγραφεί μόνο
με ξεχωριστή έγκριση. Η κύρια διαφορά είναι η αποφυγή επαναλαμβανόμενων μεγάλων
κειμένων, όχι η μείωση ακρίβειας των αποθηκευμένων μετρήσεων. Τα νέα covering indexes
εξακολουθούν δικαιολογημένα να αποθηκεύουν και τιμές για γρήγορη ανάγνωση.

Η δοκιμή δημιούργησε συνολικά περίπου 16.61 GiB WAL,
ενώ το καταγεγραμμένο retained WAL ήταν 1.00 GiB.
Το συνολικό WAL περιλαμβάνει και το αρχικό πείραμα incremental index, το οποίο
αντικαταστάθηκε τοπικά από το τελικό concurrent build. Δεν είναι μέτρηση καθαρής
τελικής διαδικασίας ούτε peak χρήσης δίσκου. Τα 12 GiB temp_bytes της βάσης ήταν
αθροιστική καταγραφή προσωρινών εγγραφών, **όχι** peak ταυτόχρονης κατανάλωσης.

Το VPS στο τελικό read-only audit είχε περίπου 56.85 GiB διαθέσιμα στο Volume και
25.47 GiB στον root δίσκο. Υπάρχει περιθώριο για το δεύτερο σχήμα, WAL και προσωρινά
αρχεία· χρειάζεται νέα μέτρηση αμέσως πριν την παραγωγική εργασία και παρακολούθηση
της πραγματικής κατανάλωσης, όχι υπόθεση ότι οι τοπικοί χρόνοι/peaks μεταφέρονται αυτούσιοι.

## API και απόδοση

**8/8 ίδια αποτελέσματα** με την ίδια νέα δεκαδική πολιτική και στα δύο σχήματα.
Μετρήθηκαν οι πραγματικές Python συναρτήσεις ιστορικού/latest. Παρακάτω wall-clock
ms ανά κλήση, με σύνδεση DB, query και μετασχηματισμό αποτελέσματος. Τρεις εκτελέσεις
ανά variant, η πρώτη ως warm-up, διάμεσος των άλλων δύο. Τοπικό Docker, amd64 μέσω
emulation σε Mac ARM, 2 CPU και 3 GiB RAM· οι απόλυτοι χρόνοι δεν προβλέπουν το VPS.

| Query | Legacy ms | Compact ms | Ίδια απάντηση |
|---|---:|---:|---|
| status_plug_15m | 50.7 | 55.8 | PASS |
| status_pro3em_15m | 53.3 | 55.9 | PASS |
| history_pro3em_power_24h | 102.0 | 105.5 | PASS |
| history_pro3em_all_24h | 1591.4 | 310.4 | PASS |
| history_plug_all_24h | 181.1 | 93.5 | PASS |
| history_pro3em_power_7d | 1141.7 | 717.9 | PASS |
| history_pro3em_power_30d | 2147.4 | 806.8 | PASS |
| latest_pro3em_all_unbounded | 25216.9 | 21990.7 | PASS |

Τα σύντομα queries έχουν κυρίως κόστος σύνδεσης και μικρές διαφορές. Το ιστορικό
πολλών μετρικών και μεγαλύτερων διαστημάτων ωφελείται. Το unbounded latest εξακολουθεί
να σαρώνει μεγάλο ιστορικό και παραμένει αργό. Δεν παρουσιάζεται ως λυμένο.

## Πρόσθετοι έλεγχοι

- **16 πραγματικά PostgreSQL integration tests**: πλήρη πεδία/NULL/NaN/Inf/signed zero,
  ταυτόχρονη δημιουργία σειράς, atomicity, αποτυχία χωρίς μισές εγγραφές,
  checkpoint/continuation, συγκρούσεις, reverse recovery, CLI και capacity guard.
- Μαζί με Compose guards: **22 passed**, 5 subtests.
- Υπάρχοντα API/ingestion regression tests: **392 passed**, 12 subtests,
  **137 skipped** επειδή δεν δόθηκαν τα ανεξάρτητα fixtures τους.
- Τα πραγματικά candidate images έγραψαν fixture MQTT μέσω του κανονικού callback
  και εξυπηρέτησαν HTTP latest/history, κενή συσκευή και decimal tie 286.45→286.5.
  Legacy και compact JSON ήταν ίδια. Το υπάρχον `/internal/data/health` δέχθηκε
  σωστό token και απέρριψε έλλειψη token. Δεν προστέθηκε νέο authentication endpoint.
- Images χτίστηκαν από git archive και τα ακριβή υπάρχοντα runtime images.
  Δεν εγκαταστάθηκαν/αναβαθμίστηκαν production βιβλιοθήκες.
- `git diff --check` και έλεγχος shell syntax πέρασαν.

Τα unit tests έτρεξαν με τοπικό Python 3.12 virtualenv. Το container smoke test
επαλήθευσε επιπλέον τις πραγματικές production εξαρτήσεις των candidate images.

## Production παρέμεινε αμετάβλητο

Τελικό read-only audit:

- Volume preflight PASS, Volume `106884142`.
- PostgreSQL container: `3e8abad67a5a4bbfdb2dd0773287d808e83b49a294634cff9d4d57af8efc5e1d`.
- Postmaster start: `2026-09-16T17:37:17.821965+03:00`, restart count **0**.
- API και Shelly ingestor είχαν τα ίδια images/IDs/start times.
- API health: `status=ok`, `database=connected`.
- Νέα Shelly εγγραφή: ID `86919919`, event time `2026-09-16T19:34:29.789699+03:00`.
- `to_regnamespace('shelly_compact')` επέστρεψε NULL.

## Συγκεκριμένο επόμενο παραγωγικό στάδιο

Με επιβεβαίωση του σταδίου: ανανέωση/επαλήθευση backup και preflight,
δημιουργία μόνο των νέων objects, σύντομη παύση/drain μόνο του Shelly ingestor,
καταγραφή high-water mark και εκκίνηση του candidate σε **dual** mode. Το API
παραμένει στο παλιό σχήμα. Έλεγχος των πραγματικών νέων εγγραφών και στα δύο
σχήματα, περιορισμένη backfill, concurrent covering index και πλήρης ισότητα.

Μετά αυτό το σημείο παρουσιάζονται τα παραγωγικά αποτελέσματα πριν από αλλαγή του
API σε compact reads. Δεν προτείνεται ακόμη διαγραφή παλιού πίνακα ή μετάβαση σε
compact-only writes. Για rollback κατά το dual στάδιο αρκεί επαναφορά του παλιού
application image/config, με την PostgreSQL να συνεχίζει να λειτουργεί.

Το αναλυτικό RUNBOOK περιγράφει επίσης την πιο απαιτητική επιστροφή μετά από
compact-only writes: pause, reverse tail copy, πλήρης επαλήθευση και επανεκκίνηση
του ingestor. Η κοινή sequence παραμένει ιδιοκτησία του παλιού πίνακα και πρέπει
να μεταφερθεί ρητά πριν από μελλοντικό retirement.

## Αρχεία τεκμηρίωσης

- `full-copy.json`, `full-index.json`, `full-verify.json`.
- `before-copy.json`, `after-copy-index.json`, `api-benchmark.json`, `benchmark_api.py`.
- `validation-summary.json`, `image-smoke.json`, `images.json`.
- `candidate-images.tar`: 212,365,824 bytes,
  SHA-256 `579eac17e55363042c5464a62a2888d2176e6ab4ed0c0ee2638a7b1412831f20`.
- Τα `images.json` καταγράφουν τόσο local OCI IDs όσο και image config digests,
  επειδή Docker Desktop/containerd και κλασικό Docker μπορεί να εμφανίσουν διαφορετικό
  είδος digest. Επαλήθευση archive/labels/config μετά από μελλοντικό docker load.

[PostgreSQL snapshot semantics](https://www.postgresql.org/docs/15/transaction-iso.html)
και [sequence semantics](https://www.postgresql.org/docs/15/functions-sequence.html).
