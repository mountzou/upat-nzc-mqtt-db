# Shelly compact migration — τοπική πρόβα, 2026-09-16

Ιστορικό αποδεικτικό της τοπικής υλοποίησης και πλήρους πρόβας που προηγήθηκε της
παραγωγικής ενεργοποίησης. Η μεταγενέστερη ολοκλήρωση καταγράφεται στο
[παραγωγικό stage 3](PRODUCTION-STAGE3-20260916.md). Τα migration/recovery εργαλεία
και τα staged Compose αρχεία έχουν αποσυρθεί· ο κώδικας διατηρείται στο Git history.
Τα παρακάτω αποτελέσματα αφορούν την καταγεγραμμένη έκδοση.

## Έκδοση και αντικείμενο

- Branch: `codex/shelly-compact-migration-20260916`.
- Κώδικας των δοκιμασμένων images: `586ce99272372fdc4b6107a561017d5c40ead562`.
- Διορθωμένο HTTP smoke test: `dc118b7eba930ed1aee464846d3c01c043e1e632`.

Η πρόβα έλεγξε dictionary ανά `(device_id,metric,unit)` και compact μετρήσεις
`(id,series_id,value,event_time)`, με διατήρηση των αρχικών float8 τιμών, IDs,
NULLs, μονάδων και timestamps. Μόνο οι API μέσοι όροι χρησιμοποιούσαν
`ROUND(AVG(value::numeric),1)`. Ελέγχθηκαν atomic dual writes μαζί με raw MQTT,
συσκευές και counters, επαναλήψιμη αντιγραφή με checkpoint, συγκρούσεις IDs,
reverse recovery και concurrent κατασκευή covering index μετά την αντιγραφή.
Η μέθοδος και οι περιορισμοί διατήρησης/επιστροφής συνοψίζονται στο [RUNBOOK](RUNBOOK.md).

## Πλήρης επαλήθευση δεδομένων

Χρησιμοποιήθηκε ξεχωριστό τοπικό clone του επαληθευμένου post-Volume backup
`pg-volume-live-backup-20260916-1810`, με το ίδιο PostgreSQL 15.17 amd64 image
της παραγωγής. Το αρχικό backup/restore volume παρέμεινε αμετάβλητο.

- Εγγραφές σε κάθε σχήμα: **64,266,981**.
- Εύρος IDs: **10938671–86891014**.
- Σύγκριση και των έξι αρχικών πεδίων, για όλες τις εγγραφές, σε μία κοινή
  READ ONLY REPEATABLE READ snapshot με ordered binary COPY.
- Bytes σε κάθε δυαδική ροή: **5,462,983,998**.
- Κοινό SHA-256: `cd2ddb60a2a2871c88fc2c63b5f6b2b1a705df2798a629ee22aadc1c9f498c96`.
- Αποτέλεσμα: **PASS**. Όλα τα απαιτούμενα indexes/constraints ήταν έγκυρα.

Η πλήρης πρόβα αφορούσε σταθερό snapshot. Ταυτόχρονες εγγραφές, μερική αποτυχία,
διακοπή/συνέχιση, out-of-order commits και reverse recovery ελέγχθηκαν σε ξεχωριστό
πραγματικό PostgreSQL fixture. Δεν έγινε πλήρης ιστορική πρόβα με live production
traffic ή αποτυχία λειτουργικού/δίσκου.

## Χώρος και API

| Αντικείμενο στο τοπικό snapshot | GiB |
|---|---:|
| Αρχικός πίνακας μαζί με indexes/TOAST | 13.214 |
| Νέο schema μαζί με measurements, series, indexes και progress | 6.896 |
| Διαφορά | 6.318 |

Η διαφορά **47.81%** αφορά το τοπικό layout και προκύπτει από την αποφυγή
επαναλαμβανόμενων κειμένων, με διατήρηση της ακρίβειας των μετρήσεων. Η πρόβα δεν
απελευθέρωσε χώρο στο VPS. Η διατήρηση του παλιού πίνακα και το μελλοντικό retirement
καλύπτονται στο RUNBOOK.

Οι πραγματικές Python συναρτήσεις status/history/latest έδωσαν **8/8 ίδια
αποτελέσματα**, με την ίδια `decimal_1` πολιτική και στα δύο σχήματα. Καλύφθηκαν
plug/Pro3EM, status 15 λεπτών, ιστορικό 24 ωρών/7 ημερών/30 ημερών και unbounded latest.
Το ιστορικό πολλών μετρικών και μεγαλύτερων διαστημάτων ωφελήθηκε στις τοπικές
μετρήσεις· το unbounded latest παρέμεινε αργό. Οι μετρήσεις έγιναν σε Docker με amd64
emulation σε Mac ARM και δεν προβλέπουν χρόνους ή ανάγκες χώρου της παραγωγής.

## Πρόσθετοι έλεγχοι

- **16 πραγματικά PostgreSQL integration tests**: πλήρη πεδία/NULL/NaN/Inf/signed zero,
  ταυτόχρονη δημιουργία σειράς, atomicity, αποτυχία χωρίς μισές εγγραφές,
  checkpoint/continuation, συγκρούσεις, reverse recovery, CLI και capacity guard.
- Μαζί με Compose guards: **22 passed**, **5 subtests**.
- API/ingestion regression tests: **392 passed**, **12 subtests**, **137 skipped**
  επειδή δεν δόθηκαν τα ανεξάρτητα fixtures τους.
- Τα πραγματικά candidate images έγραψαν fixture MQTT μέσω του κανονικού callback
  και εξυπηρέτησαν HTTP latest/history, κενή συσκευή και decimal tie **286.45→286.5**.
  Legacy και compact JSON ήταν ίδια. Το `/internal/data/health` δέχθηκε σωστό token
  και απέρριψε έλλειψη token.
- Τα images χτίστηκαν από git archive και τα ακριβή υπάρχοντα runtime images,
  διατηρώντας τις production εξαρτήσεις. Τα container smoke tests τις επαλήθευσαν.

Κατά αυτή την πρόβα η παραγωγή παρέμεινε αμετάβλητη: Volume preflight και API health
πέρασαν, διατηρήθηκαν τα images/IDs των API και ingestor, και η PostgreSQL είχε
restart count **0**. Τότε το `shelly_compact` δεν υπήρχε ακόμη στο VPS.

## Αποδεικτικά

- Πλήρης αντιγραφή/index/ισότητα: `full-copy.json`, `full-index.json`, `full-verify.json`.
- Κατάσταση και μετρήσεις: `before-copy.json`, `after-copy-index.json`,
  `api-benchmark.json`, `benchmark_api.py`.
- Σύνοψη και images: `validation-summary.json`, `image-smoke.json`, `images.json`.
- `candidate-images.tar`: **212,365,824 bytes**,
  SHA-256 `579eac17e55363042c5464a62a2888d2176e6ab4ed0c0ee2638a7b1412831f20`.

Το `images.json` καταγράφει χωριστά local OCI IDs και image config digests.
Οι αναλυτικές μετρήσεις καταγράφηκαν στα αποδεικτικά· τα παραπάνω αποτελέσματα είναι
ιστορική καταγραφή της πρόβας.
