# Shelly compact — αποκλειστικές εγγραφές στο νέο σχήμα, 2026-09-16

**PASS.** Από τις 21:43:22 ώρα Ελλάδας ο Shelly ingestor γράφει με
`SHELLY_MEASUREMENTS_WRITE_MODE=compact`. Το API παραμένει σε compact reads.
Η PostgreSQL στο Volume και το API δεν επανεκκινήθηκαν.

## Έκδοση και ακριβής αλλαγή

- Application commit: `586ce99272372fdc4b6107a561017d5c40ead562`.
- Operations commit: `4a940318ae7daaa1c90805131bdec833c107c43a`.
- Αμετάβλητο writer image: `sha256:f92a082e0fbbb5cbbd4f3ab922b589df1ca22b403d4f291bbd8f30ace3308d7b`.
- Νέο writer container: `cf170389b4231bb22420203e1c273ed60aacc180228e62f5ff2aa067df7e412f`.
- Release: `/opt/upat-shelly-compact-stage3-20260916-r2`.
- Μόνη διαφορά στο resolved Compose: write mode `dual` → `compact`.
- Canonical Compose SHA-256: `282845fc84ac3fcac013809c8cc75d71e658e686bf4fbc530bb52a0cfd117aba`.
- Το `.env` διατηρήθηκε αμετάβλητο.
- Στοχευμένο stop/recreate μόνο του `shelly-ingestor`, με `--no-deps --no-build --pull never`.
- Από stop έως recreation: **31.99 sec**. Το μικρό διάστημα
  διακοπής είχε εγκριθεί. Δεν υπάρχει ισχυρισμός επαναπαράδοσης QoS0 MQTT μηνυμάτων
  που πιθανόν έφτασαν όσο ο ingestor ήταν σταματημένος.

## Backup και έλεγχοι πριν από τη μετάβαση

Νέο πλήρες database export: 1.70 GB περίπου μαζί με τα συνοδευτικά αρχεία.
Η τοπική δοκιμή επαναφοράς πέρασε για **32 πίνακες / 136,563,736 γραμμές** συνολικά,
με έλεγχο catalog, indexes, constraints, 19 sequences και διατήρησης δεδομένων μετά
από restart της απομονωμένης **τοπικής** PostgreSQL. Η παραγωγική PostgreSQL δεν
επανεκκινήθηκε. Το restore container δεν είχε δίκτυο ή published ports και έχει
σταματήσει, με διατήρηση του τοπικού volume.

- Mac: `backups/shelly-compact-production-stage3-20260916/backup-before`.
- Chris: `/Volumes/Chris/VPS-Backups/shelly-stage3-before-20260916`.
- **41 αρχεία**, πλήρης ανάγνωση και αντιστοίχιση SHA-256 στον εξωτερικό δίσκο.
- Οι ήδη εγκεκριμένες ρυθμίσεις επαναφοράς φυλάσσονται σε ιδιωτικό φάκελο.

Πλήρης read-only επαλήθευση παραγωγής σε ένα repeatable-read snapshot:
**64,336,440 μετρήσεις**, ίδια IDs και τα έξι αρχικά πεδία,
με δυαδικό SHA-256 `648b40a0b8e1c50d7c3723a557574081e8df7c5721fbf20f2acb0aa9cc2d7da1`.
Διάρκεια **278.09 sec**, με τον dual writer ενεργό.

Μετά την παύση του writer, επαληθεύτηκε ξανά η ουρά από το αρχικό ασφαλές όριο
**86926633** μέχρι **86962336**, με SHARE locks και περιορισμένα timeouts.
Το όριο αυτό καλύπτει και συναλλαγές των οποίων τα IDs είχαν εκχωρηθεί νωρίτερα
αλλά ολοκληρώθηκαν μετά το snapshot. Καταγράφηκε πιστοποιημένο reverse checkpoint
στο **86962336**, χωρίς περιττή επανεισαγωγή όλου του ιστορικού.
Η απόδειξη προϋποθέτει τον ελεγμένο append-only writer· ιστορικά updates από άλλον
μελλοντικό writer απαιτούν νέα εξέταση.

Τοπικά πέρασαν **24 διαφορετικές δοκιμές** σε πραγματική PostgreSQL: 23 στο αρχικό
σύνολο και επανέλεγχος 6 δοκιμών μετά την προσθήκη του CLI regression test.
Καλύφθηκαν ακριβείς τιμές, πραγματικό MQTT callback σε compact-only mode,
ατομικότητα raw/counters/measurements, εκκρεμείς συναλλαγές, συγκρούσεις,
rollback της νέας ουράς και επάνοδος σε dual mode.

## Ενεργοί καταναλωτές

Ο έλεγχος ενεργού API image επιβεβαίωσε compact reader και shared latest/history
readers. Τα operational telemetry checks χρησιμοποιούν `shelly_raw_messages`.
Η προγραμματισμένη energy-aggregator image διαβάζει `shelly_energy_counters`,
όχι τον παλιό πίνακα measurements. Ελέγχθηκαν τα στοχευμένα cron/systemd scripts
και οι database dependencies: δεν βρέθηκαν εξαρτώμενα views, functions ή triggers
στον παλιό πίνακα. Παλιά αρχεία πηγαίου κώδικα στο host δεν είναι ενεργά code mounts.
Η τοπική αυτοματοποίηση ελέγχου υγείας δεν κατονομάζει τον παλιό πίνακα· δεν άλλαξε.
Αυτό καλύπτει τους καταναλωτές που ελέγχθηκαν, όχι άγνωστες εξωτερικές συνδέσεις.

## Επαλήθευση μετά τη μετάβαση

Στις **21:46:15 ώρα Ελλάδας**:

- **954 νέες μετρήσεις** μόνο στο compact.
- Παλιό max ID: **86962336**, νέο max ID: **86963290**.
- API history για 21:44–21:46: **2 κλειστά λεπτά / 12 δείγματα**,
  όλα με IDs μετά το checkpoint και ακριβή συμφωνία με PostgreSQL.
- API history χρόνος **0.0838 sec** μέσω loopback.
- Latest timestamp **2026-09-16T21:46:00+03:00**, χρόνος **4.1072 sec**.
- Raw-message age **1.075067 sec**, counter age **9.672045 sec**.
- Hourly energy: **4 buckets**, **0.0446 sec**.
- Δημόσιο authenticated health, απόρριψη λανθασμένου/απόντος token και SchoolHeroZ
  CORS: PASS. Δεν έγινε πραγματικό user login/browser UI test.
- **0** νέα error/exception/traceback/fatal matches σε API και ingestor.
- **0** invalid compact indexes. PostgreSQL ownership guard: PASS.
- PostgreSQL container, API, TTN, Caddy και Mosquitto διατήρησαν ταυτότητες,
  χρόνους εκκίνησης και restart counts. PostgreSQL postmaster:
  `2026-09-16T17:37:17.821965+03:00`, restart count **0**.
- Ελεύθερα: Volume **48.02 GiB**, κύριος δίσκος **25.47 GiB**.

Οι χρόνοι είναι ενδεικτικές λειτουργικές μετρήσεις, όχι νέο benchmark.
Οι πραγματικές float8 τιμές διατηρούνται· το ένα δεκαδικό αφορά τους API μέσους όρους.

## Επιστροφή και διατήρηση παλιού πίνακα

Ο παλιός πίνακας διατηρεί όλο το ιστορικό μέχρι τη μετάβαση και **δεν ενημερώνεται**.
Παραμένει η ιδιοκτησία της κοινής sequence στον παλιό πίνακα. Δεν έγινε DROP,
DELETE, REINDEX ή VACUUM FULL. Ο χώρος του παλιού πίνακα δεν έχει ανακτηθεί·
το όφελος τώρα είναι ότι σταμάτησε η διπλή αύξηση measurements/indexes.
Raw MQTT messages και energy counters παραμένουν ξεχωριστές λειτουργικές εγγραφές.

Read-only status:
```sh
python3 /opt/upat-shelly-compact-stage3-20260916-r2/ops/shelly-compact/stage3.py status
```

Αν απαιτηθεί εγκεκριμένη επιστροφή, ο runner `stage3.py rollback` σταματά μόνο τον
writer, επεκτείνει το reverse checkpoint, αντιγράφει τη νέα ουρά compact → legacy,
επαληθεύει δυαδικά τις τιμές και μετά επαναφέρει το αποθηκευμένο dual Compose.
Το API παραμένει compact. Σε conflict ο writer παραμένει σταματημένος προς εξέταση.
**Δεν εκτελέστηκε παραγωγικό rollback**, επειδή η μετάβαση πέρασε.
Απλό toggle σε dual/legacy ή παλιό API δεν είναι ασφαλής διαδικασία επιστροφής πλέον.

Οι παλαιοί stage1/stage2 guards έχουν προηγούμενες ταυτότητες/modes και δεν είναι
οι τρέχοντες έλεγχοι λειτουργίας. Μελλοντική διαγραφή του παλιού πίνακα απαιτεί
ξεχωριστή έγκριση και μεταφορά ownership της κοινής sequence πριν από DROP.

## Αποδεικτικά και περιορισμοί

Το πρώτο read-only verifier σταμάτησε πριν τη σάρωση λόγω nested connection
context. Διορθώθηκε με ξεχωριστό committed release και τοπικό CLI test· δεν είχε
αλλάξει υπηρεσία ή δεδομένα. Το πρώτο HTTP history probe πήρε το αναμενόμενο 404
στο δημόσιο endpoint που δεν εκθέτει ο Caddy. Η πραγματική δοκιμή έγινε στην
υπάρχουσα loopback διαδρομή· δεν άλλαξαν κανόνες έκθεσης ή authentication.

Αποδεικτικά: `/Users/mountzou/upat-nzc-mqtt-db/backups/shelly-compact-production-stage3-20260916`.
Το deployment report και τα scripts είναι versioned στο branch
`codex/shelly-compact-migration-20260916`. Δεν έγινε push ή αλλαγή των άσχετων
τροποποιήσεων του κύριου working tree. Τα backups προγράμματος/συχνότητας
παραμένουν ξεχωριστό requirement.
