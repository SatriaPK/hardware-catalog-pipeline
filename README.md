# Panduan Penggunaan Hardware Catalog Pipeline

Proyek UTS Data Engineering: **eBay + Rakuten -> MinIO RAW -> cleaning dan
 deduplication -> MinIO CLEAN/QUARANTINE -> PostgreSQL MART**.

Target adalah **100.000 listing unik valid gabungan kedua marketplace**. Jumlah
request, objek RAW, hasil API dan snapshot harga bukan jumlah listing unik.

## 1. Sebelum mulai

- Buka PowerShell di folder repository yang berisi `compose.yaml`.
- Pastikan Docker Desktop, PostgreSQL dan MinIO sudah berjalan.
- Gunakan credential API/storage pada konfigurasi Docker privat yang sudah ada.
  Jangan menyalin credential ke command, README atau Git.
- Jangan menjalankan dua proses acquisition/collect pada namespace yang sama.

Semua contoh berikut memakai namespace default **mp_acq_dataset**. Bila memakai
namespace lain, tambahkan `--namespace mp_acq_nama` pada setiap perintah marketpipe.
Nama setelah `mp_acq_` terdiri dari 3-24 huruf kecil/angka.

## 2. Persiapan pertama atau setelah kode berubah

Build ulang image setelah pull/update kode, dependency atau config search plan:

```powershell
docker compose --profile pipeline build pipeline
```

Siapkan schema, migrasi dan tiga bucket pipeline. Jalankan saat tidak ada batch lain:

```powershell
docker compose --profile pipeline run --no-deps pipeline marketpipe init
```

`init` aman diulang untuk resource milik pipeline. Ini bukan reset PostgreSQL/MinIO;
perintah tidak menghapus tabel, data, bucket atau volume. Tidak perlu menjalankannya
setiap memulai collect jika storage dan migrasi sudah siap.

## 3. Pilot: periksa satu request data

Gunakan run-id baru untuk pilot pertama:

```powershell
docker compose --profile pipeline run --no-deps pipeline marketpipe pilot --source ebay --run-id pilot-ebay-uts --max-requests 2
docker compose --profile pipeline run --no-deps pipeline marketpipe pilot --source rakuten --run-id pilot-rakuten-uts --max-requests 1
```

Pilot menyimpan dan memproses data nyata. eBay membutuhkan satu request OAuth dan
maksimum satu request data; Rakuten maksimum satu request data. Pilot tidak retry.
Status `partial` akibat budget habis dapat normal. Periksa `accepted`,
`dataset_unique_accepted_listings`, `raw_objects`, `clean_objects`, dan `quarantine_objects`.
Jika run-id tersebut sudah ada, gunakan resume, bukan mengulang pilot dengan ID sama.

## 4. Cara utama: collect dua marketplace sampai target

```powershell
docker compose --profile pipeline run --no-deps pipeline marketpipe collect --run-id uts-100k --target 100000 --max-requests 2000 --interval 2
```

Program menjalankan **satu worker eBay dan satu worker Rakuten secara paralel**.
Biarkan terminal terbuka dan komputer tetap menyala. Hindari sleep/hibernate selama
pengumpulan. Setiap halaman selesai, terminal menampilkan progres JSON:

| Field | Arti |
| --- | --- |
| unique_valid_listings | Total listing unik valid di namespace, termasuk run sebelumnya |
| target | Target jumlah unik valid |
| source | Marketplace yang baru menyelesaikan halaman |
| http_attempts_this_invocation | Request gabungan sejak perintah ini dimulai, termasuk OAuth/retry |
| page_counts | Accepted, quarantine dan duplicate untuk halaman yang baru selesai |
| elapsed_seconds | Waktu berjalan sejak perintah ini dimulai |

`--max-requests 2000` adalah **batas gabungan dua marketplace**, bukan per marketplace.
Sesuaikan dengan sisa kuota akun. `--interval 2` memberi jeda minimum dua detik per
sumber; kedua sumber tetap berjalan bersamaan. Jika kuota kedua akun mengizinkan
satu request/detik, gunakan `--interval 1`. Retry tetap mengikuti backoff/Retry-After.

`--run-id uts-100k` menghasilkan dua run internal: `uts-100k-ebay` dan
`uts-100k-rakuten`. Semua listing yang sudah ada di namespace ikut dihitung untuk
mencapai target. Target belum dianggap tercapai sampai angka aktual mencukupi.

### Menghentikan dan melanjutkan collect

Tekan **Ctrl+C** untuk meminta berhenti. Request/upload yang sedang berlangsung
mungkin perlu selesai atau timeout terlebih dahulu. Hindari mematikan paksa Docker.

Untuk melanjutkan setelah terputus atau budget habis, jalankan **perintah yang sama**:

```powershell
docker compose --profile pipeline run --no-deps pipeline marketpipe collect --run-id uts-100k --target 100000 --max-requests 2000 --interval 2
```

Tidak perlu mengganti ke `resume` untuk collect. Checkpoint yang selesai dilewati;
RAW yang sudah tersimpan dipakai ulang untuk recovery. Budget command baru tidak
menghapus kuota provider atau batas total per-run pada plan yang sudah disimpan.

### Memahami alasan program berhenti

| stop_reason | Tindakan |
| --- | --- |
| target_reached | Target unik valid tercapai; periksa status untuk jumlah akhir |
| request_budget_exhausted | Periksa kuota; jalankan ulang collect yang sama jika masih tersedia budget. Jika cap per-run tersimpan sudah habis, pengulangan tidak menghapus cap tersebut |
| queries_exhausted | Semua query selesai; perluas query relevan, build ulang image, lalu gunakan run-id baru |
| worker_failed | Periksa detail sumber/error, perbaiki penyebabnya, lalu lanjutkan collect dengan ID yang sama |

Request yang sudah berjalan ketika target tercapai tetap dapat selesai, sehingga
jumlah akhir bisa sedikit di atas target. Jika query habis, program tidak mengarang
record atau mengulang tanpa kemajuan. Plan awal belum menjamin 100.000 unik valid.

## 5. Memeriksa jumlah data

Boleh dijalankan dari terminal PowerShell lain saat collect berjalan:

```powershell
# Total unik valid gabungan sumber
docker compose --profile pipeline run --no-deps pipeline marketpipe status

# Rincian setiap sumber dalam collect
docker compose --profile pipeline run --no-deps pipeline marketpipe status --run-id uts-100k-ebay
docker compose --profile pipeline run --no-deps pipeline marketpipe status --run-id uts-100k-rakuten
```

Pada `status`, gunakan **dataset_unique_accepted_listings** untuk menghitung target.
`dataset_accepted_observations` adalah jumlah snapshot harga; listing sama dapat
memiliki beberapa snapshot pada run berbeda. Pada output collect, jumlah unik yang
sama dinamai `unique_valid_listings`.

Di DBeaver, query read-only:

```sql
SELECT provider, marketplace_id, count(*) AS unique_valid_listings
FROM mp_acq_dataset.dim_listing
GROUP BY provider, marketplace_id;

SELECT * FROM mp_acq_dataset.acquisition_progress;

SELECT kind, count(*) AS objects, sum(row_count) AS records
FROM mp_acq_dataset.acquisition_output GROUP BY kind;
```

### Melihat RAW di MinIO

1. Buka **http://localhost:9001** dan login dengan akun MinIO privatmu.
2. Buka bucket **mp-acq-dataset-raw** untuk namespace default.
3. Masuk ke folder `ebay/` atau `rakuten/`, kemudian folder run-id.
4. Download file `<request-id>.json.gz`, ekstrak dengan 7-Zip, lalu buka JSON di VS Code.

Satu file RAW adalah satu respons request API, bukan satu listing. File tersebut
bisa berisi banyak listing, termasuk duplikat atau record yang kemudian dikarantina.
MinIO menampilkan daftar objek; semua isi gzip tidak otomatis digabung menjadi tabel.
Untuk melihat data yang sudah dibersihkan sebagai tabel, gunakan DBeaver di bawah.
Download tidak mengubah objek asli. Jangan membagikan RAW di luar hak penggunaan data.

Cari lokasi 20 respons RAW terbaru melalui DBeaver:

```sql
SELECT source, run_id, object_bucket, object_key,
       rows_reported, status, observed_at
FROM mp_acq_dataset.ingest_request
ORDER BY observed_at DESC, request_id
LIMIT 20;
```

`rows_reported` adalah jumlah record dalam respons yang berhasil diparsing; dapat
NULL jika RAW tersimpan tetapi parsing/proses belum selesai. Untuk melihat file
CLEAN/QUARANTINE, buka bucket terkait pada bagian 8, atau cari manifestnya:

```sql
SELECT run_id, kind, object_bucket, object_key, row_count
FROM mp_acq_dataset.acquisition_output
ORDER BY run_id, request_id, kind
LIMIT 100;
```

### Melihat listing di DBeaver

Buka koneksi PostgreSQL yang sudah tersedia, buat SQL Editor, lalu jalankan query.
Semua contoh berikut read-only. Ganti `mp_acq_dataset` jika memakai namespace lain.
Gunakan LIMIT dan pagination hasil DBeaver agar tidak memuat seluruh dataset sekaligus.

```sql
SELECT listing_key, provider, marketplace_id, source_listing_id,
       title_original, category_canonical, first_seen_at, last_seen_at
FROM mp_acq_dataset.dim_listing
ORDER BY last_seen_at DESC, listing_key
LIMIT 100;
```

Untuk memilih sumber/kategori, letakkan filter sebelum ORDER BY, misalnya
`WHERE provider = 'ebay' AND category_canonical = 'gpu'`.

### Melihat listing beserta semua snapshot harganya

Informasi listing ada di `dim_listing`; harga ada di `fact_price_snapshot`.
Gabungkan berdasarkan `listing_key`:

```sql
SELECT l.provider, l.marketplace_id, l.source_listing_id,
       l.title_original, l.category_canonical,
       s.price_amount, s.currency_code, s.observed_at, s.run_id
FROM mp_acq_dataset.dim_listing AS l
JOIN mp_acq_dataset.fact_price_snapshot AS s
    ON s.listing_key = l.listing_key
ORDER BY s.observed_at DESC, s.snapshot_key
LIMIT 100;
```

Listing yang sama dapat muncul beberapa kali karena diambil pada run/waktu berbeda.
Ini snapshot observasi, bukan tambahan listing unik. Harga adalah harga listing
marketplace; produk yang sama antarpenjual atau antarmarketplace belum dicocokkan.
Selalu tampilkan currency_code: angka USD dan JPY tidak bisa dibandingkan langsung.

### Melihat hanya harga terbaru per listing

Query ini mengambil satu snapshot terakhir per listing, kemudian menampilkan
100 listing dengan observasi terbaru. snapshot_key menjadi pemecah seri bila
waktu observasi sama; jangan menganggapnya urutan waktu tambahan.

```sql
WITH latest AS (
    SELECT DISTINCT ON (listing_key)
           listing_key, price_amount, currency_code, observed_at, run_id
    FROM mp_acq_dataset.fact_price_snapshot
    ORDER BY listing_key, observed_at DESC, snapshot_key DESC
)
SELECT l.listing_key, l.provider, l.marketplace_id, l.source_listing_id,
       l.title_original, l.category_canonical,
       s.price_amount, s.currency_code, s.observed_at, s.run_id
FROM mp_acq_dataset.dim_listing AS l
JOIN latest AS s ON s.listing_key = l.listing_key
ORDER BY s.observed_at DESC, l.listing_key
LIMIT 100;
```

### Menelusuri harga suatu listing sampai RAW asalnya

Salin listing_key dari hasil query sebelumnya, lalu ganti placeholder berikut:

```sql
SELECT s.observed_at, s.price_amount, s.currency_code, s.run_id,
       r.object_bucket, r.object_key
FROM mp_acq_dataset.fact_price_snapshot AS s
JOIN mp_acq_dataset.ingest_request AS r
    ON r.request_id = s.raw_request_id AND r.run_id = s.run_id
WHERE s.listing_key = 'GANTI_DENGAN_LISTING_KEY'
ORDER BY s.observed_at DESC, s.snapshot_key DESC
LIMIT 100;
```

object_bucket dan object_key menunjukkan file respons asli yang dapat dibuka di
MinIO. Judul dalam dim_listing adalah judul tersimpan terakhir; untuk memastikan
isi listing pada snapshot lama, periksa RAW yang dirujuk snapshot tersebut.

### Eksplorasi tambahan untuk analisis UTS

Contoh berikut juga read-only dan memakai namespace default. Gunakan hasil data
aktual untuk laporan, bukan angka fixture/test. Jalankan satu query setiap kali.

#### A. Komposisi listing unik per sumber dan kategori

```sql
SELECT provider, marketplace_id, category_canonical,
       count(*) AS unique_valid_listings
FROM mp_acq_dataset.dim_listing
GROUP BY provider, marketplace_id, category_canonical
ORDER BY provider, unique_valid_listings DESC;
```

Ini menghitung listing unik, bukan snapshot. Kategori berasal dari pemetaan/query
pipeline, bukan hasil pemeriksaan manual setiap produk.

#### B. Mencari listing dari kata pada judul

Ganti kata pencarian dan kategori sesuai kebutuhan; ILIKE tidak membedakan huruf
besar/kecil. Tanda persen berarti ada teks lain sebelum/sesudah kata tersebut.

```sql
SELECT listing_key, provider, source_listing_id, title_original,
       category_canonical, last_seen_at
FROM mp_acq_dataset.dim_listing
WHERE title_original ILIKE '%RTX 4060%'
  AND category_canonical = 'gpu'
ORDER BY last_seen_at DESC, listing_key
LIMIT 100;
```

Judul yang mirip belum membuktikan produk identik; hasil pencarian bisa mencakup
aksesori atau varian yang berbeda.

#### C. Ringkasan harga terbaru per kategori dan mata uang

Ambil satu snapshot terbaru per listing agar listing yang sering diambil tidak
memiliki bobot berlebih. Jangan menggabungkan harga dari mata uang berbeda.

```sql
WITH latest AS (
    SELECT DISTINCT ON (listing_key)
           listing_key, price_amount, currency_code
    FROM mp_acq_dataset.fact_price_snapshot
    ORDER BY listing_key, observed_at DESC, snapshot_key DESC
)
SELECT l.provider, l.marketplace_id, l.category_canonical, s.currency_code,
       count(*) AS listings,
       min(s.price_amount) AS minimum_price,
       round(avg(s.price_amount), 2) AS average_price,
       round((percentile_cont(0.5) WITHIN GROUP (
           ORDER BY s.price_amount
       ))::numeric, 2) AS median_price,
       max(s.price_amount) AS maximum_price
FROM latest AS s
JOIN mp_acq_dataset.dim_listing AS l USING (listing_key)
GROUP BY l.provider, l.marketplace_id, l.category_canonical, s.currency_code
ORDER BY l.provider, l.category_canonical, s.currency_code;
```

Statistik kategori mencampur model/kondisi/penjual yang berbeda. Gunakan sebagai
deskripsi sampel terkumpul, bukan kesimpulan bahwa satu marketplace lebih murah
untuk produk yang sama atau representasi seluruh pasar.

#### D. Progres dan alasan berhenti setiap run

```sql
SELECT r.run_id, r.source, r.marketplace_id, r.started_at,
       r.status, r.error_summary,
       p.fetched_records, p.accepted, p.quarantined,
       p.excluded_duplicates, p.raw_objects, p.http_attempts
FROM mp_acq_dataset.pipeline_run AS r
JOIN mp_acq_dataset.acquisition_progress AS p USING (run_id)
ORDER BY r.started_at DESC, r.run_id
LIMIT 50;
```

Status run disimpan pada checkpoint/status akhir pemanggilan; jangan menganggapnya
indikator real-time apakah container masih hidup. Gunakan progres terminal untuk
melihat proses aktif. fetched_records pada view ini menghitung occurrences yang
sudah masuk ledger, bukan semua listing di RAW yang parsing-nya belum selesai.

#### E. Jumlah dan persentase accepted, karantina, serta duplikat

```sql
SELECT r.source, o.run_id,
       count(*) AS processed_occurrences,
       count(*) FILTER (WHERE o.outcome = 'accepted') AS accepted,
       count(*) FILTER (WHERE o.outcome = 'quarantined') AS quarantined,
       count(*) FILTER (WHERE o.outcome = 'excluded_duplicate') AS duplicates,
       round(100.0 * count(*) FILTER (WHERE o.outcome = 'quarantined')
             / NULLIF(count(*), 0), 2) AS quarantine_percent,
       round(100.0 * count(*) FILTER (WHERE o.outcome = 'excluded_duplicate')
             / NULLIF(count(*), 0), 2) AS duplicate_percent
FROM mp_acq_dataset.acquisition_occurrence AS o
JOIN mp_acq_dataset.pipeline_run AS r USING (run_id)
GROUP BY r.source, o.run_id
ORDER BY r.source, o.run_id;
```

Denominator adalah record/occurrences yang sudah diproses pada run itu, termasuk
duplikat, bukan jumlah unik global. Accepted + quarantined + duplicates harus sama
dengan processed_occurrences. Run tanpa ledger belum muncul dalam query ini.

#### F. Alasan record masuk QUARANTINE

```sql
SELECT r.source, reason.code AS reason_code,
       count(*) AS rejected_occurrences_with_reason
FROM mp_acq_dataset.acquisition_occurrence AS o
JOIN mp_acq_dataset.pipeline_run AS r USING (run_id)
CROSS JOIN LATERAL jsonb_array_elements_text(o.reason_codes) AS reason(code)
WHERE o.outcome = 'quarantined'
GROUP BY r.source, reason.code
ORDER BY rejected_occurrences_with_reason DESC, r.source, reason.code;
```

Satu record dapat memiliki beberapa reason_codes, sehingga total frekuensi alasan
bisa lebih besar daripada jumlah record karantina. Untuk memeriksa contoh record,
cari lokasi file QUARANTINE dan indeks record dalam respons RAW:

```sql
SELECT o.run_id, o.request_id, o.row_index, o.reason_codes,
       a.object_bucket, a.object_key
FROM mp_acq_dataset.acquisition_occurrence AS o
JOIN mp_acq_dataset.acquisition_output AS a
    ON a.request_id = o.request_id AND a.kind = 'quarantine'
WHERE o.outcome = 'quarantined'
ORDER BY o.run_id, o.request_id, o.row_index
LIMIT 50;
```

row_index dimulai dari 0 dan menunjuk posisi record dalam array respons RAW, bukan
posisi record dalam file QUARANTINE yang sudah menyaring record lain.

#### G. Halaman yang belum selesai diproses

```sql
SELECT p.run_id, p.query_id, p.page_number, p.status AS page_status,
       r.status AS raw_status, r.object_bucket, r.object_key, r.rows_reported
FROM mp_acq_dataset.acquisition_page AS p
LEFT JOIN mp_acq_dataset.ingest_request AS r USING (request_id)
WHERE p.status <> 'completed'
ORDER BY p.run_id, p.query_id, p.page_number
LIMIT 100;
```

prepared berarti intent halaman sudah tersimpan; belum tentu request data berhasil.
durable berarti RAW sudah tercatat tetapi proses halaman belum selesai. Nilai NULL
di manifest tidak selalu berarti file RAW tidak ada: upload bisa berhasil sebelum
transaksi manifest terputus. Gunakan resume/collect untuk recovery, bukan menghapus
objek atau mengedit checkpoint secara manual.

#### H. Listing yang harganya berubah antarobservasi

```sql
WITH history AS (
    SELECT listing_key, snapshot_key, currency_code, observed_at, price_amount,
           lag(price_amount) OVER (
               PARTITION BY listing_key, currency_code
               ORDER BY observed_at, snapshot_key
           ) AS previous_price
    FROM mp_acq_dataset.fact_price_snapshot
)
SELECT l.provider, l.source_listing_id, l.title_original,
       h.currency_code, h.observed_at, h.previous_price, h.price_amount,
       h.price_amount - h.previous_price AS price_difference,
       round(100.0 * (h.price_amount - h.previous_price)
             / NULLIF(h.previous_price, 0), 2) AS change_percent
FROM history AS h
JOIN mp_acq_dataset.dim_listing AS l USING (listing_key)
WHERE h.previous_price IS NOT NULL
  AND h.price_amount <> h.previous_price
ORDER BY h.observed_at DESC, h.snapshot_key DESC
LIMIT 100;
```

Query membandingkan listing yang sama dalam mata uang yang sama. Jika baru satu
observasi per listing atau harga belum berubah, hasil kosong adalah wajar. Bila
timestamp sama, snapshot_key hanya memberikan urutan deterministik; jangan menafsirkan
selisih pada timestamp tersebut sebagai urutan waktu yang pasti.

#### Menyimpan hasil eksplorasi

Gunakan fitur ekspor result set DBeaver ke CSV untuk kebutuhan analisis/tugas sesuai
hak penggunaan data. Perhatikan apakah yang diekspor hanya halaman hasil yang sudah
dimuat atau seluruh hasil query. Jika query memakai LIMIT 100, ekspornya juga hanya
sampel tersebut. Simpan ekspor privat di data/ atau lokasi aman, bukan Git. Hindari
mengunggah RAW atau data sensitif saat yang dibutuhkan hanya tabel agregat.

## 6. Batch manual satu marketplace (opsional)

Gunakan `acquire` jika ingin mengatur satu sumber saja:

```powershell
docker compose --profile pipeline run --no-deps pipeline marketpipe acquire --source ebay --run-id batch-ebay-uts --max-requests 200
docker compose --profile pipeline run --no-deps pipeline marketpipe acquire --source rakuten --run-id batch-rakuten-uts --max-requests 100
```

Untuk melanjutkan run manual atau pilot yang sudah ada:

```powershell
docker compose --profile pipeline run --no-deps pipeline marketpipe resume --source ebay --run-id batch-ebay-uts --max-requests 200
docker compose --profile pipeline run --no-deps pipeline marketpipe resume --source rakuten --run-id batch-rakuten-uts --max-requests 100
```

| Kebutuhan | Gunakan |
| --- | --- |
| Memulai run manual baru | pilot atau acquire dengan run-id baru |
| Melanjutkan run manual/pilot | resume dengan source dan run-id lama |
| Memulai/melanjutkan pengumpulan paralel | collect dengan run-id koleksi yang sama |
| Mengambil observasi baru setelah run selesai | run-id baru; resume tidak mengulang halaman selesai |
| Menggunakan query plan yang sudah diubah | build ulang dan run-id baru |

## 7. Mengatur query dan batas pengambilan

Edit **config/acquisition-plan.yaml**. Di sana tersedia query pencarian, kategori,
page_size, max_pages, max_results, dan max_requests_per_run. Snapshot plan disimpan
saat run dibuat; resume tetap memakai plan lama. Config masuk ke image saat build,
bukan bind mount, sehingga perubahan membutuhkan build ulang.

Contoh menggunakan plan yang telah diperluas tanpa mengganti run lama:

```powershell
docker compose --profile pipeline build pipeline
docker compose --profile pipeline run --no-deps pipeline marketpipe collect --run-id uts-100k-lanjutan --target 100000 --max-requests 2000 --interval 2
```

Data sebelumnya tetap dihitung dan dideduplikasi. Jangan membuat run baru untuk
menghindari kuota provider. Opsional `--retention-days N` pada pilot/acquire/collect
mencatat batas retensi yang benar-benar berlaku; isi N dengan angka yang sesuai
ketentuan sumber. Tidak ada penghapusan otomatis; lifecycle seluruh lapisan data
termasuk backup tetap perlu dikelola sesuai ketentuan.

## 8. Lokasi data dan keamanan

| Lapisan | Lokasi default |
| --- | --- |
| RAW | Bucket mp-acq-dataset-raw, respons asli gzip |
| CLEAN | Bucket mp-acq-dataset-clean, Parquet valid |
| QUARANTINE | Bucket mp-acq-dataset-quarantine, JSON record invalid/konflik dan reason_codes |
| MART + checkpoint | PostgreSQL schema mp_acq_dataset |

Data MinIO/PostgreSQL berada di **Docker named volumes**, bukan otomatis di folder
data/ host. Folder data/ adalah bind mount /app/data dan bisa memuat file privat,
exports atau hasil verifikasi. Jangan menghapusnya hanya karena RAW berada di MinIO.

Identitas unik: provider + marketplace + listing ID. CLEAN/QUARANTINE ditulis sebelum
commit MART/checkpoint. Objek immutable memakai checksum; resume tidak menimpa data
berbeda. Satu proses acquisition/collect diperbolehkan per namespace; collect sendiri
memiliki dua worker provider. Migrasi lama tetap diperlukan untuk database existing.

Gunakan API resmi sesuai ketentuan akses, penggunaan, penyimpanan dan publikasi.
Rakuten memakai Allowed IP egress publik, bukan alamat LAN seperti 192.168.1.1.
Origin/Referer hanya diisi sesuai website aplikasi yang terdaftar. Jangan commit .env,
credential atau bukti pribadi. Tidak ada registry administratif internal.

## 9. Troubleshooting singkat

| Gejala | Langkah |
| --- | --- |
| BucketSetupError / NoSuchTagSet | Hentikan batch lain lalu jalankan init. Tag hanya diperbaiki otomatis pada bucket tanpa tag yang kosong, termasuk versi historis |
| untagged_bucket_contains_data | Jangan hapus bucket. Tinjau nama bucket/kepemilikan; data tidak diubah otomatis |
| ClientError | Periksa operation, storage_error_code dan http_status; jangan bagikan credential |
| Rakuten CLIENT_IP_NOT_ALLOWED | Cocokkan Allowed IP aplikasi dengan egress publik jaringan Docker |
| schema_drift | Lihat response_structure/schema_violations. Jangan menghapus validasi agar data dipaksa lolos |
| Run ID sudah ada | Gunakan resume untuk run manual, atau collect dengan ID koleksi yang sama |
| Proses lain aktif | Jangan jalankan batch/collect kedua pada namespace sama; status tetap dapat digunakan |

## 10. Pengujian (bukan pengumpulan data nyata)

```powershell
docker compose --profile pipeline run --no-deps pipeline python -m pytest -q
docker compose --profile pipeline run --no-deps pipeline python -m ruff check .

# Opt-in: storage nyata, provider HTTP tetap mocked.
# Membuat namespace test terisolasi dan mempertahankannya; tidak cleanup otomatis.
docker compose --profile pipeline run --no-deps pipeline python -m pytest -q -s --storage-integration
```

Verifikasi kode terakhir: **34 tests passed, 2 integration tests skipped** pada
runtime agent. Ruff/format lulus. Live collect dan target 100.000 setelah perubahan
belum dibuktikan oleh pengujian offline tersebut.

## 11. Panduan merapikan folder

Daftar ini berdasarkan inspeksi isi/nama file repository saat README diperbarui.
Periksa ulang jika kamu sudah menambahkan file baru. Bagian ini bukan perintah
penghapusan otomatis; tidak ada data yang dihapus saat pembaruan README.

### Aman dibersihkan: cache dan sisa folder lama

| File/folder | Keterangan |
| --- | --- |
| .pytest_cache/ | Cache pytest; dibuat ulang saat tes |
| .ruff_cache/ | Cache lint; dibuat ulang |
| __pycache__/ dan *.pyc di src/, tests/, migrations/, scripts/ | Bytecode Python; dibuat ulang |
| scripts/ | Saat inspeksi hanya berisi __pycache__ dari script lama; tidak ada script aktif |
| tests/contract/ | Saat inspeksi hanya berisi cache test lama |
| tests/fixtures/ | Saat inspeksi hanya folder ebay/ dan rakuten/ kosong; tests aktif memakai mocked payload di kode |
| reports/verification/acquisition/, e2e/, pre-live-increment-1/, pre-live-increment-2/ | Folder sisa kosong, termasuk bugfix-ordering/ yang kosong |
| reports/final/ | Saat inspeksi hanya .gitkeep; boleh dibuang jika tidak dipakai untuk pengumpulan tugas |

### Opsional, tetapi pahami konsekuensinya

| File/folder | Konsekuensi |
| --- | --- |
| .venv/ | Boleh dihapus jika hanya memakai Docker; menjalankan Python/test lokal memerlukan install ulang environment |
| .ai/ | Catatan proyek untuk agent; tidak dipakai runtime, tetapi riwayat/catatan kerja hilang jika dibuang |
| reports/verification/runtime-audit/ | Bukan dependency runtime, tetapi berisi bukti pengujian privat; arsipkan dahulu jika ingin memindahkan |
| docs/evidence/ | Berisi bukti persetujuan eBay; simpan atau pindahkan ke arsip privat yang aman, jangan hapus sembarangan |

Jangan menghapus seluruh reports/ atau docs/ sekaligus: keduanya masih memiliki
arsip tersebut. Tidak perlu menghapus .venv hanya untuk menjalankan Docker.

### Pertahankan kode dan data ini

- src/, config/acquisition-plan.yaml, migrations/, alembic.ini.
- compose.yaml, dockerfile, requirements.txt, pyproject.toml.
- tests/unit/, tests/integration/, tests/support.py, tests/conftest.py, tests/__init__.py.
- README.md, .gitignore, .dockerignore, .env.example, dan .git/.
- .env, file secret, data/ dan seluruh dataset/export pribadi.
- Docker named volumes pg_data dan minio_data (nama aktual dapat diberi prefix Compose).
- Bucket MinIO, PostgreSQL MART, manifest dan checkpoint run yang masih diperlukan.

Jangan memakai `docker compose down -v`, volume prune, DROP DATABASE atau penghapusan
bucket untuk sekadar merapikan repository. Menghapus migrations lama juga tidak
aman: migrasi 0001 sampai 0006 adalah rantai schema yang masih dipakai.
