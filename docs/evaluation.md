# Laporan implementasi dan evaluasi — 2026-11-03

## 1. Ringkasan dan status

Arsitektur provider registry, adapter contract, generic mapping, canonical envelope,
extension attributes, versioned CLEAN dan migrasi sumber tambahan telah diterapkan.
Eksperimen sumber ketiga melewati pipeline bersama tanpa cabang nama provider baru.
Pengujian offline berhasil; **verifikasi PostgreSQL/MinIO nyata masih tertunda**.
Docker daemon tidak tersedia, termasuk setelah pengecekan di luar sandbox.
Tidak ada acquisition API nyata atau migrasi database pengguna yang dijalankan.

Fitur storage, checkpoint, hashing bisnis, deduplikasi dan recovery sudah ada sebelum
refactor; kontraknya dipertahankan dan diuji regresi. Detail baseline ada pada
[baseline-audit.md](baseline-audit.md). Dokumen ini membedakan hasil simulasi dari
bukti akses API atau kondisi penyimpanan produksi.

## 2. File yang diubah/ditambahkan

| Kelompok | File |
| --- | --- |
| Kontrak dan registrasi baru | src/marketplace_pipeline/adapters/contracts.py; providers.py |
| Adapter | adapters/production.py; adapters/simulated.py (baru) |
| Standardisasi | mapping.py (baru); cleaning.py; domain.py |
| Eksekusi dan penyimpanan | acquisition.py; acquisition_plan.py; runtime.py; cli.py; storage/parquet.py |
| Migrasi | migrations/versions/0007_extensible_sources.py (baru) |
| Pengujian | tests/unit/test_acquisition.py; tests/unit/test_extensibility.py (baru); tests/integration/test_storage_pipeline.py |
| Fixture/config baru | tests/fixtures/simulated/catalog.json; config/simulated-plan.yaml |
| Evaluasi baru | scripts/benchmark_mapping.py; scripts/evaluate_extensibility.py |
| Dokumentasi | README.md; docs/baseline-audit.md; docs/source-onboarding.md; dokumen ini; tiga JSON hasil benchmark/evaluasi |
| Checklist lokal | .ai/Task.md (folder ini diabaikan oleh Git sesuai konfigurasi existing) |

Perubahan lokal sebelumnya pada `config/acquisition-plan.yaml`, `requirements.txt`,
`app.py`, dan `config/.backup.yaml` dipertahankan. Tes query lama diperbaiki agar
memeriksa isi/batas, bukan mengasumsikan selalu tepat delapan query.

## 3. Keputusan desain

Provider-specific factory dan mapping berada di composition root/adapter. Core
menggunakan registry; tes AST memeriksa tidak ada konstanta nama ketiga provider
pada lima modul core. Kapabilitas batas API dan kebijakan retry/interval dipisahkan.
Mapping deklaratif memiliki nested paths, alias, konversi, optional/required dan
default eksplisit. Optional type invalid pada mapping v2 menghasilkan reason code;
perilaku legacy dipertahankan untuk resume.

Canonical envelope memisahkan provenance dari typed listing payload. Listing saja
yang didukung saat ini; kategori dapat didefinisikan policy, tetapi currency masih
menggunakan subset konservatif yang sudah ada. Extension JSON mempertahankan atribut
sumber tanpa mengklaim semuanya telah distandardisasi secara semantik.

Identitas MART dan hash bisnis lama dipertahankan. Entity kind tersirat dalam tabel
listing dan eksplisit dalam canonical v2. Run berbeda boleh menambah observasi;
replay run sama tidak menambah snapshot. Perubahan extension saja tidak mengubah
hash bisnis; RAW masih mempertahankan setiap occurrence.

## 4. Migrasi dan kompatibilitas

Migrasi 0007 mengganti CHECK source dengan pola identifier aman. Tidak ada penghapusan
record, tabel, volume, bucket atau namespace. SQL migrasi diverifikasi secara offline;
eksekusinya pada PostgreSQL nyata belum dibuktikan. Gunakan `marketpipe init` pada
namespace milik pipeline setelah worker berhenti dan storage integration tersedia.

Plan baru mematok versi mapping, canonical dan endpoint. Plan lama tanpa pin memakai
legacy-1/CLEAN v1. Reader menerima v1 dan v2, menolak schema/version mismatch.
Verifikasi tambahan terhadap kode `git HEAD` sebelum refactor membuktikan bahwa
dua contoh provider dan Parquet kosong menghasilkan **byte v1 identik**, termasuk
model bisnisnya. Tes recovery juga memakai plan tanpa pin dan orphan CLEAN upload.
Detail migrasi ada pada [onboarding guide](source-onboarding.md).

## 5. Perintah dan hasil aktual

Semua perintah Python memakai `.venv/Scripts/python.exe` (Python 3.14.8).

| Perintah | Hasil |
| --- | --- |
| `-m pytest -q` sebelum perubahan | 33 passed, 1 failed, 2 skipped; 7.71 s |
| `-m pytest -q` setelah perubahan | 62 passed, 3 skipped; 5.64 s pada eksekusi terakhir |
| `-m ruff check src tests migrations scripts` | All checks passed |
| `-m ruff check .` | 18 temuan pada app.py lokal existing: 1 import ordering, 17 panjang baris |
| `scripts/evaluate_extensibility.py docs/evaluation-results.json` | Berhasil; metrik simulasi disimpan |
| `scripts/benchmark_mapping.py docs/benchmark-before.json` | Diambil sebelum perubahan kode core |
| `scripts/benchmark_mapping.py docs/benchmark-after.json` | Berhasil; workload sama sesudah perubahan |
| `docker ps --format ...` | Daemon Docker tidak tersedia; storage integration tidak dijalankan |

Tiga tes storage tersedia sebagai opt-in: eBay, Rakuten dan simulasi. HTTP provider
tetap mocked; namespace acak dipertahankan setelah pengujian. Tidak ada type checker
yang dikonfigurasi dalam pyproject, sehingga tidak ada klaim type-check pass.
Pemindaian pola secret pada file tracked dan kandidat non-ignored menghasilkan
0 temuan; `.env`/`.aws` tidak tracked. Ini bukan audit keamanan menyeluruh.

Pengujian mencakup tiga sumber bersamaan, isolasi namespace, duplicate/conflict,
observasi baru, replay, checksum corruption, label simulasi, schema evolution,
provenance, ownership checks, dan enam titik interupsi: setelah RAW, sebelum
manifest commit, setelah CLEAN, setelah QUARANTINE, sebelum/sesudah MART commit.
Tes offline memakai transaction rollback dan immutable S3 doubles; bukan bukti
atomicity infrastruktur nyata.

## 6. Metrik penelitian (SIMULASI)

Hasil dapat direproduksi melalui script evaluasi; socket jaringan diblokir.

| Metrik | Numerator/denominator | Hasil |
| --- | --- | --- |
| Canonical conformance sebelum deduplikasi | 3/5 occurrences | 60% |
| Mapping coverage pada nilai field dalam cakupan yang tersedia | 19/20 | 95% |
| Accepted | 2/5 | 40% |
| Quarantined | 2/5 | 40% |
| Excluded duplicate | 1/5 | 20% |
| CLEAN provenance ke manifest RAW | 2/2 | 100% |
| Shared core files berubah untuk onboarding sumber ketiga | 0 | Bukan jumlah file refactor awal |
| Modul implementasi provider tambahan | 1 | adapters/simulated.py |
| Fixture/config tambahan | 1 JSON + 1 YAML | Registrasi eksplisit melalui bootstrap tes |
| Record API nyata diperoleh dalam pekerjaan ini | 0 | Tidak ada live acquisition |

Mapping coverage menghitung keberhasilan konversi field yang hadir dalam tujuh
target mapping, termasuk pada record yang akhirnya ditolak karena field lain.
Field wajib yang hilang terhitung dalam conformance, bukan denominator field hadir.
Coverage bukan persentase record diterima dan tidak menghitung salinan RAW sebagai
standardisasi. Extension preservation dibuktikan melalui assertion nilai, bukan
dianggap sebagai tambahan canonical conformance.

Benchmark: lima sampel, masing-masing 10.000 mapping listing eBay sintetis identik,
stamp tetap, tanpa jaringan/storage. Median sebelum **0.464593 s / 21,524 record/s**;
sesudah **0.827150 s / 12,090 record/s**. Waktu transformasi naik sekitar **78%**;
throughput turun sekitar **44%**. Mapping deklaratif, validation tambahan dan
preservasi atribut menambah pekerjaan; benchmark ini tidak membuktikan penyebab
masing-masing biaya tanpa profiling. Network time **0 s** pada kedua pengukuran.
Angka mentah tersedia di benchmark-before.json dan benchmark-after.json.

## 7. Batasan dan ancaman validitas

- Fixture ketiga hanya lima occurrences terkontrol, bukan representasi marketplace.
- Adapter API diuji dengan MockTransport; akses, kuota, retention dan drift nyata
  tetap bergantung akun/provider. Dokumentasi resmi dirujuk pada onboarding guide.
- Storage doubles tidak membuktikan PostgreSQL transaction/MinIO race sebenarnya.
- Benchmark satu mesin/interpreter, warm-cache dan proses terpisah, bukan uji
  statistik lintas mesin atau benchmark end-to-end. Durasi pytest bukan speedup.
- Hanya listing/price observation didukung; tidak ada sales, matching produk lintas
  sumber, semantic mapping universal atau jaminan target 100.000 tercapai.
- Penambahan sumber diukur sebagai perubahan file, bukan estimasi jam onboarding.
- app.py masih memiliki lint existing; fungsionalitas UI tidak diubah oleh refactor.

## 8. Langkah berikutnya

Jalankan tiga tes integrasi pada runtime PostgreSQL/MinIO terisolasi setelah Docker
aktif; verifikasi migrasi 0007 dan recovery pada storage nyata sebelum penerapan
ke namespace dataset. Jalankan API pilot hanya melalui instruksi terpisah dengan
izin/kuota yang tersedia. Pertimbangkan profiling mapping bila throughput lokal
menjadi kebutuhan; perluas typed entity hanya dengan kontrak dan serving terpisah.
