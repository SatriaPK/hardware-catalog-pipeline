import os
import re

import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from pathlib import Path

from dotenv import load_dotenv

_here = Path(__file__).resolve().parent
for _candidate in (_here / ".env", _here.parent / ".env", Path.cwd() / ".env"):
    if _candidate.is_file():
        load_dotenv(_candidate, override=False)

st.set_page_config(page_title="Hardware Catalog Dashboard", page_icon="🖥️", layout="wide")

NS_RE = re.compile(r"^mp_acq_[a-z0-9]{3,24}$")
ALL = "Semua"


@st.cache_resource
def get_engine(url: str):
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"options": "-c default_transaction_read_only=on"},
    )


@st.cache_data(ttl=60, show_spinner=False)
def run_query(url: str, sql: str, params: tuple) -> pd.DataFrame:
    with get_engine(url).connect() as conn:
        return pd.read_sql(text(sql), conn, params=dict(params))


# ------------------------------------------------------------------ sidebar
with st.sidebar:
    st.title("Pengaturan")
    db_host = st.text_input(
        "DB_HOST", value=os.getenv("DB_HOST", "localhost"),
        help="Pakai localhost bila dijalankan di komputer sendiri, atau nama service Postgres bila lewat Docker Compose.",
    )
    db_port = st.text_input("DB_PORT", value=os.getenv("DB_PORT", "5432"))
    db_name = st.text_input("DB_NAME", value=os.getenv("DB_NAME", ""))
    db_user = st.text_input("DB_USER", value=os.getenv("DB_USER", ""))
    db_password = st.text_input("DB_PASSWORD", value=os.getenv("DB_PASSWORD", ""), type="password")
    namespace = st.text_input("Namespace", value=os.getenv("PIPELINE_NAMESPACE", "mp_acq_dataset"))
    target = st.number_input("Target listing unik", min_value=1, value=100_000, step=1_000)
    if st.button("Muat ulang data"):
        st.cache_data.clear()
    st.caption("Data di-cache 60 detik supaya database tidak terbebani saat collect berjalan.")

if not (db_name and db_user and db_password) or not db_port.isdigit():
    st.info("Lengkapi DB_NAME, DB_USER, DB_PASSWORD, dan DB_PORT di sidebar atau lewat environment variable.")
    st.stop()
db_url = URL.create(
    "postgresql+psycopg2",
    username=db_user,
    password=db_password,
    host=db_host,
    port=int(db_port),
    database=db_name,
).render_as_string(hide_password=False)
if not NS_RE.match(namespace):
    st.error("Namespace harus berbentuk mp_acq_ diikuti 3 sampai 24 huruf kecil atau angka.")
    st.stop()


def Q(sql: str, **params) -> pd.DataFrame:
    """Jalankan query read-only; {ns} diganti namespace yang sudah divalidasi."""
    try:
        return run_query(db_url, sql.replace("{ns}", namespace), tuple(sorted(params.items())))
    except Exception as exc:  # tampilkan ringkas, jangan bocorkan kredensial
        st.error(f"Query gagal: {str(exc).splitlines()[0][:300]}")
        return pd.DataFrame()


st.title("Hardware Catalog Dashboard")
st.caption(
    "Gambaran dari data eBay dan Rakuten yang sudah melewati cleaning dan deduplication. "
    "Harga adalah harga listing di marketplace."
)

summary = Q(
    "SELECT provider, marketplace_id, count(*) AS listings "
    "FROM {ns}.dim_listing GROUP BY 1, 2 ORDER BY 3 DESC"
)
providers = [ALL] + sorted(summary["provider"].unique().tolist()) if not summary.empty else [ALL]

tab_progress, tab_catalog, tab_price, tab_search, tab_move, tab_quality = st.tabs(
    ["Progres target", "Isi katalog", "Sebaran harga", "Cari produk", "Pergerakan harga", "Kualitas data"]
)

# ------------------------------------------------------------ 1. progres
with tab_progress:
    st.subheader("Target Progress")
    total = int(summary["listings"].sum()) if not summary.empty else 0
    snap = Q("SELECT count(*) AS n FROM {ns}.fact_price_snapshot")
    snapshots = int(snap["n"].iloc[0]) if not snap.empty else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Listing unik valid", f"{total:,}")
    c2.metric("Pencapaian target", f"{total / target:.1%}")
    c3.metric("Snapshot harga", f"{snapshots:,}")
    st.progress(min(total / target, 1.0))
    st.caption("Target dihitung dari listing unik.")

    pace = Q(
        "SELECT count(*) FILTER (WHERE first_seen_at > now() - interval '24 hours') AS last_24h "
        "FROM {ns}.dim_listing"
    )
    if not pace.empty:
        per_day = int(pace["last_24h"].iloc[0])
        remaining = max(target - total, 0)
        if remaining == 0:
            st.success("Target sudah terlampaui.")
        elif per_day > 0:
            st.info(
                f"Dalam 24 jam terakhir bertambah {per_day:,} listing unik. "
                f"Dengan laju itu, sisa {remaining:,} listing butuh sekitar {remaining / per_day:.1f} hari."
            )
        else:
            st.warning("Belum ada listing baru dalam 24 jam terakhir. Cek apakah collect masih berjalan.")

    if not summary.empty:
        left, right = st.columns([1, 2])
        left.dataframe(summary, hide_index=True, use_container_width=True)
        growth = Q(
            "SELECT date_trunc('hour', first_seen_at) AS jam, provider, count(*)::float8 AS baru "
            "FROM {ns}.dim_listing GROUP BY 1, 2 ORDER BY 1"
        )
        if not growth.empty:
            wide = growth.pivot_table(index="jam", columns="provider", values="baru", fill_value=0).cumsum()
            right.markdown("**Pertumbuhan kumulatif listing unik per jam**")
            right.line_chart(wide)

    st.markdown("**Ringkasan setiap run**")
    runs = Q(
        """
        SELECT r.run_id, r.source, r.status, r.started_at,
               p.fetched_records, p.accepted, p.quarantined, p.excluded_duplicates,
               p.raw_objects, p.http_attempts, r.error_summary
        FROM {ns}.pipeline_run r
        JOIN {ns}.acquisition_progress p USING (run_id)
        ORDER BY r.started_at DESC LIMIT 50
        """
    )
    st.dataframe(runs, hide_index=True, use_container_width=True)
    st.caption("Status run berasal dari checkpoint terakhir.")

# ------------------------------------------------------------ 2. katalog
with tab_catalog:
    st.subheader("Isi Katalog")
    comp = Q(
        "SELECT provider, marketplace_id, category_canonical AS kategori, count(*) AS listing_unik "
        "FROM {ns}.dim_listing GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC"
    )
    if comp.empty:
        st.write("Belum ada data.")
    else:
        chart = comp.pivot_table(index="kategori", columns="provider", values="listing_unik", fill_value=0)
        st.bar_chart(chart.astype(float))
        st.dataframe(comp, hide_index=True, use_container_width=True)
        st.caption("Kategori berasal dari pemetaan query pipeline.")

# ------------------------------------------------------------ 3. harga
with tab_price:
    st.subheader("Sebaran Harga")
    cur_df = Q(
        "SELECT currency_code, count(*) AS n FROM {ns}.fact_price_snapshot GROUP BY 1 ORDER BY 2 DESC"
    )
    cat_df = Q("SELECT DISTINCT category_canonical AS c FROM {ns}.dim_listing ORDER BY 1")
    if cur_df.empty:
        st.write("Belum ada snapshot harga.")
    else:
        f1, f2 = st.columns(2)
        currency = f1.selectbox("Mata uang", cur_df["currency_code"].tolist())
        category = f2.selectbox("Kategori", [ALL] + cat_df["c"].dropna().tolist())

        latest_cte = """
            WITH latest AS (
                SELECT DISTINCT ON (listing_key) listing_key, price_amount
                FROM {ns}.fact_price_snapshot
                WHERE currency_code = :cur
                ORDER BY listing_key, observed_at DESC, snapshot_key DESC
            )
        """
        stats = Q(
            latest_cte
            + """
            SELECT l.provider, l.category_canonical AS kategori, count(*) AS listing,
                   min(s.price_amount)::float8 AS minimum,
                   (percentile_cont(0.25) WITHIN GROUP (ORDER BY s.price_amount))::float8 AS p25,
                   (percentile_cont(0.5)  WITHIN GROUP (ORDER BY s.price_amount))::float8 AS median,
                   (percentile_cont(0.75) WITHIN GROUP (ORDER BY s.price_amount))::float8 AS p75,
                   max(s.price_amount)::float8 AS maksimum
            FROM latest s JOIN {ns}.dim_listing l USING (listing_key)
            WHERE (:cat = 'Semua' OR l.category_canonical = :cat)
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
            cur=currency, cat=category,
        )
        st.dataframe(stats.round(2), hide_index=True, use_container_width=True)

        prices = Q(
            latest_cte
            + """
            SELECT l.provider, s.price_amount::float8 AS price
            FROM latest s JOIN {ns}.dim_listing l USING (listing_key)
            WHERE (:cat = 'Semua' OR l.category_canonical = :cat)
            LIMIT 200000
            """,
            cur=currency, cat=category,
        )
        if not prices.empty:
            cap = float(prices["price"].quantile(0.99))
            bins = np.linspace(0, cap if cap > 0 else 1, 31)
            hist = {}
            for prov, grp in prices.groupby("provider"):
                counts, _ = np.histogram(grp["price"].clip(upper=cap), bins=bins)
                hist[prov] = counts
            hdf = pd.DataFrame(hist, index=[f"{b:,.0f}" for b in bins[:-1]])
            st.markdown(f"**Histogram harga terbaru per listing ({currency})**")
            st.bar_chart(hdf.astype(float))
            st.caption(
                "Harga di atas persentil ke-99 digabung ke kotak terakhir agar grafik terbaca. "
                "Statistik ini mencampur model, kondisi, dan penjual yang berbeda."
                "Mata uang berbeda tidak boleh dibandingkan langsung."
            )

# ------------------------------------------------------------ 4. cari
with tab_search:
    st.subheader("Pencarian Listing & Riwayat Harga")
    s1, s2, s3 = st.columns([2, 1, 1])
    kw = s1.text_input("Kata pada judul", value="RTX 4060")
    s_prov = s2.selectbox("Sumber", providers, key="sprov")
    s_cat = s3.selectbox("Kategori ", [ALL] + (cat_df["c"].dropna().tolist() if not cat_df.empty else []))

    if kw.strip():
        found = Q(
            """
            SELECT l.listing_key, l.provider, l.source_listing_id, l.title_original,
                   l.category_canonical, s.price_amount::float8 AS harga_terbaru,
                   s.currency_code, s.observed_at AS diamati
            FROM {ns}.dim_listing l
            LEFT JOIN LATERAL (
                SELECT price_amount, currency_code, observed_at
                FROM {ns}.fact_price_snapshot f
                WHERE f.listing_key = l.listing_key
                ORDER BY observed_at DESC, snapshot_key DESC LIMIT 1
            ) s ON true
            WHERE strpos(lower(l.title_original), lower(:kw)) > 0
              AND (:prov = 'Semua' OR l.provider = :prov)
              AND (:cat = 'Semua' OR l.category_canonical = :cat)
            ORDER BY s.observed_at DESC NULLS LAST
            LIMIT 200
            """,
            kw=kw.strip(), prov=s_prov, cat=s_cat,
        )
        st.write(f"{len(found)} listing ditampilkan (maksimum 200).")
        st.dataframe(found.drop(columns=["listing_key"]), hide_index=True, use_container_width=True)
        st.caption("Judul yang mirip belum tentu produk yang sama. Hasil bisa memuat aksesori atau varian lain.")

        if not found.empty:
            labels = {
                r.listing_key: f"[{r.provider}] {str(r.title_original)[:90]}" for r in found.itertuples()
            }
            pick = st.selectbox("Lihat riwayat harga", list(labels), format_func=lambda k: labels[k])
            hist_df = Q(
                """
                SELECT s.observed_at, s.price_amount::float8 AS harga, s.currency_code,
                       s.run_id, r.object_bucket, r.object_key
                FROM {ns}.fact_price_snapshot s
                LEFT JOIN {ns}.ingest_request r
                  ON r.request_id = s.raw_request_id AND r.run_id = s.run_id
                WHERE s.listing_key = :lk
                ORDER BY s.observed_at
                """,
                lk=str(pick),
            )
            if not hist_df.empty:
                if hist_df["currency_code"].nunique() == 1:
                    st.line_chart(hist_df.set_index("observed_at")["harga"])
                    st.caption(f"Mata uang: {hist_df['currency_code'].iloc[0]}")
                else:
                    st.warning("Listing ini punya lebih dari satu mata uang, grafik tidak ditampilkan.")
                st.markdown("**Asal data (file RAW di MinIO)**")
                st.dataframe(hist_df, hide_index=True, use_container_width=True)

# ------------------------------------------------------------ 5. pergerakan
with tab_move:
    st.subheader("Pergerakan Harga")
    multi = Q(
        "SELECT count(*) AS n FROM (SELECT listing_key FROM {ns}.fact_price_snapshot "
        "GROUP BY listing_key HAVING count(*) > 1) t"
    )
    n_multi = int(multi["n"].iloc[0]) if not multi.empty else 0
    st.metric("Listing dengan 2 observasi atau lebih", f"{n_multi:,}")
    min_pct = st.slider("Perubahan minimum (persen, nilai mutlak)", 0.0, 50.0, 5.0, 0.5)
    moves = Q(
        """
        WITH history AS (
            SELECT listing_key, snapshot_key, currency_code, observed_at, price_amount,
                   lag(price_amount) OVER (
                       PARTITION BY listing_key, currency_code ORDER BY observed_at, snapshot_key
                   ) AS previous_price
            FROM {ns}.fact_price_snapshot
        ), changes AS (
            SELECT *, round(100.0 * (price_amount - previous_price) / NULLIF(previous_price, 0), 2) AS pct
            FROM history WHERE previous_price IS NOT NULL AND price_amount <> previous_price
        )
        SELECT l.provider, l.source_listing_id, l.title_original, c.currency_code,
               c.observed_at, c.previous_price::float8 AS harga_sebelum,
               c.price_amount::float8 AS harga_sesudah, c.pct::float8 AS perubahan_persen
        FROM changes c JOIN {ns}.dim_listing l USING (listing_key)
        WHERE abs(c.pct) >= :minpct
        ORDER BY c.observed_at DESC LIMIT 200
        """,
        minpct=float(min_pct),
    )
    if moves.empty:
        st.info(
            "Belum ada perubahan yang memenuhi batas. Hasil kosong wajar bila tiap listing baru "
            "diamati sekali atau harganya belum berubah."
        )
    else:
        st.dataframe(moves, hide_index=True, use_container_width=True)
    st.caption("Perbandingan hanya untuk listing yang sama dalam mata uang yang sama.")

# ------------------------------------------------------------ 6. kualitas
with tab_quality:
    st.subheader("Kualitas Data")
    qual = Q(
        """
        SELECT r.source, o.run_id, count(*) AS diproses,
               count(*) FILTER (WHERE o.outcome = 'accepted') AS accepted,
               count(*) FILTER (WHERE o.outcome = 'quarantined') AS quarantined,
               count(*) FILTER (WHERE o.outcome = 'excluded_duplicate') AS duplikat,
               round(100.0 * count(*) FILTER (WHERE o.outcome = 'quarantined')
                     / NULLIF(count(*), 0), 2)::float8 AS persen_karantina,
               round(100.0 * count(*) FILTER (WHERE o.outcome = 'excluded_duplicate')
                     / NULLIF(count(*), 0), 2)::float8 AS persen_duplikat
        FROM {ns}.acquisition_occurrence o
        JOIN {ns}.pipeline_run r USING (run_id)
        GROUP BY 1, 2 ORDER BY 1, 2
        """
    )
    if qual.empty:
        st.write("Belum ada occurrence yang tercatat.")
    else:
        q1, q2 = st.columns(2)
        q1.markdown("**Hasil pemrosesan per run**")
        q1.bar_chart(qual.set_index("run_id")[["accepted", "quarantined", "duplikat"]].astype(float))
        q2.markdown("**Tabel rinci**")
        q2.dataframe(qual, hide_index=True, use_container_width=True)
        st.caption("Penyebut adalah record yang sudah diproses pada run itu, termasuk duplikat.")

    st.markdown("**Alasan record masuk karantina**")
    reasons = Q(
        """
        SELECT r.source, reason.code AS alasan, count(*) AS jumlah
        FROM {ns}.acquisition_occurrence o
        JOIN {ns}.pipeline_run r USING (run_id)
        CROSS JOIN LATERAL jsonb_array_elements_text(o.reason_codes) AS reason(code)
        WHERE o.outcome = 'quarantined'
        GROUP BY 1, 2 ORDER BY 3 DESC
        """
    )
    if reasons.empty:
        st.write("Belum ada record karantina.")
    else:
        st.bar_chart(reasons.pivot_table(index="alasan", columns="source", values="jumlah", fill_value=0).astype(float))
        st.dataframe(reasons, hide_index=True, use_container_width=True)
        st.caption("Satu record bisa punya beberapa alasan, jadi totalnya bisa melebihi jumlah record karantina.")

    st.markdown("**Halaman yang belum selesai diproses**")
    pending = Q(
        """
        SELECT p.run_id, p.query_id, p.page_number, p.status AS status_halaman,
               r.status AS status_raw, r.rows_reported, r.object_key
        FROM {ns}.acquisition_page p
        LEFT JOIN {ns}.ingest_request r USING (request_id)
        WHERE p.status <> 'completed'
        ORDER BY p.run_id, p.query_id, p.page_number LIMIT 100
        """
    )
    if pending.empty:
        st.success("Semua halaman yang tercatat sudah selesai.")
    else:
        st.dataframe(pending, hide_index=True, use_container_width=True)
        st.caption("Pemulihan dilakukan dengan menjalankan ulang perintah collect yang sama.")