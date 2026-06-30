#!/usr/bin/env python3
"""
Courier EWS Dashboard — Data Updater
======================================
Fetches the latest weekly cohort signals from Wolt Snowflake, computes
composite health scores, and rewrites the DATA_START/DATA_END block in
courier-ews.html.

Local usage:
    SNOWFLAKE_USER=you@wolt.com python scripts/update_data.py

CI usage:
    Triggered by .github/workflows/update-dashboard.yml every Monday.
    Requires non-SSO credentials stored as GitHub Secrets.
"""

import os, re, sys, math
from collections import defaultdict
from datetime import date, timedelta

# ─── Snowflake connection ────────────────────────────────────────────────────
try:
    import snowflake.connector
except ImportError:
    sys.exit("Missing dependency: pip install snowflake-connector-python")

SF_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT",   "doordash-ig78751_aws_eu_west_1")
SF_USER      = os.environ["SNOWFLAKE_USER"]           # required
SF_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD")   # set for service account auth
SF_PRIVATE_KEY_PATH = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")  # alternative: key-pair auth
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE",  "EXPLORATION_M")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",   "PRODUCTION")
SF_ROLE      = os.environ.get("SNOWFLAKE_ROLE",       "BASE_USER")

# ─── ⚠️  TODO: set this to the actual table / view ──────────────────────────
# Expected columns (all per country per cohort week):
#   COUNTRY_CODE       VARCHAR  — ISO-2 code (DE, FI, etc.)
#   COHORT_WEEK        DATE     — Monday of the cohort week
#   FTD_CONV_RATE      FLOAT    — % couriers completing 1st delivery within 7d
#   T7_DELIVERIES      FLOAT    — median deliveries in first 7 days (among converters)
#   FTD_EARNINGS       FLOAT    — avg gross earnings from first delivery (local currency)
#   T14_DUTY_HOURS     FLOAT    — avg active hours in first 14 days
#   TAR                FLOAT    — Task Acceptance Rate: % of task offers accepted
#                                (= 1 − offer_decline_rate). Higher = better.
#
# Retention columns (sourced from a separate retention table / join):
#   M2_RETENTION       FLOAT    — % of couriers with ≥1 delivery in days 30–60 after
#                                 their first delivery (= "second month" retention)
#   M3_RETENTION       FLOAT    — % of couriers with ≥1 delivery in days 60–90 after
#                                 their first delivery (= "third month" retention)
#
# M2 definition: a courier counts as retained at M2 if they completed at least
# one delivery in the window [first_delivery_date + 30 days, first_delivery_date + 60 days).
# SQL pattern:
#   SUM(CASE WHEN deliveries_in_days_30_60 >= 1 THEN 1 ELSE 0 END) * 100.0
#   / NULLIF(COUNT(DISTINCT courier_id), 0)  AS M2_RETENTION
COHORT_TABLE = os.environ.get(
    "COHORT_TABLE",
    "PRODUCTION.COURIER_ANALYTICS.WEEKLY_COHORT_SIGNALS"  # ← replace with real name
)

# ─── Static market metadata ───────────────────────────────────────────────────
MARKETS_META = {
    "DE": {"country": "Germany",     "flag": "🇩🇪"},
    "IL": {"country": "Israel",      "flag": "🇮🇱"},
    "DK": {"country": "Denmark",     "flag": "🇩🇰"},
    "EE": {"country": "Estonia",     "flag": "🇪🇪"},
    "FI": {"country": "Finland",     "flag": "🇫🇮"},
    "IS": {"country": "Iceland",     "flag": "🇮🇸"},
    "LT": {"country": "Lithuania",   "flag": "🇱🇹"},
    "LU": {"country": "Luxembourg",  "flag": "🇱🇺"},
    "LV": {"country": "Latvia",      "flag": "🇱🇻"},
    "NO": {"country": "Norway",      "flag": "🇳🇴"},
    "SE": {"country": "Sweden",      "flag": "🇸🇪"},
    "AT": {"country": "Austria",     "flag": "🇦🇹"},
    "CZ": {"country": "Czechia",     "flag": "🇨🇿"},
    "HR": {"country": "Croatia",     "flag": "🇭🇷"},
    "HU": {"country": "Hungary",     "flag": "🇭🇺"},
    "PL": {"country": "Poland",      "flag": "🇵🇱"},
    "RS": {"country": "Serbia",      "flag": "🇷🇸"},
    "SK": {"country": "Slovakia",    "flag": "🇸🇰"},
    "SI": {"country": "Slovenia",    "flag": "🇸🇮"},
    "GR": {"country": "Greece",      "flag": "🇬🇷"},
    "CY": {"country": "Cyprus",      "flag": "🇨🇾"},
    "MT": {"country": "Malta",       "flag": "🇲🇹"},
    "AZ": {"country": "Azerbaijan",  "flag": "🇦🇿"},
    "GE": {"country": "Georgia",     "flag": "🇬🇪"},
    "KZ": {"country": "Kazakhstan",  "flag": "🇰🇿"},
}

# Signal definitions
SIGNAL_KEYS = ["ftdConv", "deliveries", "ftdEarn", "dutyHours", "tar"]
SIGNAL_UNIT = {"ftdConv": "%", "deliveries": "", "ftdEarn": "", "dutyHours": "h", "tar": "%"}
SIGNAL_HB   = {"ftdConv": True, "deliveries": True, "ftdEarn": True, "dutyHours": True, "tar": True}
# Column index in query result (0=country, 1=week, 2..6=signals)
COL_IDX = {"ftdConv": 2, "deliveries": 3, "ftdEarn": 4, "dutyHours": 5, "tar": 6}

# Scoring weights (must sum to 1.0)
WEIGHTS = {"ftdConv": 0.333, "deliveries": 0.222, "ftdEarn": 0.167, "dutyHours": 0.167, "tar": 0.111}  # sums to 1.0; auto-normalised below
# Remaining 10% (onboarding completion — not yet in pipeline) distributed proportionally
_w_sum = sum(WEIGHTS.values())
WEIGHTS = {k: v / _w_sum for k, v in WEIGHTS.items()}


# ─── Scoring helpers ─────────────────────────────────────────────────────────

def signal_score(v: float, b: float, hb: bool) -> float:
    """Score a single signal 0–100 vs its 8-week median baseline."""
    if b == 0:
        return 50.0
    delta_pct = (v - b) / abs(b) * 100
    if not hb:
        delta_pct = -delta_pct          # invert: lower value = worse (not used for TAR since hb=True)
    if delta_pct >= 0:
        return 100.0
    elif delta_pct >= -15:
        return 100.0 + (delta_pct / 15.0 * 50.0)   # 100 → 50
    else:
        return max(0.0, 50.0 + ((delta_pct + 15.0) / 15.0 * 50.0))   # 50 → 0


def composite_score(signals: dict) -> float:
    return round(sum(WEIGHTS[k] * signal_score(s["v"], s["b"], s["hb"])
                     for k, s in signals.items()), 1)


def market_status(score: float, signals: dict) -> str:
    bad = sum(
        1 for k, s in signals.items()
        if (s["hb"] and s["d"] < 0 and abs(s["d"]) / max(s["b"], 0.01) > 0.02)
        or (not s["hb"] and s["d"] > 0 and abs(s["d"]) / max(s["b"], 0.01) > 0.02)
    )
    if score < 50 or bad >= 3:
        return "critical"
    if score < 70 or bad >= 2:
        return "watch"
    return "stable"


def derive_driver_hypo(signals: dict, status: str):
    if status == "stable":
        return "—", "All signals within normal variance relative to the 8-week market baseline.", None

    DRIVER_LABELS = {
        "ftdConv":     "Low FTD conv.",
        "deliveries":  "Low T7 trips",
        "ftdEarn":     "Low earnings",
        "dutyHours":   "Low duty hours",
        "tar":         "Low TAR",
    }
    HYPOS = {
        "ftdConv":     "FTD conversion is below baseline — new couriers are activating but not completing a first delivery within 7 days. Check onboarding friction, equipment availability, and zone coverage.",
        "deliveries":  "Median trips in first 7 days are below baseline — couriers who start are not building a delivery habit. Consider early-trip nudges or earnings transparency improvements.",
        "ftdEarn":     "Earnings from the first delivery are below expectations — may indicate unfavourable zone assignment, low order density, or short-distance first orders. Review order allocation for new couriers.",
        "dutyHours":   "Active duty hours in the first 14 days are low — couriers are not engaging deeply after onboarding. Check shift availability and scheduling UX.",
        "tar":         "Task acceptance rate (TAR) is below baseline — couriers are rejecting more task offers than usual. Check offer quality (distance, pay), zone demand-supply balance, and recent weather or event effects.",
    }

    bad = sorted(
        [(k, abs(s["d"]) / max(s["b"], 0.01))
         for k, s in signals.items()
         if (s["hb"] and s["d"] < 0) or (not s["hb"] and s["d"] > 0)],
        key=lambda x: x[1], reverse=True
    )

    if not bad:
        return "—", "Signal deviation is modest — monitor for further deterioration.", 5

    top = bad[0][0]
    drivers = " + ".join(DRIVER_LABELS[k] for k, _ in bad[:2])
    hypo = HYPOS.get(top, "Multiple signals below baseline — investigate with the local market team.")
    m1weeks = 3 if status == "critical" else 5
    return drivers, hypo, m1weeks


# ─── JS formatting ────────────────────────────────────────────────────────────

def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def format_js_market(mid, meta, signals, score, score_delta, status,
                     driver, hypo, m1weeks, trend, trend_labels, data_week) -> str:
    sig_parts = ", ".join(
        f"{k}:{{v:{s['v']},b:{s['b']},d:{s['d']},u:'{s['u']}',hb:{'true' if s['hb'] else 'false'}}}"
        for k, s in signals.items()
    )
    trend_js   = "[" + ",".join(str(v) for v in trend) + "]"
    labels_js  = "['" + "','".join(trend_labels) + "']"
    m1w        = "null" if m1weeks is None else str(m1weeks)
    lever      = "No intervention needed" if status == "stable" else f"Investigate {driver.lower()}"
    return (
        f"  {{id:'{mid}',country:'{meta['country']}',flag:'{meta['flag']}'"
        f",status:'{status}',score:{score},scoreDelta:{score_delta},\n"
        f"   signals:{{{sig_parts}}},\n"
        f"   driver:\"{_esc(driver)}\",hypo:\"{_esc(hypo)}\","
        f"lever:\"{_esc(lever)}\",m1weeks:{m1w},\n"
        f"   trend:{trend_js},trendLabels:{labels_js},dataWeek:'{data_week}'}}"
    )


# ─── Snowflake fetch ──────────────────────────────────────────────────────────

def connect():
    kwargs = dict(
        account=SF_ACCOUNT, user=SF_USER,
        warehouse=SF_WAREHOUSE, database=SF_DATABASE, role=SF_ROLE,
    )
    if SF_PRIVATE_KEY_PATH:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        with open(SF_PRIVATE_KEY_PATH, "rb") as f:
            p_key = load_pem_private_key(
                f.read(),
                password=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").encode() or None,
                backend=default_backend()
            )
        from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
        kwargs["private_key"] = p_key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())
    else:
        kwargs["password"] = SF_PASSWORD
    return snowflake.connector.connect(**kwargs)


def fetch_data(conn):
    ids_sql = ", ".join(f"'{k}'" for k in MARKETS_META)
    sql = f"""
        SELECT
            COUNTRY_CODE,
            COHORT_WEEK,
            FTD_CONV_RATE,
            T7_DELIVERIES,
            FTD_EARNINGS,
            T14_DUTY_HOURS,
            TAR  -- Task Acceptance Rate (% offers accepted). If your table stores offer_decline_rate, use: (1 - OFFER_DECLINE_RATE) * 100 AS TAR
        FROM {COHORT_TABLE}
        WHERE COHORT_WEEK >= DATEADD(week, -10, (SELECT MAX(COHORT_WEEK) FROM {COHORT_TABLE}))
          AND COUNTRY_CODE IN ({ids_sql})
        ORDER BY COUNTRY_CODE, COHORT_WEEK
    """
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()

    # Get latest week
    latest_week = max(r[1] for r in rows) if rows else date.today()
    return rows, latest_week


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to Snowflake…")
    conn = connect()

    print(f"Querying {COHORT_TABLE}…")
    rows, latest_week = fetch_data(conn)
    conn.close()

    data_week = latest_week.strftime("%Y-%m-%d")
    print(f"Latest cohort week: {data_week} — {len(rows)} rows fetched")

    # Group rows by market
    by_market = defaultdict(list)
    for row in rows:
        by_market[row[0]].append(row)

    # Per-market processing
    markets_js = []
    for mid in MARKETS_META:
        meta  = MARKETS_META[mid]
        weeks = sorted(by_market.get(mid, []), key=lambda r: r[1])
        if not weeks:
            print(f"  ⚠️  No data for {mid} — skipping")
            continue

        curr          = next((r for r in weeks if r[1] == latest_week), weeks[-1])
        baseline_rows = [r for r in weeks if r[1] < latest_week][-8:]

        signals = {}
        for k in SIGNAL_KEYS:
            ci  = COL_IDX[k]
            v   = float(curr[ci] or 0)
            bvs = [float(r[ci]) for r in baseline_rows if r[ci] is not None]
            b   = sum(bvs) / len(bvs) if bvs else v
            signals[k] = {
                "v":  round(v, 1),
                "b":  round(b, 1),
                "d":  round(v - b, 2),
                "u":  SIGNAL_UNIT[k],
                "hb": SIGNAL_HB[k],
            }

        score       = composite_score(signals)
        status      = market_status(score, signals)
        driver, hypo, m1weeks = derive_driver_hypo(signals, status)

        # Trend: last 8 weeks of FTD conversion
        trend_data   = [(r[1], float(r[2])) for r in weeks if r[2] is not None][-8:]
        trend_vals   = [round(t[1], 1) for t in trend_data]
        trend_labels = [t[0].strftime("%m/%d") for t in trend_data]

        markets_js.append(format_js_market(
            mid, meta, signals, score, 0, status,
            driver, hypo, m1weeks,
            trend_vals, trend_labels, data_week
        ))
        print(f"  {meta['flag']} {mid}: score={score} status={status}")

    # Build replacement block
    new_block = (
        "// DATA_START — auto-generated by scripts/update_data.py — do not edit manually\n"
        "const MARKETS = [\n"
        + ",\n".join(markets_js)
        + "];\n"
        "// DATA_END"
    )

    # Patch the HTML file
    html_path = os.path.join(os.path.dirname(__file__), "..", "courier-ews.html")
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    pattern = r"// DATA_START.*?// DATA_END"
    new_html, n = re.subn(pattern, new_block, html, flags=re.DOTALL)
    if n != 1:
        sys.exit(f"❌ Expected 1 DATA_START/DATA_END marker pair in HTML, found {n}")

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(new_html)

    print(f"\n✅ courier-ews.html updated — {len(markets_js)} markets for week {data_week}")


if __name__ == "__main__":
    main()
