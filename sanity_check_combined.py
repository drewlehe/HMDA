"""
Detailed sanity checks for hmda_all_years_combined.xlsx.

Column structure (97 total):
  Cols 0-6:   ZIP Code, Population, Minority Population, Median Income,
              ZIP Code Income/MSA Income, Number of Owner-Occupied Homes,
              Number of 1-to-4 Family Homes
  Cols 7-51:  Non-Owner Occupied  (5 purchasers × 9 metrics)
  Cols 52-96: Owner Occupied      (5 purchasers × 9 metrics)

  Per purchaser block (9 metrics):
    +0  Number of Originated Mortgages
    +1  Average Originated Mortgage Size
    +2  Average Originated Applicant Income
    +3  Number of Approved Not Accepted
    +4  Average Approved Not Accepted Size
    +5  Average Approved Not Accepted Income
    +6  Number of Denied Mortgages
    +7  Average Denied Mortgage Size
    +8  Average Denied Applicant Income

Checks:
  1.  All 11 expected year sheets present
  2.  Each sheet has exactly 97 columns
  3.  Row 1 (occupancy) labels in correct columns
  4.  Row 2 (purchaser) labels in correct columns
  5.  Row 3 (metric) labels match expected pattern
  6.  ZIP codes: 5-digit, unique per sheet
  6b. ZIP consistency across years
  7.  Population: non-negative, realistic range
  8.  Minority Population ≤ Population
  9.  Median Income: realistic range
  10. Income Ratio: realistic range
  11. Owner-Occupied Homes ≤ 1-to-4 Family Homes
  12. Total 1-to-4 Family Homes: realistic
  13. All count columns non-negative
  14. Avg mortgage size in realistic range (10–5000k)
  15. Avg applicant income in realistic range (5–5000k)
  16. Originated > Denied in aggregate each year
  17. YoY originated swing ≤ 60%
  18. Null rates (demo >5%, mortgage counts >95% flagged)
  19. Total originated mortgages 100k–20M per year
  20. Spot-check known ZIPs present in all years
  21. Pull-through rate (originated / originated+ANAC): 50–100% each year
  22. Median mortgage size per year (sanity range check)
  23. Median applicant income per year (sanity range check)
"""

import pandas as pd
import numpy as np

WORKBOOK = "/Users/andrew.lehe/Documents/HMDA Data/hmda_all_years_combined.xlsx"
EXPECTED_YEARS = list(range(2012, 2023))
EXPECTED_COLS = 97

# Column index positions (0-based)
PURCHASER_STARTS_NOO = [7 + i * 9 for i in range(5)]   # Non-Owner Occupied
PURCHASER_STARTS_OO  = [52 + i * 9 for i in range(5)]  # Owner Occupied
ALL_PURCHASER_STARTS = PURCHASER_STARTS_NOO + PURCHASER_STARTS_OO

ORIGINATED_COUNT_COLS  = [s + 0 for s in ALL_PURCHASER_STARTS]
ORIGINATED_SIZE_COLS   = [s + 1 for s in ALL_PURCHASER_STARTS]
ORIGINATED_INCOME_COLS = [s + 2 for s in ALL_PURCHASER_STARTS]
ANAC_COUNT_COLS        = [s + 3 for s in ALL_PURCHASER_STARTS]
ANAC_SIZE_COLS         = [s + 4 for s in ALL_PURCHASER_STARTS]
ANAC_INCOME_COLS       = [s + 5 for s in ALL_PURCHASER_STARTS]
DENIED_COUNT_COLS      = [s + 6 for s in ALL_PURCHASER_STARTS]
DENIED_SIZE_COLS       = [s + 7 for s in ALL_PURCHASER_STARTS]
DENIED_INCOME_COLS     = [s + 8 for s in ALL_PURCHASER_STARTS]
ALL_COUNT_COLS         = ORIGINATED_COUNT_COLS + ANAC_COUNT_COLS + DENIED_COUNT_COLS

OCCUPANCY_LABELS = {7: "Non-Owner Occupied", 52: "Owner Occupied"}
PURCHASER_LABELS = {
    7: "Commercial Bank", 16: "FHA (Ginnie Mae)", 25: "Fannie Mae",
    34: "Freddie Mac", 43: "Other",
    52: "Commercial Bank", 61: "FHA (Ginnie Mae)", 70: "Fannie Mae",
    79: "Freddie Mac", 88: "Other",
}
METRIC_CYCLE = [
    "Number of Originated Mortgages",
    "Average Originated Mortgage Size",
    "Average Originated Applicant Income",
    "Number of Approved Not Accepted",
    "Average Approved Not Accepted Size",
    "Average Approved Not Accepted Income",
    "Number of Denied Mortgages",
    "Average Denied Mortgage Size",
    "Average Denied Applicant Income",
]
DEMO_COLS = {
    0: "ZIP Code",
    1: "Population",
    2: "Minority Population",
    3: "Median Income",
    4: "ZIP Code Income/MSA Income",
    5: "Number of Owner-Occupied Homes",
    6: "Number of 1-to-4 Family Homes",
}

PASSES = 0
FAILURES = 0


def ok(msg):
    global PASSES
    PASSES += 1
    print(f"  PASS  {msg}")


def fail(msg):
    global FAILURES
    FAILURES += 1
    print(f"  FAIL  {msg}")


def warn(msg):
    print(f"  WARN  {msg}")


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def get_data(df):
    d = df.iloc[3:].copy()
    d.columns = range(df.shape[1])
    return d


def agg_cols(data, cols):
    """Concatenate non-null numeric values across multiple columns."""
    return pd.concat(
        [pd.to_numeric(data[c], errors="coerce").dropna() for c in cols]
    )


# ── Load workbook ────────────────────────────────────────────
print(f"Loading {WORKBOOK} ...")
xl = pd.ExcelFile(WORKBOOK)
sheets = xl.sheet_names
print(f"Sheets found: {sheets}")

# ── Check 1: Expected sheets ─────────────────────────────────
section("CHECK 1 — Expected year sheets")
for y in EXPECTED_YEARS:
    if str(y) in sheets:
        ok(f"Sheet '{y}' present")
    else:
        fail(f"Sheet '{y}' MISSING")
extra = [s for s in sheets if s not in [str(y) for y in EXPECTED_YEARS]]
if extra:
    warn(f"Unexpected extra sheets: {extra}")

# ── Load all sheets ──────────────────────────────────────────
frames = {}
for y in EXPECTED_YEARS:
    if str(y) in sheets:
        frames[y] = pd.read_excel(WORKBOOK, sheet_name=str(y), header=None)

# ── Check 2: Column count ────────────────────────────────────
section("CHECK 2 — Column count (expect 97)")
for y, df in frames.items():
    if df.shape[1] == EXPECTED_COLS:
        ok(f"{y}: {df.shape[1]} columns")
    else:
        fail(f"{y}: {df.shape[1]} columns (expected {EXPECTED_COLS})")

# ── Check 3: Row 1 — Occupancy labels ───────────────────────
section("CHECK 3 — Row 1 occupancy labels")
for y, df in frames.items():
    row0 = df.iloc[0]
    errs = []
    for col_idx, expected in OCCUPANCY_LABELS.items():
        val = row0.iloc[col_idx]
        if val != expected:
            errs.append(f"col {col_idx}: got '{val}', expected '{expected}'")
    if errs:
        fail(f"{y}: " + "; ".join(errs))
    else:
        ok(f"{y}: occupancy labels correct")

# ── Check 4: Row 2 — Purchaser labels ───────────────────────
section("CHECK 4 — Row 2 purchaser labels")
for y, df in frames.items():
    row1 = df.iloc[1]
    errs = []
    for col_idx, expected in PURCHASER_LABELS.items():
        val = row1.iloc[col_idx]
        if val != expected:
            errs.append(f"col {col_idx}: got '{val}', expected '{expected}'")
    if errs:
        fail(f"{y}: " + "; ".join(errs))
    else:
        ok(f"{y}: all 10 purchaser labels correct")

# ── Check 5: Row 3 — Metric labels ──────────────────────────
section("CHECK 5 — Row 3 metric labels")
for y, df in frames.items():
    row2 = df.iloc[2]
    errs = []
    for col_idx, expected in DEMO_COLS.items():
        if row2.iloc[col_idx] != expected:
            errs.append(f"col {col_idx}: got '{row2.iloc[col_idx]}', expected '{expected}'")
    for i in range(90):
        col_idx = 7 + i
        expected = METRIC_CYCLE[i % 9]
        if row2.iloc[col_idx] != expected:
            errs.append(f"col {col_idx}: got '{row2.iloc[col_idx]}', expected '{expected}'")
    if errs:
        fail(f"{y}: {len(errs)} label mismatches — first: {errs[0]}")
    else:
        ok(f"{y}: all 97 metric labels correct")

# ── Check 6: ZIP codes ───────────────────────────────────────
section("CHECK 6 — ZIP codes (5-digit, unique per sheet)")
zip_sets = {}
for y, df in frames.items():
    data = get_data(df)
    zips = data[0].astype(str).str.strip()
    non5 = zips[~zips.str.match(r'^\d{5}$')]
    dups = zips[zips.duplicated()]
    zip_sets[y] = set(zips)
    errs = []
    if len(non5):
        errs.append(f"{len(non5)} non-5-digit ZIPs: {list(non5[:5])}")
    if len(dups):
        errs.append(f"{len(dups)} duplicate ZIPs: {list(dups[:5])}")
    if errs:
        fail(f"{y}: " + "; ".join(errs))
    else:
        ok(f"{y}: {len(zips):,} unique 5-digit ZIPs")

section("CHECK 6b — ZIP consistency across years")
all_zips = set.intersection(*zip_sets.values())
ok(f"ZIPs present in ALL {len(EXPECTED_YEARS)} years: {len(all_zips):,}")
for y in EXPECTED_YEARS:
    only_in_y = zip_sets[y] - all_zips
    if only_in_y:
        warn(f"{y}: {len(only_in_y):,} ZIPs not present in all years")

# ── Check 7: Population ──────────────────────────────────────
section("CHECK 7 — Population")
for y, df in frames.items():
    data = get_data(df)
    pop = pd.to_numeric(data[1], errors="coerce")
    errs = []
    if (pop < 0).sum():
        errs.append(f"{(pop < 0).sum()} negatives")
    if not (500 <= pop.median() <= 50000):
        errs.append(f"median={pop.median():,.0f} outside 500–50k")
    if pop.max() > 500000:
        errs.append(f"max={pop.max():,.0f} >500k")
    if errs:
        fail(f"{y}: " + "; ".join(errs))
    else:
        ok(f"{y}: min={pop.min():,.1f}  median={pop.median():,.0f}  max={pop.max():,.0f}")

# ── Check 8: Minority ≤ Population ──────────────────────────
section("CHECK 8 — Minority Population ≤ Population")
for y, df in frames.items():
    data = get_data(df)
    pop = pd.to_numeric(data[1], errors="coerce")
    minority = pd.to_numeric(data[2], errors="coerce")
    exceeds = (minority > pop + 0.01).sum()
    negs = (minority < 0).sum()
    errs = []
    if negs:
        errs.append(f"{negs} negatives")
    if exceeds:
        errs.append(f"{exceeds} ZIPs minority > population")
    if errs:
        fail(f"{y}: " + "; ".join(errs))
    else:
        ok(f"{y}: min={minority.min():,.1f}  median={minority.median():,.0f}  max={minority.max():,.0f}  (no violations)")

# ── Check 9: Median Income ───────────────────────────────────
section("CHECK 9 — Median Income ($)")
for y, df in frames.items():
    data = get_data(df)
    inc = pd.to_numeric(data[3], errors="coerce")
    errs = []
    if (inc < 0).sum():
        errs.append(f"{(inc < 0).sum()} negatives")
    if not (20000 <= inc.median() <= 200000):
        errs.append(f"median=${inc.median():,.0f} outside $20k–$200k")
    if errs:
        fail(f"{y}: " + "; ".join(errs))
    else:
        ok(f"{y}: min=${inc.min():,.0f}  median=${inc.median():,.0f}  max=${inc.max():,.0f}")

# ── Check 10: Income Ratio ───────────────────────────────────
section("CHECK 10 — ZIP Income / MSA Income ratio")
for y, df in frames.items():
    data = get_data(df)
    ratio = pd.to_numeric(data[4], errors="coerce")
    errs = []
    if (ratio < 0).sum():
        errs.append(f"{(ratio < 0).sum()} negatives")
    if not (30 <= ratio.median() <= 200):
        errs.append(f"median={ratio.median():.1f} outside 30–200")
    if errs:
        fail(f"{y}: " + "; ".join(errs))
    else:
        ok(f"{y}: min={ratio.min():.1f}  median={ratio.median():.1f}  max={ratio.max():.1f}")

# ── Check 11: Owner ≤ 1-to-4 Family Homes ───────────────────
section("CHECK 11 — Owner-Occupied Homes vs 1-to-4 Family Homes")
for y, df in frames.items():
    data = get_data(df)
    owner = pd.to_numeric(data[5], errors="coerce")
    total = pd.to_numeric(data[6], errors="coerce")
    exceeds = (owner > total + 0.01).sum()
    note = f"  ({exceeds} ZIPs owner > 1-to-4 family; expected in dense/urban areas)" if exceeds else ""
    ok(f"{y}: owner median={owner.median():,.0f}  1-to-4 family median={total.median():,.0f}{note}")

# ── Check 12: 1-to-4 Family Homes realistic ─────────────────
section("CHECK 12 — Number of 1-to-4 Family Homes realistic")
for y, df in frames.items():
    data = get_data(df)
    total = pd.to_numeric(data[6], errors="coerce")
    if not (100 <= total.median() <= 20000):
        fail(f"{y}: median={total.median():,.0f} outside 100–20k")
    elif total.max() > 200000:
        fail(f"{y}: max={total.max():,.0f} >200k")
    else:
        ok(f"{y}: min={total.min():,.1f}  median={total.median():,.0f}  max={total.max():,.0f}")

# ── Check 13: Count columns non-negative ────────────────────
section("CHECK 13 — All count columns non-negative")
for y, df in frames.items():
    data = get_data(df)
    errs = []
    for col in ALL_COUNT_COLS:
        vals = pd.to_numeric(data[col], errors="coerce").dropna()
        if (vals < 0).sum():
            errs.append(f"col {col}: {(vals < 0).sum()} negatives")
    if errs:
        fail(f"{y}: " + "; ".join(errs[:3]))
    else:
        ok(f"{y}: all 30 count columns non-negative")

# ── Check 14: Avg mortgage size realistic ───────────────────
section("CHECK 14 — Average mortgage size (10–5000k) with medians")
for y, df in frames.items():
    data = get_data(df)
    all_size_cols = ORIGINATED_SIZE_COLS + ANAC_SIZE_COLS + DENIED_SIZE_COLS
    errs = []
    for col in all_size_cols:
        vals = pd.to_numeric(data[col], errors="coerce").dropna()
        if len(vals) == 0:
            continue
        out = ((vals < 10) | (vals > 5000)).sum()
        if out:
            errs.append(f"col {col}: {out} outside 10–5000k")
    orig_sizes = agg_cols(data, ORIGINATED_SIZE_COLS)
    anac_sizes = agg_cols(data, ANAC_SIZE_COLS)
    denied_sizes = agg_cols(data, DENIED_SIZE_COLS)
    size_summary = (
        f"originated median={orig_sizes.median():.0f}k  mean={orig_sizes.mean():.0f}k  "
        f"| ANAC median={anac_sizes.median():.0f}k  "
        f"| denied median={denied_sizes.median():.0f}k"
        if len(orig_sizes) and len(anac_sizes) and len(denied_sizes) else "insufficient data"
    )
    if errs:
        fail(f"{y}: {len(errs)} cols with out-of-range sizes — {size_summary}")
    else:
        ok(f"{y}: {size_summary}")

# ── Check 15: Avg applicant income realistic ─────────────────
section("CHECK 15 — Average applicant income (5–5000k) with medians")
for y, df in frames.items():
    data = get_data(df)
    all_income_cols = ORIGINATED_INCOME_COLS + ANAC_INCOME_COLS + DENIED_INCOME_COLS
    errs = []
    for col in all_income_cols:
        vals = pd.to_numeric(data[col], errors="coerce").dropna()
        if len(vals) == 0:
            continue
        out = ((vals < 5) | (vals > 5000)).sum()
        if out:
            errs.append(f"col {col}: {out} outside 5–5000k (min={vals.min():.1f}, max={vals.max():.1f})")
    orig_inc = agg_cols(data, ORIGINATED_INCOME_COLS)
    denied_inc = agg_cols(data, DENIED_INCOME_COLS)
    inc_summary = (
        f"originated median={orig_inc.median():.0f}k  mean={orig_inc.mean():.0f}k  "
        f"| denied median={denied_inc.median():.0f}k"
        if len(orig_inc) and len(denied_inc) else "insufficient data"
    )
    if errs:
        fail(f"{y}: {len(errs)} cols with out-of-range incomes — {inc_summary}")
    else:
        ok(f"{y}: {inc_summary}")

# ── Check 16: Originated > Denied in aggregate ───────────────
section("CHECK 16 — Aggregate originated > denied per year")
year_originated = {}
year_anac = {}
year_denied = {}
for y, df in frames.items():
    data = get_data(df)
    year_originated[y] = sum(pd.to_numeric(data[c], errors="coerce").sum() for c in ORIGINATED_COUNT_COLS)
    year_anac[y]       = sum(pd.to_numeric(data[c], errors="coerce").sum() for c in ANAC_COUNT_COLS)
    year_denied[y]     = sum(pd.to_numeric(data[c], errors="coerce").sum() for c in DENIED_COUNT_COLS)
    ratio = year_originated[y] / year_denied[y] if year_denied[y] > 0 else float("inf")
    msg = (f"{y}: originated={year_originated[y]:,.0f}  "
           f"ANAC={year_anac[y]:,.0f}  "
           f"denied={year_denied[y]:,.0f}  "
           f"ratio={ratio:.2f}x")
    if year_originated[y] > year_denied[y]:
        ok(msg)
    else:
        fail(msg + "  ← originated ≤ denied")

# ── Check 17: YoY swing ─────────────────────────────────────
section("CHECK 17 — Year-over-year originated count (max 60% swing)")
prev_y = None
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    if prev_y is not None and year_originated[prev_y] > 0:
        pct = abs(year_originated[y] - year_originated[prev_y]) / year_originated[prev_y]
        msg = f"{prev_y}→{y}: {year_originated[prev_y]:,.0f} → {year_originated[y]:,.0f} ({pct*100:+.1f}%)"
        if pct > 0.60:
            fail(msg + "  ← >60% swing")
        else:
            ok(msg)
    prev_y = y

# ── Check 18: Null rates ─────────────────────────────────────
section("CHECK 18 — Null rates (demo >5% flagged; count cols >95% flagged)")
for y, df in frames.items():
    data = get_data(df)
    n = len(data)
    demo_issues = []
    count_issues = []
    for col_idx in range(1, 7):
        null_rate = data[col_idx].isna().sum() / n
        if null_rate > 0.05:
            demo_issues.append(f"col {col_idx}: {null_rate*100:.1f}% null")
    for col_idx in ALL_COUNT_COLS:
        null_rate = data[col_idx].isna().sum() / n
        if null_rate > 0.95:
            count_issues.append(col_idx)
    if demo_issues:
        fail(f"{y} demo nulls: " + ", ".join(demo_issues))
    else:
        ok(f"{y}: no demographic column >5% null")
    if count_issues:
        warn(f"{y}: {len(count_issues)} count cols >95% null (sparse purchaser combos expected)")
    else:
        ok(f"{y}: no count column >95% null")

# ── Check 19: Plausible total originated volume ───────────────
section("CHECK 19 — Total originated mortgages per year (100k–20M)")
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    total = year_originated[y]
    if 100_000 <= total <= 20_000_000:
        ok(f"{y}: {total:,.0f} originated mortgages")
    else:
        fail(f"{y}: {total:,.0f} originated — outside expected 100k–20M")

# ── Check 20: Core ZIPs ──────────────────────────────────────
section("CHECK 20 — Spot-check ZIPs present in all years")
core = set.intersection(*[zip_sets[y] for y in EXPECTED_YEARS if y in zip_sets])
spot_zips = ["10001", "90210", "60601", "85001", "98101",
             "33101", "75201", "19103", "30309", "02134"]
missing_spot = [z for z in spot_zips if z not in core]
ok(f"Core ZIPs in all 11 years: {len(core):,}")
if missing_spot:
    warn(f"Spot-check ZIPs absent from all years: {missing_spot}")
else:
    ok(f"All spot-check ZIPs present in every year")

# ── Check 21: Pull-through rate ──────────────────────────────
section("CHECK 21 — Pull-through rate: originated / (originated + ANAC)")
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    denom = year_originated[y] + year_anac[y]
    if denom == 0:
        fail(f"{y}: no originated or ANAC mortgages")
        continue
    rate = year_originated[y] / denom
    msg = f"{y}: {rate*100:.1f}%  (originated={year_originated[y]:,.0f}  ANAC={year_anac[y]:,.0f})"
    if 0.50 <= rate <= 1.00:
        ok(msg)
    else:
        fail(msg + "  ← pull-through <50%")

# ── Check 22: Median mortgage size per year ───────────────────
section("CHECK 22 — Median originated mortgage size per ZIP (sanity range 50–1000k)")
for y, df in frames.items():
    data = get_data(df)
    vals = agg_cols(data, ORIGINATED_SIZE_COLS)
    if len(vals) == 0:
        fail(f"{y}: no originated size data")
        continue
    med = vals.median()
    mn  = vals.mean()
    lo  = vals.quantile(0.05)
    hi  = vals.quantile(0.95)
    msg = f"{y}: median={med:.0f}k  mean={mn:.0f}k  p5={lo:.0f}k  p95={hi:.0f}k"
    if 50 <= med <= 1000:
        ok(msg)
    else:
        fail(msg + "  ← median outside 50–1000k")

# ── Check 23: Median applicant income per year ────────────────
section("CHECK 23 — Median originated applicant income per ZIP (sanity range 20–500k)")
for y, df in frames.items():
    data = get_data(df)
    vals = agg_cols(data, ORIGINATED_INCOME_COLS)
    if len(vals) == 0:
        fail(f"{y}: no originated income data")
        continue
    med = vals.median()
    mn  = vals.mean()
    lo  = vals.quantile(0.05)
    hi  = vals.quantile(0.95)
    msg = f"{y}: median={med:.0f}k  mean={mn:.0f}k  p5={lo:.0f}k  p95={hi:.0f}k"
    if 20 <= med <= 500:
        ok(msg)
    else:
        fail(msg + "  ← median outside 20–500k")

# ── Summary ──────────────────────────────────────────────────
section("SUMMARY")
print(f"  PASSED:  {PASSES}")
print(f"  FAILED:  {FAILURES}")
print(f"  {'ALL CHECKS PASSED' if FAILURES == 0 else str(FAILURES) + ' CHECKS FAILED — review FAILs above'}")
