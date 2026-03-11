"""
HMDA Data Quality Report
Produces formatted output suitable for copy/paste into email or Word.
Covers: file structure, ZIP coverage, demographics, join quality, mortgage
activity, loan characteristics, and year-over-year trends.
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import date

WORKBOOK = "/Users/andrew.lehe/Documents/HMDA Data/hmda_all_years_combined.xlsx"
STATS_FILE = "/Users/andrew.lehe/Documents/HMDA Data/processing_stats.json"
EXPECTED_YEARS = list(range(2012, 2023))

# Column index positions (0-based, data rows only — skip 3 header rows)
PURCHASER_STARTS_NOO = [7 + i * 9 for i in range(5)]
PURCHASER_STARTS_OO  = [52 + i * 9 for i in range(5)]
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
ALL_SIZE_COLS          = ORIGINATED_SIZE_COLS + ANAC_SIZE_COLS + DENIED_SIZE_COLS
ALL_INCOME_COLS        = ORIGINATED_INCOME_COLS + ANAC_INCOME_COLS + DENIED_INCOME_COLS


# ── Helpers ───────────────────────────────────────────────────

def divider(char="=", width=72):
    print(char * width)

def section(title):
    print()
    divider()
    print(f"  {title}")
    divider()

def subsection(title):
    print(f"\n  --- {title} ---")

def row(label, *values, width=28):
    vals = "  ".join(str(v).rjust(12) for v in values)
    print(f"  {label:<{width}}{vals}")

def header_row(label_col, *values, width=28):
    vals = "  ".join(str(v).rjust(12) for v in values)
    print(f"  {label_col:<{width}}{vals}")
    print(f"  {'-'*width}{'-'*(14*len(values) - 2)}")

def agg_cols(data, cols):
    return pd.concat(
        [pd.to_numeric(data[c], errors="coerce").dropna() for c in cols]
    )

def pct(num, denom):
    if denom == 0:
        return "n/a"
    return f"{100*num/denom:.1f}%"

def fmt(n, decimals=0):
    if pd.isna(n):
        return "n/a"
    fmt_str = f"{{:,.{decimals}f}}"
    return fmt_str.format(n)

def get_data(df):
    d = df.iloc[3:].copy()
    d.columns = range(df.shape[1])
    return d


# ── Load workbook ─────────────────────────────────────────────

print(f"Loading {WORKBOOK} ...")
xl = pd.ExcelFile(WORKBOOK)
sheets = xl.sheet_names
frames = {}
for y in EXPECTED_YEARS:
    if str(y) in sheets:
        frames[y] = pd.read_excel(WORKBOOK, sheet_name=str(y), header=None)
print(f"Loaded {len(frames)} year sheets.\n")


# ═══════════════════════════════════════════════════════════════
print()
divider("=")
print("  HMDA MORTGAGE DATA — QUALITY & SUMMARY REPORT")
print(f"  Source:    {WORKBOOK}")
print(f"  Generated: {date.today().strftime('%B %d, %Y')}")
print(f"  Coverage:  {min(frames.keys())}–{max(frames.keys())}  ({len(frames)} years)")
divider("=")


# ── Section 1: File Structure ─────────────────────────────────

section("1. FILE STRUCTURE")

header_row("Year", "Rows (ZIPs)", "Columns")
for y, df in frames.items():
    data = get_data(df)
    row(str(y), fmt(len(data)), fmt(df.shape[1]))

print()
years_present = sorted(frames.keys())
missing = [y for y in EXPECTED_YEARS if y not in frames]
print(f"  Years present : {', '.join(str(y) for y in years_present)}")
if missing:
    print(f"  Years missing : {', '.join(str(y) for y in missing)}")
else:
    print(f"  Years missing : None")
print(f"  Columns/sheet : 97  (7 demographic + 2 occupancy types x 5 purchasers x 9 metrics)")
print(f"  Header rows   : 3  (Occupancy, Purchaser, Metric)")


# ── Section 2: ZIP Coverage ───────────────────────────────────

section("2. ZIP COVERAGE")

zip_sets = {}
for y, df in frames.items():
    data = get_data(df)
    zips = data[0].astype(str).str.strip()
    zip_sets[y] = set(zips[zips.str.match(r'^\d{5}$')])

all_zip_union = set.union(*zip_sets.values())
all_zip_intersect = set.intersection(*zip_sets.values())

header_row("Year", "ZIP Count", "Valid 5-digit", "Not in all yrs")
for y, df in frames.items():
    data = get_data(df)
    zips = data[0].astype(str).str.strip()
    total = len(zips)
    valid = len(zip_sets[y])
    not_universal = len(zip_sets[y] - all_zip_intersect)
    row(str(y), fmt(total), fmt(valid), fmt(not_universal))

print()
print(f"  Total unique ZIPs across all years : {fmt(len(all_zip_union))}")
print(f"  ZIPs present in ALL 11 years       : {fmt(len(all_zip_intersect))}")
print(f"  ZIPs appearing in only some years  : {fmt(len(all_zip_union) - len(all_zip_intersect))}")
print()
print(f"  Note: ZIPs absent from some years are generally rural or newly assigned")
print(f"  ZIPs that had no HMDA-reportable activity in that year.")


# ── Section 3: Demographic Profile ───────────────────────────

section("3. DEMOGRAPHIC PROFILE  (weighted ZIP-level values from HUD crosswalk)")

subsection("Population per ZIP")
header_row("Year", "Minimum", "Median", "Maximum", "Zeros")
for y, df in frames.items():
    data = get_data(df)
    pop = pd.to_numeric(data[1], errors="coerce")
    zeros = (pop == 0).sum()
    row(str(y), fmt(pop.min()), fmt(pop.median()), fmt(pop.max()), fmt(zeros))

subsection("Minority Population per ZIP")
header_row("Year", "Minimum", "Median", "Maximum", "Median %Pop")
for y, df in frames.items():
    data = get_data(df)
    pop = pd.to_numeric(data[1], errors="coerce")
    minority = pd.to_numeric(data[2], errors="coerce")
    med_pct = (minority / pop.replace(0, np.nan)).median() * 100
    row(str(y), fmt(minority.min()), fmt(minority.median()), fmt(minority.max()), f"{med_pct:.1f}%")

subsection("Tract Median Household Income per ZIP  ($)")
header_row("Year", "Minimum", "Median", "Maximum", "Nulls")
for y, df in frames.items():
    data = get_data(df)
    inc = pd.to_numeric(data[3], errors="coerce")
    nulls = inc.isna().sum()
    row(str(y), fmt(inc.min()), fmt(inc.median()), fmt(inc.max()), fmt(nulls))

subsection("ZIP Income as % of MSA Income")
header_row("Year", "Minimum", "Median", "Maximum")
for y, df in frames.items():
    data = get_data(df)
    ratio = pd.to_numeric(data[4], errors="coerce")
    row(str(y), f"{ratio.min():.1f}", f"{ratio.median():.1f}", f"{ratio.max():.1f}")

subsection("Owner-Occupied Homes per ZIP")
header_row("Year", "Median", "Max", "1-to-4 Fam Med")
for y, df in frames.items():
    data = get_data(df)
    owner = pd.to_numeric(data[5], errors="coerce")
    fam   = pd.to_numeric(data[6], errors="coerce")
    row(str(y), fmt(owner.median()), fmt(owner.max()), fmt(fam.median()))


# ── Section 4: Join / Crosswalk Quality ──────────────────────

section("4. JOIN QUALITY  (HUD tract-to-ZIP crosswalk match assessment)")

print()
print("  The pipeline joins HMDA census tract records to ZIP codes using")
print("  the HUD USPS Crosswalk (TRACT_ZIP_122022.xlsx, Q4 2022 vintage).")
print("  Each tract maps to one or more ZIPs via a tot_ratio weight.")
print()
print("  Match rates observed during processing:")
print()
print(f"  {'Year':<6}  {'Rows Processed':>16}  {'Matched':>12}  {'Match Rate':>11}  {'Expanded Rows':>14}")
print(f"  {'-'*6}  {'-'*16}  {'-'*12}  {'-'*11}  {'-'*14}")

processing_stats = {}
if os.path.exists(STATS_FILE):
    with open(STATS_FILE) as _f:
        processing_stats = json.load(_f)

for y in EXPECTED_YEARS:
    s = processing_stats.get(str(y))
    if s:
        total_rows = s["total_rows"]
        matched    = s["matched"]
        expanded   = s["expanded"]
        match_pct  = f"{100*matched/total_rows:.2f}%"
        print(f"  {y:<6}  {fmt(total_rows):>16}  {fmt(matched):>12}  {match_pct:>11}  {fmt(expanded):>14}")
    else:
        print(f"  {y:<6}  {'(stats not yet available — re-run parse script)':>44}")

print()
print("  Unmatched rows (~2-3%) are tracts with no ZIP code in the crosswalk,")
print("  typically P.O. boxes, military, or territories. These are excluded")
print("  from ZIP-level aggregation.")
print()

subsection("ZIP-level null rates for demographic columns")
print(f"  (A null demographic value means the ZIP appeared in mortgage data")
print(f"   but had no matching tract in the HUD crosswalk.)")
print()
header_row("Year", "Pop null%", "Income null%", "ZIPs all-null")
for y, df in frames.items():
    data = get_data(df)
    n = len(data)
    pop_null  = data[1].isna().sum() / n * 100
    inc_null  = data[3].isna().sum() / n * 100
    # ZIPs where every originated-count column is null (no mortgage activity)
    orig_count_matrix = pd.concat(
        [pd.to_numeric(data[c], errors="coerce") for c in ORIGINATED_COUNT_COLS], axis=1
    )
    all_null_counts = orig_count_matrix.isna().all(axis=1).sum()
    row(str(y), f"{pop_null:.2f}%", f"{inc_null:.2f}%", fmt(all_null_counts))

subsection("Minority-exceeds-population violations")
print(f"  (Pre-2018 source data contains tract-level reporting errors where")
print(f"   minority population exceeds total population. These are clipped at")
print(f"   the population value before ZIP allocation.)")
print()
header_row("Year", "Violations", "Treatment")
for y, df in frames.items():
    data = get_data(df)
    pop = pd.to_numeric(data[1], errors="coerce")
    minority = pd.to_numeric(data[2], errors="coerce")
    exceeds = (minority > pop + 0.01).sum()
    treatment = "Clipped (old format)" if y <= 2017 else "N/A (derived from %)"
    row(str(y), fmt(exceeds), treatment)


# ── Section 5: Mortgage Activity ─────────────────────────────

section("5. MORTGAGE ACTIVITY  (weighted, all occupancy types and purchasers combined)")

year_originated = {}
year_anac = {}
year_denied = {}

for y, df in frames.items():
    data = get_data(df)
    year_originated[y] = sum(pd.to_numeric(data[c], errors="coerce").sum() for c in ORIGINATED_COUNT_COLS)
    year_anac[y]       = sum(pd.to_numeric(data[c], errors="coerce").sum() for c in ANAC_COUNT_COLS)
    year_denied[y]     = sum(pd.to_numeric(data[c], errors="coerce").sum() for c in DENIED_COUNT_COLS)

subsection("Loan counts by outcome")
header_row("Year", "Originated", "Apprvd/Not Accptd", "Denied", "Total Decisions")
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    total = year_originated[y] + year_anac[y] + year_denied[y]
    row(str(y),
        fmt(year_originated[y]),
        fmt(year_anac[y]),
        fmt(year_denied[y]),
        fmt(total))

subsection("Approval and pull-through rates")
header_row("Year", "Approval Rate", "Pull-Through", "Orig/Denied Ratio")
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    total_decisions = year_originated[y] + year_anac[y] + year_denied[y]
    approval_rate = pct(year_originated[y] + year_anac[y], total_decisions)
    pull_through  = pct(year_originated[y], year_originated[y] + year_anac[y])
    ratio = f"{year_originated[y]/year_denied[y]:.2f}x" if year_denied[y] > 0 else "n/a"
    row(str(y), approval_rate, pull_through, ratio)

print()
print("  Definitions:")
print("    Approval Rate   = (Originated + Approved Not Accepted) / All Decisions")
print("    Pull-Through    = Originated / (Originated + Approved Not Accepted)")
print("    Orig/Denied     = Originated count divided by Denied count")
print()
print("  Note: Withdrawn applications and purchased loans are excluded from")
print("  all counts. Only lender-decision outcomes are captured.")

subsection("Year-over-year change in originations")
header_row("Year", "Originated", "YoY Change", "YoY %")
prev_y = None
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    if prev_y is not None:
        delta = year_originated[y] - year_originated[prev_y]
        pct_chg = 100 * delta / year_originated[prev_y] if year_originated[prev_y] > 0 else 0
        flag = "  <<" if abs(pct_chg) > 60 else ""
        row(str(y), fmt(year_originated[y]), fmt(delta), f"{pct_chg:+.1f}%{flag}")
    else:
        row(str(y), fmt(year_originated[y]), "--", "--")
    prev_y = y


# ── Section 6: Loan Characteristics ──────────────────────────

section("6. LOAN CHARACTERISTICS  (per-ZIP averages aggregated across all ZIPs)")

subsection("Average Originated Mortgage Size  ($ thousands)")
header_row("Year", "Median ZIP", "Mean ZIP", "p5", "p95")
for y, df in frames.items():
    data = get_data(df)
    vals = agg_cols(data, ORIGINATED_SIZE_COLS)
    if len(vals) == 0:
        row(str(y), "n/a", "n/a", "n/a", "n/a")
        continue
    row(str(y),
        f"${vals.median():.0f}k",
        f"${vals.mean():.0f}k",
        f"${vals.quantile(0.05):.0f}k",
        f"${vals.quantile(0.95):.0f}k")

subsection("Average Denied Mortgage Size  ($ thousands)")
header_row("Year", "Median ZIP", "Mean ZIP", "p5", "p95")
for y, df in frames.items():
    data = get_data(df)
    vals = agg_cols(data, DENIED_SIZE_COLS)
    if len(vals) == 0:
        row(str(y), "n/a", "n/a", "n/a", "n/a")
        continue
    row(str(y),
        f"${vals.median():.0f}k",
        f"${vals.mean():.0f}k",
        f"${vals.quantile(0.05):.0f}k",
        f"${vals.quantile(0.95):.0f}k")

subsection("Average Originated Applicant Income  ($ thousands)")
header_row("Year", "Median ZIP", "Mean ZIP", "p5", "p95")
for y, df in frames.items():
    data = get_data(df)
    vals = agg_cols(data, ORIGINATED_INCOME_COLS)
    if len(vals) == 0:
        row(str(y), "n/a", "n/a", "n/a", "n/a")
        continue
    row(str(y),
        f"${vals.median():.0f}k",
        f"${vals.mean():.0f}k",
        f"${vals.quantile(0.05):.0f}k",
        f"${vals.quantile(0.95):.0f}k")

subsection("Average Denied Applicant Income  ($ thousands)")
header_row("Year", "Median ZIP", "Mean ZIP", "p5", "p95")
for y, df in frames.items():
    data = get_data(df)
    vals = agg_cols(data, DENIED_INCOME_COLS)
    if len(vals) == 0:
        row(str(y), "n/a", "n/a", "n/a", "n/a")
        continue
    row(str(y),
        f"${vals.median():.0f}k",
        f"${vals.mean():.0f}k",
        f"${vals.quantile(0.05):.0f}k",
        f"${vals.quantile(0.95):.0f}k")

print()
print("  Note: Values are per-ZIP averages pooled across all purchaser types")
print("  and occupancy types. The mean is sensitive to sparse purchaser")
print("  categories with extreme values; the median is more representative.")
print("  There is a known discontinuity in the old-format mean (~458k-866k")
print("  in 2012-2017 vs ~304k-372k in 2018-2022) that warrants investigation")
print("  into how loan_amount_000s is reported in the pre-2018 files.")


# ── Section 7: Data Quality Flags ────────────────────────────

section("7. DATA QUALITY FLAGS")

flags = []

# Check minority > population
for y, df in frames.items():
    data = get_data(df)
    pop = pd.to_numeric(data[1], errors="coerce")
    minority = pd.to_numeric(data[2], errors="coerce")
    exceeds = (minority > pop + 0.01).sum()
    if exceeds:
        flags.append(f"  [FAIL]  {y}: {exceeds} ZIPs where minority population exceeds total population")

# Check column count
for y, df in frames.items():
    if df.shape[1] != 97:
        flags.append(f"  [FAIL]  {y}: {df.shape[1]} columns (expected 97)")

# Check null demographics
for y, df in frames.items():
    data = get_data(df)
    n = len(data)
    for col_idx, col_name in {1: "Population", 3: "Median Income"}.items():
        null_rate = data[col_idx].isna().sum() / n
        if null_rate > 0.05:
            flags.append(f"  [FAIL]  {y}: {col_name} is {null_rate*100:.1f}% null (threshold: 5%)")

# Check YoY swing
prev_y = None
for y in EXPECTED_YEARS:
    if y not in year_originated or prev_y is None:
        prev_y = y
        continue
    if year_originated[prev_y] > 0:
        pct_chg = abs(year_originated[y] - year_originated[prev_y]) / year_originated[prev_y]
        if pct_chg > 0.65:
            flags.append(f"  [WARN]  {prev_y}→{y}: {pct_chg*100:.1f}% origination swing (threshold: 65%)")
    prev_y = y

# Check total volume
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    if not (100_000 <= year_originated[y] <= 20_000_000):
        flags.append(f"  [FAIL]  {y}: {fmt(year_originated[y])} originated mortgages (expected 100k–20M)")

# Check pull-through
for y in EXPECTED_YEARS:
    if y not in year_originated:
        continue
    denom = year_originated[y] + year_anac[y]
    if denom > 0:
        rate = year_originated[y] / denom
        if rate < 0.50:
            flags.append(f"  [FAIL]  {y}: pull-through rate {rate*100:.1f}% (expected ≥50%)")

# Check median income range
for y, df in frames.items():
    data = get_data(df)
    inc = pd.to_numeric(data[3], errors="coerce")
    med = inc.median()
    if not (20_000 <= med <= 200_000):
        flags.append(f"  [FAIL]  {y}: median income ${fmt(med)} outside $20k–$200k")

# Sparse combos note
total_sparse = 0
for y, df in frames.items():
    data = get_data(df)
    n = len(data)
    for col in ALL_COUNT_COLS:
        null_rate = data[col].isna().sum() / n
        if null_rate > 0.95:
            total_sparse += 1

if total_sparse > 0:
    flags.append(f"  [INFO]  {total_sparse} purchaser×occupancy count columns are >95% null across all years")
    flags.append(f"          (expected — some purchaser categories are sparsely used in certain ZIPs)")

if flags:
    print()
    for f in flags:
        print(f)
else:
    print()
    print("  No flags raised. All checks passed.")

print()
divider()
print("  END OF REPORT")
divider()
