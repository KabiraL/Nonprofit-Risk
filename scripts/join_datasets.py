"""
Join all nonprofit datamart and balance sheet CSV files into one combined dataset.

Scope:
  The primary analysis focuses on orgs with revenue under $1M. The over-1M
  datamart is included ONLY for orgs that also have under-1M years (i.e., orgs
  that crossed the $1M threshold in some years). We keep only the over-1M rows
  that have matching balance sheet data — those are exactly the crossover filings.

Workflow:
  1. Load the 3 datamart files; filter over-1M to only rows with balance sheet data
  2. Load & stack the 2 balance sheet files (big = over 1M crossovers, data = under 1M + EZ)
  3. Left-join datamart → balance sheet on URL
  4. Quality checks and summary
  5. Export combined CSV

Join key: URL (datamart) ↔ xml_url (balance sheet)
"""

import pandas as pd
from pathlib import Path

# Setting path to data files
project_root = Path('..') 
data_dir     = project_root / 'data'

# ============================================================================
# PATHS 
# ============================================================================
DATAMART_990_UNDER1M = data_dir / 'tax990_under1M_full.csv'
DATAMART_990_OVER1M  = data_dir / 'tax990_over1M_full.csv'
DATAMART_990EZ       = data_dir / 'tax990EZ_full.csv'

BALANCE_SHEET_BIG    = data_dir / 'balance_sheet_big_20260321_221220.csv'
BALANCE_SHEET_DATA   = data_dir / 'balance_sheet_data_20260321_220158.csv'

OUTPUT_PATH = data_dir / 'nonprofit_combined.csv'

# ============================================================================
# STEP 1: Load datamart files
# ============================================================================
print("=== Step 1: Loading datamart files ===")

dm_under = pd.read_csv(DATAMART_990_UNDER1M, dtype=str)
dm_over  = pd.read_csv(DATAMART_990_OVER1M, dtype=str)
dm_ez    = pd.read_csv(DATAMART_990EZ, dtype=str)

print(f"  990 under 1M: {len(dm_under):,} rows, {len(dm_under.columns)} cols")
print(f"  990 over 1M:  {len(dm_over):,} rows (before filtering), {len(dm_over.columns)} cols")
print(f"  990-EZ:       {len(dm_ez):,} rows, {len(dm_ez.columns)} cols")

# Filter over-1M to only rows that have balance sheet data.
# These are the "crossover" orgs that had some years under $1M and some over.
# The balance_sheet_big file contains exactly these crossover filings.
bs_big_urls = set(pd.read_csv(BALANCE_SHEET_BIG, usecols=['xml_url'], dtype=str)['xml_url'])
dm_over_before = len(dm_over)
dm_over = dm_over[dm_over['URL'].isin(bs_big_urls)]
print(f"  990 over 1M:  {len(dm_over):,} rows (after filtering to crossover orgs)")
print(f"    Removed {dm_over_before - len(dm_over):,} rows with no balance sheet data")

# Stack all three — columns that only exist in one form type become NaN in others
dm_all = pd.concat([dm_under, dm_over, dm_ez], ignore_index=True)
print(f"\n  Stacked datamart: {len(dm_all):,} rows, {len(dm_all.columns)} cols")

# Sanity check: no duplicate URLs (each filing should have a unique XML URL)
url_dupes = dm_all['URL'].duplicated().sum()
print(f"  Duplicate URLs in datamart: {url_dupes}")
if url_dupes > 0:
    print("  WARNING: Duplicate URLs found — the join may produce extra rows.")
    print("  Dropping duplicates, keeping first occurrence...")
    dm_all = dm_all.drop_duplicates(subset='URL', keep='first')
    print(f"  After dedup: {len(dm_all):,} rows")

# ============================================================================
# STEP 2: Load and stack balance sheet files
# ============================================================================
print("\n=== Step 2: Loading balance sheet files ===")

bs_big  = pd.read_csv(BALANCE_SHEET_BIG, dtype=str)
bs_data = pd.read_csv(BALANCE_SHEET_DATA, dtype=str)

print(f"  Balance sheet big (over 1M):       {len(bs_big):,} rows, {len(bs_big.columns)} cols")
print(f"  Balance sheet data (under 1M + EZ): {len(bs_data):,} rows, {len(bs_data.columns)} cols")

# Stack them — balance_sheet_data has extra EZ-specific columns (land_buildings_eoy,
# other_assets_eoy, cash_savings_investments_eoy) that balance_sheet_big lacks
bs_all = pd.concat([bs_big, bs_data], ignore_index=True)
print(f"\n  Stacked balance sheet: {len(bs_all):,} rows, {len(bs_all.columns)} cols")

# Check for duplicate xml_urls in balance sheet
bs_url_dupes = bs_all['xml_url'].duplicated().sum()
print(f"  Duplicate xml_urls in balance sheet: {bs_url_dupes}")
if bs_url_dupes > 0:
    print("  Dropping duplicates, keeping first occurrence...")
    bs_all = bs_all.drop_duplicates(subset='xml_url', keep='first')
    print(f"  After dedup: {len(bs_all):,} rows")

# Drop columns from balance sheet that are redundant with datamart or just for QA
# ein, tax_year, form_type → already in datamart as FILEREIN, TAXYEAR, RETURNTYPE
# total_assets_eoy → already in datamart as TOASEOOYY
# xml_ein, xml_tax_year, source_file → validation artifacts, not needed in final data
bs_drop_cols = ['ein', 'tax_year', 'form_type', 'total_assets_eoy',
                'xml_ein', 'xml_tax_year', 'source_file']
bs_all = bs_all.drop(columns=[c for c in bs_drop_cols if c in bs_all.columns])

print(f"  Balance sheet after dropping redundant cols: {len(bs_all.columns)} cols remaining")
print(f"  Columns kept: {list(bs_all.columns)}")

# ============================================================================
# STEP 3: Left-join datamart ← balance sheet on URL
# ============================================================================
print("\n=== Step 3: Joining datamart with balance sheet ===")

combined = dm_all.merge(
    bs_all,
    left_on='URL',
    right_on='xml_url',
    how='left',
    indicator=True
)

# Report join results
merge_counts = combined['_merge'].value_counts()
print(f"  Join results:")
print(f"    Matched (both):     {merge_counts.get('both', 0):,}")
print(f"    Datamart only:      {merge_counts.get('left_only', 0):,}")

# Drop the merge indicator and the redundant xml_url column
combined = combined.drop(columns=['_merge', 'xml_url'])

print(f"\n  Final combined dataset: {len(combined):,} rows, {len(combined.columns)} cols")

# ============================================================================
# STEP 4: Quality checks
# ============================================================================
print("\n=== Step 4: Quality checks ===")

# Row count should match stacked datamart (left join preserves all datamart rows)
assert len(combined) == len(dm_all), (
    f"Row count mismatch! Combined={len(combined)}, Datamart={len(dm_all)}. "
    "Likely caused by duplicate URLs in balance sheet creating extra rows."
)
print(f"  ✓ Row count matches stacked datamart ({len(combined):,})")

# Check balance sheet field coverage by form type
print("\n  Balance sheet field coverage by RETURNTYPE:")
for rt in ['990', '990EZ']:
    subset = combined[combined['RETURNTYPE'] == rt]
    print(f"\n  {rt} ({len(subset):,} rows):")

    # 990-specific fields
    if rt == '990':
        for col in ['land_buildings_equipment_eoy', 'investments_pub_traded_eoy',
                     'investments_other_sec_eoy', 'investments_program_related_eoy']:
            if col in combined.columns:
                non_null = subset[col].notna().sum()
                pct = 100 * non_null / len(subset) if len(subset) > 0 else 0
                print(f"    {col}: {non_null:,} ({pct:.1f}%)")

    # EZ-specific fields
    if rt == '990EZ':
        for col in ['land_buildings_eoy', 'other_assets_eoy',
                     'cash_savings_investments_eoy']:
            if col in combined.columns:
                non_null = subset[col].notna().sum()
                pct = 100 * non_null / len(subset) if len(subset) > 0 else 0
                print(f"    {col}: {non_null:,} ({pct:.1f}%)")

# ============================================================================
# STEP 5: Save
# ============================================================================
print(f"\n=== Step 5: Saving to {OUTPUT_PATH} ===")
combined.to_csv(OUTPUT_PATH, index=False)
print(f"  ✓ Saved {len(combined):,} rows × {len(combined.columns)} columns")

# Final column listing
print(f"\n=== All columns in combined dataset ({len(combined.columns)}) ===")
for i, col in enumerate(combined.columns, 1):
    print(f"  {i:2d}. {col}")
