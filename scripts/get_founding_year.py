#!/usr/bin/env python3
"""
IRS Business Master File - Founding Year Extractor
===================================================

Purpose:
    Extracts the founding year (IRS Ruling Date) for a list of nonprofit EINs
    from the IRS Exempt Organizations Business Master File (EO BMF).

Data Source:
    IRS EO BMF: https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
    
    The BMF contains the "RULING" field which is the year/month (YYYYMM) when
    the IRS granted tax-exempt status. This is effectively the "founding year"
    for tax-exempt purposes.

Input:
    - CSV file with a column containing EINs (your deduplicated list)
    
Output:
    - CSV file with EIN and founding year (plus other useful BMF fields)

Usage:
    # Basic usage - provide your EIN list
    python get_founding_year.py --ein-file my_eins.csv
    
    # Specify which column has the EINs
    python get_founding_year.py --ein-file my_eins.csv --ein-column FILEREIN
    
    # Use a specific state's BMF file (default is Colorado)
    python get_founding_year.py --ein-file my_eins.csv --state CO

Author: Created for nonprofit financial distress prediction project
Date: February 2026
"""

import os
import sys
import csv
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package not installed.")
    print("Install with: pip install requests")
    sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================

# IRS BMF download URLs by state
# Format: https://www.irs.gov/pub/irs-soi/eo_{state_code}.csv
BMF_BASE_URL = "https://www.irs.gov/pub/irs-soi/eo_{state}.csv"

# Regional files (if you need multiple states)
BMF_REGIONS = {
    'region1': 'https://www.irs.gov/pub/irs-soi/eo1.csv',  # Northeast
    'region2': 'https://www.irs.gov/pub/irs-soi/eo2.csv',  # Mid-Atlantic & Great Lakes
    'region3': 'https://www.irs.gov/pub/irs-soi/eo3.csv',  # Gulf Coast & Pacific (includes CO)
    'region4': 'https://www.irs.gov/pub/irs-soi/eo4.csv',  # International
}

# Output directory
OUTPUT_DIR = Path("./bmf_output")

# Logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


# ============================================================================
# BMF FIELD DEFINITIONS
# ============================================================================

"""
Key fields from the IRS EO BMF (see https://www.irs.gov/pub/irs-soi/eo-info.pdf):

EIN              - Employer Identification Number
NAME             - Organization name
ICO              - In Care Of name
STREET           - Street address
CITY             - City
STATE            - State
ZIP              - ZIP code
GROUP            - Group Exemption Number (0000 if not part of group)
SUBSECTION       - IRS subsection code (e.g., 03 = 501(c)(3))
AFFILIATION      - Affiliation code
CLASSIFICATION   - Classification code(s)
RULING           - Ruling date (YYYYMM) - THIS IS THE FOUNDING YEAR
DEDUCTIBILITY    - Deductibility code
FOUNDATION       - Foundation code
ACTIVITY         - Activity codes
ORGANIZATION     - Organization code
STATUS           - Status code
TAX_PERIOD       - Tax period
ASSET_CD         - Asset code
INCOME_CD        - Income code
FILING_REQ_CD    - Filing requirement code
PF_FILING_REQ_CD - Private foundation filing requirement
ACCT_PD          - Accounting period
ASSET_AMT        - Asset amount
INCOME_AMT       - Income amount
REVENUE_AMT      - Revenue amount
NTEE_CD          - NTEE code (National Taxonomy of Exempt Entities)
SORT_NAME        - Sort name
"""

# Columns we want to extract from BMF
BMF_COLUMNS_TO_KEEP = [
    'EIN',
    'NAME',
    'CITY',
    'STATE',
    'ZIP',
    'SUBSECTION',      # Type of 501(c) - useful feature
    'RULING',          # Founding year (YYYYMM format)
    'FOUNDATION',      # Foundation status code
    'NTEE_CD',         # NTEE classification - useful feature
    'ASSET_AMT',       # Most recent asset amount
    'INCOME_AMT',      # Most recent income amount
    'REVENUE_AMT',     # Most recent revenue amount
]


# ============================================================================
# SETUP
# ============================================================================

def setup_directories():
    """Create output directories."""
    OUTPUT_DIR.mkdir(exist_ok=True)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(OUTPUT_DIR / f"bmf_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        ]
    )
    
    return logging.getLogger('bmf_extractor')


# ============================================================================
# EIN LIST HANDLING
# ============================================================================

def load_ein_list(file_path: str, ein_column: str = None) -> Set[str]:
    """
    Load and deduplicate EINs from a CSV file.
    
    Args:
        file_path: Path to CSV file containing EINs
        ein_column: Name of column containing EINs. If None, tries common names.
    
    Returns:
        Set of normalized EINs (9 digits, no hyphens)
    """
    logger = logging.getLogger('bmf_extractor')
    
    file_path = os.path.expanduser(file_path)
    
    if not os.path.exists(file_path):
        logger.error(f"EIN file not found: {file_path}")
        return set()
    
    logger.info(f"Loading EINs from: {file_path}")
    
    eins = set()
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Auto-detect EIN column if not specified
        if ein_column is None:
            possible_columns = ['EIN', 'ein', 'FILEREIN', 'filerein', 'EmployerID', 'employer_id']
            for col in possible_columns:
                if col in reader.fieldnames:
                    ein_column = col
                    break
            
            if ein_column is None:
                logger.error(f"Could not find EIN column. Available columns: {reader.fieldnames}")
                return set()
        
        logger.info(f"Using EIN column: {ein_column}")
        
        for row in reader:
            ein = row.get(ein_column, '').strip()
            if ein:
                # Normalize: remove hyphens, pad to 9 digits
                ein_clean = ein.replace('-', '').replace(' ', '')
                if ein_clean.isdigit():
                    ein_normalized = ein_clean.zfill(9)
                    eins.add(ein_normalized)
    
    logger.info(f"Loaded {len(eins):,} unique EINs")
    return eins


# ============================================================================
# BMF DOWNLOAD AND PROCESSING
# ============================================================================

def download_bmf(state: str = 'co', force: bool = False) -> Path:
    """
    Download the BMF file for a specific state.
    
    Args:
        state: Two-letter state code (lowercase)
        force: If True, re-download even if file exists
    
    Returns:
        Path to downloaded file
    """
    logger = logging.getLogger('bmf_extractor')
    
    url = BMF_BASE_URL.format(state=state.lower())
    local_path = OUTPUT_DIR / f"eo_{state.lower()}.csv"
    
    if local_path.exists() and not force:
        logger.info(f"Using cached BMF file: {local_path}")
        return local_path
    
    logger.info(f"Downloading BMF from: {url}")
    
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Downloaded BMF to: {local_path}")
        return local_path
        
    except requests.RequestException as e:
        logger.error(f"Failed to download BMF: {e}")
        return None


def download_all_bmf_files(states: List[str], force: bool = False) -> List[Path]:
    """
    Download BMF files for multiple states.
    
    Args:
        states: List of state codes
        force: If True, re-download even if files exist
    
    Returns:
        List of paths to downloaded files
    """
    paths = []
    for state in states:
        path = download_bmf(state, force)
        if path:
            paths.append(path)
    return paths


def extract_founding_years(
    bmf_path: Path, 
    ein_set: Set[str]
) -> Dict[str, Dict]:
    """
    Extract founding year and other fields from BMF for matching EINs.
    
    Args:
        bmf_path: Path to BMF CSV file
        ein_set: Set of EINs to look up
    
    Returns:
        Dictionary mapping EIN to extracted fields
    """
    logger = logging.getLogger('bmf_extractor')
    logger.info(f"Extracting data from: {bmf_path}")
    
    results = {}
    matched = 0
    
    with open(bmf_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            ein = row.get('EIN', '').strip().zfill(9)
            
            if ein in ein_set:
                # Extract ruling date and convert to year
                ruling = row.get('RULING', '')
                founding_year = None
                if ruling and len(ruling) >= 4:
                    try:
                        founding_year = int(ruling[:4])
                    except ValueError:
                        founding_year = None
                
                # Build result record
                result = {
                    'ein': ein,
                    'name': row.get('NAME', ''),
                    'city': row.get('CITY', ''),
                    'state': row.get('STATE', ''),
                    'zip': row.get('ZIP', ''),
                    'subsection': row.get('SUBSECTION', ''),
                    'ruling_date': ruling,
                    'founding_year': founding_year,
                    'foundation_code': row.get('FOUNDATION', ''),
                    'ntee_code': row.get('NTEE_CD', ''),
                    'asset_amount': row.get('ASSET_AMT', ''),
                    'income_amount': row.get('INCOME_AMT', ''),
                    'revenue_amount': row.get('REVENUE_AMT', ''),
                }
                
                results[ein] = result
                matched += 1
    
    logger.info(f"Matched {matched:,} of {len(ein_set):,} EINs")
    
    return results


def extract_from_multiple_states(
    bmf_paths: List[Path],
    ein_set: Set[str]
) -> Dict[str, Dict]:
    """
    Extract data from multiple state BMF files.
    
    Args:
        bmf_paths: List of paths to BMF CSV files
        ein_set: Set of EINs to look up
    
    Returns:
        Combined dictionary mapping EIN to extracted fields
    """
    all_results = {}
    
    for path in bmf_paths:
        results = extract_founding_years(path, ein_set)
        all_results.update(results)
    
    return all_results


# ============================================================================
# OUTPUT
# ============================================================================

def save_results(
    results: Dict[str, Dict],
    ein_set: Set[str],
    output_path: Path = None
) -> Path:
    """
    Save extracted data to CSV, including EINs not found in BMF.
    
    Args:
        results: Dictionary of extracted BMF data
        ein_set: Original set of EINs (to identify missing ones)
        output_path: Where to save results
    
    Returns:
        Path to saved file
    """
    logger = logging.getLogger('bmf_extractor')
    
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = OUTPUT_DIR / f"founding_years_{timestamp}.csv"
    
    # Prepare output rows
    fieldnames = [
        'ein', 'founding_year', 'ruling_date', 'name', 'city', 'state', 'zip',
        'subsection', 'foundation_code', 'ntee_code', 
        'asset_amount', 'income_amount', 'revenue_amount',
        'found_in_bmf'
    ]
    
    rows = []
    
    # Add matched EINs
    for ein, data in results.items():
        row = data.copy()
        row['found_in_bmf'] = 'Y'
        rows.append(row)
    
    # Add unmatched EINs
    missing_eins = ein_set - set(results.keys())
    for ein in missing_eins:
        rows.append({
            'ein': ein,
            'founding_year': None,
            'ruling_date': None,
            'name': None,
            'city': None,
            'state': None,
            'zip': None,
            'subsection': None,
            'foundation_code': None,
            'ntee_code': None,
            'asset_amount': None,
            'income_amount': None,
            'revenue_amount': None,
            'found_in_bmf': 'N'
        })
    
    # Sort by EIN
    rows.sort(key=lambda x: x['ein'])
    
    # Write CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    logger.info(f"Results saved to: {output_path}")
    logger.info(f"Total rows: {len(rows):,}")
    logger.info(f"Found in BMF: {len(results):,}")
    logger.info(f"Not found: {len(missing_eins):,}")
    
    return output_path


def print_summary(results: Dict[str, Dict], ein_set: Set[str]):
    """Print summary statistics."""
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total = len(ein_set)
    found = len(results)
    missing = total - found
    
    print(f"\nTotal EINs in your list: {total:,}")
    print(f"Found in BMF: {found:,} ({100*found/total:.1f}%)")
    print(f"Not found in BMF: {missing:,} ({100*missing/total:.1f}%)")
    
    # Founding year distribution
    years = [r['founding_year'] for r in results.values() if r['founding_year']]
    
    if years:
        print(f"\nFounding Year Statistics:")
        print(f"  Earliest: {min(years)}")
        print(f"  Latest: {max(years)}")
        print(f"  Median: {sorted(years)[len(years)//2]}")
        
        # Decade distribution
        decades = {}
        for y in years:
            decade = (y // 10) * 10
            decades[decade] = decades.get(decade, 0) + 1
        
        print(f"\n  By Decade:")
        for decade in sorted(decades.keys()):
            count = decades[decade]
            print(f"    {decade}s: {count:,} ({100*count/len(years):.1f}%)")
    
    # NTEE distribution (top 10)
    ntee_codes = {}
    for r in results.values():
        ntee = r.get('ntee_code', '')
        if ntee:
            # Get major category (first letter)
            major = ntee[0] if ntee else 'Unknown'
            ntee_codes[major] = ntee_codes.get(major, 0) + 1
    
    if ntee_codes:
        print(f"\n  NTEE Major Categories (top 10):")
        for code, count in sorted(ntee_codes.items(), key=lambda x: -x[1])[:10]:
            print(f"    {code}: {count:,}")
    
    print("=" * 60)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point."""
    
    parser = argparse.ArgumentParser(
        description='Extract founding year from IRS Business Master File',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic usage with Colorado BMF
    python get_founding_year.py --ein-file my_eins.csv
    
    # Specify the EIN column name
    python get_founding_year.py --ein-file my_eins.csv --ein-column FILEREIN
    
    # Use multiple states
    python get_founding_year.py --ein-file my_eins.csv --state CO WY NM
    
    # Force re-download of BMF files
    python get_founding_year.py --ein-file my_eins.csv --force-download
        """
    )
    
    parser.add_argument(
        '--ein-file', '-e',
        type=str,
        required=True,
        help='Path to CSV file containing EINs'
    )
    
    parser.add_argument(
        '--ein-column', '-c',
        type=str,
        default=None,
        help='Name of column containing EINs (auto-detected if not specified)'
    )
    
    parser.add_argument(
        '--state', '-s',
        type=str,
        nargs='+',
        default=['CO'],
        help='State code(s) for BMF download (default: CO)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output file path'
    )
    
    parser.add_argument(
        '--force-download', '-f',
        action='store_true',
        help='Force re-download of BMF files'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup
    setup_directories()
    logger = setup_logging(args.verbose)
    
    print("\n" + "=" * 60)
    print("IRS Business Master File - Founding Year Extractor")
    print("=" * 60)
    
    # Step 1: Load EIN list
    logger.info("\n--- Step 1: Loading EIN List ---")
    ein_set = load_ein_list(args.ein_file, args.ein_column)
    
    if not ein_set:
        logger.error("No EINs loaded. Exiting.")
        sys.exit(1)
    
    # Step 2: Download BMF files
    logger.info("\n--- Step 2: Downloading BMF Files ---")
    bmf_paths = download_all_bmf_files(args.state, args.force_download)
    
    if not bmf_paths:
        logger.error("No BMF files downloaded. Exiting.")
        sys.exit(1)
    
    # Step 3: Extract founding years
    logger.info("\n--- Step 3: Extracting Founding Years ---")
    results = extract_from_multiple_states(bmf_paths, ein_set)
    
    # Step 4: Save results
    logger.info("\n--- Step 4: Saving Results ---")
    output_path = Path(args.output) if args.output else None
    output_path = save_results(results, ein_set, output_path)
    
    # Step 5: Print summary
    print_summary(results, ein_set)
    
    print(f"\n✓ Success! Results saved to: {output_path}")
    print(f"\nThe 'founding_year' column contains the year the IRS granted tax-exempt status.")
    print(f"Join this with your main dataset on EIN.")


if __name__ == '__main__':
    main()
