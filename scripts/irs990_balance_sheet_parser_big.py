#!/usr/bin/env python3
"""
IRS Form 990 Balance Sheet Parser - Big File Version
=====================================================
Single-input version using tax990_big.csv (Form 990 only)

Purpose:
    Extracts balance sheet fields from IRS 990 XML files that are NOT available
    in the GivingTuesday preprocessed datamarts. Specifically:
    
    Form 990 (Part X - Balance Sheet):
        - Line 10c: Land, buildings, equipment (net of depreciation)
        - Line 11:  Investments - publicly traded securities
        - Line 12:  Investments - other securities
        - Line 13:  Investments - program-related

Input File:
    - tax990_big.csv: CSV with columns [FILEREIN, TAXYEAR, URL] for Form 990

Usage:
    # Test with small sample (10 files)
    python irs990_balance_sheet_parser_big.py --sample 10
    
    # Full run with default input file
    python irs990_balance_sheet_parser_big.py
    
    # Specify input file and output location
    python irs990_balance_sheet_parser_big.py --file tax990_big.csv --output ./my_output/

Author: Created for nonprofit financial distress prediction project
Date: March 2026
"""

import os
import sys
import csv
import time
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package not installed.")
    print("Install with: pip install requests")
    sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """
    Central configuration for the parser.
    Modify these values to customize behavior.
    """
    
    # Default input file path (can be overridden via command line)
    DEFAULT_INPUT_FILE = "tax990_big.csv"
    
    # Output directory — separate from the original script's output
    OUTPUT_DIR = Path("./irs990_big_output")
    
    # Subdirectories
    XML_CACHE_DIR = OUTPUT_DIR / "xml_cache"
    RESULTS_DIR = OUTPUT_DIR / "results"
    LOGS_DIR = OUTPUT_DIR / "logs"
    
    # Download settings
    MAX_WORKERS = 5          # Parallel download threads (be respectful of the server)
    REQUEST_TIMEOUT = 30     # Seconds to wait for each request
    RETRY_ATTEMPTS = 3       # Number of retries for failed downloads
    RETRY_DELAY = 2          # Seconds between retries
    
    # Rate limiting (requests per second - be nice to the server)
    REQUESTS_PER_SECOND = 10
    
    # Logging
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


# ============================================================================
# XPATH MAPPINGS FOR BALANCE SHEET FIELDS
# ============================================================================

"""
These XPaths extract the specific balance sheet fields we need.

IMPORTANT: IRS XML schemas change between versions! The XPaths below
handle the most common patterns for 2010-2023 tax years.

Reference: http://www.irsx.info/metadata/parts/part_x.html
"""

# Form 990 (Full) - Part X Balance Sheet
# These fields are at End of Year (Column B)
XPATH_990 = {
    # Line 10c: Land, buildings, equipment (net of accumulated depreciation)
    'land_buildings_equipment': [
        './/LandBldgEquipBasisNetGrp/EOYAmt',           # 2013+ format
        './/LandBldgEquipmentBasisNet/EOY',             # Older format
        './/LandBldgEquipmentBasisNetGrp/EOYAmt',       # Variant
        './/LandBuildingsEquipmentBasisNet/EOYAmt',     # Another variant
    ],
    
    # Line 11: Investments - publicly traded securities
    'investments_pub_traded': [
        './/InvestmentsPubTradedSecGrp/EOYAmt',         # 2013+ format
        './/InvestmentsPubTradedSec/EOY',               # Older format
        './/InvestmentsPubTradedSecurities/EOYAmt',     # Variant
    ],
    
    # Line 12: Investments - other securities
    'investments_other_sec': [
        './/InvestmentsOtherSecuritiesGrp/EOYAmt',      # 2013+ format
        './/InvestmentsOtherSecurities/EOY',            # Older format
    ],
    
    # Line 13: Investments - program-related
    'investments_program_related': [
        './/InvestmentsProgramRelatedGrp/EOYAmt',       # 2013+ format
        './/InvestmentsProgramRelated/EOY',             # Older format
    ],
    
    # Additional fields for validation (these ARE in the datamart, but useful for QA)
    'cash_non_interest': [
        './/CashNonInterestBearingGrp/EOYAmt',
        './/CashNonInterestBearing/EOY',
    ],
    'savings_temp_investments': [
        './/SavingsAndTempCashInvstGrp/EOYAmt',
        './/SavingsAndTempCashInvestments/EOY',
    ],
    'total_assets': [
        './/TotalAssetsGrp/EOYAmt',
        './/TotalAssets/EOY',
        './/TotalAssetsEOYAmt',
    ],
}


# ============================================================================
# SETUP AND LOGGING
# ============================================================================

def setup_directories(output_dir: Path = None):
    """Create output directories if they don't exist."""
    if output_dir:
        Config.OUTPUT_DIR = Path(output_dir)
        Config.XML_CACHE_DIR = Config.OUTPUT_DIR / "xml_cache"
        Config.RESULTS_DIR = Config.OUTPUT_DIR / "results"
        Config.LOGS_DIR = Config.OUTPUT_DIR / "logs"
    
    Config.OUTPUT_DIR.mkdir(exist_ok=True)
    Config.XML_CACHE_DIR.mkdir(exist_ok=True)
    Config.RESULTS_DIR.mkdir(exist_ok=True)
    Config.LOGS_DIR.mkdir(exist_ok=True)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Configure logging to both console and file.
    
    Args:
        verbose: If True, set log level to DEBUG
    
    Returns:
        Configured logger instance
    """
    log_level = logging.DEBUG if verbose else Config.LOG_LEVEL
    
    # Create logger
    logger = logging.getLogger('irs990_parser_big')
    logger.setLevel(log_level)
    
    # Clear any existing handlers
    logger.handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(Config.LOG_FORMAT))
    logger.addHandler(console_handler)
    
    # File handler
    log_file = Config.LOGS_DIR / f"parser_big_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # Always log everything to file
    file_handler.setFormatter(logging.Formatter(Config.LOG_FORMAT))
    logger.addHandler(file_handler)
    
    return logger


# ============================================================================
# CSV INPUT HANDLING
# ============================================================================

def load_url_csv(file_path: str) -> List[Dict]:
    """
    Load a CSV file containing EIN, tax year, and XML URLs.
    
    Expected columns: FILEREIN, TAXYEAR, URL
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        List of dictionaries with keys: ein, tax_year, url, form_type
    """
    logger = logging.getLogger('irs990_parser_big')
    
    # Expand ~ to home directory
    file_path = os.path.expanduser(file_path)
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return []
    
    logger.info(f"Loading Form 990 URLs from: {file_path}")
    
    entries = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Handle different possible column name formats
            ein = row.get('FILEREIN') or row.get('filerein') or row.get('EIN') or row.get('ein')
            tax_year = row.get('TAXYEAR') or row.get('taxyear') or row.get('TaxYear') or row.get('tax_year')
            url = row.get('URL') or row.get('url')
            
            if ein and tax_year and url:
                entries.append({
                    'ein': str(ein).strip(),
                    'tax_year': str(tax_year).strip(),
                    'url': url.strip(),
                    'form_type': '990'
                })
    
    logger.info(f"Loaded {len(entries):,} Form 990 entries")
    return entries


def sample_entries(entries: List[Dict], sample_size: int) -> List[Dict]:
    """
    Randomly sample entries for testing.
    
    Args:
        entries: List of entry dictionaries
        sample_size: Number of entries to sample
    
    Returns:
        Sampled list of entries
    """
    import random
    
    if len(entries) <= sample_size:
        return entries
    
    return random.sample(entries, sample_size)


# ============================================================================
# XML DOWNLOADING
# ============================================================================

def download_xml(entry: Dict) -> Optional[Path]:
    """
    Download a single XML file from the Data Lake.
    
    Files are cached locally to avoid re-downloading.
    
    Args:
        entry: Dictionary with 'url', 'ein', 'tax_year', 'form_type'
    
    Returns:
        Path to the downloaded file, or None if download failed
    """
    logger = logging.getLogger('irs990_parser_big')
    
    url = entry['url']
    ein = entry['ein']
    tax_year = entry['tax_year']
    
    # Create local filename: {EIN}_{TaxYear}_990.xml
    filename = f"{ein}_{tax_year}_990.xml"
    local_path = Config.XML_CACHE_DIR / filename
    
    # Check cache
    if local_path.exists():
        return local_path
    
    # Download with retries
    for attempt in range(Config.RETRY_ATTEMPTS):
        try:
            response = requests.get(url, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            return local_path
            
        except requests.RequestException as e:
            if attempt < Config.RETRY_ATTEMPTS - 1:
                logger.warning(f"Download failed for {ein}/{tax_year} (attempt {attempt + 1}): {e}")
                time.sleep(Config.RETRY_DELAY)
            else:
                logger.error(f"Failed to download {ein}/{tax_year} after {Config.RETRY_ATTEMPTS} attempts: {e}")
                return None
    
    return None


def download_xml_batch(entries: List[Dict]) -> List[Dict]:
    """
    Download multiple XML files in parallel.
    
    Args:
        entries: List of entry dictionaries
    
    Returns:
        List of entries with 'local_path' added (None if download failed)
    """
    logger = logging.getLogger('irs990_parser_big')
    logger.info(f"Starting download of {len(entries):,} XML files...")
    logger.info(f"Using {Config.MAX_WORKERS} parallel workers")
    
    results = []
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        # Submit all download tasks
        future_to_entry = {
            executor.submit(download_xml, entry): entry
            for entry in entries
        }
        
        # Process completed downloads
        completed = 0
        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            
            try:
                local_path = future.result()
                entry['local_path'] = local_path
                results.append(entry)
                
                if local_path:
                    successful += 1
                else:
                    failed += 1
                    
            except Exception as e:
                logger.error(f"Unexpected error downloading {entry['ein']}: {e}")
                entry['local_path'] = None
                results.append(entry)
                failed += 1
            
            completed += 1
            
            # Progress indicator every 100 files or at the end
            if completed % 100 == 0 or completed == len(entries):
                pct = (completed / len(entries)) * 100
                print(f"\rDownload progress: {completed:,}/{len(entries):,} ({pct:.1f}%) "
                      f"[{successful:,} OK, {failed:,} failed]", 
                      end='', flush=True)
            
            # Rate limiting
            time.sleep(1 / Config.REQUESTS_PER_SECOND)
    
    print()  # Newline after progress
    logger.info(f"Download complete: {successful:,} successful, {failed:,} failed")
    
    return results


# ============================================================================
# XML PARSING
# ============================================================================

def extract_value(root: ET.Element, xpaths: List[str]) -> Optional[str]:
    """
    Extract a value from XML using multiple possible XPaths.
    
    Tries each XPath pattern until one matches. Handles namespace variations.
    
    Args:
        root: XML root element
        xpaths: List of XPath patterns to try
    
    Returns:
        The extracted value as a string, or None if not found
    """
    for xpath in xpaths:
        # Try direct XPath (handles most files)
        try:
            elements = root.findall(xpath)
            if elements and elements[0].text:
                return elements[0].text.strip()
        except:
            pass
        
        # Try with namespace wildcard for stubborn files
        # Convert './/ElementName/SubElement' to './/{*}ElementName/{*}SubElement'
        try:
            # Build a namespace-agnostic version
            parts = xpath.replace('.//', '').split('/')
            ns_xpath = './/' + '/'.join([f"*[local-name()='{p}']" for p in parts if p])
            
            # Use iter to find elements by local name
            for part in parts:
                if part:
                    for elem in root.iter():
                        local_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                        if local_name == parts[-1] and elem.text:
                            return elem.text.strip()
        except:
            pass
    
    return None


def extract_header_info(root: ET.Element) -> Dict[str, Optional[str]]:
    """
    Extract header information (EIN, tax year) from XML for validation.
    
    Args:
        root: XML root element
    
    Returns:
        Dictionary with 'ein' and 'tax_year' keys
    """
    ein_paths = [
        './/Filer/EIN',
        './/ReturnHeader/Filer/EIN',
        './/EIN',
    ]
    
    tax_year_paths = [
        './/ReturnHeader/TaxYr',
        './/ReturnHeader/TaxYear',
        './/TaxYr',
        './/TaxYear',
    ]
    
    return {
        'ein': extract_value(root, ein_paths),
        'tax_year': extract_value(root, tax_year_paths),
    }


def parse_990(xml_path: Path, entry: Dict) -> Optional[Dict]:
    """
    Parse balance sheet fields from a Form 990 XML file.
    
    Args:
        xml_path: Path to the XML file
        entry: Original entry dict with ein, tax_year, url
    
    Returns:
        Dictionary with extracted fields, or None if parsing failed
    """
    logger = logging.getLogger('irs990_parser_big')
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Get header info for validation
        header = extract_header_info(root)
        
        # Use entry values as primary (they're from your datamart)
        # Header values are backup/validation
        result = {
            'ein': entry['ein'],
            'tax_year': entry['tax_year'],
            'form_type': '990',
            'source_file': xml_path.name,
            'xml_url': entry['url'],
            
            # ===== PRIMARY FIELDS (NOT in datamart) =====
            'land_buildings_equipment_eoy': extract_value(root, XPATH_990['land_buildings_equipment']),
            'investments_pub_traded_eoy': extract_value(root, XPATH_990['investments_pub_traded']),
            'investments_other_sec_eoy': extract_value(root, XPATH_990['investments_other_sec']),
            'investments_program_related_eoy': extract_value(root, XPATH_990['investments_program_related']),
            
            # ===== VALIDATION FIELDS (for QA - compare to datamart) =====
            'cash_non_interest_eoy': extract_value(root, XPATH_990['cash_non_interest']),
            'savings_temp_investments_eoy': extract_value(root, XPATH_990['savings_temp_investments']),
            'total_assets_eoy': extract_value(root, XPATH_990['total_assets']),
            
            # Header validation
            'xml_ein': header['ein'],
            'xml_tax_year': header['tax_year'],
        }
        
        return result
        
    except ET.ParseError as e:
        logger.error(f"XML parse error in {xml_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing {xml_path}: {e}")
        return None


def parse_all_files(entries: List[Dict]) -> List[Dict]:
    """
    Parse all downloaded XML files.
    
    Args:
        entries: List of entry dicts (must have 'local_path' from download step)
    
    Returns:
        List of parsed result dictionaries
    """
    logger = logging.getLogger('irs990_parser_big')
    logger.info(f"Parsing {len(entries):,} XML files...")
    
    results = []
    successful = 0
    failed = 0
    skipped = 0
    
    for i, entry in enumerate(entries):
        local_path = entry.get('local_path')
        
        # Skip if download failed
        if not local_path or not local_path.exists():
            skipped += 1
            continue
        
        result = parse_990(local_path, entry)
        
        if result:
            results.append(result)
            successful += 1
        else:
            failed += 1
        
        # Progress indicator
        if (i + 1) % 500 == 0 or (i + 1) == len(entries):
            pct = ((i + 1) / len(entries)) * 100
            print(f"\rParse progress: {i + 1:,}/{len(entries):,} ({pct:.1f}%) "
                  f"[{successful:,} OK, {failed:,} failed, {skipped:,} skipped]",
                  end='', flush=True)
    
    print()  # Newline after progress
    logger.info(f"Parsing complete: {successful:,} successful, {failed:,} failed, {skipped:,} skipped")
    
    return results


# ============================================================================
# OUTPUT
# ============================================================================

def save_results(results: List[Dict], output_path: Optional[Path] = None) -> Path:
    """
    Save parsed results to CSV file.
    
    Args:
        results: List of parsed record dictionaries
        output_path: Where to save. If None, auto-generates in RESULTS_DIR.
    
    Returns:
        Path to the saved file
    """
    logger = logging.getLogger('irs990_parser_big')
    
    if not results:
        logger.warning("No results to save!")
        return None
    
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = Config.RESULTS_DIR / f"balance_sheet_big_{timestamp}.csv"
    
    # Order fields logically
    fieldnames = [
        'ein', 'tax_year', 'form_type',
        'land_buildings_equipment_eoy',
        'investments_pub_traded_eoy', 'investments_other_sec_eoy', 'investments_program_related_eoy',
        'cash_non_interest_eoy', 'savings_temp_investments_eoy',
        'total_assets_eoy',
        'source_file', 'xml_url', 'xml_ein', 'xml_tax_year'
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    logger.info(f"Results saved to: {output_path}")
    logger.info(f"Total records: {len(results):,}")
    
    return output_path


def print_summary(results: List[Dict]):
    """
    Print a summary of the parsing results.
    
    Args:
        results: List of parsed record dictionaries
    """
    if not results:
        print("\nNo results to summarize")
        return
    
    # Count by tax year
    year_counts = {}
    for r in results:
        yr = r.get('tax_year', 'unknown')
        year_counts[yr] = year_counts.get(yr, 0) + 1
    
    primary_fields = [
        'land_buildings_equipment_eoy', 'investments_pub_traded_eoy',
        'investments_other_sec_eoy', 'investments_program_related_eoy'
    ]
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    print(f"\nTotal Form 990 records parsed: {len(results):,}")
    
    print("\nBy Tax Year:")
    for yr, count in sorted(year_counts.items()):
        print(f"  {yr}: {count:,}")
    
    print(f"\nField Coverage (non-null values) across {len(results):,} records:")
    for field in primary_fields:
        non_null = sum(1 for r in results if r.get(field))
        pct = (non_null / len(results)) * 100
        print(f"  {field}: {non_null:,} ({pct:.1f}%)")
    
    print("=" * 60)


# ============================================================================
# MAIN WORKFLOW
# ============================================================================

def run_pipeline(
    input_file: str,
    sample_size: Optional[int] = None,
    skip_download: bool = False,
    output_dir: Optional[str] = None,
    verbose: bool = False
) -> Path:
    """
    Run the complete parsing pipeline.
    
    Args:
        input_file: Path to CSV with Form 990 URLs
        sample_size: If set, sample this many records (for testing)
        skip_download: If True, only parse already-downloaded XML files
        output_dir: Custom output directory (optional)
        verbose: If True, enable debug logging
    
    Returns:
        Path to the output CSV file
    """
    # Setup
    setup_directories(Path(output_dir) if output_dir else None)
    logger = setup_logging(verbose)
    
    print("\n" + "=" * 60)
    print("IRS 990 Balance Sheet Parser (Big File Version)")
    print("=" * 60)
    
    # Step 1: Load URL file
    logger.info("\n--- Step 1: Loading URL File ---")
    entries = load_url_csv(input_file)
    
    if not entries:
        logger.error("No entries loaded from input file!")
        return None
    
    # Step 2: Sample if requested (for testing)
    if sample_size:
        logger.info(f"\n--- Step 2: Sampling {sample_size} entries ---")
        entries = sample_entries(entries, sample_size)
        logger.info(f"Sampled: {len(entries)} Form 990 entries")
    
    logger.info(f"Total entries to process: {len(entries):,}")
    
    # Step 3: Download XMLs
    logger.info("\n--- Step 3: Downloading XML Files ---")
    
    if skip_download:
        logger.info("Skipping download, using cached files...")
        for entry in entries:
            filename = f"{entry['ein']}_{entry['tax_year']}_990.xml"
            local_path = Config.XML_CACHE_DIR / filename
            entry['local_path'] = local_path if local_path.exists() else None
        
        cached_count = sum(1 for e in entries if e.get('local_path'))
        logger.info(f"Found {cached_count:,} cached files")
    else:
        entries = download_xml_batch(entries)
    
    # Step 4: Parse XMLs
    logger.info("\n--- Step 4: Parsing XML Files ---")
    results = parse_all_files(entries)
    
    # Step 5: Save results
    logger.info("\n--- Step 5: Saving Results ---")
    output_path = save_results(results)
    
    # Step 6: Print summary
    print_summary(results)
    
    return output_path


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Main entry point with argument parsing."""
    
    parser = argparse.ArgumentParser(
        description='Extract balance sheet fields from IRS 990 XML files using tax990_big.csv',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Test with small sample (10 files)
    python irs990_balance_sheet_parser_big.py --sample 10
    
    # Full run with default input file (tax990_big.csv)
    python irs990_balance_sheet_parser_big.py
    
    # Specify input file location
    python irs990_balance_sheet_parser_big.py --file "~/Documents/job search 2025/tax990_big.csv"
    
    # Skip download (parse cached files only)
    python irs990_balance_sheet_parser_big.py --skip-download
    
    # Custom output directory
    python irs990_balance_sheet_parser_big.py --output ~/Desktop/990_big_output/
        """
    )
    
    parser.add_argument(
        '--file',
        type=str,
        default=Config.DEFAULT_INPUT_FILE,
        help=f'Path to CSV with Form 990 URLs (default: {Config.DEFAULT_INPUT_FILE})'
    )
    
    parser.add_argument(
        '--sample',
        type=int,
        help='Sample size for testing (e.g., 10 takes 10 random entries)'
    )
    
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download, parse cached files only'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Custom output directory'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    output_path = run_pipeline(
        input_file=args.file,
        sample_size=args.sample,
        skip_download=args.skip_download,
        output_dir=args.output,
        verbose=args.verbose
    )
    
    if output_path:
        print(f"\n✓ Success! Results saved to: {output_path}")
        print(f"\nNext steps:")
        print(f"  1. Load this CSV into Python/R")
        print(f"  2. Join with your GivingTuesday datamart on EIN + TaxYear")
        print(f"  3. Calculate property-heavy ratio and other features!")
    else:
        print("\n✗ Pipeline failed. Check logs for details.")
        sys.exit(1)


if __name__ == '__main__':
    main()
