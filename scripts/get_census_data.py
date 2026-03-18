#!/usr/bin/env python3
"""
Census ACS Data Downloader for Colorado
========================================

Purpose:
    Downloads population and median household income data from the U.S. Census
    American Community Survey (ACS) 5-Year Estimates for Colorado.
    
    Gets data for:
    - ZIP Code Tabulation Areas (ZCTAs) 
    - Places (cities, towns, CDPs)

Data Tables:
    - B01003_001E: Total Population
    - B19013_001E: Median Household Income (in inflation-adjusted dollars)

Years Available:
    ACS 5-Year estimates are available from 2009 (representing 2005-2009) through
    2024 (representing 2020-2024). Each estimate represents a 5-year period.

Output:
    - colorado_census_zcta.csv: Population & income by ZIP code and year
    - colorado_census_places.csv: Population & income by city and year
    - colorado_census_combined.csv: Both combined with geography type indicator

Usage:
    python get_census_data.py
    
    # With custom API key
    python get_census_data.py --api-key YOUR_KEY
    
    # Specific years only
    python get_census_data.py --years 2015-2023

Author: Created for nonprofit financial distress prediction project
Date: February 2026
"""

import os
import sys
import csv
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package not installed.")
    print("Install with: pip install requests")
    sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================

# Your Census API Key
DEFAULT_API_KEY = "4e9a773026c178dc0dbbf72793de0fd31e2b9d92"

# Census API base URL
CENSUS_API_BASE = "https://api.census.gov/data"

# Colorado FIPS code
COLORADO_FIPS = "08"

# Variables to download
VARIABLES = {
    'B01003_001E': 'total_population',
    'B19013_001E': 'median_household_income',
}

# ACS 5-Year estimate years available
# The year represents the END of the 5-year period
# e.g., 2022 = 2018-2022 estimates
ACS5_YEARS_AVAILABLE = list(range(2009, 2025))  # 2009 through 2024

# Output directory
OUTPUT_DIR = Path("./census_output")

# Rate limiting (be nice to the Census API)
REQUEST_DELAY = 0.5  # seconds between requests

# Logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


# ============================================================================
# SETUP
# ============================================================================

def setup_directories():
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(exist_ok=True)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler()]
    )
    
    return logging.getLogger('census_downloader')


# ============================================================================
# CENSUS API FUNCTIONS
# ============================================================================

def build_api_url(year: int, dataset: str = "acs/acs5") -> str:
    """
    Build the base URL for a Census API request.
    
    Args:
        year: The ACS year (end year of 5-year period)
        dataset: The dataset path (default: acs/acs5 for 5-year estimates)
    
    Returns:
        Base URL string
    """
    return f"{CENSUS_API_BASE}/{year}/{dataset}"


def fetch_census_data(
    year: int,
    variables: List[str],
    geography: str,
    state_fips: str = COLORADO_FIPS,
    api_key: str = None
) -> Optional[List[List]]:
    """
    Fetch data from the Census API.
    
    Args:
        year: ACS year
        variables: List of variable codes (e.g., ['B01003_001E', 'B19013_001E'])
        geography: Geography type ('zip code tabulation area:*' or 'place:*')
        state_fips: State FIPS code (08 for Colorado)
        api_key: Census API key
    
    Returns:
        List of lists (rows) from the API, or None if request failed
    """
    logger = logging.getLogger('census_downloader')
    
    # Build URL
    base_url = build_api_url(year)
    
    # Build variable list
    var_string = ','.join(['NAME'] + variables)
    
    # Build geography specification
    # For ZCTAs, we need special handling - they're not "in" a state in the API
    if 'zip code' in geography.lower():
        # ZCTAs are requested without state filter, then we filter Colorado ZCTAs ourselves
        geo_string = f"for={geography}"
    else:
        # Places (cities) are within a state
        geo_string = f"for={geography}&in=state:{state_fips}"
    
    # Complete URL
    url = f"{base_url}?get={var_string}&{geo_string}"
    
    if api_key:
        url += f"&key={api_key}"
    
    logger.debug(f"Requesting: {url[:100]}...")
    
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        return data
        
    except requests.exceptions.HTTPError as e:
        if response.status_code == 204:
            logger.warning(f"No data available for {year}")
        else:
            logger.error(f"HTTP error for {year}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {year}: {e}")
        return None
    except ValueError as e:
        logger.error(f"JSON decode error for {year}: {e}")
        return None


def filter_colorado_zctas(data: List[List], year: int) -> List[Dict]:
    """
    Filter ZCTA data to only Colorado ZIP codes.
    
    Colorado ZIP codes generally start with 80 or 81.
    
    Args:
        data: Raw data from Census API (list of lists)
        year: Year for the data
    
    Returns:
        List of dictionaries with Colorado ZCTA data
    """
    if not data or len(data) < 2:
        return []
    
    # First row is header
    header = data[0]
    
    # Find column indices
    name_idx = header.index('NAME') if 'NAME' in header else 0
    pop_idx = header.index('B01003_001E') if 'B01003_001E' in header else 1
    income_idx = header.index('B19013_001E') if 'B19013_001E' in header else 2
    zcta_idx = header.index('zip code tabulation area') if 'zip code tabulation area' in header else -1
    
    results = []
    
    for row in data[1:]:  # Skip header
        try:
            # Get ZCTA code
            if zcta_idx >= 0:
                zcta = row[zcta_idx]
            else:
                # Try to extract from NAME field
                name = row[name_idx]
                zcta = name.replace('ZCTA5 ', '').strip()
            
            # Filter to Colorado ZCTAs (start with 80 or 81)
            if not (zcta.startswith('80') or zcta.startswith('81')):
                continue
            
            # Parse values
            population = row[pop_idx] if pop_idx < len(row) else None
            income = row[income_idx] if income_idx < len(row) else None
            
            # Handle null/negative values (Census uses negative numbers for missing data)
            if population and str(population).lstrip('-').isdigit():
                population = int(population)
                if population < 0:
                    population = None
            else:
                population = None
            
            if income and str(income).lstrip('-').isdigit():
                income = int(income)
                if income < 0:
                    income = None
            else:
                income = None
            
            results.append({
                'year': year,
                'acs_period': f"{year-4}-{year}",
                'geography_type': 'ZCTA',
                'geography_id': zcta,
                'geography_name': f"ZCTA {zcta}",
                'zip_code': zcta,
                'total_population': population,
                'median_household_income': income,
            })
            
        except (IndexError, ValueError) as e:
            continue
    
    return results


def parse_places_data(data: List[List], year: int) -> List[Dict]:
    """
    Parse place (city) data from Census API response.
    
    Args:
        data: Raw data from Census API (list of lists)
        year: Year for the data
    
    Returns:
        List of dictionaries with place data
    """
    if not data or len(data) < 2:
        return []
    
    # First row is header
    header = data[0]
    
    # Find column indices
    name_idx = header.index('NAME') if 'NAME' in header else 0
    pop_idx = header.index('B01003_001E') if 'B01003_001E' in header else 1
    income_idx = header.index('B19013_001E') if 'B19013_001E' in header else 2
    place_idx = header.index('place') if 'place' in header else -1
    state_idx = header.index('state') if 'state' in header else -1
    
    results = []
    
    for row in data[1:]:  # Skip header
        try:
            name = row[name_idx]
            
            # Clean up name (remove ", Colorado" suffix)
            city_name = name.replace(', Colorado', '').strip()
            
            # Get place FIPS code
            place_fips = row[place_idx] if place_idx >= 0 and place_idx < len(row) else None
            
            # Parse values
            population = row[pop_idx] if pop_idx < len(row) else None
            income = row[income_idx] if income_idx < len(row) else None
            
            # Handle null/negative values
            if population and str(population).lstrip('-').isdigit():
                population = int(population)
                if population < 0:
                    population = None
            else:
                population = None
            
            if income and str(income).lstrip('-').isdigit():
                income = int(income)
                if income < 0:
                    income = None
            else:
                income = None
            
            results.append({
                'year': year,
                'acs_period': f"{year-4}-{year}",
                'geography_type': 'Place',
                'geography_id': place_fips,
                'geography_name': city_name,
                'city_name': city_name,
                'total_population': population,
                'median_household_income': income,
            })
            
        except (IndexError, ValueError) as e:
            continue
    
    return results


# ============================================================================
# MAIN DOWNLOAD FUNCTIONS
# ============================================================================

def download_zcta_data(
    years: List[int],
    api_key: str = None
) -> List[Dict]:
    """
    Download ZCTA (ZIP code) data for all specified years.
    
    Args:
        years: List of ACS years to download
        api_key: Census API key
    
    Returns:
        List of all ZCTA records across all years
    """
    logger = logging.getLogger('census_downloader')
    logger.info(f"Downloading ZCTA data for {len(years)} years...")
    
    all_results = []
    variables = list(VARIABLES.keys())
    
    for year in years:
        logger.info(f"  Fetching ZCTAs for {year} ({year-4}-{year} estimates)...")
        
        data = fetch_census_data(
            year=year,
            variables=variables,
            geography='zip code tabulation area:*',
            api_key=api_key
        )
        
        if data:
            results = filter_colorado_zctas(data, year)
            all_results.extend(results)
            logger.info(f"    Found {len(results)} Colorado ZCTAs")
        else:
            logger.warning(f"    No data returned for {year}")
        
        time.sleep(REQUEST_DELAY)
    
    logger.info(f"Total ZCTA records: {len(all_results):,}")
    return all_results


def download_places_data(
    years: List[int],
    api_key: str = None
) -> List[Dict]:
    """
    Download Place (city) data for all specified years.
    
    Args:
        years: List of ACS years to download
        api_key: Census API key
    
    Returns:
        List of all Place records across all years
    """
    logger = logging.getLogger('census_downloader')
    logger.info(f"Downloading Place (city) data for {len(years)} years...")
    
    all_results = []
    variables = list(VARIABLES.keys())
    
    for year in years:
        logger.info(f"  Fetching Places for {year} ({year-4}-{year} estimates)...")
        
        data = fetch_census_data(
            year=year,
            variables=variables,
            geography='place:*',
            state_fips=COLORADO_FIPS,
            api_key=api_key
        )
        
        if data:
            results = parse_places_data(data, year)
            all_results.extend(results)
            logger.info(f"    Found {len(results)} Colorado places")
        else:
            logger.warning(f"    No data returned for {year}")
        
        time.sleep(REQUEST_DELAY)
    
    logger.info(f"Total Place records: {len(all_results):,}")
    return all_results


# ============================================================================
# OUTPUT FUNCTIONS
# ============================================================================

def save_zcta_data(data: List[Dict], output_path: Path = None) -> Path:
    """Save ZCTA data to CSV."""
    logger = logging.getLogger('census_downloader')
    
    if not data:
        logger.warning("No ZCTA data to save")
        return None
    
    if output_path is None:
        output_path = OUTPUT_DIR / "colorado_census_zcta.csv"
    
    fieldnames = [
        'year', 'acs_period', 'zip_code', 
        'total_population', 'median_household_income'
    ]
    
    # Sort by ZIP and year
    data_sorted = sorted(data, key=lambda x: (x['zip_code'], x['year']))
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data_sorted)
    
    logger.info(f"ZCTA data saved to: {output_path}")
    return output_path


def save_places_data(data: List[Dict], output_path: Path = None) -> Path:
    """Save Places (city) data to CSV."""
    logger = logging.getLogger('census_downloader')
    
    if not data:
        logger.warning("No Places data to save")
        return None
    
    if output_path is None:
        output_path = OUTPUT_DIR / "colorado_census_places.csv"
    
    fieldnames = [
        'year', 'acs_period', 'city_name', 'geography_id',
        'total_population', 'median_household_income'
    ]
    
    # Sort by city name and year
    data_sorted = sorted(data, key=lambda x: (x['city_name'], x['year']))
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data_sorted)
    
    logger.info(f"Places data saved to: {output_path}")
    return output_path


def save_combined_data(zcta_data: List[Dict], places_data: List[Dict], output_path: Path = None) -> Path:
    """Save combined ZCTA and Places data to a single CSV."""
    logger = logging.getLogger('census_downloader')
    
    if output_path is None:
        output_path = OUTPUT_DIR / "colorado_census_combined.csv"
    
    fieldnames = [
        'year', 'acs_period', 'geography_type', 'geography_id', 'geography_name',
        'total_population', 'median_household_income'
    ]
    
    # Combine and sort
    all_data = zcta_data + places_data
    all_data_sorted = sorted(all_data, key=lambda x: (x['geography_type'], x['geography_name'], x['year']))
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_data_sorted)
    
    logger.info(f"Combined data saved to: {output_path}")
    return output_path


def print_summary(zcta_data: List[Dict], places_data: List[Dict]):
    """Print summary statistics."""
    
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    
    # ZCTA summary
    if zcta_data:
        zcta_years = set(d['year'] for d in zcta_data)
        zcta_zips = set(d['zip_code'] for d in zcta_data)
        print(f"\nZCTA (ZIP Code) Data:")
        print(f"  Years: {min(zcta_years)} - {max(zcta_years)} ({len(zcta_years)} years)")
        print(f"  Unique ZIP codes: {len(zcta_zips)}")
        print(f"  Total records: {len(zcta_data):,}")
    
    # Places summary
    if places_data:
        places_years = set(d['year'] for d in places_data)
        places_cities = set(d['city_name'] for d in places_data)
        print(f"\nPlace (City) Data:")
        print(f"  Years: {min(places_years)} - {max(places_years)} ({len(places_years)} years)")
        print(f"  Unique cities/places: {len(places_cities)}")
        print(f"  Total records: {len(places_data):,}")
    
    # Sample of data
    if zcta_data:
        print(f"\nSample ZCTA data (Denver area ZIPs):")
        denver_zips = [d for d in zcta_data if d['zip_code'].startswith('802') and d['year'] == max(zcta_years)][:5]
        for d in denver_zips:
            pop = f"{d['total_population']:,}" if d['total_population'] else 'N/A'
            inc = f"${d['median_household_income']:,}" if d['median_household_income'] else 'N/A'
            print(f"    {d['zip_code']}: Pop={pop}, Income={inc}")
    
    print("=" * 60)


# ============================================================================
# MAIN
# ============================================================================

def parse_year_range(year_string: str) -> List[int]:
    """Parse a year range string like '2015-2023' into a list of years."""
    if '-' in year_string:
        start, end = year_string.split('-')
        return list(range(int(start), int(end) + 1))
    else:
        return [int(year_string)]


def main():
    """Main entry point."""
    
    parser = argparse.ArgumentParser(
        description='Download Census ACS data for Colorado (population and median income)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download all available years (2009-2024)
    python get_census_data.py
    
    # Download specific years
    python get_census_data.py --years 2015-2023
    
    # Use a different API key
    python get_census_data.py --api-key YOUR_KEY_HERE
    
Output Files:
    - colorado_census_zcta.csv: Data by ZIP code
    - colorado_census_places.csv: Data by city/place
    - colorado_census_combined.csv: Both combined

Notes:
    - ACS 5-year estimates represent a 5-year period (e.g., 2022 = 2018-2022)
    - ZIP code data uses ZCTAs (ZIP Code Tabulation Areas)
    - Colorado ZCTAs start with 80 or 81
        """
    )
    
    parser.add_argument(
        '--api-key', '-k',
        type=str,
        default=DEFAULT_API_KEY,
        help='Census API key (get free at api.census.gov)'
    )
    
    parser.add_argument(
        '--years', '-y',
        type=str,
        default='2009-2024',
        help='Year range to download (e.g., 2015-2023). Default: 2009-2024'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output directory (default: ./census_output)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup
    if args.output:
        global OUTPUT_DIR
        OUTPUT_DIR = Path(args.output)
    
    setup_directories()
    logger = setup_logging(args.verbose)
    
    print("\n" + "=" * 60)
    print("Census ACS Data Downloader for Colorado")
    print("=" * 60)
    
    # Parse years
    requested_years = parse_year_range(args.years)
    
    # Filter to available years
    available_years = [y for y in requested_years if y in ACS5_YEARS_AVAILABLE]
    
    if not available_years:
        logger.error(f"No valid years in range. Available: {min(ACS5_YEARS_AVAILABLE)}-{max(ACS5_YEARS_AVAILABLE)}")
        sys.exit(1)
    
    logger.info(f"Will download data for years: {min(available_years)}-{max(available_years)}")
    logger.info(f"API Key: {'Provided' if args.api_key else 'Not provided (rate limited)'}")
    
    # Download ZCTA data
    print("\n--- Downloading ZCTA (ZIP Code) Data ---")
    zcta_data = download_zcta_data(available_years, args.api_key)
    
    # Download Places data
    print("\n--- Downloading Place (City) Data ---")
    places_data = download_places_data(available_years, args.api_key)
    
    # Save results
    print("\n--- Saving Results ---")
    save_zcta_data(zcta_data)
    save_places_data(places_data)
    save_combined_data(zcta_data, places_data)
    
    # Print summary
    print_summary(zcta_data, places_data)
    
    print(f"\n✓ Success! Data saved to: {OUTPUT_DIR}/")
    print(f"\nTo join with your nonprofit data:")
    print(f"  - Use 'zip_code' column to match with nonprofit ZIP codes")
    print(f"  - Use 'year' column to match with tax year")
    print(f"  - Note: ACS 'year' represents end of 5-year period")


if __name__ == '__main__':
    main()