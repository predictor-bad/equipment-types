# equipment_type_analysis.py

import pandas as pd
import re
import argparse
from collections import defaultdict, Counter
from rapidfuzz import fuzz, process
from tqdm import tqdm  # Progress bar library
from tqdm.contrib.concurrent import process_map
import os

# -------------------------------
# Configuration
# -------------------------------
DEFAULT_FUZZY_MATCH_THRESHOLD = 80
DEFAULT_MIN_MATCH_COUNT = 2
DEFAULT_CHUNKSIZE = 100  # Optimal chunk size for tqdm warning mitigation

# -------------------------------
# Step 1: Load and Normalize Data
# -------------------------------
def normalize_text(text):
    text = str(text).lower().strip()
    text = re.sub(r'[^a-z0-9 ]', '', text)  # Remove special characters
    return text

def load_data(csv_path):
    df = pd.read_csv(csv_path, dtype={'Tenant_ID': str})
    df['Normalized_Manufacturer'] = df['Manufacturer'].apply(normalize_text)
    df['Normalized_Model'] = df['Model'].apply(normalize_text)

    # Remove rows where Manufacturer or Model is missing/empty after normalization
    df = df[(df['Normalized_Manufacturer'] != '') & (df['Normalized_Model'] != '')]

    df['Normalized_Equipment'] = df['Normalized_Manufacturer'] + " " + df['Normalized_Model']
    return df

# ------------------------------------------------------
# Step 2: Fuzzy Grouping on Manufacturer, Exact on Model
# ------------------------------------------------------
def process_model_group(model, df, threshold):
    df_model_group = df[df['Normalized_Model'] == model]
    assigned = set()
    manufacturer_names = df_model_group['Normalized_Manufacturer'].unique().tolist()
    groups = []

    for name in manufacturer_names:
        if name in assigned:
            continue
        group = [name]
        matches = process.extract(name, manufacturer_names, scorer=fuzz.token_sort_ratio)
        for match_name, score, _ in matches:
            if score >= threshold and match_name != name:
                group.append(match_name)
                assigned.add(match_name)
        assigned.add(name)
        groups.append([(model, n) for n in group])
    return groups

# ---------------------------------------------------
# Helper function for multiprocessing (must be top-level)
# ---------------------------------------------------
def unpack_and_process(args_tuple):
    return process_model_group(*args_tuple)

def group_similar_types(df, threshold):
    unique_models = df['Normalized_Model'].unique()
    args = [(model, df, threshold) for model in unique_models]
    results = process_map(unpack_and_process, args, max_workers=os.cpu_count(), chunksize=DEFAULT_CHUNKSIZE, desc="Processing models")
    all_groups = [group for group_list in results for group in group_list]
    return all_groups

# ---------------------------------
# Step 3: Frequency Count Analysis
# ---------------------------------
def compute_frequency(df, groups):
    type_to_group = {}
    for group in groups:
        representative = group[0]
        rep_equipment = representative[1] + " " + representative[0]  # Manufacturer + Model
        for model, manufacturer in group:
            norm_eq = manufacturer + " " + model
            type_to_group[norm_eq] = rep_equipment

    df['Group'] = df['Normalized_Equipment'].map(type_to_group)
    grouped = df.groupby(['Group', 'Tenant_ID']).size().reset_index().groupby('Group').size()
    return grouped.sort_values(ascending=False)

# -----------------------
# Step 4: Generate Master
# -----------------------
def generate_master_list(freq_series, min_match_count=DEFAULT_MIN_MATCH_COUNT):
    return freq_series[freq_series >= min_match_count]

# ------------------
# Sample Entry Point
# ------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze and group equipment types across tenants.")
    parser.add_argument("--csv", type=str, default="tenant_equipment_data.csv", help="Path to the input CSV file")
    parser.add_argument("--threshold", type=int, default=DEFAULT_FUZZY_MATCH_THRESHOLD, help="Fuzzy match threshold (0-100)")
    parser.add_argument("--min-matches", type=int, default=DEFAULT_MIN_MATCH_COUNT, help="Minimum number of total matches required for inclusion")
    args = parser.parse_args()

    csv_path = args.csv
    fuzzy_threshold = args.threshold
    min_match_count = args.min_matches

    df = load_data(csv_path)
    groups = group_similar_types(df, threshold=fuzzy_threshold)
    freq_series = compute_frequency(df, groups)
    master_list = generate_master_list(freq_series, min_match_count=min_match_count)

    pd.set_option('display.max_rows', 5000)

    print("\nTop Equipment Types (Master List):")
    print(master_list)
