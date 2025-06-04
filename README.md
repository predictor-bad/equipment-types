# Equipment Analysis

This Python script analyzes equipment data across multiple customers (tenants) to identify and consolidate common equipments. It performs fuzzy matching on manufacturer names and exact matching on model names to group similar equipment  and generate a master list of commonly used equipment.

---

## Features

* Cleans and normalizes manufacturer and model data
* Groups manufacturers by fuzzy similarity (RapidFuzz)
* Uses exact model name matching
* Parallel processing with progress reporting via `tqdm`
* Outputs most frequently occurring equipment groups across all tenants

---

## Requirements

Install dependencies using:

```bash
pip install pandas rapidfuzz tqdm
```

---

## Input Format

Input must be a CSV file with the following columns:

| Tenant\_ID | Manufacturer | Model  |
| ---------- | ------------ | ------ |
| A001       | Carrier      | 24ABC6 |
| A002       | carrier corp | 24abc6 |
| A001       | Trane        | XR13   |

### Notes:

* Manufacturer and Model values will be normalized (lowercase, no punctuation).
* Empty or invalid rows are removed during processing.

---

## Usage

Run the script from the terminal:

```bash
python equipment-man-model-analysis.py --csv tenant_equipment_data.csv --threshold 80 --min-matches 2
```

### Arguments:

* `--csv`: Path to your input CSV file.
* `--threshold`: Fuzzy match threshold for manufacturer names (0–100).
* `--min-matches`: Minimum number of occurrences of an equipment group for it to appear in the final master list.

---

## Output

The script prints the top equipment types (grouped Manufacturer + Model) used across tenants:

```
Top Equipment Types (Master List):
carrier 24abc6         82
trane xr13             65
...
```

---

## Performance Notes

* Script is parallelized using all CPU cores.
* If you have a large dataset (e.g. > 1M records), performance may vary depending on disk I/O and memory.

---

## License

This project is provided without warranty under an open license. Modify freely for your organization's needs.
