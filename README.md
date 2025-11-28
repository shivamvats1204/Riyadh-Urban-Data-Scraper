# **Riyadh Urban Data Scraper & Geospatial ETL 🇸🇦**

A full geospatial data engineering pipeline that reconstructs the entire urban parcel map of **Riyadh, Saudi Arabia** from raw vector tiles.
This tool extracts, repairs, enriches, and exports **1.15M+ land parcels** with zoning rules, building codes, and setback regulations.

---

## 🚀 **Overview**

This project solves a complex geospatial reverse-engineering problem by:

* Extracting raw **Mapbox Vector Tile (MVT)** data
* Correcting projection errors (fixing the *“Null Island / Pacific Ocean displacement”*)
* Enriching parcel centroids with API-based zoning regulations
* Handling mixed-use parcels and dynamic attribute schemas

---

## 🔑 **Key Capabilities**

### **Vector Tile Processing**

* Decodes binary Protocol Buffers (`.pbf`)
* Repairs invalid geometries (self-intersections, null areas) using **Shapely**

### **Coordinate Projection Engine**

* Converts tile integers (0–4096 extent) to **WGS84 lat/lon**
* Fixes Web Mercator Y-axis inversion
* Produces accurate parcel centroids

### **High-Throughput ETL**

* Utilizes `concurrent.futures` for **~50 requests/sec** scraping
* Includes retries, rate-limit handling, and automatic backoff

### **Smart Resume System**

* Checkpoints progress for both steps
* Automatically resumes after a crash or runtime disconnect

### **Mixed-Use Parcel Handling**

* Detects parcels with multiple zoning rules
* “Explodes” them into multiple rows while preserving relationships

### **Dynamic Schema Detection**

* Reads all attributes returned by the API
* No hardcoded field definitions
* Automatically adapts to future API changes

---

## 📂 **Project Structure**

```
├── requirements.txt         # Dependencies (pandas, shapely, mapbox-vector-tile, etc.)
├── tiles_downloader.py      # STEP 1: Extract parcels + centroids from MVT tiles
└── data_downloader.py       # STEP 2: API scraper for building rules/zoning data
```

---

## 🛠️ **Installation**

Clone the repository:

```bash
git clone https://github.com/yourusername/riyadh-urban-scraper.git
cd riyadh-urban-scraper
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## ⚙️ **Usage**

### **Step 1: Extract Geospatial Centroids**

Run:

```bash
python tiles_downloader.py
```

**Input:**
Direct request to Vector Tile Server

**Process:**

* Scans ~4000 tiles (Zoom Level 16)
* Converts tile geometry → WGS84
* Repairs polygons
* Deduplicates overlapping parcels

**Output:**
`riyadh_parcels_centroids_corrected.csv`
*(Parcel ID + Latitude + Longitude)*

---

### **Step 2: Enrich with Building Regulations**

Run:

```bash
python data_downloader.py
```

**Input:**
`riyadh_parcels_centroids_corrected.csv`

**Process:**

* High-concurrency API calls
* Handles mixed-use data
* Expands 1-to-many relationships
* Automatically extracts all returned fields

**Output:**
`riyadh_parcels_full_data_final.csv`
*(Full zoning + regulation dataset)*

> **Note:** Update `INPUT_CSV` and `OUTPUT_CSV` paths in both scripts as needed.

---

## 🧩 **Technical Deep Dive**

### **1. Fixing the "Pacific Ocean" Projection Error**

Raw MVT coordinates are integers relative to each tile. Naively plotting them places Riyadh thousands of kilometers away.

This project implements custom logic similar to **mercantile**:

```python
# Simplified logic from tiles_downloader.py
def tile_coords_to_lonlat(px, py, z, x, y, extent=4096):
    # Correct Web Mercator Y-flip
    fy = (extent - py) / extent  
    ...
```

This correctly transforms tile coordinates → WGS84.

---

### **2. Handling Mixed-Use Zoning**

Some parcels return a list of rules:

```json
"data": [
  { "id": "Rule A" },
  { "id": "Rule B" }
]
```

The script:

* Detects lists
* Iterates each rule
* Creates a separate row per rule
* Preserves parcel → rule relationships

This ensures **no regulatory info is lost**.

---

## ⚠️ **Disclaimer**

This project is for **educational and research purposes only**.
All extracted data belongs to their respective owners.
Please respect the **Terms of Service**, **robots.txt**, and legal restrictions of any API or website you access.
The author is not responsible for misuse.

---