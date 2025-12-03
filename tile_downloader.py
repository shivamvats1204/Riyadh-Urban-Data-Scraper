import requests
import mapbox_vector_tile
from shapely.geometry import shape
import csv
import math
import pandas as pd
import concurrent.futures
from tqdm import tqdm

# -------------------------------
# CONFIGURATION
# -------------------------------
ZOOM = 15
EXTENT = 4096 # Standard MVT extent
CSV_FILE = "/content/drive/MyDrive/suhail/riyadh_parcels_centroids.csv"
BASE_URL = "https://tiles.suhail.ai/maps/riyadh/{z}/{x}/{y}.vector.pbf"

# Bounding box (Defined as North, West, South, East)
# Note: LAT1 is North, LAT2 is South.
NORTH, WEST = 25.05353592113, 46.39363649381  # Upper-left (North, West)
SOUTH, EAST = 24.44301255252, 47.05126713773  # Lower-right (South, East)

MAX_WORKERS = 8
# -------------------------------

def lonlat_to_tile(lon, lat, z):
    """Converts WGS84 (lon, lat) to Z/X/Y tile coordinates."""
    # Standard Web Mercator (Slippy Map) tile calculation
    n = 2 ** z
    x_tile = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y_tile = int((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)
    return x_tile, y_tile

def tile_coords_to_lonlat(px, py, z, tile_x, tile_y, extent=EXTENT):
    """
    Converts tile-local pixel coordinates (px, py) back to WGS84 (lon, lat).

    Fix: Mapbox Vector Tiles use a Y-down coordinate system (top-left origin).
    The tile coordinates must be flipped before transformation to lat/lon.

    px, py are 0 to EXTENT-1.
    """
    # 1. Normalize tile-local pixel to 0-1 range (0-1 for fx, fy)
    fx = px / extent

    # 2. **CRITICAL FIX**: Flip the Y-coordinate (py).
    # MVT coordinates are Y-down (origin at top-left).
    # Web Mercator math expects a Y-up coordinate system (origin at bottom-left) OR
    # the coordinate must be adjusted from the top-left origin.
    fy = (extent - py) / extent # Flips Y and normalizes

    # 3. Apply the standard mercator inverse projection using the normalized coordinates
    n = 2 ** z

    # Compute the fractional tile part for both X and Y
    x_frac = tile_x + fx
    y_frac = tile_y + fy

    lon = (x_frac / n) * 360.0 - 180.0

    # Inverse Mercator projection formula
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y_frac / n)))
    lat = math.degrees(lat_rad)

    return lon, lat

def process_tile(tile_info):
    x, y = tile_info
    centroids = []
    tile_url = BASE_URL.format(z=ZOOM, x=x, y=y)

    try:
        response = requests.get(tile_url, timeout=10)
        if response.status_code != 200:
            # We silently skip 404 (empty desert tiles)
            # print(f"Failed tile {x},{y}: {response.status_code}")
            return centroids

        tile = mapbox_vector_tile.decode(response.content)

        # FIX: The layer name is often not 'parcels', but the first key in the decoded tile.
        if tile:
            # Get the name of the first (and usually only) layer
            layer_name = next(iter(tile))
            layer = tile[layer_name]

            for feature in layer.get("features", []):
                props = feature.get("properties", {})

                # Use a common pattern to find the ID
                parcel_id = props.get("parcel_id") or props.get("id")
                parcel_objectid = props.get("parcel_objectid") or props.get("OBJECTID")

                geom = feature.get("geometry")

                if parcel_id and parcel_objectid and geom:
                    try:
                        # Shapely converts the raw MVT geometry data (in EXTENT units)
                        poly = shape(geom)

                        # Find the centroid in MVT coordinates (0 to EXTENT-1)
                        centroid = poly.centroid

                        # Convert the tile-local centroid to WGS84 lon/lat
                        lon, lat = tile_coords_to_lonlat(
                            centroid.x, centroid.y, ZOOM, x, y, EXTENT
                        )

                        centroids.append({
                            "parcel_id": parcel_id,
                            "parcel_objectid": parcel_objectid,
                            "lon": lon,
                            "lat": lat
                        })
                    except Exception as e:
                        # Log error for a specific parcel
                        print(f"Failed processing parcel in tile {x},{y}: {e}")

    except requests.exceptions.Timeout:
        # Log error for tile download
        print(f"Timeout downloading tile {x},{y}")
    except Exception as e:
        # Log error for general issues (e.g., bad connection, corrupt tile)
        print(f"Error downloading/decoding tile {x},{y}: {e}")

    return centroids

# -------------------------------
# EXECUTION
# -------------------------------

# Compute tile ranges based on the corrected coordinates: (min/max LON, min/max LAT)
x_min, y_min = lonlat_to_tile(WEST, NORTH, ZOOM)
x_max, y_max = lonlat_to_tile(EAST, SOUTH, ZOOM)

# Generate the full grid of tiles to scan
# Ensure the range accounts for the possibility that the y_min > y_max
x_start = min(x_min, x_max)
x_end = max(x_min, x_max)
y_start = min(y_min, y_max)
y_end = max(y_min, y_max)

tiles = [(x, y) for x in range(x_start, x_end + 1)
                for y in range(y_start, y_end + 1)]

print(f"Calculated X range: {x_start} to {x_end}")
print(f"Calculated Y range: {y_start} to {y_end}")
print(f"Total tiles to process: {len(tiles)}")

all_centroids = []

# Use ThreadPoolExecutor with progress bar
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Use list() to force evaluation and collect results from the map generator
    results = list(tqdm(executor.map(process_tile, tiles), total=len(tiles), desc="Processing tiles"))

# Collect all data
for r in results:
    all_centroids.extend(r)

# Filter for uniqueness before saving (essential when scanning tiles due to overlap)
df = pd.DataFrame(all_centroids).drop_duplicates(subset=['parcel_id'])
final_count = len(df)

# Save CSV
with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
    fieldnames = ["parcel_id", "parcel_objectid", "lon", "lat"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(df.to_dict('records'))

print(f"\nSaved {final_count} unique parcel centroids to {CSV_FILE}")