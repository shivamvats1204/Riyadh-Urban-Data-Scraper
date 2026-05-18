import argparse
import concurrent.futures
import csv
import logging
import math

import pandas as pd
import requests
from tqdm import tqdm

try:
    import mapbox_vector_tile
except ImportError:
    mapbox_vector_tile = None

try:
    from shapely.geometry import shape
except ImportError:
    shape = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ZOOM = 15
EXTENT = 4096
BASE_URL = "https://tiles.suhail.ai/maps/riyadh/{z}/{x}/{y}.vector.pbf"

NORTH, WEST = 25.05353592113, 46.39363649381
SOUTH, EAST = 24.44301255252, 47.05126713773
MAX_WORKERS = 8


def lonlat_to_tile(lon: float, lat: float, z: int):
    n = 2**z
    x_tile = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y_tile = int((1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n)
    return x_tile, y_tile


def tile_coords_to_lonlat(px, py, z, tile_x, tile_y, extent=EXTENT):
    fx = px / extent
    fy = (extent - py) / extent
    n = 2**z
    x_frac = tile_x + fx
    y_frac = tile_y + fy
    lon = (x_frac / n) * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y_frac / n)))
    return lon, math.degrees(lat_rad)


def process_tile(tile_info):
    if mapbox_vector_tile is None or shape is None:
        raise ImportError("Install mapbox-vector-tile and shapely before running tile_downloader.py")

    x, y, zoom = tile_info
    centroids = []
    tile_url = BASE_URL.format(z=zoom, x=x, y=y)

    try:
        response = requests.get(tile_url, timeout=10)
        if response.status_code != 200:
            return centroids

        tile = mapbox_vector_tile.decode(response.content)
        if not tile:
            return centroids

        layer = tile[next(iter(tile))]
        for feature in layer.get("features", []):
            props = feature.get("properties", {})
            parcel_id = props.get("parcel_id") or props.get("id")
            parcel_objectid = props.get("parcel_objectid") or props.get("OBJECTID")
            geom = feature.get("geometry")

            if not (parcel_id and parcel_objectid and geom):
                continue

            try:
                centroid = shape(geom).centroid
                lon, lat = tile_coords_to_lonlat(centroid.x, centroid.y, zoom, x, y, EXTENT)
                centroids.append(
                    {
                        "parcel_id": parcel_id,
                        "parcel_objectid": parcel_objectid,
                        "lon": lon,
                        "lat": lat,
                    }
                )
            except Exception as exc:
                logging.debug("Failed parcel in tile %s,%s: %s", x, y, exc)

    except requests.exceptions.Timeout:
        logging.debug("Timeout downloading tile %s,%s", x, y)
    except Exception as exc:
        logging.debug("Error downloading/decoding tile %s,%s: %s", x, y, exc)

    return centroids


def build_tile_grid(zoom: int):
    x_min, y_min = lonlat_to_tile(WEST, NORTH, zoom)
    x_max, y_max = lonlat_to_tile(EAST, SOUTH, zoom)
    x_start, x_end = min(x_min, x_max), max(x_min, x_max)
    y_start, y_end = min(y_min, y_max), max(y_min, y_max)
    return [(x, y, zoom) for x in range(x_start, x_end + 1) for y in range(y_start, y_end + 1)]


def download_centroids(output_csv: str, zoom: int = ZOOM, max_workers: int = MAX_WORKERS) -> None:
    tiles = build_tile_grid(zoom)
    logging.info("Processing %s tiles at zoom %s.", len(tiles), zoom)

    all_centroids = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(process_tile, tiles), total=len(tiles), desc="Tiles"))

    for result in results:
        all_centroids.extend(result)

    df = pd.DataFrame(all_centroids).drop_duplicates(subset=["parcel_id"])
    with open(output_csv, "w", newline="", encoding="utf-8") as file:
        fieldnames = ["parcel_id", "parcel_objectid", "lon", "lat"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(df.to_dict("records"))

    logging.info("Saved %s unique parcel centroids to %s", len(df), output_csv)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Riyadh parcel centroids from vector tiles.")
    parser.add_argument("--output", default="riyadh_parcels_centroids.csv")
    parser.add_argument("--zoom", type=int, default=ZOOM)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()
    download_centroids(args.output, args.zoom, args.workers)


if __name__ == "__main__":
    main()
