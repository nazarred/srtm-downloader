import argparse
import logging
import pathlib
import shutil
import sys
import zipfile
import subprocess
import concurrent.futures
from requests.auth import HTTPBasicAuth
import requests
import json

logger = logging.getLogger("downloader")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# was found here https://dwtkns.com/srtm30m/srtm30m_bounding_boxes.json
SRTM_GEOJSON_PATH = "geo.json"
BASE_URL = "https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11"


class DownloadException(Exception):
    """Custom exception which could be thrown during the downloading."""

    pass


def hgt_to_geotif(hgt_path: pathlib.Path, geotif_path: pathlib.Path):
    """Convert hgt to geotiff using gdal_translate."""
    logger.info(f"Converting {hgt_path} inti {geotif_path}")
    subprocess.run(
        ["gdal_translate", str(hgt_path), str(geotif_path)],
    )


def hgt_to_geotif_ellipsoidal(hgt_path: pathlib.Path, geotif_path: pathlib.Path):
    """Convert hgt to geotiff using gdal_translate."""
    geotif_path = (
        geotif_path.parent / f"{geotif_path.stem}_wgs84ellps{geotif_path.suffix}"
    )
    logger.info(f"Converting {hgt_path} inti {geotif_path}")
    subprocess.run(
        [
            "gdalwarp",
            "-s_srs",
            "+proj=longlat +datum=WGS84 +geoidgrids=us_nga_egm96_15.tif +vunits=m +no_defs +type=crs",
            "-t_srs",
            "EPSG:4326",
            str(hgt_path),
            str(geotif_path),
        ],
    )


def download_file(url: str, target_path: pathlib.Path, username: str, password: str):
    response = requests.get(url)
    if response.history:
        # when you try to download the request will be redirect for permorming the authentification
        # then we should use the redirected url in a pair with basic auth
        final_url = response.url
    else:
        final_url = url

    with requests.get(
        final_url, stream=True, auth=HTTPBasicAuth(username, password)
    ) as r:
        if r.status_code != 200:
            raise DownloadException(
                f"Server has respond with status code {r.status_code},\ntext: {r.text}"
            )
        with open(str(target_path), "wb") as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)


def process_file(
    url: str,
    target_path: pathlib.Path,
    username: str,
    password: str,
    count: int,
    total: int,
    convert: bool = False,
    ellipsoidal: bool = False,
):
    try:
        download_file(url, target_path, username, password)
    except Exception as e:
        logger.error(f"Failed to download {url} into {target_path}")
        return
    if not target_path.exists():
        logger.error(f"Target file does not exist {target_path}")
        return
    logger.info(f"Downloaded ({count}/{total}) {url} into {target_path}")
    if convert:
        # firstly we need to unzip the file
        with zipfile.ZipFile(str(target_path), "r") as zip_ref:
            unzipped_file_path = target_path.parent / zip_ref.filelist[0].filename
            zip_ref.extractall(str(target_path.parent))
        target_path.unlink()
        geotiff_path = (
            target_path.parent
            / "geotiff"
            / unzipped_file_path.with_suffix(".tiff").name
        )
        geotiff_path.parent.mkdir(parents=True, exist_ok=True)
        if ellipsoidal:
            hgt_to_geotif_ellipsoidal(unzipped_file_path, geotiff_path)
        else:
            hgt_to_geotif(unzipped_file_path, geotiff_path)


def download(
    target_folder: pathlib.Path,
    geo_json_path: pathlib.Path,
    username: str,
    password: str,
    threads_count: int = 10,
    convert: bool = False,
    ellipsoidal: bool = False,
):
    """Download all SRTM files."""
    features = []
    geo_json_list = json.loads(geo_json_path.read_text())["features"]
    total = len(geo_json_list)
    count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads_count) as executor:
        for tile_data in geo_json_list:
            file_name = tile_data["properties"]["dataFile"]
            target_path = target_folder / file_name
            url = f"{BASE_URL}/{file_name}"
            logger.info(f"Downloading {url} into {target_path}")
            count += 1
            executor.submit(
                process_file,
                url,
                target_path,
                username,
                password,
                count,
                total,
                convert,
                ellipsoidal,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        "-t",
        help="Target folder into which SRTM filed will be downloaded, will be created if not exist",
        required=True,
    )
    parser.add_argument(
        "--username",
        "-u",
        help="Username for earthdata.nasa.gov",
        required=True,
    )
    parser.add_argument(
        "--password",
        "-p",
        help="Password for earthdata.nasa.gov",
        required=True,
    )
    parser.add_argument(
        "--convert-to-geotif",
        "-gt",
        help="Convert all files to GeoTif, required gdal to be installed!",
        action="store_true",
    )
    parser.add_argument(
        "--ellipsoidal",
        "-el",
        help="During converting to GoeTiff files will be projected to the ellipsoidal height.",
        action="store_true",
    )
    parser.add_argument(
        "--threads-count", "-tc", help="How much treads to use", type=int, default=1
    )

    args = parser.parse_args()
    target_folder = pathlib.Path(args.target)
    target_folder.mkdir(parents=True, exist_ok=True)

    download(
        target_folder,
        pathlib.Path(SRTM_GEOJSON_PATH),
        args.username,
        args.password,
        convert=args.convert_to_geotif,
        ellipsoidal=args.ellipsoidal,
        threads_count=args.threads_count,
    )
