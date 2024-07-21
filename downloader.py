import argparse
import logging
import pathlib
import shutil
import sys
import tarfile
import zipfile
import subprocess
import concurrent.futures

from bs4 import BeautifulSoup
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
BASE_SRTM_URL = "https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11"
ASTER_DATA_INDEX_URL = "https://e4ftl01.cr.usgs.gov/ASTT/ASTGTM.003/2000.03.01"
BASE_ASTER_URL = "https://e4ftl01.cr.usgs.gov/ASTT/ASTGTM.003/2000.03.01"


class DownloadException(Exception):
    """Custom exception which could be thrown during the downloading."""

    pass


def hgt_to_geotif(hgt_path: pathlib.Path, geotif_path: pathlib.Path):
    """Convert hgt to geotiff using gdal_translate."""
    logger.info(f"Converting {hgt_path} inti {geotif_path}")
    subprocess.run(
        ["gdal_translate", str(hgt_path), str(geotif_path)],
    )


def hgt_tif_to_geotif_ellipsoidal(input_path: pathlib.Path, output_path: pathlib.Path):
    """Re-project hgt/tif to ellipsoidal geotiff using gdalwarp."""
    output_path = (
        output_path.parent / f"{output_path.stem}_wgs84ellps{output_path.suffix}"
    )
    logger.info(f"Converting {input_path} inti {output_path}")
    subprocess.run(
        [
            "gdalwarp",
            "-s_srs",
            (
                "+proj=longlat +datum=WGS84 +geoidgrids=us_nga_egm96_15.tif +vunits=m"
                " +no_defs +type=crs"
            ),
            "-t_srs",
            "EPSG:4326",
            str(input_path),
            str(output_path),
        ],
    )


def convert_copernicus(input_path: pathlib.Path, output_path: pathlib.Path):
    """Convert copernicus dt2 filr to COG"""
    # We need to remove the overlaped pixel first
    output_path_tif = output_path.with_suffix(".tif")
    subprocess.run(
        [
            "/opt/gdal/bin/gdal_translate",
            "-srcwin",
            "0",
            "0",
            "3600",
            "3600",
            str(input_path),
            str(output_path_tif),
        ],
    )

    output_path = output_path.parent / f"{output_path.stem}_wgs84ellps.tif"
    logger.info(f"Converting {input_path} inti {output_path}")
    subprocess.run(
        [
            "/opt/gdal/bin/gdalwarp",
            "-s_srs",
            (
                "+proj=longlat +datum=WGS84 +geoidgrids=us_nga_egm96_15.tif +vunits=m"
                " +no_defs +type=crs"
            ),
            "-t_srs",
            "EPSG:4326",
            # make output cloud optimized GEOTiff
            "-of",
            "COG",
            "-co",
            "OVERVIEW_COUNT=7",
            "-multi",
            "-wo",
            "NUM_THREADS=ALL_CPUS",
            str(output_path_tif),
            str(output_path),
        ],
    )
    output_path.unlink()


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
    data_type: str,
    target_file: pathlib.Path,
    username: str,
    password: str,
    count: int,
    total: int,
    convert: bool = False,
    unzip: bool = False,
    ellipsoidal: bool = False,
):
    try:
        download_file(url, target_file, username, password)
    except Exception as e:
        logger.error(f"Failed to download {url} into {target_file}")
        return
    if not target_file.exists():
        logger.error(f"Target file does not exist {target_file}")
        return
    logger.info(f"Downloaded ({count}/{total}) {url} into {target_file}")
    if unzip or convert or ellipsoidal:
        geotiff_folder = target_file.parent / "geotiff"
        geotiff_folder.parent.mkdir(parents=True, exist_ok=True)

        if data_type in ["srtm", "aster"]:
            # firstly we need to unzip the file
            with zipfile.ZipFile(str(target_file), "r") as zip_ref:
                if data_type == "srtm":
                    unzipped_file_path = (
                        target_file.parent / zip_ref.filelist[0].filename
                    )
                    zip_ref.extractall(str(target_file.parent))
                else:
                    dem_files = [
                        f.filename for f in zip_ref.filelist if "_dem" in f.filename
                    ]
                    if not dem_files:
                        logger.error(f"Failed to find the DEM file in {target_file}")
                        return
                    unzipped_file_path = geotiff_folder / dem_files[0]
                    zip_ref.extractall(str(geotiff_folder))
        else:
            # Copernicus data sored as a tar archive
            with tarfile.open(str(target_file), "r") as tar_file:
                dt2_files = [
                    f for f in tar_file.getnames() if pathlib.Path(f).suffix == ".dt2"
                ]
                if not dt2_files:
                    logger.error(f"Failed to find dt2 file in {target_file}")
                    return
                dt2_file = dt2_files[0]
                tar_file.extract(dt2_file, path=str(target_file.parent))
                unzipped_file_path = target_file.parent / dt2_file
        target_file.unlink()
        if ellipsoidal:
            geotiff_path = geotiff_folder / "ellipsoidal" / unzipped_file_path.name
            geotiff_path.parent.mkdir(parents=True, exist_ok=True)
            if data_type == "copernicus":
                convert_copernicus(unzipped_file_path, geotiff_path)
            else:
                hgt_tif_to_geotif_ellipsoidal(unzipped_file_path, geotiff_path)
        elif convert and data_type == "srtm":
            geotiff_path = geotiff_folder / unzipped_file_path.with_suffix(".tiff").name
            hgt_to_geotif(unzipped_file_path, geotiff_path)


def download(
    target_folder: pathlib.Path,
    links: set,
    data_type: str,
    username: str,
    password: str,
    threads_count: int = 10,
    convert: bool = False,
    ellipsoidal: bool = False,
    unzip: bool = False,
    skip: bool = False,
):
    """Download all SRTM files."""
    features = []
    total = len(links)
    count = 0

    prefixes_to_ignore = set()
    (target_folder / "geotiff" / "ellipsoidal").mkdir(parents=True, exist_ok=True)
    if skip:
        prefixes_to_ignore = set([
            f.name.split(".")[0].replace("_dem_wgs84ellps", "")
            for f in (target_folder / "geotiff" / "ellipsoidal").iterdir()
        ])

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads_count) as executor:
        for link in links:
            file_name = pathlib.Path(link).name
            target_path = target_folder / file_name
            if skip:
                prefix_to_skip = target_path.name.split(".")[0]
                if prefix_to_skip in prefixes_to_ignore:
                    logger.info(f"Skipping {file_name}")
                    continue
            logger.info(f"Downloading {link} into {target_path}")
            count += 1
            executor.submit(
                process_file,
                link,
                data_type,
                target_path,
                username,
                password,
                count,
                total,
                convert,
                unzip,
                ellipsoidal,
            )


def parse_aster_links() -> set:
    logger.info(f"Fetching ASTER data links from {ASTER_DATA_INDEX_URL}")
    r = requests.get(ASTER_DATA_INDEX_URL)
    soup = BeautifulSoup(r.content, features="html.parser")
    links = soup.select("a")
    results = set()
    for link in links:
        name = link.attrs.get("href")
        if name.lower().endswith("zip"):
            results.add(f"{BASE_ASTER_URL}/{name}")
    logger.info(f"Fetched {len(results)} links")
    return results


def parse_srtm_links(geo_json_path: pathlib.Path) -> set:
    geo_json_list = json.loads(geo_json_path.read_text())["features"]
    results = set()
    for tile_data in geo_json_list:
        file_name = tile_data["properties"]["dataFile"]
        results.add(f"{BASE_SRTM_URL}/{file_name}")
    return results


def get_copernicus_links(data_format: str, release: str):
    """Get copernicus links based on the data format and release.

    data_format on of DTED, DGED
    release one of 2021_1 2022_1, 2023_1
    """
    url = f"https://prism-dem-open.copernicus.eu/pd-desk-open-access/publicDemURLs/COP-DEM_GLO-30-{data_format}__{release}"

    response = requests.get(url, headers={"Accept": "json"})
    if response.status_code != 200:
        raise DownloadException(
            f"Failed to fetch links from {url}, status code: {response.status_code}"
        )

    data = response.json()
    logger.info(f"Found {len(data)} links")
    return set([i["nativeDemUrl"] for i in data])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        "-t",
        help=(
            "Target folder into which SRTM filed will be downloaded, will be created if"
            " not exist"
        ),
        required=True,
    )
    parser.add_argument(
        "--username",
        "-u",
        help="Username for earthdata.nasa.gov",
        required=False,
    )
    parser.add_argument(
        "--password",
        "-p",
        help="Password for earthdata.nasa.gov",
        required=False,
    )

    parser.add_argument(
        "--data_type",
        "-dt",
        help="SRTM, ASTER, copernicus",
        required=True,
    )

    parser.add_argument(
        "--convert-to-geotif",
        "-gt",
        help=(
            "Convert all files to GeoTif (only for SRTM data), required gdal to be"
            " installed!"
        ),
        action="store_true",
    )
    parser.add_argument(
        "--unzip",
        "-uz",
        help="Unzip downloaded data",
        action="store_true",
    )
    parser.add_argument(
        "--ellipsoidal",
        "-el",
        help=(
            "During converting to GoeTiff files will be projected to the ellipsoidal"
            " height."
        ),
        action="store_true",
    )
    parser.add_argument(
        "--threads-count", "-tc", help="How much treads to use", type=int, default=1
    )

    parser.add_argument(
        "--skip-if-exist",
        "-se",
        help="Skip downloading if file exist",
        action="store_true",
    )

    args = parser.parse_args()
    target_folder = pathlib.Path(args.target)
    target_folder.mkdir(parents=True, exist_ok=True)

    data_type = args.data_type.lower()

    if data_type == "srtm":
        links = parse_srtm_links(pathlib.Path(SRTM_GEOJSON_PATH))
    elif data_type == "aster":
        links = parse_aster_links()
        if args.convert_to_geotif:
            logger.warning(
                f"--convert-to-geotif will be ignored "
                f"as ASTER data is already in TIF format"
            )
    elif data_type == "copernicus":
        links = get_copernicus_links("DTED", "2023_1")
    else:
        logger.error(f"Unsupported data type: {data_type}")
        sys.exit(1)

    download(
        target_folder,
        links,
        data_type,
        args.username,
        args.password,
        convert=args.convert_to_geotif,
        ellipsoidal=args.ellipsoidal,
        threads_count=args.threads_count,
        unzip=args.unzip,
        skip=args.skip_if_exist,
    )
