Python script which allows downloading the SRTM data (Version 3.0, 1 arc-second) https://earthdata.nasa.gov/learn/articles/nasa-shuttle-radar-topography-mission-srtm-version-3-0-global-1-arc-second-data-released-over-asia-and-australia.

Each file is 1° X 1° tile at 1 arc-second (about 30 meters) resolution.


python downloader.py -t /mnt/storage/SRTM-1-arc-original -u username -p password -gt -el -tc 20 -se
