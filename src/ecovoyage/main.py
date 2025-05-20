"""Main module for EcoVoyage."""

import argparse
import requests
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from ecovoyage.dag import run_download_dag


def validate_directories(feeds):
    """Ensure all target directories exist before processing"""
    print("🔍 Verifying directory structure...")
    directories_created = 0
    
    for feed in feeds:
        dir_path = os.path.dirname(feed["local_path"])
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            print(f"📁 Created directory: {dir_path}")
            directories_created += 1
            
    print(f"✅ Verified {len(feeds)} target directories "
          f"({directories_created} new directories created)\n")


def check_feed_update(url, local_path):
    """Check if remote feed is newer than local copy"""
    try:
        # Get remote metadata
        response = requests.head(url, timeout=10)
        response.raise_for_status()
        
        last_modified = response.headers.get('Last-Modified')
        content_length = response.headers.get('Content-Length')

        if not last_modified:
            print("⚠️ No last-modified header - will download")
            return True

        remote_time = parsedate_to_datetime(last_modified)
        local_exists = os.path.exists(local_path)

        if not local_exists:
            print(f"➡️ New feed: {os.path.basename(local_path)}")
            return True

        local_time = datetime.fromtimestamp(
            os.path.getmtime(local_path), tz=timezone.utc
        )
        
        if remote_time > local_time:
            print(f"🔄 Update available: {os.path.basename(local_path)} "
                  f"(Remote: {remote_time.date()} vs Local: {local_time.date()})")
            return True
        
        return False

    except requests.exceptions.RequestException as e:
        print(f"🚨 Error checking {url}: {e}")
        return False


def download(url, local_path):
    """Download a GTFS feed with error handling and progress tracking"""
    try:
        print(f"⏳ Downloading {os.path.basename(local_path)}...")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        # Get server timestamp before writing file
        last_modified = response.headers.get('Last-Modified')
        remote_time = parsedate_to_datetime(last_modified).timestamp() if last_modified else None

        # Stream write with progress
        total_size = int(response.headers.get('Content-Length', 0))
        downloaded = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\r📥 {progress:.1f}% complete", end="")

        # Preserve server timestamp if available
        if remote_time:
            os.utime(local_path, (remote_time, remote_time))

        print(f"\n✅ Saved {os.path.basename(local_path)} ({downloaded//1024} KB)")
        return True

    except Exception as e:
        print(f"\n❌ Failed to download {url}: {e}")
        return False


def download_feeds(max_workers=3):
    """Download and update GTFS and OSM feeds"""
    # CONFIGURATION
    FEEDS = [
        # GTFS Feeds
        {
            "url": "https://api.transitous.org/gtfs/at_Linz-AG-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_linz.gtfs.zip"
        },
        {
            "url": "https://api.transitous.org/gtfs/at_PTA-Carinthia-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_carinthia.gtfs.zip"
        },
        {
            "url": "https://api.transitous.org/gtfs/at_PTA-Eastern-Region-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_vor.gtfs.zip"
        },
        {
            "url": "https://api.transitous.org/gtfs/at_PTA-Salzburg-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_salzburg.gtfs.zip"
        },
        {
            "url": "https://api.transitous.org/gtfs/at_PTA-Styria-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_styria.gtfs.zip"
        },
        {
            "url": "https://api.transitous.org/gtfs/at_PTA-Tyrol-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_tyrol.gtfs.zip"
        },
        {
            "url": "https://api.transitous.org/gtfs/at_PTA-Upper-Austria-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_upperaustria.gtfs.zip"
        }, 
        {
            "url": "https://api.transitous.org/gtfs/at_PTA-Vorarlberg-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_vorarlberg.gtfs.zip"
        },
        {
            "url": "https://api.transitous.org/gtfs/at_Railway-Current-Reference-Data-2025.gtfs.zip",
            "local_path": "/workspace/data/austria/gtfs/at_railway.gtfs.zip"
        }, 
        # OSM Data Feed
        {
            "url": "https://download.geofabrik.de/europe/austria-latest.osm.pbf",
            "local_path": "/workspace/data/austria/osm/austria.osm.pbf"
        },
    ]

    # Run DAG-based download
    run_download_dag(FEEDS, max_workers=max_workers)


def main():
    """Run the main function of the EcoVoyage package."""
    parser = argparse.ArgumentParser(description='EcoVoyage - Planning eco-friendly travel')
    parser.add_argument('--download', action='store_true', help='Download and update GTFS and OSM feeds')
    parser.add_argument('--workers', type=int, default=3, help='Number of concurrent downloads (default: 3)')
    
    args = parser.parse_args()
    
    if args.download:
        download_feeds(max_workers=args.workers)
    else:
        print("EcoVoyage - Planning eco-friendly travel")
        print("Use --download to update GTFS and OSM feeds")
        print("Use --workers to set number of concurrent downloads")


if __name__ == "__main__":
    main() 