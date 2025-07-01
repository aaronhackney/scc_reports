import os
import sys
import requests
from dotenv import load_dotenv
from pathlib import Path
import hashlib
from loguru import logger
from tqdm import tqdm
from typing import List, Dict
import gzip
import shutil

class FileDownloader:
    def __init__(self, api_url: str, api_key: str, download_dir: str, export_dir: str):
        """Initialize the file downloader"""
        self.api_url = api_url
        self.api_key = api_key
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.setup_logging()

    def setup_logging(self):
        """Configure logging"""
        logger.remove()
        logger.add(
            self.download_dir / "download.log",
            rotation="10 MB",
            retention="7 days"
        )
        logger.add(lambda msg: tqdm.write(msg, end=""))

    def get_remote_files(self) -> List[Dict]:
        """Fetch list of files from API"""
        try:
            logger.info(f"Fetching file list from {self.api_url}")
            response = self.session.get(self.api_url, timeout=30, headers={"Authorization": f"Bearer {self.api_key}"})
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching files: {e}")
            return []

    def get_local_files(self) -> Dict[str, str]:
        """Get local files with checksums"""
        local_files = {}
        for f in self.download_dir.glob('*'):
            if f.is_file():
                checksum = self.calculate_checksum(f)
                local_files[f.name] = checksum
        return local_files

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum"""
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum: {e}")
            return ""

    def download_file(self, file_info: Dict) -> bool:
        """Download a single file with progress bar"""
        try:
            file_url = file_info['download_url']
            file_name = file_info['file_name']  
            file_path = self.download_dir / file_name

            logger.info(f"Starting download of {file_name}")
            response = self.session.get(file_url, stream=True, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            
            with tqdm(
                desc=file_name,
                total=total_size,
                unit='iB',
                unit_scale=True
            ) as pbar:
                with open(file_path, 'wb') as f:
                    for data in response.iter_content(1024):
                        size = f.write(data)
                        pbar.update(size)

            logger.success(f"Successfully downloaded {file_name}")
            return True

        except requests.RequestException as e:
            logger.error(f"Download error: {e}")
            return False

    def ungzip_file(self, file_path: Path) -> bool:
        """Decompress a gzip file if it's compressed
        
        Args:
            file_path (Path): Path to the gzip file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not file_path.exists():
                logger.error(f"File {file_path} does not exist")
                return False

            # Check if file is gzip compressed
            with open(file_path, 'rb') as f:
                magic_number = f.read(2)
            if magic_number != b'\x1f\x8b':  # Gzip magic number
                logger.info(f"{file_path} is not a gzip file")
                return True
            
            logger.info(f"Decompressing {file_path} to {self.export_dir}")
            
            with gzip.open(file_path, 'rb') as f_in:
                with open(f"{self.export_dir}/{file_path.name.rstrip('.gz')}", 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            logger.success(f"Successfully decompressed {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error decompressing {file_path}: {e}")
            return False

    def sync_files(self):
        """Download missing or updated files and decompress if needed"""
        logger.info("Starting sync process")
        
        remote_files = self.get_remote_files().get("download_status", [])
        if not remote_files:
            logger.warning("No files found")
            return
            
        local_files = self.get_local_files()
        logger.info(f"Found {len(remote_files)} remote files")
        
        files_to_download = []
        for file_info in remote_files:
            file_name = file_info['file_name']
            if file_name not in local_files:
                files_to_download.append(file_info)
        
        logger.info(f"Need to download {len(files_to_download)} files")
        
        for file_info in files_to_download:
            if self.download_file(file_info):
                file_path = self.download_dir / file_info['file_name']
                # we only want to temporarily unzip these files...
                self.ungzip_file(file_path)

def main(api_url: str, api_key: str, download_dir: str, export_dir: str, verbose: bool):
    """Synchronize files from remote API"""
    if verbose:
        logger.add(sys.stderr, level="DEBUG")
    
    downloader = FileDownloader(api_url, api_key, download_dir, export_dir)
    downloader.sync_files()

if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("API_KEY")
    api_url = os.getenv("API_URL")
    download_dir = os.getenv("DOWNLOAD_DIR", "./downloads")
    export_dir= os.getenv("EXPORT_DIR", "./exports")
    main(api_url, api_key, download_dir, export_dir, False)
