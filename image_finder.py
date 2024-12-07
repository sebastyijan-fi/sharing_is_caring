import os  # For environment variables and file handling
import hashlib  # To create a unique fingerprint (hash) for each file
import psycopg2  # To connect to a PostgreSQL database
import psycopg2.extras  # For easier handling of batch inserts
from psycopg2 import pool  # To manage a pool of database connections
from pathlib import Path  # Makes working with file paths simpler
import psutil  # Helps detect USB drives or mounted devices
from tqdm import tqdm  # Displays a progress bar during long operations
import argparse  # Allows users to interact with the script via commands
import logging  # Keeps a log of what happens during the script's execution
from contextlib import (
    contextmanager,
)  # Simplifies working with resources like database connections
from dotenv import load_dotenv  # For securely loading environment variables
from typing import List, Dict, Optional  # For type annotations (helps readability)

# Step 1: Load environment variables securely from a .env file
load_dotenv()

# Step 2: Set up logging to track what the script is doing
logging.basicConfig(
    filename="photo_vault.log",  # Logs will be saved here
    filemode="a",  # Append to the log file, don't overwrite it
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    level=logging.INFO,  # Only log INFO, WARNING, and ERROR messages
)

# Step 3: Configure the database connection settings
# These values will come from the .env file. Change placeholders in your .env file.
DB_NAME = os.getenv("DB_NAME", "your_database_name")
DB_USER = os.getenv("DB_USER", "your_database_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_database_password")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Step 4: Define which file extensions to scan for (these are image file types)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp", ".dng"}

# Step 5: Set the directories to scan for images
SEARCH_DIRS = [
    Path.home() / "Pictures",  # Default user "Pictures" folder
    Path.home() / "Documents",  # Add more directories here as needed
]

# Step 6: Set directories to exclude from scanning
EXCLUDE_DIRS = {
    "venv",
    ".venv",
    "__pycache__",
}  # Exclude common temporary or virtual environment folders


# Step 7: Database connection pool management
@contextmanager
def get_db_connection(pool: pool.SimpleConnectionPool):
    """
    Handles database connections using a connection pool.
    Automatically releases the connection back to the pool when done.
    """
    conn = pool.getconn()  # Borrow a connection from the pool
    try:
        yield conn  # Allow the connection to be used
    finally:
        pool.putconn(conn)  # Return the connection to the pool


# Step 8: Detect USB devices and add them to the search list (optional feature)
def detect_usb_devices() -> List[Path]:
    """
    Scans for USB devices or mounted drives and returns their paths.
    """
    usb_dirs = []
    partitions = psutil.disk_partitions(all=False)  # Get all mounted partitions
    for partition in partitions:
        if "/media" in partition.mountpoint or "/mnt" in partition.mountpoint:
            usb_dirs.append(Path(partition.mountpoint))
    logging.info(f"Detected USB mount points: {usb_dirs}")
    return usb_dirs


# Step 9: Initialize the database and ensure the necessary table exists
def initialize_database() -> pool.SimpleConnectionPool:
    """
    Sets up a connection pool and ensures the 'images' table exists in the database.
    """
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        with get_db_connection(db_pool) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS images (
                        id SERIAL PRIMARY KEY,
                        file_name TEXT,
                        path TEXT UNIQUE,
                        extension TEXT,
                        size BIGINT,
                        timestamp BIGINT,
                        hash TEXT UNIQUE
                    );
                    """
                )
                conn.commit()
        logging.info("Database initialized successfully.")
        return db_pool
    except psycopg2.Error as e:
        logging.error(f"Database initialization failed: {e}")
        raise


# Step 10: Calculate a unique hash (fingerprint) for each file
def calculate_file_hash(file_path: Path) -> str:
    """
    Reads a file and computes its SHA-256 hash for duplicate detection.
    """
    sha256 = hashlib.sha256()  # Create a SHA-256 hash object
    try:
        with open(file_path, "rb") as f:  # Open the file in binary mode
            for chunk in iter(
                lambda: f.read(8192), b""
            ):  # Read in chunks of 8192 bytes
                sha256.update(chunk)
        return sha256.hexdigest()  # Return the final hash as a string
    except Exception as e:
        logging.error(f"Failed to calculate hash for {file_path}: {e}")
        return ""


# Step 11: Extract metadata for each file
def process_file_metadata(file_path: Path) -> Optional[Dict]:
    """
    Gathers metadata (name, path, size, timestamp, and hash) for a file.
    """
    try:
        if any(
            part in EXCLUDE_DIRS for part in file_path.parts
        ):  # Skip excluded folders
            return None
        file_stat = file_path.stat()  # Get file stats (size, modification time, etc.)
        file_hash = calculate_file_hash(file_path)  # Compute the file's hash
        if not file_hash:  # Skip files that couldn't be hashed
            return None
        return {
            "file_name": file_path.name,
            "path": str(file_path.resolve()),
            "extension": file_path.suffix.lower(),
            "size": file_stat.st_size,
            "timestamp": int(file_stat.st_mtime),
            "hash": file_hash,
        }
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None


# Step 12: Find all image files in the specified directories
def collect_files(base_dirs: List[Path]) -> List[Path]:
    """
    Recursively searches for image files in the specified directories.
    """
    all_files = []
    for base_dir in base_dirs:
        if base_dir.exists():  # Ensure the directory exists
            for file_path in base_dir.rglob("*"):  # Recursively search for files
                if (
                    file_path.is_file()
                    and file_path.suffix.lower() in IMAGE_EXTENSIONS
                    and not file_path.name.startswith(".")  # Skip hidden files
                    and not any(part in EXCLUDE_DIRS for part in file_path.parts)
                ):
                    all_files.append(file_path)
    logging.info(f"Collected {len(all_files)} files to process.")
    return all_files


# Step 13: Insert metadata into the database in batches
def insert_image_records(conn, records: List[Dict]) -> int:
    """
    Inserts a batch of image metadata records into the database.
    """
    if not records:
        return 0
    query = """
        INSERT INTO images (file_name, path, extension, size, timestamp, hash)
        VALUES %s
        ON CONFLICT (hash) DO NOTHING;
    """
    values = [
        (
            record["file_name"],
            record["path"],
            record["extension"],
            record["size"],
            record["timestamp"],
            record["hash"],
        )
        for record in records
    ]
    with conn.cursor() as cursor:
        psycopg2.extras.execute_values(cursor, query, values, page_size=1000)
        inserted = cursor.rowcount  # Count how many records were inserted
        conn.commit()
        logging.info(f"Inserted {inserted} records.")
        return inserted


# Step 14: Catalog all images and store their metadata in the database
def catalog_images(conn_pool, base_dirs: List[Path], batch_size=500):
    """
    Scans for image files, extracts metadata, and saves it to the database.
    """
    all_files = collect_files(base_dirs)  # Find all image files
    records = []  # To store metadata
    with get_db_connection(conn_pool) as conn:
        with tqdm(
            total=len(all_files), desc="Cataloging files"
        ) as pbar:  # Show a progress bar
            for file_path in all_files:
                record = process_file_metadata(file_path)
                if record:
                    records.append(record)
                    if len(records) >= batch_size:  # Insert records in batches
                        insert_image_records(conn, records)
                        records = []
                pbar.update(1)
            if records:  # Insert any remaining records
                insert_image_records(conn, records)


# Step 15: Parse command-line arguments for user interaction
def parse_arguments():
    """
    Sets up the command-line interface for the script.
    """
    parser = argparse.ArgumentParser(description="Image Cataloging Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)
    catalog_parser = subparsers.add_parser("catalog", help="Catalog image files")
    catalog_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of records to insert per batch.",
    )
    return parser.parse_args()


# Step 16: Main entry point for the script
def main():
    args = parse_arguments()
    SEARCH_DIRS.extend(detect_usb_devices())  # Add USB devices to the search
    try:
        conn_pool = initialize_database()  # Set up the database
    except Exception as e:
        print("Failed to initialize database. Check logs for details.")
        logging.error(f"Failed to initialize database: {e}")
        return
    if args.command == "catalog":
        catalog_images(conn_pool, SEARCH_DIRS, batch_size=args.batch_size)
    conn_pool.closeall()  # Close the connection pool


if __name__ == "__main__":
    main()
