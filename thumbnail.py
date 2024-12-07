import os  # Helps us interact with the operating system and manage files
from PIL import Image  # Python library to open, manipulate, and save images
import psycopg2  # Connects us to a PostgreSQL database
import logging  # Helps us record everything the script does
from dotenv import (
    load_dotenv,
)  # Loads sensitive information (like passwords) from a .env file
from pathlib import Path  # Makes working with file paths easier and cleaner
from tqdm import tqdm  # Displays a nice progress bar to show how much work is done

# Step 1: Load Settings
# This pulls sensitive information (like database username and password) from a .env file
load_dotenv()

# Step 2: Database Details
# These are placeholders. Replace them in your .env file with your real database info.
DB_NAME = os.getenv("DB_NAME", "your_database_name")
DB_USER = os.getenv("DB_USER", "your_database_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_database_password")
DB_HOST = os.getenv("DB_HOST", "localhost")

# Step 3: Where to Save Thumbnails
# Thumbnails will be saved in this folder. If it doesn't exist, we create it.
THUMBNAIL_DIR = Path("public/thumbnails")
THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)

# Step 4: Image Types We Support
# This script only works with these types of images. Others will be ignored.
SUPPORTED_IMAGE_EXTENSIONS = {".webp", ".jpeg", ".png", ".bmp", ".jpg", ".tiff"}

# Step 5: Logging
# This keeps a record of everything that happens, so we can debug problems later.
logging.basicConfig(
    level=logging.INFO,  # Log messages like "INFO", "WARNING", and "ERROR"
    format="%(asctime)s [%(levelname)s] %(message)s",  # Show the time, level, and message
    handlers=[
        logging.FileHandler("thumbnail_generation.log"),  # Save logs to this file
        logging.StreamHandler(),  # Also show logs on the screen
    ],
)


# Step 6: Connect to the Database
def get_db_connection():
    """
    Connect to the PostgreSQL database using the credentials from the .env file.
    """
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )


# Step 7: Add a Place for Thumbnails in the Database
def ensure_thumbnail_column():
    """
    Add a column to the images table in the database to store the thumbnail path.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            ALTER TABLE images
            ADD COLUMN IF NOT EXISTS thumbnail_path TEXT;
            """
        )
        conn.commit()  # Save the change
    except Exception as e:
        logging.error(f"Error adding thumbnail_path column: {e}")
    finally:
        cursor.close()  # Close the cursor
        conn.close()  # Close the connection


# Step 8: Create a Thumbnail
def generate_thumbnail(image_path, thumbnail_path):
    """
    Create a smaller version (thumbnail) of the image.
    - Resizes the image to fit within 300x300 pixels while keeping the proportions.
    - Converts the image to a JPEG format if needed.
    """
    try:
        with Image.open(image_path) as img:  # Open the image
            if img.mode in ("RGBA", "P"):  # Convert transparent images to RGB
                img = img.convert("RGB")
            img.thumbnail((300, 300))  # Resize the image
            img.save(thumbnail_path, "JPEG")  # Save it as a JPEG
        logging.info(f"Thumbnail created for {image_path}")
        return True
    except Exception as e:
        logging.error(f"Error creating thumbnail for {image_path}: {e}")
        return False


# Step 9: Process All Images
def create_thumbnails():
    """
    For every image in the database that doesn't already have a thumbnail:
    - Generate a thumbnail
    - Save it to the thumbnails folder
    - Update the database with its location
    """
    ensure_thumbnail_column()  # Ensure the database has space for thumbnail paths

    conn = get_db_connection()  # Connect to the database
    cursor = conn.cursor()

    # Metrics to track progress
    total_images = 0
    total_processed = 0
    total_skipped = 0
    total_errors = 0
    total_success = 0

    try:
        # Fetch images that don't already have thumbnails
        query = """
        SELECT id, file_name, path, extension FROM images
        WHERE extension = ANY(%s) AND thumbnail_path IS NULL
        """
        cursor.execute(query, (list(SUPPORTED_IMAGE_EXTENSIONS),))
        images = cursor.fetchall()  # Get all rows that match the query

        total_images = len(images)  # How many images need thumbnails?

        # Process each image
        with tqdm(total=total_images, desc="Processing images") as pbar:
            for image_id, file_name, path, extension in images:
                if not Path(path).exists():  # Skip missing files
                    logging.warning(f"File not found: {path}")
                    total_skipped += 1
                    pbar.update(1)
                    continue

                # Generate a unique name for the thumbnail
                thumbnail_name = f"{image_id}_thumbnail.jpg"
                thumbnail_path = THUMBNAIL_DIR / thumbnail_name

                if (
                    not thumbnail_path.exists()
                ):  # If the thumbnail doesn't already exist
                    success = generate_thumbnail(path, thumbnail_path)
                    total_processed += 1

                    if success:
                        total_success += 1
                        # Update the database with the thumbnail's location
                        cursor.execute(
                            "UPDATE images SET thumbnail_path = %s WHERE id = %s",
                            (str(thumbnail_path), image_id),
                        )
                        conn.commit()  # Save the change to the database
                    else:
                        total_errors += 1

                pbar.update(1)  # Update the progress bar

    except Exception as e:
        logging.error(f"Error during thumbnail generation: {e}")
    finally:
        # Log a summary of what happened
        logging.info("===== THUMBNAIL GENERATION SUMMARY =====")
        logging.info(f"Total images found: {total_images}")
        logging.info(f"Total processed: {total_processed}")
        logging.info(f"Total successful: {total_success}")
        logging.info(f"Total skipped (file missing): {total_skipped}")
        logging.info(f"Total errors: {total_errors}")

        cursor.close()  # Close the cursor
        conn.close()  # Close the database connection


# Step 10: Run the Script
if __name__ == "__main__":
    create_thumbnails()
