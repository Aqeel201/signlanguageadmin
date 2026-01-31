import os
import sys
import argparse
import logging
import threading
from pathlib import Path
from typing import Tuple, Optional

import cv2
import boto3
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError, BotoCoreError
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# --------------------------------------------------------------
# 1. Load environment & initialise logging
# --------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------
# 2. S3 client (same config as FastAPI)
# --------------------------------------------------------------
try:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
    BUCKET = os.getenv("S3_BUCKET_NAME")
    # quick sanity check
    s3.head_bucket(Bucket=BUCKET)
    S3_OK = True
    log.info(f"S3 bucket '{BUCKET}' reachable")
except (Boto3Error, BotoCoreError, ClientError) as exc:
    log.error(f"S3 init failed: {exc}")
    S3_OK = False
    s3 = None
    BUCKET = None

# --------------------------------------------------------------
# 3. MongoDB client (same as FastAPI)
# --------------------------------------------------------------
MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client: Optional[MongoClient] = None
collection = None


def connect_mongo() -> bool:
    global mongo_client, collection
    if collection is not None:
        return True

    for attempt in range(1, 4):
        try:
            mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            mongo_client.admin.command("ping")
            db = mongo_client["sign_videos"]
            collection = db["videos"]
            log.info("MongoDB connected")
            return True
        except PyMongoError as e:
            log.warning(f"MongoDB attempt {attempt} failed: {e}")
            if attempt < 3:
                threading.Event().wait(3)
    log.error("MongoDB connection failed after retries")
    return False


# --------------------------------------------------------------
# 4. Helper: video metadata (duration + resolution)
# --------------------------------------------------------------
def video_metadata(path: str) -> Tuple[Optional[float], Optional[str]]:
    try:
        cap = cv2.VideoCapture(path)
        if not cap.isOpened():
            return None, None
        w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.release()
        duration = round(frame_count / fps, 2) if fps > 0 else None
        resolution = f"{w}x{h}"
        return duration, resolution
    except Exception as e:
        log.debug(f"Metadata error for {path}: {e}")
        return None, None


# --------------------------------------------------------------
# 5. Helper: check if object already exists in S3
# --------------------------------------------------------------
def s3_exists(key: str) -> bool:
    if not S3_OK:
        return False
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise  # unexpected error


# --------------------------------------------------------------
# 6. Core upload function
# --------------------------------------------------------------
def upload_file(local_path: Path, key: str, dry_run: bool = False) -> Optional[str]:
    if not S3_OK:
        log.error("S3 not available – cannot upload")
        return None

    if s3_exists(key):
        log.debug(f"Skip (already in S3): {key}")
        return f"https://{BUCKET}.s3.amazonaws.com/{key}"

    if dry_run:
        log.info(f"[DRY-RUN] Would upload: {local_path} → s3://{BUCKET}/{key}")
        return f"https://{BUCKET}.s3.amazonaws.com/{key}"

    try:
        log.info(f"Uploading: {local_path} → s3://{BUCKET}/{key}")
        s3.upload_file(str(local_path), BUCKET, key)
        url = f"https://{BUCKET}.s3.amazonaws.com/{key}"
        log.info(f"Uploaded: {url}")
        return url
    except Exception as e:
        log.error(f"Upload failed for {local_path}: {e}")
        return None


# --------------------------------------------------------------
# 7. Bulk push logic (mirrors your FastAPI bulk_upload_folder)
# --------------------------------------------------------------
def push_dataset(root_folder: str, dry_run: bool = False) -> None:
    root_path = Path(root_folder).resolve()
    if not root_path.is_dir():
        log.error(f"Folder not found: {root_path}")
        return

    mongo_ok = connect_mongo()
    total_uploaded = 0
    total_skipped = 0

    # Collect all operations in a list → bulk_write later (faster)
    mongo_ops = []

    for file_path in root_path.rglob("*.mp4"):
        rel = file_path.relative_to(root_path)
        parts = rel.parts  # e.g. ('PSL', 'hello.mp4') or ('EN', 'cat.mp4')

        # --------------------------------------------------
        # Determine language & word
        # --------------------------------------------------
        language = parts[0].upper() if len(parts) > 1 else "PSL"
        word = file_path.stem.lower()  # filename without extension

        key = f"{language}/{word}.mp4"

        # --------------------------------------------------
        # 1. Upload (or dry-run)
        # --------------------------------------------------
        url = upload_file(file_path, key, dry_run=dry_run)

        if not url:
            continue  # upload failed → skip DB entry

        # --------------------------------------------------
        # 2. Prepare DB entry (only if Mongo is connected)
        # --------------------------------------------------
        if mongo_ok:
            # Skip if already in DB
            if collection.find_one({"word": word, "language": language}):
                total_skipped += 1
                continue

            duration, resolution = video_metadata(str(file_path))
            doc = {
                "word": word,
                "language": language,
                "file_path": url,
                "type": "word" if len(word) > 1 else "letter",
                "duration": duration,
                "resolution": resolution,
            }
            mongo_ops.append(ReplaceOne({"word": word, "language": language}, doc, upsert=True))

        total_uploaded += 1

    # --------------------------------------------------
    # 3. Bulk write to MongoDB (if any)
    # --------------------------------------------------
    if mongo_ok and mongo_ops:
        try:
            result = collection.bulk_write(mongo_ops)
            log.info(
                f"MongoDB bulk_write: {result.upserted_count} upserted, "
                f"{result.modified_count} modified"
            )
        except PyMongoError as e:
            log.error(f"MongoDB bulk_write error: {e}")

    log.info(
        f"Finished. Uploaded: {total_uploaded}, Skipped (already present): {total_skipped}"
    )


# --------------------------------------------------------------
# 8. CLI entry point
# --------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push entire sign-language video dataset to S3 (and optionally MongoDB)."
    )
    parser.add_argument(
        "--path",
        type=str,
        default=r"D:\Sign Language\FYP_Backend\INDIAN SIGN LANGUAGE ANIMATED VIDEOS",
        help="Root folder containing language sub-folders with .mp4 files",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without touching S3 or DB",
    )
    args = parser.parse_args()

    if args.dry_run:
        log.info("=== DRY-RUN MODE ===")

    push_dataset(args.path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()