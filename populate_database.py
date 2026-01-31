import sqlite3
import os
from moviepy.editor import VideoFileClip
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def populate_database(db_path="sign_videos.db", video_dirs=None, languages=None):
    """Populate the database with video metadata."""
    if not video_dirs or not languages:
        logger.error("Video directories and languages must be provided")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for language, video_dir in zip(languages, video_dirs):
            if not os.path.exists(video_dir):
                logger.error(f"Directory not found: {video_dir}")
                continue
                
            for filename in os.listdir(video_dir):
                if filename.endswith('.mp4'):
                    word = filename.split('.')[0].lower()
                    file_path = os.path.join(video_dir, filename)
                    video_type = "word" if len(word) > 1 else "letter"
                    
                    # Extract metadata (optional)
                    try:
                        clip = VideoFileClip(file_path)
                        duration = clip.duration
                        resolution = f"{clip.w}x{clip.h}"
                        clip.close()
                    except Exception as e:
                        logger.warning(f"Error processing {filename}: {e}")
                        duration = None
                        resolution = None
                    
                    # Insert into database
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO videos (word, language, file_path, type, duration, resolution, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, (
                            word,
                            language,
                            f"/videos/{language}/{filename}",
                            video_type,
                            duration,
                            resolution
                        ))
                        logger.info(f"Added {word} ({language}) to database")
                    except sqlite3.IntegrityError as e:
                        logger.warning(f"Duplicate file_path for {filename}: {e}")
        
        conn.commit()
        logger.info("Finished populating database")
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    video_dirs = [
        r"D:\Sign Language\FYP_Backend\INDIAN SIGN LANGUAGE ANIMATED VIDEOS"
    ]
    languages = ["PSL", "GSL", "ASL"]
    populate_database(video_dirs=video_dirs, languages=languages)