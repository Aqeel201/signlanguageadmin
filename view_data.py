import sqlite3

def view_data(db_path="sign_videos.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, word, language, resolution, duration FROM videos LIMIT 10")
    rows = cursor.fetchall()
    conn.close()

    print("\n📋 Sample Data from videos table:")
    for row in rows:
        print(row)

if __name__ == "__main__":
    view_data()
