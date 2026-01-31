import os
import nltk
import time
import logging
import tempfile
import shutil
import secrets
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException, Depends, status
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import uvicorn
from dotenv import load_dotenv
import boto3
from nltk.tokenize import word_tokenize

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============ CONFIG ============
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'eu-north-1')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'echosignuobs')

# ============ S3 CLIENT ============
s3_client = None
S3_AVAILABLE = False
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    s3_client.head_bucket(Bucket=BUCKET_NAME)
    S3_AVAILABLE = True
    logger.info(f"S3 Connected: {BUCKET_NAME}")
except Exception as e:
    logger.error(f"S3 Failed: {e}")

# ============ SPEECH FALLBACK (VERCEL LITE) ============
speech_recognizer = None

try:
    import speech_recognition as sr
    speech_recognizer = sr.Recognizer()
    logger.info("Google Speech Recognition initialized (Vercel Lite)")
except Exception as e:
    logger.error(f"Speech Recognition initialization failed: {e}")

nltk.download('punkt', quiet=True)

# ============ INDICES ============
video_index = {}    # Approved
pending_index = {}  # Pending

def get_public_url(key):
    return f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"

def index_s3_files():
    global video_index, pending_index
    if not S3_AVAILABLE: return

    logger.info("Indexing S3...")
    v_idx = {}
    p_idx = {}

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if not key.endswith('.mp4'): continue
                    
                    if key.startswith('pending/'):
                        filename = key.split('/')[-1]
                        p_idx[key] = {
                            "key": key,
                            "filename": filename,
                            "url": get_public_url(key),
                            "date": obj['LastModified'].strftime("%Y-%m-%d %H:%M"),
                            "size": f"{round(obj['Size']/1024, 1)} KB"
                        }
                    else: 
                        # Any other .mp4 is considered an approved sign
                        filename = key.split('/')[-1]
                        word = filename.replace('.mp4', '').lower().replace('_', ' ').strip()
                        if word:
                            v_idx[word] = {
                                "word": word,
                                "key": key,
                                "url": get_public_url(key),
                                "type": "word" if len(word) > 1 else "letter"
                            }
        
        video_index = v_idx
        pending_index = p_idx
        logger.info(f"Indexed: {len(video_index)} Approved, {len(pending_index)} Pending")
    except Exception as e:
        logger.error(f"Index failed: {e}")

# ============ APP ============
app = FastAPI()

# Mount Static
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")
os.makedirs("templates", exist_ok=True)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ============ AUTH HELPER ============
def check_auth(request: Request):
    """Simple check for admin session cookie."""
    if request.cookies.get("admin_session") == "active":
        return True
    return False

# ============ DUPLICATE CHECK ============
def check_duplicate(name: str):
    base_name = name.strip().lower().replace(' ', '_')
    search_word = base_name.replace('_', ' ')
    
    if search_word in video_index: return f"Video '{search_word}' already exists in Approved."
    
    target_filename = f"{base_name}.mp4"
    for p_key in pending_index:
        if p_key.endswith(f"/{target_filename}"): return f"Video '{search_word}' is already in Pending."
    return None

# ============ ROUTES ============

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("admin.html", {
        "request": request, 
        "page": "login",
        "stats": {"approved": 0, "pending": 0, "s3": "N/A", "whisper": "N/A"},
        "pending_videos": [],
        "approved_videos": []
    })

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    if username == "admin" and password == "admin":
        response = RedirectResponse(url="/admin", status_code=303)
        response.set_cookie(key="admin_session", value="active")
        return response
    else:
        return RedirectResponse(url="/login?error=Invalid Credentials", status_code=303)

@app.get("/logout")
def logout():
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("admin_session")
    return response

@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request):
    if not check_auth(request):
        return RedirectResponse("/login")

    error = request.query_params.get("error")
    success = request.query_params.get("success")
    
    approved_list = sorted(list(video_index.values()), key=lambda x: x['word'])
    
    engine_status = "ACTIVE (Google Cloud)" if speech_recognizer else "INACTIVE"
    
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "page": "dashboard",
        "stats": {
            "approved": len(video_index),
            "pending": len(pending_index),
            "s3": "ONLINE" if S3_AVAILABLE else "OFFLINE",
            "whisper": engine_status
        },
        "pending_videos": list(pending_index.values()),
        "approved_videos": approved_list,
        "error": error,
        "success": success
    })

# --- ACTIONS ---

@app.post("/admin/action")
async def admin_action(request: Request, key: str = Form(...), action: str = Form(...)):
    if not check_auth(request): return RedirectResponse("/login")
    if not S3_AVAILABLE: raise HTTPException(500, "S3 Offline")
    
    try:
        if action == "approve":
            filename = key.split('/')[-1]
            s3_client.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': key}, Key=filename)
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
            
        elif action == "reject":
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
            
        index_s3_files()
        return RedirectResponse(url="/admin", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin?error={str(e)}", status_code=303)

@app.post("/admin/delete")
async def admin_delete(request: Request, key: str = Form(...)):
    if not check_auth(request): return RedirectResponse("/login")
    if not S3_AVAILABLE: raise HTTPException(500, "S3 Offline")
    
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
        index_s3_files()
        return RedirectResponse(url="/admin?success=Deleted", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin?error={str(e)}", status_code=303)

@app.post("/admin/edit")
async def admin_edit(request: Request, old_key: str = Form(...), new_name: str = Form(...)):
    """Renames file in S3 by Copy + Delete"""
    if not check_auth(request): return RedirectResponse("/login")
    if not S3_AVAILABLE: raise HTTPException(500, "S3 Offline")

    clean_name = new_name.strip().lower().replace(' ', '_')
    clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
    new_filename = f"{clean_name}.mp4"
    
    if new_filename == old_key:
        return RedirectResponse(url="/admin", status_code=303) # No change

    # Check collision
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=new_filename)
        return RedirectResponse(url=f"/admin?error=Name '{new_name}' already taken", status_code=303)
    except:
        pass # Good, doesn't exist

    try:
        # Copy to new name
        s3_client.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': old_key}, Key=new_filename)
        # Delete old
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=old_key)
        
        index_s3_files()
        return RedirectResponse(url="/admin?success=Renamed", status_code=303)
    except Exception as e:
        return RedirectResponse(url=f"/admin?error={str(e)}", status_code=303)

@app.post("/admin/upload")
async def admin_upload(request: Request, file: UploadFile = File(...), name: str = Form(...)):
    if not check_auth(request): return RedirectResponse("/login")
    if not S3_AVAILABLE: raise HTTPException(500, "S3 Offline")
    
    err = check_duplicate(name)
    if err: return RedirectResponse(url=f"/admin?error={err}", status_code=303)
    
    clean_name = name.strip().lower().replace(' ', '_')
    clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
    filename = f"{clean_name}.mp4"

    try:
        content = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            tmp.write(content)
            temp_path = tmp.name
        s3_client.upload_file(temp_path, BUCKET_NAME, filename, ExtraArgs={'ContentType': 'video/mp4'})
        os.unlink(temp_path)
        index_s3_files()
        return RedirectResponse(url="/admin?success=Uploaded", status_code=303)
    except Exception as e:
         return RedirectResponse(url=f"/admin?error={str(e)}", status_code=303)

# ============ MOBILE API ============
@app.post("/upload")
async def mobile_upload(file: UploadFile = File(...), name: str = Form(...)):
    if not S3_AVAILABLE: raise HTTPException(500, "S3 Offline")
    
    err = check_duplicate(name)
    if err: raise HTTPException(400, err)
    
    clean_name = name.strip().lower().replace(' ', '_')
    clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
    filename = f"{clean_name}.mp4"
    key = f"pending/{filename}"

    try:
        content = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            tmp.write(content)
            temp_path = tmp.name
        s3_client.upload_file(temp_path, BUCKET_NAME, key, ExtraArgs={'ContentType': 'video/mp4'})
        os.unlink(temp_path)
        
        global pending_index
        pending_index[key] = {
             "key": key,
             "filename": filename,
             "url": get_public_url(key),
             "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
             "size": "New"
        }
        return {"message": "Video uploaded for review!", "status": "pending"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/")
def home(request: Request):
    if check_auth(request): return RedirectResponse("/admin")
    return RedirectResponse("/login")

@app.post("/translate")
async def translate(sentence: str = Form(None), audio: UploadFile = File(None)):
    text = ""
    # Only try whisper if audio is present AND model loaded
    # Only try whisper if audio is present AND model loaded
    if audio:
        try:
            content = await audio.read()
            # Create a temp file
            # If we utilize Google Speech Recognition, it prefers WAV.
            # We will assume the frontend is updated to send WAV or we try to process as is.
            # Determine suffix based on what we expect (frontend sends .wav but it might be M4A container)
            # We will save as .mp4 first to be safe since MoviePy handles it well
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
                tmp.write(content)
                temp_path = tmp.name
                
            final_path = temp_path

            # Conversion: Use MoviePy to ensure we have a valid WAV for SpeechRecognition
            try:
                # Import inside function to avoid startup delay
                from moviepy.editor import AudioFileClip
                
                wav_path = temp_path + ".converted.wav"
                # Load audio (works with m4a, mp3, mp4, etc. using built-in ffmpeg binary)
                clip = AudioFileClip(temp_path)
                clip.write_audiofile(wav_path, codec='pcm_s16le', verbose=False, logger=None)
                clip.close()
                
                final_path = wav_path
            except Exception as e:
                logger.error(f"MoviePy conversion failed: {e}")
                # If conversion fails, we fall back to trying the original file
                pass
            
            if speech_recognizer:
                # Google Speech (Now receives a valid WAV from MoviePy)
                try:
                    with sr.AudioFile(final_path) as source:
                        audio_data = speech_recognizer.record(source)
                        text = speech_recognizer.recognize_google(audio_data)
                except sr.UnknownValueError:
                     logger.warning("Google Speech could not understand audio")
                     # If we have sentence, we can ignore this error
                     if not sentence:
                         return {"error": "Could not understand audio. Please speak clearly.", "videos": []}
                except sr.RequestError as e:
                     logger.error(f"Google Speech error: {e}")
                     if not sentence:
                         return {"error": "Speech service unavailable.", "videos": []}
            
            # Cleanup
            if os.path.exists(temp_path): os.unlink(temp_path)
            if os.path.exists(final_path) and final_path != temp_path: os.unlink(final_path)
            
        except Exception as e:
            logger.error(f"Transcription failed: {e}")
            # Don't return error immediately, maybe they sent text too? 
            # But if audio was main intent, let them know.
            if not sentence:
                return {"error": f"Transcription failed: {str(e)}", "videos": []}
    
    if sentence:
        text = sentence.strip() if not text else text + " " + sentence.strip()

    if not text: return {"transcribed_text": "", "keywords": [], "videos": []}

    processed = text.capitalize()
    try:
        keywords = [w for w in word_tokenize(processed) if w.isalpha()]
    except Exception:
        # Fallback if NLTK punkt is missing or fails
        keywords = [w for w in processed.split() if w.isalpha()]
        
    lower_words = [w.lower() for w in keywords]
    
    videos = []
    for word in lower_words:
        if word in video_index:
            videos.append(video_index[word])
        else:
            for char in word:
                if char in video_index:
                    v = video_index[char].copy()
                    v['type'] = 'letter'
                    v['word'] = char.upper() 
                    videos.append(v)
                    
    return {"transcribed_text": processed, "keywords": keywords, "videos": videos}

@app.get("/")
def read_root():
    return {"message": "EchoSign API is running", "engine": "Active"}

@app.get("/health")
def health():
    return {"status": "ok", "indexed": len(video_index)}

@app.on_event("startup")
async def startup():
    # Download NLTK data for production
    try:
        import nltk
        nltk.download('punkt', quiet=True)
        nltk.download('punkt_tab', quiet=True)
    except:
        pass
    index_s3_files()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)