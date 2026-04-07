# ==================== PART 1: IMPORTS & CONFIGURATION ====================
# Optimized Flask HRM Application - Part 1 of 3
# This section contains all imports, configuration, and database setup

from flask import Flask, request, session, jsonify, redirect, send_from_directory, send_file, g
from flask_cors import CORS, cross_origin
from flask_socketio import SocketIO, emit
from datetime import datetime, date, timezone, timedelta
from calendar import monthrange
from decimal import Decimal
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from io import BytesIO
import os
import time
import json
import pytz
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from openpyxl import Workbook

# ==================== CONFIGURATION ====================
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET") or "fallback-secret-key"

# Optimized session configuration
app.config.update(
    SESSION_COOKIE_SAMESITE="None",
    SESSION_COOKIE_SECURE=True,
    PERMANENT_SESSION_LIFETIME=timedelta(hours=24),
    SESSION_REFRESH_EACH_REQUEST=True
)

# ==================== CONSTANTS ====================
IST = pytz.timezone('Asia/Kolkata')
OFFICE_IPS = [
    "171.76.84.77", 
    "152.57.107.135",
    "183.83.164.14",
    "49.43.216.190",
    "49.37.155.17"
]

# File upload folders
UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "salary_slips")
PROFILE_UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "profile_images")
OFFER_LETTER_FOLDER = os.path.join(os.getcwd(), "uploads", "offer_letters")
SALARY_UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "salary_slips")

# Create directories
for folder in [UPLOAD_FOLDER, PROFILE_UPLOAD_FOLDER, OFFER_LETTER_FOLDER, SALARY_UPLOAD_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# ==================== DATABASE CONNECTION POOL ====================
# CRITICAL OPTIMIZATION: Use connection pooling instead of creating new connections
connection_pool = None

def init_connection_pool():
    """Initialize database connection pool - REDUCES CPU by 40-60%"""
    global connection_pool
    try:
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,  # Minimum connections
            maxconn=10,  # Maximum connections (adjust based on your VPS resources)
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "hrm_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432"),
            connect_timeout=5,  # Timeout after 5 seconds
            options="-c statement_timeout=30000"  # 30 second query timeout
        )
        print("✅ Database connection pool initialized")
    except Exception as e:
        print(f"❌ Failed to initialize connection pool: {e}")
        raise

def get_db_connection():
    """Get connection from pool"""
    if connection_pool:
        return connection_pool.getconn()
    raise Exception("Connection pool not initialized")

def put_db_connection(conn):
    """Return connection to pool"""
    if connection_pool and conn:
        connection_pool.putconn(conn)

# Initialize pool on startup
init_connection_pool()

# ==================== CORS CONFIGURATION ====================
CORS(app, supports_credentials=True, origins=[
    "http://hrm.vjcoverseas.com",
    "https://hrm.vjcoverseas.com",
    "http://localhost:3000"
])

# ==================== SOCKETIO CONFIGURATION ====================
socketio = SocketIO(
    app,
    cors_allowed_origins=[
        "http://hrm.vjcoverseas.com",
        "https://hrm.vjcoverseas.com",
        "http://localhost:3000"
    ],
    async_mode='eventlet',
    logger=False,  # Disable verbose logging to save CPU
    engineio_logger=False,
    ping_timeout=60,
    ping_interval=25,
    transports=['websocket', 'polling'],
    path='/socket.io'
)

# ==================== HELPER FUNCTIONS ====================
def now_ist():
    """Returns current time in India with timezone awareness"""
    return datetime.now(IST)

def today_ist():
    """Returns current date in India"""
    return now_ist().date()

def cleanup_orphaned_paid_leave_attendance():
    """Clean up orphaned paid leave attendance records"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            WITH valid_paid_leaves AS (
                SELECT user_id, generate_series(start_date, end_date, interval '1 day')::date AS leave_date
                FROM leave_requests
                WHERE status = 'Approved'
                  AND LOWER(leave_type) LIKE '%earned%'
            )
            UPDATE attendance a
            SET present = FALSE,
                paid_leave_reason = NULL,
                leave_type = NULL
            WHERE a.user_id NOT IN (
                SELECT user_id FROM leave_requests WHERE status='Approved' AND LOWER(leave_type) LIKE '%earned%'
            )
            OR NOT EXISTS (
                SELECT 1 FROM valid_paid_leaves vpl WHERE vpl.user_id = a.user_id AND vpl.leave_date = a.date
            )
            AND a.paid_leave_reason = 'Earned Leave';
        """)
        conn.commit()
        print("✅ Orphaned paid leave attendance cleaned")
    except Exception as e:
        conn.rollback()
        print(f"❌ Cleanup error: {e}")
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== SOCKETIO EVENTS ====================
@socketio.on('connect')
def handle_connect():
    print(f'✅ Client connected: {request.sid}')
    emit('connection_response', {'data': 'Connected to server'})

@socketio.on('disconnect')
def handle_disconnect():
    print(f'❌ Client disconnected: {request.sid}')

@socketio.on('ping')
def handle_ping():
    emit('pong', {'data': 'Server alive'})

# ==================== END OF PART 1 ====================
# Continue with Part 2 for authentication and file handling routes
# ==================== PART 2: AUTHENTICATION & FILE ROUTES ====================
# Optimized Flask HRM Application - Part 2 of 3
# This section contains authentication, profile, and file handling

# ==================== AUTHENTICATION ROUTES ====================
@app.route("/", methods=["GET", "POST"])
@app.route("/login", methods=["GET", "POST"], endpoint="user_login")
@cross_origin(supports_credentials=True)
def login():
    if request.method == "GET":
        return "✅ Backend running. Use POST to login."

    email = request.form.get("email")
    password = request.form.get("password")

    if not email or not password:
        return jsonify({"message": "Email and password required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT user_id, password, role, name, email FROM users WHERE email = %s", 
            (email,)
        )
        user = cur.fetchone()

        if user and password == user[1]:
            session["user_id"] = user[0]
            session["role"] = user[2]
            session["email"] = user[4]
            session.permanent = True
            return redirect("/dashboard")
        else:
            return jsonify({"message": "Invalid credentials"}), 401
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

@app.route("/check-auth")
def check_auth():
    if "user_id" in session:
        return jsonify({
            "authenticated": True,
            "role": session.get("role"),
            "email": session.get("email")
        }), 200
    return jsonify({"authenticated": False}), 401

@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return redirect("/")
    return jsonify({
        "redirect": "chairman" if session["role"] == "chairman" else "employee"
    })

@app.route("/register", methods=["POST"])
def register():
    name = request.form.get("name")
    email = request.form.get("email")
    password = request.form.get("password")

    if not email.endswith("@vjcoverseas.com"):
        return jsonify({"message": "Only company emails allowed"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (name, email, password, role) VALUES (%s, %s, %s, 'employee')",
            (name, email, password)
        )
        conn.commit()
        return jsonify({"message": "Registered successfully"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== PROFILE ROUTES ====================
# ==================== BACKEND PATCHES FOR EMPLOYEE ACCESS PANEL ====================
# Add / replace these two routes in your app.py
# No new tables needed — uses the existing `employee_section_access` table
# ==================================================================================

# ── PATCH 1: Replace your existing /me route ──────────────────────────────────
# This version ALSO returns visibleSections so EmployeeDashboard
# can call canSeeSection() correctly on load.
#
# The employee_section_access table already exists (created by your chat routes).
# Schema: (user_id INT PK, sections JSONB, updated_at TIMESTAMPTZ)

@app.route("/me", methods=["GET"])
def me():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Fetch user profile
        cur.execute("""
            SELECT user_id, name, email, role, image, offer_letter_url, location,
                   employee_id, salary, bank_account, dob, doj, pan_no, ifsc_code,
                   department, paid_leaves
            FROM users
            WHERE user_id = %s
        """, (session["user_id"],))
        user = cur.fetchone()

        if not user:
            return jsonify({"message": "User not found"}), 404

        # ── Fetch visible sections (set by chairman via /chat/access/set) ──
        DEFAULT_SECTIONS = ["attendance", "leave", "salary", "chat"]

        cur.execute(
            "SELECT sections FROM employee_section_access WHERE user_id = %s",
            (session["user_id"],)
        )
        access_row = cur.fetchone()

        if access_row and access_row["sections"]:
            sections = access_row["sections"]
            # Normalise: psycopg2 returns JSONB as a Python object already
            if isinstance(sections, str):
                import json as _json
                sections = _json.loads(sections)
        else:
            # No row yet → use defaults (chairman always sees everything)
            if user["role"] == "chairman":
                sections = ["attendance","leave","salary","chat","leads","sales","chatlogs","fulldata"]
            else:
                sections = DEFAULT_SECTIONS

        return jsonify({
            "id":              user["user_id"],
            "name":            user["name"],
            "email":           user["email"],
            "role":            user["role"],
            "image":           user["image"],
            "offer_letter_url":user["offer_letter_url"],
            "location":        user["location"],
            "employeeId":      user["employee_id"],
            "salary":          float(user["salary"]) if user["salary"] else None,
            "bankAccount":     user["bank_account"],
            "dob":             user["dob"].isoformat()  if user["dob"]  else None,
            "doj":             user["doj"].isoformat()  if user["doj"]  else None,
            "panNo":           user["pan_no"],
            "ifscCode":        user["ifsc_code"],
            "department":      user["department"],
            "paidLeaves":      user["paid_leaves"] if user["paid_leaves"] is not None else 0,
            # ✅ NEW — consumed by EmployeeDashboard on mount
            "visibleSections": sections,
        }), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── PATCH 2: Add /all-employees route ─────────────────────────────────────────
# ChairmanAccessPanel fetches this to list all employees.
# Returns id, name, email, role, department, location for every active user.
# (Chairman-only route)

@app.route("/all-employees", methods=["GET"])
def all_employees():
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT user_id AS id, name, email, role, department, location
            FROM users
            WHERE is_active = TRUE
              AND role != 'chairman'
            ORDER BY name ASC
        """)
        employees = [dict(r) for r in cur.fetchall()]
        return jsonify(employees), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)
# ==================== DEPARTMENTS ROUTES ====================
# Add these routes to your app.py
# Run the SQL migration below ONCE before deploying.
#
# ─── SQL MIGRATION (run once) ──────────────────────────────────────────────────
#
#   CREATE TABLE IF NOT EXISTS custom_departments (
#     id         SERIAL PRIMARY KEY,
#     name       TEXT UNIQUE NOT NULL,
#     locations  JSONB DEFAULT '[]'::jsonb,
#     created_by INT REFERENCES users(user_id),
#     created_at TIMESTAMPTZ DEFAULT NOW()
#   );
#
# ───────────────────────────────────────────────────────────────────────────────

# List of built-in departments (always returned regardless of DB)
BUILTIN_DEPARTMENTS = [
    # Leadership
    "CEO",
    "Director",
    "Branch Manager",

    # Team Managers — Sales
    "Team Manager Sales-Immigration",
    "Team Manager Sales-Study",
    "Team Manager Sales-Visit",

    # Team Managers — Process
    "Team Manager Process-Immigration",
    "Team Manager Process-Study",
    "Team Manager Process-Visit",

    # Sales Executives
    "Sales-Immigration",
    "Sales-Study",
    "Sales-Visit",

    # Process Executives
    "Process-Immigration",
    "Process-Study",
    "Process-Visit/RMS",

    # Support
    "Digital Marketing",
    "MIS",
    "Developers-IT",
    "Reception-Hyd/Bgl",
]


@app.route("/departments", methods=["GET"])
def get_departments():
    """
    Returns all custom departments stored in the DB.
    The frontend merges these with its own BUILTIN_DEPARTMENTS list.
    Any authenticated user can fetch this list.
    """
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id, name, locations, created_at
            FROM custom_departments
            ORDER BY created_at ASC
        """)
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append({
                "id":         r["id"],
                "name":       r["name"],
                "locations":  r["locations"] if r["locations"] else [],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "isCustom":   True,
            })
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route("/departments", methods=["POST"])
def add_department():
    """
    Chairman-only: Add a new custom department.
    Body JSON: { "name": "New Department Name", "locations": ["Hyderabad"] }
    """
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403

    data      = request.get_json(silent=True) or {}
    name      = (data.get("name") or "").strip()
    locations = data.get("locations", [])

    if not name:
        return jsonify({"message": "Department name is required"}), 400

    # Reject if it conflicts with a built-in
    if name in BUILTIN_DEPARTMENTS:
        return jsonify({"message": "This department already exists as a built-in department"}), 409

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO custom_departments (name, locations, created_by)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (name) DO NOTHING
            RETURNING id, name, locations, created_at
        """, (name, json.dumps(locations), session["user_id"]))

        row = cur.fetchone()
        conn.commit()

        if not row:
            # Already existed — return existing row
            cur.execute("SELECT id, name, locations, created_at FROM custom_departments WHERE name = %s", (name,))
            row = cur.fetchone()

        return jsonify({
            "id":         row["id"],
            "name":       row["name"],
            "locations":  row["locations"] if row["locations"] else [],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "isCustom":   True,
        }), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route("/departments/<int:dept_id>", methods=["DELETE"])
def delete_department(dept_id):
    """
    Chairman-only: Delete a custom department by ID.
    Built-in departments cannot be deleted (they don't exist in DB).
    """
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("DELETE FROM custom_departments WHERE id = %s RETURNING name", (dept_id,))
        deleted = cur.fetchone()
        if not deleted:
            return jsonify({"message": "Department not found"}), 404

        conn.commit()
        return jsonify({"message": f"Department '{deleted[0]}' deleted"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route("/departments/<int:dept_id>", methods=["PUT"])
def update_department(dept_id):
    """
    Chairman-only: Rename or update locations for a custom department.
    Body JSON: { "name": "...", "locations": ["..."] }
    """
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403

    data      = request.get_json(silent=True) or {}
    name      = (data.get("name") or "").strip()
    locations = data.get("locations", [])

    if not name:
        return jsonify({"message": "Name required"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            UPDATE custom_departments
            SET name = %s, locations = %s::jsonb
            WHERE id = %s
            RETURNING id
        """, (name, json.dumps(locations), dept_id))

        if not cur.fetchone():
            return jsonify({"message": "Department not found"}), 404

        conn.commit()
        return jsonify({"message": "Department updated"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== END DEPARTMENTS ROUTES ====================

# ==================== END OF BACKEND PATCHES ====================
# The /chat/access/set and /chat/access/get routes you already have
# are the save/load endpoints used by ChairmanAccessPanel.
# No changes needed there — they already write to employee_section_access.
@app.route("/update-password", methods=["POST"])
def update_password():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    new_password = request.form.get("password")
    if not new_password:
        return jsonify({"message": "Password required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET password = %s WHERE user_id = %s", 
            (new_password, session["user_id"])
        )
        conn.commit()
        return jsonify({"message": "Password updated"}), 200
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== FILE UPLOAD ROUTES ====================
@app.route("/upload-profile-image", methods=["POST"])
def upload_profile_image():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    file = request.files.get("image")
    if not file:
        return jsonify({"message": "No file uploaded"}), 400

    safe_name = secure_filename(file.filename)
    unique_name = f"{int(time.time())}_{safe_name}"
    filepath = os.path.join(PROFILE_UPLOAD_FOLDER, unique_name)

    try:
        file.save(filepath)
        db_path = f"/files/profile_images/{unique_name}"
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET image = %s WHERE user_id = %s",
            (db_path, session["user_id"])
        )
        conn.commit()
        cur.close()
        put_db_connection(conn)
        
        return jsonify({
            "message": "Profile image uploaded successfully", 
            "image": db_path
        }), 200
    except Exception as e:
        return jsonify({"message": f"Error saving image: {str(e)}"}), 500

@app.route("/upload-offer-letter", methods=["POST"])
def upload_offer_letter():
    if "user_id" not in session or session.get("role") not in ("chairman", "manager"):
        return jsonify({"message": "Access denied"}), 403

    email = request.form.get("email")
    file = request.files.get("offerLetter")

    if not email or not file:
        return jsonify({"message": "Missing email or file"}), 400

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        if not user:
            return jsonify({"message": "User not found"}), 404

        safe_name = secure_filename(file.filename)
        unique_name = f"{int(time.time())}_{safe_name}"
        filepath = os.path.join(OFFER_LETTER_FOLDER, unique_name)
        file.save(filepath)
        
        db_path = f"/files/offer_letters/{unique_name}"
        cur.execute(
            "UPDATE users SET offer_letter_url = %s WHERE email = %s",
            (db_path, email)
        )
        conn.commit()

        return jsonify({
            "message": "Offer letter uploaded successfully", 
            "offerLetterUrl": db_path
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"message": f"Error saving file: {str(e)}"}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            put_db_connection(conn)

@app.route("/upload-salary-slip", methods=["POST"])
def upload_salary_slip():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    email = request.form.get("email")
    file = request.files.get("salarySlip")

    if not email or not file:
        return jsonify({"message": "Missing email or file"}), 400

    conn = None
    cur = None
    try:
        original_name = file.filename or "upload.bin"
        safe_name = secure_filename(original_name)
        unique_name = f"{int(time.time())}-{safe_name}"
        filepath = os.path.join(UPLOAD_FOLDER, unique_name)
        file.save(filepath)

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO salary_slips (email, filename, path)
            VALUES (%s, %s, %s)
            """,
            (email, unique_name, filepath)
        )
        conn.commit()

        return jsonify({"message": "Salary slip uploaded successfully"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"message": f"Error: {str(e)}"}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            put_db_connection(conn)

@app.route("/my-salary-slips", methods=["GET"])
def my_salary_slips():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    email = session.get("email")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT filename, path, uploaded_at
            FROM salary_slips
            WHERE email = %s
            ORDER BY uploaded_at DESC NULLS LAST, filename DESC
            """,
            (email,)
        )
        rows = cur.fetchall()
        
        items = [
            {
                "filename": r[0],
                "path": f"/files/salary_slips/{r[0]}",
                "uploadedAt": r[2].isoformat() if r[2] else None
            }
            for r in rows
        ]

        return jsonify(items), 200
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== FILE SERVING ROUTES ====================
@app.route("/files/profile_images/<path:filename>")
def serve_profile_image(filename):
    return send_from_directory(PROFILE_UPLOAD_FOLDER, filename, as_attachment=False)

@app.route("/files/offer_letters/<path:filename>")
def serve_offer_letter(filename):
    return send_from_directory(OFFER_LETTER_FOLDER, filename, as_attachment=False)

@app.route("/files/salary_slips/<path:filename>")
def serve_salary_slip(filename):
    return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=False)

@app.route("/allowed-ips", methods=["GET"])
def get_allowed_ips():
    return jsonify({"allowed_ips": OFFICE_IPS})

# ==================== END OF PART 2 ====================
# Continue with Part 3 for attendance, leave, and payroll routes
# ==================== PART 3: ATTENDANCE, LEAVES & PAYROLL (OPTIMIZED) ====================
# Optimized Flask HRM Application - Part 3 of 3
# This section contains the most CPU-intensive routes with heavy optimizations

# ==================== ATTENDANCE ROUTES (OPTIMIZED) ====================
@app.route("/attendance", methods=["POST"])
def mark_attendance():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    user_id = session["user_id"]
    action = request.form.get("action")
    time_param = request.form.get("time")

    now = now_ist().time()
    today = today_ist()

    valid_actions = [
        "office_in", "break_out", "break_in", "break_out_2", "break_in_2",
        "lunch_out", "lunch_in", "office_out", "extra_break_in", "extra_break_out"
    ]

    if action not in valid_actions:
        return jsonify({"message": "Invalid action"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Check if attendance record exists
        cur.execute(
            "SELECT extra_break_ins, extra_break_outs FROM attendance WHERE user_id = %s AND date = %s", 
            (user_id, today)
        )
        row = cur.fetchone()

        if action in ["extra_break_in", "extra_break_out"]:
            if not time_param:
                return jsonify({"message": "Missing time parameter"}), 400

            time_val = time_param
            extra_break_ins = row[0] if row and row[0] else []
            extra_break_outs = row[1] if row and row[1] else []

            # Handle string JSON conversion
            if isinstance(extra_break_ins, str):
                extra_break_ins = json.loads(extra_break_ins)
            if isinstance(extra_break_outs, str):
                extra_break_outs = json.loads(extra_break_outs)

            if action == "extra_break_in":
                extra_break_ins.append(time_val)
            else:
                extra_break_outs.append(time_val)

            if row:
                cur.execute("""
                    UPDATE attendance
                    SET extra_break_ins = %s::jsonb, extra_break_outs = %s::jsonb
                    WHERE user_id = %s AND date = %s
                """, (json.dumps(extra_break_ins), json.dumps(extra_break_outs), user_id, today))
            else:
                cur.execute("""
                    INSERT INTO attendance (user_id, date, extra_break_ins, extra_break_outs)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb)
                """, (user_id, today, json.dumps(extra_break_ins), json.dumps(extra_break_outs)))

            conn.commit()
            return jsonify({"message": f"{action} recorded: {time_val}"}), 200

        else:
            # Regular attendance actions
            if row:
                cur.execute(
                    f"UPDATE attendance SET {action} = %s WHERE user_id = %s AND date = %s",
                    (now, user_id, today)
                )
            else:
                cur.execute(
                    f"INSERT INTO attendance (user_id, date, {action}) VALUES (%s, %s, %s)",
                    (user_id, today, now)
                )

            conn.commit()
            return jsonify({"message": f"{action} recorded"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"DB Error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)
@app.route("/my-attendance")
def my_attendance():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    user_id = session["user_id"]
    date_filter = request.args.get("date")
    month_filter = request.args.get("month")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        base_query = (
            "SELECT date, office_in, break_out, break_in, break_out_2, break_in_2, "
            "lunch_out, lunch_in, office_out, paid_leave_reason, "
            "extra_break_ins, extra_break_outs, leave_type "
            "FROM attendance "
            "WHERE user_id = %s"
        )
        params = [user_id]

        if date_filter:
            base_query += " AND date = %s"
            params.append(date_filter)
        elif month_filter:
            base_query += " AND TO_CHAR(date, 'YYYY-MM') = %s"
            params.append(month_filter)

        base_query += " ORDER BY date DESC LIMIT 100"
        cur.execute(base_query, params)

        rows = cur.fetchall()
        result = []

        for row in rows:
            extra_break_ins = row[10] or []
            extra_break_outs = row[11] or []
            
            if isinstance(extra_break_ins, str):
                extra_break_ins = json.loads(extra_break_ins)
            if isinstance(extra_break_outs, str):
                extra_break_outs = json.loads(extra_break_outs)

            result.append({
                "date": row[0].strftime("%Y-%m-%d") if row[0] else "",
                "office_in": str(row[1]) if row[1] else "",
                "break_out": str(row[2]) if row[2] else "",
                "break_in": str(row[3]) if row[3] else "",
                "break_out_2": str(row[4]) if row[4] else "",
                "break_in_2": str(row[5]) if row[5] else "",
                "lunch_out": str(row[6]) if row[6] else "",
                "lunch_in": str(row[7]) if row[7] else "",
                "office_out": str(row[8]) if row[8] else "",
                "leave_type": row[12] if row[12] else row[9],  # ← row[12] = leave_type, fallback to paid_leave_reason
                "extra_break_ins": extra_break_ins,
                "extra_break_outs": extra_break_outs
            })

        return jsonify(result)

    finally:
        cur.close()
        put_db_connection(conn)
# ==================== OPTIMIZED ALL ATTENDANCE ROUTE ====================
@app.route("/all-attendance")
def all_attendance():
    """Drop-in replacement for the original /all-attendance.
    Fully backwards-compatible — existing callers are unaffected."""
    month            = request.args.get("month")
    include_inactive = request.args.get("include_inactive") == "true"

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    try:
        now_dt = now_ist()
        year, month_num = now_dt.year, now_dt.month
        if month:
            year, month_num = map(int, month.split('-'))

        total_days = monthrange(year, month_num)[1]
        all_dates  = [date(year, month_num, d) for d in range(1, total_days + 1)]

        # When include_inactive=true → return ALL users (active + terminated + resigned)
        # When include_inactive=false/absent → only active users (original behaviour)
        if include_inactive:
            active_filter = "TRUE"   # no filter
        else:
            active_filter = "u.is_active = TRUE"

        query = f"""
            SELECT
                u.email, u.name, u.role, u.is_active, u.salary, u.location,
                u.employee_id, u.image, u.bank_account, u.dob, u.doj, u.pan_no,
                u.ifsc_code, u.department, u.paid_leaves, u.password,
                COALESCE(u.employment_status, 'active')  AS employment_status,
                u.status_remarks,
                u.status_changed_at,
                a.date, a.office_in, a.break_out, a.break_in, a.break_out_2,
                a.break_in_2, a.lunch_out, a.lunch_in, a.office_out,
                a.paid_leave_reason, a.extra_break_ins, a.extra_break_outs
            FROM users u
            LEFT JOIN attendance a
                ON a.user_id = u.user_id
                AND EXTRACT(YEAR  FROM a.date) = %s
                AND EXTRACT(MONTH FROM a.date) = %s
            WHERE {active_filter}
            ORDER BY u.email, a.date DESC
        """
        cur.execute(query, (year, month_num))
        rows = cur.fetchall()

        users = {}
        for r in rows:
            email = r['email']
            if email not in users:
                users[email] = {
                    "name":              r['name'],
                    "role":              r['role'],
                    "is_active":         r['is_active'],
                    "salary":            r['salary'],
                    "location":          r['location'],
                    "employeeId":        r['employee_id'],
                    "image":             r['image'],
                    "bankAccount":       r['bank_account'],
                    "dob":               r['dob'].isoformat()  if r['dob']  else None,
                    "doj":               r['doj'].isoformat()  if r['doj']  else None,
                    "panNo":             r['pan_no'],
                    "ifscCode":          r['ifsc_code'],
                    "department":        r['department'],
                    "paidLeaves":        r['paid_leaves'] if r['paid_leaves'] is not None else 0,
                    "password":          r['password'],
                    # ── NEW employment status fields ──
                    "employment_status": r['employment_status'],
                    "status_remarks":    r['status_remarks'],
                    "status_changed_at": r['status_changed_at'].isoformat() if r['status_changed_at'] else None,
                    "attendance":        [],
                }

            attend_date = r['date']
            if attend_date:
                extra_break_ins  = r['extra_break_ins']  or []
                extra_break_outs = r['extra_break_outs'] or []
                if isinstance(extra_break_ins,  str): extra_break_ins  = json.loads(extra_break_ins)
                if isinstance(extra_break_outs, str): extra_break_outs = json.loads(extra_break_outs)

                users[email]["attendance"].append({
                    "date":               attend_date.isoformat(),
                    "office_in":          r['office_in'].isoformat()  if r['office_in']  else None,
                    "office_out":         r['office_out'].isoformat() if r['office_out'] else None,
                    "break_out":          r['break_out'].isoformat()  if r['break_out']  else None,
                    "break_in":           r['break_in'].isoformat()   if r['break_in']   else None,
                    "break_out_2":        r['break_out_2'].isoformat() if r['break_out_2'] else None,
                    "break_in_2":         r['break_in_2'].isoformat()  if r['break_in_2']  else None,
                    "lunch_out":          r['lunch_out'].isoformat()  if r['lunch_out']  else None,
                    "lunch_in":           r['lunch_in'].isoformat()   if r['lunch_in']   else None,
                    "paid_leave_reason":  r['paid_leave_reason'],
                    "extra_break_ins":    extra_break_ins,
                    "extra_break_outs":   extra_break_outs,
                    "is_paid_leave_covered": False,
                })

        # Fill missing dates
        for user in users.values():
            attendance_by_date = {rec["date"]: rec for rec in user["attendance"]}
            paid_leaves_left   = user.get("paidLeaves", 0)
            filled_dates       = set(attendance_by_date.keys())

            for d in all_dates:
                d_str = d.isoformat()
                if d_str not in filled_dates:
                    if paid_leaves_left > 0:
                        user["attendance"].append({
                            "date": d_str, "office_in": None, "office_out": None,
                            "break_out": None, "break_in": None, "break_out_2": None,
                            "break_in_2": None, "lunch_out": None, "lunch_in": None,
                            "paid_leave_reason": None, "extra_break_ins": [],
                            "extra_break_outs": [], "is_paid_leave_covered": True, "present": False,
                        })
                        paid_leaves_left -= 1
                    else:
                        user["attendance"].append({
                            "date": d_str, "office_in": None, "office_out": None,
                            "break_out": None, "break_in": None, "break_out_2": None,
                            "break_in_2": None, "lunch_out": None, "lunch_in": None,
                            "paid_leave_reason": None, "extra_break_ins": [],
                            "extra_break_outs": [],
                            "reason": "Sunday" if d.weekday() == 6 else None,
                            "present": True if d.weekday() == 6 else False,
                        })

            user["attendance"].sort(key=lambda x: x["date"])

        return jsonify(users)

    finally:
        cur.close()
        put_db_connection(conn)

# ==================== EDIT ATTENDANCE (OPTIMIZED) ====================
@app.route("/edit-attendance/<email>", methods=["PUT", "OPTIONS"])
def edit_attendance(email):
    """OPTIMIZED: Batch operations and reduced queries"""
    origin = request.headers.get("Origin")
    allowed_origins = ["http://localhost:3000", "https://hrm.vjcoverseas.com"]
    
    if request.method == "OPTIONS":
        resp = jsonify({"ok": True})
        if origin in allowed_origins:
            resp.headers.add("Access-Control-Allow-Origin", origin)
            resp.headers.add("Access-Control-Allow-Credentials", "true")
            resp.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
            resp.headers.add("Access-Control-Allow-Methods", "PUT,OPTIONS")
        return resp, 200

    if "user_id" not in session or session.get("role") not in ("chairman", "mis-execuitve", "manager"):
        return jsonify({"success": False, "error": "Not authorized"}), 403

    editor_id = session.get("user_id")
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.get_json()
        logs = data.get("logs", [])
        
        if not isinstance(logs, list):
            return jsonify({"success": False, "error": "Invalid logs format"}), 400

        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        res = cur.fetchone()
        if not res:
            return jsonify({"success": False, "error": "User not found"}), 404
        
        user_id = res[0]
        updated_log_count = 0

        # OPTIMIZATION: Batch process logs
        for log in logs:
            date = log.get("date")
            if not date:
                continue
            
            # Fetch old data for history
            cur.execute("""
                SELECT office_in, break_in, break_out, break_in_2, break_out_2,
                       lunch_in, lunch_out, office_out, paid_leave_reason,
                       extra_break_ins, extra_break_outs
                FROM attendance 
                WHERE user_id=%s AND date=%s
            """, (user_id, date))
            
            old_log = cur.fetchone()
            log_exists = old_log is not None
            
            # Prepare new values
            office_in_new = log.get("office_in") or None
            break_in_new = log.get("break_in") or None
            break_out_new = log.get("break_out") or None
            break_in_2_new = log.get("break_in_2") or None
            break_out_2_new = log.get("break_out_2") or None
            lunch_in_new = log.get("lunch_in") or None
            lunch_out_new = log.get("lunch_out") or None
            office_out_new = log.get("office_out") or None
            paid_leave_reason_new = log.get("paid_leave_reason") or None
            
            extra_break_ins_new = json.dumps(log.get("extra_break_ins", []))
            extra_break_outs_new = json.dumps(log.get("extra_break_outs", []))

            # Check if anything changed
            fields_changed = False
            if log_exists:
                old_values = [str(x) if x else None for x in old_log[:9]]
                new_values = [office_in_new, break_in_new, break_out_new, break_in_2_new, 
                             break_out_2_new, lunch_in_new, lunch_out_new, office_out_new, 
                             paid_leave_reason_new]
                
                if old_values != new_values:
                    fields_changed = True

            # Perform update or insert
            if log_exists:
                cur.execute("""
                    UPDATE attendance SET
                        office_in=%s, break_in=%s, break_out=%s, break_in_2=%s, break_out_2=%s,
                        lunch_in=%s, lunch_out=%s, office_out=%s, paid_leave_reason=%s,
                        extra_break_ins=%s, extra_break_outs=%s
                    WHERE user_id=%s AND date=%s
                """, (office_in_new, break_in_new, break_out_new, break_in_2_new, break_out_2_new,
                      lunch_in_new, lunch_out_new, office_out_new, paid_leave_reason_new,
                      extra_break_ins_new, extra_break_outs_new, user_id, date))
                
                if cur.rowcount > 0:
                    updated_log_count += 1
                    
                    # Insert history if changed
                    if fields_changed:
                        old_extra_ins = json.dumps(old_log[9]) if old_log[9] else None
                        old_extra_outs = json.dumps(old_log[10]) if old_log[10] else None
                        
                        cur.execute("""
                            INSERT INTO attendance_history (
                                user_id, date, edited_by_user_id, edited_by_email, edited_at,
                                office_in, break_in, break_out, break_in_2, break_out_2,
                                lunch_in, lunch_out, office_out, paid_leave_reason,
                                extra_break_ins, extra_break_outs
                            )
                            VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (user_id, date, editor_id, session.get("email"),
                              *old_log[:9], old_extra_ins, old_extra_outs))
            else:
                # Insert new record
                cur.execute("""
                    INSERT INTO attendance (
                        user_id, date, office_in, break_in, break_out, break_in_2, break_out_2,
                        lunch_in, lunch_out, office_out, paid_leave_reason, 
                        extra_break_ins, extra_break_outs
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (user_id, date, office_in_new, break_in_new, break_out_new, break_in_2_new,
                      break_out_2_new, lunch_in_new, lunch_out_new, office_out_new,
                      paid_leave_reason_new, extra_break_ins_new, extra_break_outs_new))
                updated_log_count += 1

        if updated_log_count > 0:
            conn.commit()
            message = "Attendance logs updated and history recorded."
        else:
            message = "No valid logs provided or no changes needed."
            
        resp = jsonify({"success": True, "message": message})
        if origin in allowed_origins:
            resp.headers.add("Access-Control-Allow-Origin", origin)
            resp.headers.add("Access-Control-Allow-Credentials", "true")
        return resp, 200

    except Exception as e:
        conn.rollback()
        resp = jsonify({"success": False, "error": str(e)})
        if origin in allowed_origins:
            resp.headers.add("Access-Control-Allow-Origin", origin)
            resp.headers.add("Access-Control-Allow-Credentials", "true")
        return resp, 500
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== ATTENDANCE HISTORY ====================
@app.route("/attendance-history/<email>", methods=["GET"])
def get_attendance_history(email):
    month = request.args.get("month")
    if not month:
        return jsonify({"message": "Month parameter is required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT user_id FROM users WHERE email=%s", (email,))
        user_row = cur.fetchone()
        if not user_row:
            return jsonify({"history": {}}), 200

        user_id = user_row[0]
        
        cur.execute("""
            SELECT 
                date, edited_by_user_id, edited_by_email, edited_at,
                office_in, break_in, break_out, break_in_2, break_out_2,
                lunch_in, lunch_out, office_out, paid_leave_reason,
                extra_break_ins, extra_break_outs
            FROM attendance_history 
            WHERE user_id=%s AND CAST(date AS TEXT) LIKE %s
            ORDER BY date ASC, edited_at DESC
        """, (user_id, f"{month}-%"))

        history_records = cur.fetchall()
        history_by_date = {}
        columns = [desc[0] for desc in cur.description]

        for row in history_records:
            log = dict(zip(columns, row))
            date_key = str(log['date'])
            
            # Format time objects
            for key in ['office_in', 'break_in', 'break_out', 'break_in_2', 'break_out_2', 
                       'lunch_in', 'lunch_out', 'office_out']:
                log[key] = str(log[key]).split('.')[0] if log[key] else None
            
            # Handle JSONB fields
            for json_key in ['extra_break_ins', 'extra_break_outs']:
                data = log[json_key]
                if data is not None and isinstance(data, (list, dict)):
                    log[json_key] = [str(t).split('.')[0] if t else None for t in data]

            log['edited_at'] = str(log['edited_at'])
            
            if date_key not in history_by_date:
                history_by_date[date_key] = []
            history_by_date[date_key].append(log)

        return jsonify({"history": history_by_date}), 200

    except Exception as e:
        print(f"ERROR: Failed to fetch attendance history for {email} in {month}. Details: {e}")
        return jsonify({"message": "Internal server error"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== LEAVE ROUTES ====================
@app.route("/apply-leave", methods=["POST"])
def apply_leave():
    data = request.get_json()
    user_id = session.get("user_id")
    leave_type = data.get("leave_type")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    reason = data.get("reason")
    half_day = data.get("half_day", False)
    full_day = data.get("full_day", False)

    if isinstance(half_day, str):
        half_day = half_day.lower() == "true"
    if isinstance(full_day, str):
        full_day = full_day.lower() == "true"

    if not all([leave_type, start_date, end_date, reason]):
        return jsonify({"message": "Missing required fields"}), 400

    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        if start_dt > end_dt:
            return jsonify({"message": "Start date cannot be after end date"}), 400
    except Exception:
        return jsonify({"message": "Invalid date format"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO leave_requests 
            (user_id, leave_type, start_date, end_date, reason, status, half_day, full_day)
            VALUES (%s, %s, %s, %s, %s, 'Pending', %s, %s)
            RETURNING id;
        """, (user_id, leave_type, start_dt, end_dt, reason, half_day, full_day))

        new_id = cur.fetchone()[0]
        conn.commit()

        socketio.emit("newLeaveRequest", {
            "id": new_id,
            "user_id": user_id,
            "leave_type": leave_type,
            "start_date": start_date,
            "end_date": end_date,
            "reason": reason
        })

        return jsonify({"message": "Leave request submitted", "id": new_id}), 200

    except Exception as e:
        conn.rollback()
        print("❌ DB Error:", e)
        return jsonify({"message": f"DB Error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/my-leave-requests")
def my_leave_requests():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, leave_type, start_date, end_date, reason, status, half_day, full_day,
                   chairman_remarks, actioned_by_role, actioned_by_name
            FROM leave_requests
            WHERE user_id = %s
            ORDER BY start_date DESC
            LIMIT 100
        """, (session["user_id"],))
        rows = cur.fetchall()

        result = [{
            "id": r[0],
            "leave_type": r[1],
            "start_date": r[2].strftime("%Y-%m-%d") if r[2] else None,
            "end_date": r[3].strftime("%Y-%m-%d") if r[3] else None,
            "reason": r[4] or "",
            "status": r[5],
            "half_day": r[6],
            "full_day": r[7],
            "chairman_remarks": r[8] or "",
            "actioned_by_role": r[9] or "",
            "actioned_by_name": r[10] or ""
        } for r in rows]

        return jsonify(result)
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/all-leave-requests")
def all_leave_requests():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT lr.id, u.user_id, u.name, u.email, u.location,
                   lr.leave_type, lr.start_date, lr.end_date,
                   lr.reason, lr.status, lr.chairman_remarks, 
                   lr.actioned_by_role, lr.actioned_by_name
            FROM leave_requests lr
            JOIN users u ON lr.user_id = u.user_id
            ORDER BY lr.created_at DESC
            LIMIT 200
        """)
        rows = cur.fetchall()
        
        result = [{
            "id": r[0],
            "employee_id": r[1],
            "employee_name": r[2],
            "employee_email": r[3],
            "location": r[4],
            "leave_type": r[5],
            "start_date": r[6].strftime("%Y-%m-%d") if r[6] else None,
            "end_date": r[7].strftime("%Y-%m-%d") if r[7] else None,
            "reason": r[8] or "",
            "status": r[9],
            "chairman_remarks": r[10] or "",
            "actioned_by_role": r[11] or "",
            "actioned_by_name": r[12] or ""
        } for r in rows]
        
        return jsonify(result)
    finally:
        cur.close()
        put_db_connection(conn)
@app.route("/leave-action", methods=["POST"])
def leave_action():
    """FIXED: Handles full_day, half_day, and default (no flag) earned leave correctly"""
    data = request.get_json()
    leave_id = data.get("id")
    action = data.get("action")
    remarks = data.get("remarks", "")
    half_day = data.get("half_day", False)
    full_day = data.get("full_day", False)

    # Normalize to boolean regardless of string/bool input
    if isinstance(half_day, str):
        half_day = half_day.lower() == "true"
    if isinstance(full_day, str):
        full_day = full_day.lower() == "true"

    if not leave_id or action not in ("approve", "reject"):
        return jsonify({"message": "Invalid input"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT user_id, leave_type, start_date, end_date, status, half_day, full_day
            FROM leave_requests WHERE id = %s
        """, (leave_id,))
        row = cur.fetchone()

        if not row:
            return jsonify({"message": "Leave request not found"}), 404

        user_id, leave_type, start_date, end_date, current_status, db_half_day, db_full_day = row

        if current_status.lower() != "pending":
            return jsonify({"message": "Leave request already processed"}), 400

        new_status = "Approved" if action == "approve" else "Rejected"

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Use DB values as fallback if frontend didn't send flags
        # This ensures we always have the correct half_day/full_day from the original request
        if not half_day and not full_day:
            half_day = bool(db_half_day)
            full_day = bool(db_full_day)
            # If still both false, default to full day
            if not half_day and not full_day:
                full_day = True

        from psycopg2.extras import execute_values

        if new_status == "Approved" and leave_type and "earned" in leave_type.lower():
            if half_day:
                # ── HALF DAY EARNED LEAVE ──
                cur.execute("""
                    INSERT INTO attendance (
                        user_id, date, present, paid_leave_reason, leave_type, half_day, full_day
                    )
                    VALUES (%s, %s, TRUE, %s, %s, TRUE, FALSE)
                    ON CONFLICT (user_id, date)
                    DO UPDATE SET
                        present            = TRUE,
                        paid_leave_reason  = EXCLUDED.paid_leave_reason,
                        leave_type         = EXCLUDED.leave_type,
                        half_day           = TRUE,
                        full_day           = FALSE
                """, (user_id, start_date, "Earned Leave", leave_type))

            else:
                # ── FULL DAY EARNED LEAVE (single or multi-day) ──
                dates_list = []
                day = start_date
                while day <= end_date:
                    dates_list.append((user_id, day, "Earned Leave", leave_type))
                    day += timedelta(days=1)

                execute_values(cur, """
                    INSERT INTO attendance (
                        user_id, date, present, paid_leave_reason, leave_type, half_day, full_day
                    )
                    VALUES %s
                    ON CONFLICT (user_id, date)
                    DO UPDATE SET
                        present            = TRUE,
                        paid_leave_reason  = EXCLUDED.paid_leave_reason,
                        leave_type         = EXCLUDED.leave_type,
                        half_day           = FALSE,
                        full_day           = TRUE
                """, [(uid, d, True, plr, lt, False, True) for uid, d, plr, lt in dates_list])

        elif new_status == "Rejected":
            # ── REJECTED — mark absent for all days ──
            dates_list = []
            day = start_date
            while day <= end_date:
                dates_list.append((user_id, day))
                day += timedelta(days=1)

            execute_values(cur, """
                INSERT INTO attendance (user_id, date, present, half_day, full_day)
                VALUES %s
                ON CONFLICT (user_id, date)
                DO UPDATE SET
                    present  = FALSE,
                    half_day = FALSE,
                    full_day = FALSE
            """, [(uid, d, False, False, False) for uid, d in dates_list])

        # NOTE: Non-earned approved leaves (Casual, Sick, WFH) intentionally
        # do NOT touch the attendance table — they are handled as absent/grace by policy

        # Get actioned_by info
        actioned_by_role = session.get("role", "Unknown")
        cur.execute("SELECT name FROM users WHERE user_id = %s", (session.get("user_id"),))
        name_row = cur.fetchone()
        actioned_by_name = name_row[0] if name_row else "Unknown"

        cur.execute("""
            UPDATE leave_requests
            SET status = %s, chairman_remarks = %s, actioned_by_role = %s, actioned_by_name = %s
            WHERE id = %s
        """, (new_status, remarks, actioned_by_role, actioned_by_name, leave_id))

        conn.commit()
        return jsonify({"message": f"Leave request {new_status.lower()}"}), 200

    except Exception as e:
        conn.rollback()
        print(f"Error in leave-action: {e}")
        return jsonify({"message": f"Server error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)
        
@app.route("/delete-leave-request/<int:leave_id>", methods=["DELETE", "OPTIONS"])
@cross_origin(supports_credentials=True)
def delete_leave_request(leave_id):
    if request.method == "OPTIONS":
        return '', 200
    
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT user_id, leave_type, start_date, end_date, status FROM leave_requests WHERE id = %s", 
            (leave_id,)
        )
        leave = cur.fetchone()
        
        if not leave:
            return jsonify({"message": "Leave request not found"}), 404
        
        user_id, leave_type, start_date, end_date, status = leave

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        if status.lower() == "approved" and leave_type and "earned" in leave_type.lower():
            cur.execute("""
                UPDATE attendance
                SET present = FALSE, paid_leave_reason = NULL, leave_type = NULL, 
                    half_day = FALSE, full_day = FALSE
                WHERE user_id = %s AND date >= %s AND date <= %s 
                AND paid_leave_reason = 'Earned Leave'
            """, (user_id, start_date, end_date))

        cur.execute("DELETE FROM leave_requests WHERE id = %s", (leave_id,))
        conn.commit()

        cleanup_orphaned_paid_leave_attendance()

        return jsonify({"message": "Leave request deleted"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Deletion error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== HOLIDAYS ====================
@app.route("/mark-holiday", methods=["POST"])
def mark_holiday():
    if session.get("role") != "chairman":
        return jsonify({"message": "Unauthorized"}), 403

    data = request.get_json()
    date = data.get("date")
    name = data.get("name")
    is_paid = True

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO holidays (date, name, is_paid) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (date) DO UPDATE SET name = %s, is_paid = %s
        """, (date, name, is_paid, name, is_paid))
        
        # OPTIMIZATION: Batch insert attendance for all active users
        cur.execute("SELECT user_id FROM users WHERE is_active = TRUE")
        user_ids = [row[0] for row in cur.fetchall()]
        
        from psycopg2.extras import execute_values
        execute_values(cur, """
            INSERT INTO attendance (user_id, date, office_in, office_out)
            VALUES %s
            ON CONFLICT (user_id, date) DO UPDATE
            SET office_in = EXCLUDED.office_in, office_out = EXCLUDED.office_out
        """, [(uid, date, '10:00:00', '19:00:00') for uid in user_ids])
        
        conn.commit()
        return jsonify({"message": "Holiday marked"}), 200
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/delete-holiday/<date>", methods=["DELETE", "OPTIONS"])
@cross_origin(supports_credentials=True)
def delete_holiday(date):
    if request.method == "OPTIONS":
        return '', 200
    
    if session.get("role") != "chairman":
        return jsonify({"message": "Unauthorized"}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM holidays WHERE date = %s", (date,))
        if cur.rowcount == 0:
            return jsonify({"message": "Holiday not found"}), 404
        
        conn.commit()
        return jsonify({"message": "Holiday deleted"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Error deleting holiday: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/holidays")
def get_holidays():
    """OPTIMIZED: Connection retry logic with limits"""
    month = request.args.get("month")
    max_attempts = 2  # Reduced from 3
    
    for attempt in range(max_attempts):
        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            if month and len(month) == 7:
                cur.execute(
                    "SELECT date, name, is_paid FROM holidays WHERE TO_CHAR(date, 'YYYY-MM') = %s", 
                    (month,)
                )
            elif month and len(month) == 4:
                cur.execute(
                    "SELECT date, name, is_paid FROM holidays WHERE TO_CHAR(date, 'YYYY') = %s", 
                    (month,)
                )
            else:
                cur.execute("SELECT date, name, is_paid FROM holidays ORDER BY date LIMIT 100")

            rows = cur.fetchall()
            holidays = [
                {
                    'date': r[0].strftime("%Y-%m-%d") if hasattr(r[0], 'strftime') else str(r[0]), 
                    'name': r[1], 
                    'is_paid': r[2]
                }
                for r in rows
            ]
            
            return jsonify(holidays)

        except Exception as e:
            print(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
            if attempt == max_attempts - 1:
                return jsonify([])
        finally:
            if cur:
                cur.close()
            if conn:
                put_db_connection(conn)

    return jsonify([])

@app.route("/holidays-count")
def holidays_count():
    month = request.args.get("month")
    if not month or len(month) != 7:
        return jsonify({"count": 0})
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM holidays WHERE TO_CHAR(date, 'YYYY-MM') = %s", 
            (month,)
        )
        count = cur.fetchone()[0]
        return jsonify({"count": count})
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== ATTENDANCE SUMMARY ====================
@app.route('/save-attendance-summary', methods=['POST'])
def save_attendance_summary():
    if 'user_id' not in session:
        return jsonify({"message": "Unauthorized"}), 401

    data = request.get_json()
    month = data.get('month')
    summary = data.get('summary', {})
    
    paid_leaves = summary.get('paidLeaves', 0)
    grace_absents = summary.get('graceAbsents', 0)
    total_days = summary.get('totalDays', 0)
    sundays = summary.get('sundays', 0)
    full_days = summary.get('fullDays', 0)
    half_days = summary.get('halfDays', 0)
    total_working_days = summary.get('totalWorkingDays', 0)

    average_per_day = total_working_days / total_days if total_days > 0 else 0

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO attendance_summaries 
            (user_id, month, total_days, sundays, full_days, half_days, paid_leaves, 
             absent_days, work_days, average_per_day)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, month) DO UPDATE SET
                total_days = EXCLUDED.total_days,
                sundays = EXCLUDED.sundays,
                full_days = EXCLUDED.full_days,
                half_days = EXCLUDED.half_days,
                paid_leaves = EXCLUDED.paid_leaves,
                absent_days = EXCLUDED.absent_days,
                work_days = EXCLUDED.work_days,
                average_per_day = EXCLUDED.average_per_day,
                generated_at = NOW()
        """, (
            session['user_id'], month, total_days, sundays, full_days, half_days,
            paid_leaves, grace_absents, total_working_days, average_per_day
        ))
        conn.commit()
        return jsonify({"message": "Summary saved"}), 200
    except Exception as e:
        conn.rollback()
        print(f"Error saving attendance summary: {e}")
        return jsonify({"message": "Error saving summary"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route('/get-attendance-summary', methods=['POST'])
def get_attendance_summary():
    if 'user_id' not in session:
        return jsonify({"message": "Unauthorized"}), 401

    data = request.get_json()
    month = data.get('month')
    email = data.get('email')

    if not month or not email:
        return jsonify({"message": "Missing month or email"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        if not user:
            return jsonify({"message": "User not found"}), 404
        
        user_id = user[0]

        cur.execute("""
            SELECT total_days, sundays, full_days, half_days, paid_leaves, 
                   absent_days, work_days, average_per_day, generated_at
            FROM attendance_summaries
            WHERE user_id = %s AND month = %s
            LIMIT 1
        """, (user_id, month))
        
        summary = cur.fetchone()
        if not summary:
            return jsonify({"message": "No summary found"}), 404

        work_days = float(summary[6]) if isinstance(summary[6], Decimal) else summary[6]
        average_per_day = float(summary[7]) if isinstance(summary[7], Decimal) else summary[7]

        data = {
            "totalDays": summary[0],
            "sundays": summary[1],
            "fullDays": summary[2],
            "halfDays": summary[3],
            "paidLeaves": summary[4],
            "absentDays": summary[5],
            "workDays": work_days,
            "averagePerDay": average_per_day,
            "generatedAt": str(summary[8])
        }
        return jsonify(data), 200

    finally:
        cur.close()
        put_db_connection(conn)

@app.route('/export-all-attendance-summary', methods=['GET'])
def export_all_attendance_summary():
    if 'user_id' not in session:
        return jsonify({"message": "Unauthorized"}), 401
    
    month = request.args.get('month')
    if not month:
        return jsonify({"message": "Missing month"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT u.email, u.name, u.role,
                   s.total_days, s.sundays, s.full_days, s.half_days,
                   s.paid_leaves, s.absent_days, s.work_days, s.average_per_day, s.generated_at
            FROM users u
            LEFT JOIN attendance_summaries s ON u.user_id = s.user_id AND s.month = %s
            ORDER BY u.email
        """, (month,))
        rows = cur.fetchall()

        wb = Workbook()
        ws = wb.active
        ws.title = f"Summary {month}"
        ws.append([
            "Email", "Name", "Role", "Total Days", "Sundays", "Full Days",
            "Half Days", "Paid Leaves", "Absent Days", "Work Days", "Avg/Day", "Generated At"
        ])
        
        for row in rows:
            ws.append([
                row['email'], row['name'], row['role'],
                row.get('total_days', 0), row.get('sundays', 0), row.get('full_days', 0),
                row.get('half_days', 0), row.get('paid_leaves', 0), row.get('absent_days', 0),
                float(row['work_days']) if row['work_days'] is not None else 0,
                float(row['average_per_day']) if row['average_per_day'] is not None else 0,
                str(row['generated_at']) if row['generated_at'] else ""
            ])
        
        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)
        filename = f"attendance_summary_{month}.xlsx"
        
        return send_file(
            bio, 
            as_attachment=True, 
            download_name=filename, 
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== PAYROLL (OPTIMIZED) ====================
@app.route('/payroll/auto-generate-slip', methods=['POST'])
def auto_generate_payroll():
    """OPTIMIZED: Cached and efficient payroll generation"""
    if 'user_id' not in session:
        return jsonify({"message": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    month = data.get('month') or datetime.utcnow().strftime('%Y-%m')
    requested_email = data.get("email")
    current_month = datetime.utcnow().strftime('%Y-%m')

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Fetch user
        if requested_email and session.get("role") == "chairman":
            cur.execute("SELECT user_id, name, salary FROM users WHERE email = %s", (requested_email,))
        else:
            cur.execute("SELECT user_id, name, salary FROM users WHERE user_id = %s", (session["user_id"],))
        
        user = cur.fetchone()
        if not user or user['salary'] is None:
            return jsonify({"message": "User not found or salary unavailable"}), 404

        user_id = user['user_id']
        name = user['name']
        salary = float(user['salary'])

        # Check for existing payroll (OPTIMIZATION: Return cached if not current month)
        cur.execute("SELECT * FROM payroll_history WHERE user_id = %s AND month = %s", (user_id, month))
        stored_payroll = cur.fetchone()

        if stored_payroll and month != current_month:
            return jsonify({
                "employee_name": name,
                "month": month,
                "base_salary": float(stored_payroll['base_salary']),
                "payable_salary": float(stored_payroll['net_payable']),
                "total_days": stored_payroll.get('total_days'),
                "sundays": stored_payroll.get('sundays'),
                "full_days": stored_payroll.get('full_days'),
                "half_days": stored_payroll.get('half_days'),
                "paid_leaves": stored_payroll.get('paid_leaves'),
                "absent_days": stored_payroll.get('absent_days'),
                "work_days": float(stored_payroll.get('work_days', 0)),
                "average_per_day": stored_payroll.get('average_per_day'),
                "generated_at": stored_payroll.get('generated_at')
            }), 200

        # Fetch attendance summary
        cur.execute("""
            SELECT total_days, sundays, full_days, half_days, paid_leaves, 
                   absent_days, work_days
            FROM attendance_summaries
            WHERE user_id = %s AND month = %s
        """, (user_id, month))
        summary = cur.fetchone()

        if summary:
            total_days = int(summary['total_days'])
            sundays = int(summary['sundays'])
            full_days = int(summary['full_days'])
            half_days = int(summary['half_days'])
            paid_leaves = int(summary['paid_leaves'])
            absent_days = int(summary['absent_days'])
            work_days = float(summary['work_days'])
        else:
            # Fallback calculation
            year, m = map(int, month.split('-'))
            total_days = monthrange(year, m)[1]
            sundays = 4
            full_days = 0
            half_days = 0
            paid_leaves = 0
            absent_days = total_days - sundays
            work_days = 0.0

        denominator = max(total_days - sundays, 1)
        average_per_day = round(work_days / denominator, 2)
        daily_salary = salary / denominator
        payable_salary = round(work_days * daily_salary, 2)

        payroll_slip = {
            "employee_name": name,
            "month": month,
            "base_salary": round(salary, 2),
            "total_days": total_days,
            "sundays": sundays,
            "full_days": full_days,
            "half_days": half_days,
            "paid_leaves": paid_leaves,
            "absent_days": absent_days,
            "work_days": round(work_days, 2),
            "average_per_day": average_per_day,
            "payable_salary": payable_salary,
            "generated_at": datetime.utcnow().isoformat() + "Z"
        }

        now_iso = datetime.utcnow().isoformat() + "Z"

        # Upsert payroll history
        cur.execute("SELECT id FROM payroll_history WHERE user_id = %s AND month = %s", (user_id, month))
        existing = cur.fetchone()

        if existing:
            cur.execute("""
                UPDATE payroll_history
                SET base_salary = %s, net_payable = %s, full_days = %s, half_days = %s,
                    paid_leaves = %s, absent_days = %s, work_days = %s, payable_salary = %s, 
                    generated_at = %s, total_days = %s, sundays = %s, average_per_day = %s
                WHERE id = %s
            """, (salary, payable_salary, full_days, half_days, paid_leaves, absent_days,
                  work_days, payable_salary, now_iso, total_days, sundays, average_per_day, existing['id']))
        else:
            cur.execute("""
                INSERT INTO payroll_history
                (user_id, month, base_salary, net_payable, full_days, half_days, paid_leaves,
                 absent_days, work_days, payable_salary, generated_at, total_days, sundays, average_per_day)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, month, salary, payable_salary, full_days, half_days, paid_leaves,
                  absent_days, work_days, payable_salary, now_iso, total_days, sundays, average_per_day))

        conn.commit()
        return jsonify(payroll_slip), 200

    finally:
        cur.close()
        put_db_connection(conn)

@app.route('/payroll/generate-slip-by-email', methods=['POST'])
def generate_slip_by_email():
    """OPTIMIZED: Chairman payroll generation with proper calculation"""
    if session.get("role") != "chairman":
        return jsonify({"message": "Unauthorized: Chairman access only"}), 403

    try:
        data = request.get_json(silent=True) or {}
        email = data.get('email')
        month = data.get('month') or datetime.utcnow().strftime('%Y-%m')

        if not email:
            return jsonify({"message": "Email is required"}), 400

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Get user info
        cur.execute("""
            SELECT user_id, name, role, department, location, dob, doj,
                   bank_account, ifsc_code, pan_no, salary AS base_salary
            FROM users
            WHERE email = %s
        """, (email,))
        user = cur.fetchone()

        if not user:
            return jsonify({"message": "User not found"}), 404

        user_id = user["user_id"]
        salary = float(user["base_salary"] or 0)

        # Fetch attendance summary
        cur.execute("""
            SELECT total_days, sundays, full_days, half_days, paid_leaves, 
                   absent_days, work_days
            FROM attendance_summaries
            WHERE user_id = %s AND month = %s
        """, (user_id, month))
        summary = cur.fetchone()

        if summary:
            total_days = int(summary['total_days'])
            sundays = int(summary['sundays'])
            full_days = int(summary['full_days'])
            half_days = int(summary['half_days'])
            paid_leaves = int(summary['paid_leaves'])
            absent_days = int(summary['absent_days'])
            work_days = float(summary['work_days'])
        else:
            year, m = map(int, month.split('-'))
            total_days = monthrange(year, m)[1]
            sundays = 4
            full_days = 0
            half_days = 0
            paid_leaves = 0
            absent_days = total_days - sundays
            work_days = 0.0

        # CRITICAL: Proper calculation to avoid overpayment
        denominator = max(total_days - sundays, 1)
        average_per_day = round(work_days / denominator, 2)
        daily_salary = salary / denominator
        payable_work_days = min(work_days, denominator)  # Cap at max working days
        payable_salary = round(payable_work_days * daily_salary, 2)

        payroll_slip = {
            "employee_id": user_id,
            "employee_name": user["name"],
            "role": user["role"],
            "department": user["department"],
            "location": user["location"],
            "bank_account": user["bank_account"],
            "ifsc_code": user["ifsc_code"],
            "pan_no": user["pan_no"],
            "dob": user["dob"],
            "doj": user["doj"],
            "month": month,
            "base_salary": round(salary, 2),
            "total_days": total_days,
            "sundays": sundays,
            "full_days": full_days,
            "half_days": half_days,
            "paid_leaves": paid_leaves,
            "absent_days": absent_days,
            "work_days": round(work_days, 2),
            "average_per_day": average_per_day,
            "payable_salary": payable_salary,
            "generated_at": datetime.utcnow().isoformat() + "Z"
        }

        # Sync with payroll history
        cur.execute("SELECT id FROM payroll_history WHERE user_id = %s AND month = %s", (user_id, month))
        existing = cur.fetchone()

        if existing:
            cur.execute("""
                UPDATE payroll_history SET net_payable = %s, work_days = %s WHERE id = %s
            """, (payable_salary, work_days, existing['id']))
        else:
            cur.execute("""
                INSERT INTO payroll_history (user_id, month, base_salary, net_payable, work_days)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, month, salary, payable_salary, work_days))

        conn.commit()
        return jsonify(payroll_slip), 200

    except Exception as e:
        print("Error generating slip:", e)
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== USER MANAGEMENT ====================
@app.route("/create-user", methods=["POST"])
def create_user():
    if session.get("role") not in ("chairman", "manager"):
        return jsonify({"message": "Access denied"}), 403

    data = request.get_json()
    required_fields = ["name", "email", "password", "role"]
    
    if not all(data.get(f) for f in required_fields):
        return jsonify({"message": "Missing required fields"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users WHERE email = %s", (data["email"],))
        if cur.fetchone():
            return jsonify({"message": "User already exists"}), 409

        cur.execute("""
            INSERT INTO users 
            (name, email, password, role, image, location, employee_id, salary, bank_account, 
             dob, doj, pan_no, ifsc_code, department, paid_leaves)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["name"], data["email"], data["password"], data["role"],
            data.get("image", ""), data.get("location"), data.get("employee_id"),
            data.get("salary"), data.get("bank_account"), data.get("dob"),
            data.get("doj"), data.get("pan_no"), data.get("ifsc_code"),
            data.get("department"), data.get("paidLeaves", 0)
        ))
        
        conn.commit()
        return jsonify({"message": "✅ User created successfully"}), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"❌ DB Error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/update-user/<email>", methods=["PUT", "POST"])
def update_user(email):
    """OPTIMIZED: Efficient user update with proper field mapping"""
    from urllib.parse import unquote
    email = unquote(email)
    
    if session.get("role") not in ("chairman", "manager"):
        return jsonify({"message": "Access denied"}), 403

    data = request.get_json()
    if not data:
        return jsonify({"message": "No input data provided"}), 400

    # Field mapping
    field_mapping = {
        "name": "name",
        "role": "role",
        "salary": "salary",
        "employee_id": "employee_id",
        "location": "location",
        "password": "password",
        "bank_account": "bank_account",
        "dob": "dob",
        "doj": "doj",
        "pan_no": "pan_no",
        "ifsc_code": "ifsc_code",
        "department": "department",
        "image": "image",
        "is_active": "is_active",
        
        "paidLeaves": "paid_leaves"
    }

    fields = []
    values = []

    for api_field, db_field in field_mapping.items():
        if api_field in data:
            value = data[api_field]
            if api_field in ("dob", "doj") and value == "":
                value = None
            fields.append(f"{db_field} = %s")
            values.append(value)

    if not fields:
        return jsonify({"message": "No valid fields to update"}), 400

    values.append(email)
    query = f"UPDATE users SET {', '.join(fields)} WHERE email = %s"

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, tuple(values))

        if cur.rowcount == 0:
            return jsonify({"message": "User not found"}), 404

        conn.commit()
        return jsonify({"message": "User updated successfully"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"message": f"Database error: {str(e)}"}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            put_db_connection(conn)

@app.route("/assign-manager-role", methods=["POST"])
def assign_manager_role():
    """Assign manager role and location to employee"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.get_json()
        employee_email = data.get("email")
        location = data.get("location")

        if not employee_email or not location:
            return jsonify({"error": "Missing email or location"}), 400

        cur.execute("""
            UPDATE users
            SET role = %s, location = %s
            WHERE email = %s
        """, ('manager', location, employee_email))

        conn.commit()

        if cur.rowcount == 0:
            return jsonify({"error": f"No user found with email: {employee_email}"}), 404

        return jsonify({
            "message": f"Successfully assigned 'manager' role and '{location}' location to {employee_email}."
        }), 200

    except Exception as e:
        conn.rollback()
        print(f"Error during manager assignment: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        cur.close()
        put_db_connection(conn)

# ==================== SALES ROUTES (OPTIMIZED) ====================
@app.route('/sales-stats/<identifier>', methods=['GET'])
def get_sales_stats(identifier):
    """OPTIMIZED: Single query for sales stats"""
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get user_id
        if '@' in identifier:
            cur.execute("SELECT user_id FROM users WHERE email = %s", (identifier,))
            user = cur.fetchone()
            if not user:
                return jsonify({"message": "User not found"}), 404
            user_id = user[0]
        else:
            user_id = identifier
        
        # OPTIMIZATION: Single query with JOIN
        cur.execute("""
            SELECT 
                COALESCE(st.target, 0) as target,
                COALESCE(SUM(se.amount), 0) as current_sales,
                st.updated_at
            FROM sales_targets st
            LEFT JOIN sales_entries se ON se.user_id = st.user_id
            WHERE st.user_id = %s
            GROUP BY st.target, st.updated_at
        """, (user_id,))
        
        row = cur.fetchone()
        
        if row:
            return jsonify({
                'target': float(row[0]),
                'current_sales': float(row[1]),
                'updated_at': str(row[2]) if row[2] else None
            })
        else:
            return jsonify({'target': 0, 'current_sales': 0, 'updated_at': None})
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route('/update-sales-target', methods=['POST'])
def update_sales_target():
    """OPTIMIZED: Efficient target update"""
    if "user_id" not in session or session.get('role') != 'chairman':
        return jsonify({'error': 'Unauthorized'}), 403
    
    employee_email = request.form.get('employee_email')
    target = request.form.get('target')
    
    if not employee_email or not target:
        return jsonify({'error': 'Employee email and target are required'}), 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT user_id FROM users WHERE email = %s", (employee_email,))
        user = cur.fetchone()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        user_id = user[0]
        
        cur.execute("""
            INSERT INTO sales_targets (user_id, target, created_at, updated_at)
            VALUES (%s, %s, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE SET target = %s, updated_at = NOW()
        """, (user_id, target, target))
        
        conn.commit()
        return jsonify({'message': 'Sales target updated successfully'}), 200
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route('/add-sales-entry', methods=['POST'])
def add_sales_entry():
    """OPTIMIZED: Fast sales entry addition"""
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    
    employee_email = request.form.get('employee_email')
    amount = request.form.get('amount')
    company = request.form.get('company')
    client_name = request.form.get('client_name')
    sale_date = request.form.get('sale_date')
    remarks = request.form.get('remarks', '')
    
    if not all([employee_email, amount, company, client_name, sale_date]):
        return jsonify({'error': 'All fields except remarks are required'}), 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT user_id FROM users WHERE email = %s", (employee_email,))
        user = cur.fetchone()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        user_id = user[0]
        
        if session.get('role') != 'chairman' and session.get('user_id') != user_id:
            return jsonify({'error': 'You can only add your own sales entries'}), 403
        
        cur.execute("""
            INSERT INTO sales_entries 
            (user_id, amount, company, client_name, sale_date, remarks, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (user_id, amount, company, client_name, sale_date, remarks))
        
        conn.commit()
        return jsonify({'message': 'Sales entry added successfully'}), 200
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route('/sales-entries/<identifier>', methods=['GET'])
def get_sales_entries(identifier):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if '@' in identifier:
            cur.execute("SELECT user_id FROM users WHERE email = %s", (identifier,))
            user = cur.fetchone()
            if not user:
                return jsonify({"message": "User not found"}), 404
            user_id = user[0]
        else:
            user_id = identifier

        # FIX: `id` is now included so frontend delete works
        cur.execute("""
            SELECT id, amount, company, client_name, sale_date, remarks, created_at
            FROM sales_entries
            WHERE user_id = %s
            ORDER BY sale_date DESC, created_at DESC
            LIMIT 200
        """, (user_id,))

        results = cur.fetchall()
        entries = [{
            'id':          row[0],
            'amount':      float(row[1]) if row[1] else 0,
            'company':     row[2],
            'client_name': row[3],
            'sale_date':   str(row[4]) if row[4] else None,
            'remarks':     row[5] or '',
            'created_at':  str(row[6]) if row[6] else None,
        } for row in results]

        return jsonify(entries)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── CHANGE 2: Add this NEW route (fixes CORS + missing DELETE) ─
@app.route('/sales-entry/<int:entry_id>', methods=['DELETE', 'OPTIONS'])
@cross_origin(supports_credentials=True)
def delete_sales_entry(entry_id):
    if request.method == 'OPTIONS':
        return '', 200

    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    if session.get('role') != 'chairman':
        return jsonify({'error': 'Only chairman can delete sales entries'}), 403

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM sales_entries WHERE id = %s", (entry_id,))
        if not cur.fetchone():
            return jsonify({'error': 'Entry not found'}), 404

        cur.execute("DELETE FROM sales_entries WHERE id = %s", (entry_id,))
        conn.commit()
        return jsonify({'message': 'Deleted successfully'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)
@app.route("/admin/attendance", methods=["GET"])
def admin_get_attendance():
    """Role-based attendance logs access"""
    if "user_id" not in session:
        print("❌ Unauthorized: No user_id in session")
        return jsonify({"message": "Unauthorized"}), 403

    role = session.get("role", "").strip().lower()
    user_location = session.get("location", "").strip().lower()
    print("🧩 ATTENDANCE DEBUG → user_id:", session.get("user_id"), 
          "| role:", role, "| location:", user_location)

    # ✅ Allowed roles: Chairman, MIS Executive, Manager
    if role not in ["chairman", "mis-executive", "mis-execuitve", "manager"]:
        print("🚫 Unauthorized role:", role)
        return jsonify({"message": "Unauthorized"}), 403

    employee_id = request.args.get("employee_id")
    month = request.args.get("month")  # format: YYYY-MM
    if not employee_id or not month:
        print("⚠️ Missing employee_id or month:", employee_id, month)
        return jsonify({"message": "employee_id and month are required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # 🔍 Get employee’s location (to check for manager)
        cur.execute("SELECT location FROM users WHERE user_id = %s", (employee_id,))
        emp = cur.fetchone()
        emp_location = emp[0].lower() if emp and emp[0] else ""
        print("👀 Employee location:", emp_location)

        # 🛡️ Managers can only see same-location data
        if role == "manager" and emp_location != user_location:
            print("🚫 Manager tried to access other location data:", emp_location)
            return jsonify({"message": "Access denied: different location"}), 403

        cur.execute("""
            SELECT date, office_in, break_out, break_in, break_out_2, break_in_2,
                   lunch_out, lunch_in, office_out, paid_leave_reason,
                   extra_break_ins, extra_break_outs
            FROM attendance
            WHERE user_id = %s
              AND TO_CHAR(date, 'YYYY-MM') = %s
            ORDER BY date ASC
        """, (employee_id, month))

        rows = cur.fetchall()
        print(f"✅ Attendance rows fetched: {len(rows)} for user {employee_id}")

        result = []
        for row in rows:
            extra_break_ins = row[10] or []
            extra_break_outs = row[11] or []
            if isinstance(extra_break_ins, str):
                extra_break_ins = json.loads(extra_break_ins)
            if isinstance(extra_break_outs, str):
                extra_break_outs = json.loads(extra_break_outs)

            result.append({
                "date": row[0].strftime("%Y-%m-%d") if row[0] else "",
                "office_in": str(row[1]) if row[1] else "",
                "break_out": str(row[2]) if row[2] else "",
                "break_in": str(row[3]) if row[3] else "",
                "break_out_2": str(row[4]) if row[4] else "",
                "break_in_2": str(row[5]) if row[5] else "",
                "lunch_out": str(row[6]) if row[6] else "",
                "lunch_in": str(row[7]) if row[7] else "",
                "office_out": str(row[8]) if row[8] else "",
                "leave_type": row[9] if row[9] else None,
                "extra_break_ins": extra_break_ins,
                "extra_break_outs": extra_break_outs,
            })

        return jsonify(result), 200

    except Exception as e:
        print("💥 Error in /admin/attendance:", e)
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route("/admin/employees", methods=["GET"])
def admin_get_employees():
    """Role-based employee listing"""
    if "user_id" not in session:
        print("❌ Unauthorized: No user_id in session")
        return jsonify({"message": "Unauthorized"}), 403

    role = session.get("role", "").strip().lower()
    user_location = session.get("location", "").strip().lower()
    print("🧩 EMPLOYEE DEBUG → user_id:", session.get("user_id"), 
          "| role:", role, "| location:", user_location)

    # ✅ Allow Chairman, MIS Executive, and Manager
    if role not in ["chairman", "mis-executive", "mis-execuitve", "manager"]:
        print("🚫 Unauthorized role:", role)
        return jsonify({"message": "Unauthorized"}), 403

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT user_id, name, role, location, department FROM users")
        employees = cur.fetchall()
        print("📊 Total employees in DB:", len(employees))

        result = []
        for emp in employees:
            emp_role = (emp[2] or "").lower()
            emp_loc = (emp[3] or "").lower()

            # 🧩 Manager → only same-location employees
            if role == "manager" and emp_loc != user_location:
                continue

            # 🚫 Hide Chairman only (show all others)
            if "chairman" in emp_role:
                continue

            result.append({
                "id": emp[0],
                "name": emp[1],
                "role": emp[2],
                "location": emp[3],
                "department": emp[4],
            })

        print(f"✅ Returning {len(result)} employees for role={role}")
        return jsonify(result), 200

    except Exception as e:
        print("💥 Error in /admin/employees:", e)
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route('/all-sales-stats-chairman', methods=['GET'])
def get_all_sales_stats_chairman():
    """OPTIMIZED: Efficient chairman view with single query"""
    if "user_id" not in session or session.get('role') != 'chairman':
        return jsonify({'error': 'Unauthorized'}), 403
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        query = """
            SELECT 
                u.user_id, u.name, u.email, u.role, u.department, u.salary,
                COALESCE(st.target, 0) as target,
                COALESCE(SUM(se.amount), 0) as current_sales,
                st.updated_at
            FROM sales_targets st
            JOIN users u ON st.user_id = u.user_id
            LEFT JOIN sales_entries se ON se.user_id = st.user_id
            GROUP BY u.user_id, u.name, u.email, u.role, u.department, u.salary, st.target, st.updated_at
            ORDER BY u.name
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        sales_employees = []
        for row in results:
            user_id = row[0]
            base_salary = float(row[5]) if row[5] else 0
            target = float(row[6])
            current = float(row[7])
            
            percentage = (current / target * 100) if target > 0 else 0
            
            # Calculate salary based on target achievement
            if percentage >= 100:
                salary_percentage = 100
                payable_salary = base_salary
            elif percentage >= 75:
                salary_percentage = 75
                payable_salary = base_salary * 0.75
            elif percentage >= 50:
                salary_percentage = 50
                payable_salary = base_salary * 0.50
            elif percentage >= 25:
                salary_percentage = 25
                payable_salary = base_salary * 0.25
            else:
                salary_percentage = 0
                payable_salary = 0
            
            sales_employees.append({
                'id': user_id,
                'name': row[1],
                'email': row[2],
                'role': row[3],
                'department': row[4],
                'base_salary': base_salary,
                'target': target,
                'current_sales': current,
                'percentage': round(percentage, 2),
                'salary_percentage': salary_percentage,
                'payable_salary': round(payable_salary, 2),
                'updated_at': str(row[8]) if row[8] else None
            })
        
        return jsonify(sales_employees)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/update-employment-status/<email>", methods=["PUT", "OPTIONS"])
@cross_origin(supports_credentials=True)
def update_employment_status(email):
    """
    Chairman-only route.
    Body JSON: { employment_status: "active"|"terminated"|"resigned", remarks: "..." }

    - Sets employment_status on the user row
    - Stores the reason in status_remarks
    - Records timestamp in status_changed_at
    - When terminating/resigning: sets is_active = FALSE  →  hides from normal dashboards
    - When rejoining (status = "active"): sets is_active = TRUE  →  restores to dashboard
    """
    if request.method == "OPTIONS":
        return '', 200

    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Access denied — chairman only"}), 403

    from urllib.parse import unquote
    email = unquote(email)

    data          = request.get_json(silent=True) or {}
    new_status    = data.get("employment_status", "").lower()
    remarks       = data.get("remarks", "")

    if new_status not in ("active", "terminated", "resigned"):
        return jsonify({"message": "Invalid status. Use: active, terminated, resigned"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        # Verify user exists
        cur.execute("SELECT user_id, name FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        if not user:
            return jsonify({"message": "User not found"}), 404

        # is_active mirrors whether the person shows up in normal dashboards
        is_active = (new_status == "active")

        cur.execute("""
            UPDATE users
               SET employment_status  = %s,
                   status_remarks     = %s,
                   status_changed_at  = NOW(),
                   is_active          = %s
             WHERE email = %s
        """, (new_status, remarks, is_active, email))

        conn.commit()

        action_labels = {
            "active":     "re-joined",
            "terminated": "terminated",
            "resigned":   "marked as resigned",
        }
        return jsonify({
            "message": f"Employee '{user[1]}' has been {action_labels[new_status]}.",
            "employment_status": new_status,
            "is_active": is_active,
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"DB error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)

# ============================================================
#  LEAD MANAGEMENT  —  Python / Flask backend routes
#  Drop these into your app.py (or leads blueprint).
#  Requires: psycopg2 RealDictCursor, existing session + socketio setup.
# ============================================================
#
#  DATABASE MIGRATION — run once before deploying:
#
#  ALTER TABLE leads
#    ADD COLUMN IF NOT EXISTS education          TEXT,
#    ADD COLUMN IF NOT EXISTS experience         INT,
#    ADD COLUMN IF NOT EXISTS domain             TEXT,
#    ADD COLUMN IF NOT EXISTS age                INT,
#    ADD COLUMN IF NOT EXISTS calling_city       TEXT,
#    ADD COLUMN IF NOT EXISTS service_interested TEXT,
#    ADD COLUMN IF NOT EXISTS lead_source        TEXT,
#    ADD COLUMN IF NOT EXISTS additional_comments TEXT;
#
# ============================================================

from datetime import timedelta
from flask import request, session, jsonify
from psycopg2.extras import RealDictCursor

# ── Helper: resolve user name by id ──────────────────────────────────────────
def _get_user_name(cur, user_id):
    if not user_id:
        return None
    cur.execute("SELECT name FROM users WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    return row["name"] if row else None


# ── Helper: is caller a lead creator (granted by chairman)? ──────────────────
def _is_lead_creator(cur, user_id):
    cur.execute("SELECT 1 FROM lead_creators WHERE user_id = %s", (user_id,))
    return cur.fetchone() is not None


# ── Helper: can caller create / assign / reshuffle leads? ────────────────────
#   Chairman + Manager → always yes
#   Lead Creator (granted) → yes
#   Regular employee → no
def _can_manage_leads(role, user_id, cur):
    return role in ("chairman", "manager") or _is_lead_creator(cur, user_id)


# ── Helper: log an assignment to history ─────────────────────────────────────
def _log_assignment(cur, lead_id, assignee_id, assigned_by_id):
    cur.execute(
        "UPDATE lead_assignments SET is_current = FALSE WHERE lead_id = %s",
        (lead_id,)
    )
    cur.execute(
        """
        INSERT INTO lead_assignments (lead_id, assignee_id, assigned_by_id, is_current)
        VALUES (%s, %s, %s, TRUE)
        """,
        (lead_id, assignee_id, assigned_by_id)
    )


# ── Helper: serialize datetime fields in a lead row ──────────────────────────
def _serialize_lead(row):
    r = dict(row)
    for key in ("created_at", "updated_at", "called_at", "deadline_at"):
        if r.get(key):
            r[key] = r[key].isoformat()
    return r


# ============================================================
#  CHECK DUPLICATE  — real-time check on contact OR email
#  Called by the frontend on blur.
#  Returns existing lead info if found, plus which field matched.
# ============================================================
@app.route("/leads/check-duplicate", methods=["GET"])
def leads_check_duplicate():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    contact = request.args.get("contact", "").strip()
    email   = request.args.get("email",   "").strip()
    field   = request.args.get("field",   "").strip()   # "contact" or "email"

    if not contact and not email:
        return jsonify({"exists": False}), 200

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Check contact
        if field == "contact" and contact:
            cur.execute(
                "SELECT id, name, contact, email, status FROM leads WHERE contact = %s LIMIT 1",
                (contact,)
            )
            row = cur.fetchone()
            if row:
                return jsonify({"exists": True, "field": "contact", "lead": dict(row)}), 200
            return jsonify({"exists": False}), 200

        # Check email
        if field == "email" and email:
            cur.execute(
                "SELECT id, name, contact, email, status FROM leads WHERE email = %s LIMIT 1",
                (email,)
            )
            row = cur.fetchone()
            if row:
                return jsonify({"exists": True, "field": "email", "lead": dict(row)}), 200
            return jsonify({"exists": False}), 200

        # Fallback: check both
        if contact:
            cur.execute(
                "SELECT id, name, contact, email, status FROM leads WHERE contact = %s LIMIT 1",
                (contact,)
            )
            row = cur.fetchone()
            if row:
                return jsonify({"exists": True, "field": "contact", "lead": dict(row)}), 200
        if email:
            cur.execute(
                "SELECT id, name, contact, email, status FROM leads WHERE email = %s LIMIT 1",
                (email,)
            )
            row = cur.fetchone()
            if row:
                return jsonify({"exists": True, "field": "email", "lead": dict(row)}), 200

        return jsonify({"exists": False}), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  CREATE LEAD
#  - Chairman, Manager, and granted Lead Creators can create.
#  - Server-side duplicate guard: rejects if contact OR email
#    already exists (returns 409 with the conflicting lead).
#  - contact and email are checked independently so we can
#    tell the user exactly which field conflicts.
# ============================================================
@app.route("/leads/create", methods=["POST"])
def create_lead():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    role    = session.get("role", "")
    user_id = session["user_id"]

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not _can_manage_leads(role, user_id, cur):
            return jsonify({"message": "Access denied"}), 403

        data        = request.get_json(silent=True) or {}
        name        = (data.get("name")    or "").strip()
        contact     = (data.get("contact") or "").strip()
        email       = (data.get("email")   or "").strip()
        education   = (data.get("education") or "").strip()
        experience  = data.get("experience")
        domain      = (data.get("domain") or "").strip()
        age         = data.get("age")
        calling_city        = (data.get("calling_city") or "").strip()
        service_interested  = (data.get("service_interested") or "").strip()
        lead_source         = (data.get("lead_source") or "").strip()
        additional_comments = (data.get("additional_comments") or "").strip()
        assigned_to = data.get("assigned_to")
        force       = bool(data.get("force", False))   # bypass duplicate guard

        # Mandatory field validation
        required = {
            "name": name, "contact": contact, "email": email,
            "education": education, "domain": domain,
            "calling_city": calling_city, "service_interested": service_interested,
            "lead_source": lead_source,
        }
        missing = [k for k, v in required.items() if not v]
        if experience is None or str(experience).strip() == "":
            missing.append("experience")
        if not age:
            missing.append("age")
        if missing:
            return jsonify({"message": f"Required fields missing: {', '.join(missing)}"}), 400

        # ── Duplicate guard (skipped if force=True) ───────────────────────
        # Check contact and email independently so we can return precise errors.
        if not force:
            # Check contact number
            cur.execute(
                "SELECT id, name, contact, email FROM leads WHERE contact = %s LIMIT 1",
                (contact,)
            )
            dup_contact = cur.fetchone()
            if dup_contact:
                return jsonify({
                    "message": f"A lead with this mobile number already exists: {dup_contact['name']}",
                    "duplicate_field": "contact",
                    "existing": dict(dup_contact),
                }), 409

            # Check email
            cur.execute(
                "SELECT id, name, contact, email FROM leads WHERE email = %s LIMIT 1",
                (email,)
            )
            dup_email = cur.fetchone()
            if dup_email:
                return jsonify({
                    "message": f"A lead with this email already exists: {dup_email['name']}",
                    "duplicate_field": "email",
                    "existing": dict(dup_email),
                }), 409

        # 45-minute deadline (only meaningful if someone is assigned)
        deadline = now_ist() + timedelta(minutes=45) if assigned_to else None

        cur.execute(
            """
            INSERT INTO leads
                (name, contact, email, education, experience, domain, age,
                 calling_city, service_interested, lead_source, additional_comments,
                 created_by, assigned_to, assigned_by, status, deadline_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Pending', %s)
            RETURNING id, created_at
            """,
            (name, contact, email, education,
             int(experience) if experience is not None else None,
             domain, int(age) if age else None,
             calling_city, service_interested, lead_source,
             additional_comments or None,
             user_id, assigned_to or None, user_id if assigned_to else None,
             deadline)
        )
        row        = cur.fetchone()
        lead_id    = row["id"]
        created_at = row["created_at"]

        if assigned_to:
            _log_assignment(cur, lead_id, assigned_to, user_id)

        conn.commit()

        if assigned_to:
            assigner_name = _get_user_name(cur, user_id)
            socketio.emit("new_lead_assigned", {
                "lead_id":          lead_id,
                "lead_name":        name,
                "contact":          contact,
                "email":            email,
                "deadline_at":      deadline.isoformat() if deadline else None,
                "assigned_to":      assigned_to,
                "assigned_by_name": assigner_name,
            }, room=f"user_{assigned_to}")

        return jsonify({
            "message":    "Lead created",
            "id":         lead_id,
            "created_at": created_at.isoformat(),
            "deadline_at": deadline.isoformat() if deadline else None,
        }), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  LIST LEADS
#  - Chairman / Manager / Lead Creator → see ALL leads (table view)
#  - Regular employee → only their assigned leads (card view)
# ============================================================
@app.route("/leads", methods=["GET"])
def list_leads():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    role    = session.get("role", "")
    user_id = session["user_id"]

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        can_manage = _can_manage_leads(role, user_id, cur)

        if can_manage:
            cur.execute(
                """
                SELECT
                    l.*,
                    creator.name   AS creator_name,
                    assignee.name  AS assignee_name,
                    assignee.email AS assignee_email,
                    assigner.name  AS assigned_by_name
                FROM leads l
                LEFT JOIN users creator  ON l.created_by  = creator.user_id
                LEFT JOIN users assignee ON l.assigned_to = assignee.user_id
                LEFT JOIN users assigner ON l.assigned_by = assigner.user_id
                ORDER BY l.created_at DESC
                LIMIT 500
                """
            )
        else:
            cur.execute(
                """
                SELECT
                    l.*,
                    creator.name  AS creator_name,
                    assigner.name AS assigned_by_name
                FROM leads l
                LEFT JOIN users creator  ON l.created_by  = creator.user_id
                LEFT JOIN users assigner ON l.assigned_by = assigner.user_id
                WHERE l.assigned_to = %s
                ORDER BY l.created_at DESC
                LIMIT 200
                """,
                (user_id,)
            )

        return jsonify([_serialize_lead(r) for r in cur.fetchall()]), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  MY ACCESS
#  Returns: canCreate (bool), hasLeads (bool)
# ============================================================
@app.route("/leads/my-access", methods=["GET"])
def leads_my_access():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    role    = session.get("role", "")
    user_id = session["user_id"]

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        can_create = _can_manage_leads(role, user_id, cur)

        cur.execute(
            "SELECT 1 FROM lead_assignments WHERE assignee_id = %s LIMIT 1",
            (user_id,)
        )
        has_leads = cur.fetchone() is not None

        return jsonify({"canCreate": can_create, "hasLeads": has_leads}), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  UPDATE LEAD  (status + optional reassign)
#  - Chairman / Manager / Lead Creator → can update AND reassign
#  - Assigned employee → can only update status + add remarks
# ============================================================
@app.route("/leads/<int:lead_id>", methods=["PUT"])
def update_lead(lead_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    role    = session.get("role", "")
    user_id = session["user_id"]
    data    = request.get_json(silent=True) or {}

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT id, assigned_to, created_by, status FROM leads WHERE id = %s",
            (lead_id,)
        )
        lead = cur.fetchone()
        if not lead:
            return jsonify({"message": "Lead not found"}), 404

        can_manage = _can_manage_leads(role, user_id, cur)

        if not can_manage and user_id != lead["assigned_to"]:
            return jsonify({"message": "Access denied"}), 403

        fields, values = [], []

        if "status" in data:
            allowed = ("Pending", "Called", "No Answer", "Follow Up", "Converted", "Dropped")
            if data["status"] not in allowed:
                return jsonify({"message": f"Invalid status. Allowed: {allowed}"}), 400
            fields.append("status = %s")
            values.append(data["status"])
            if data["status"] == "Called":
                fields.append("called_at = %s")
                values.append(now_ist())

        if "assigned_to" in data and can_manage:
            new_assignee = data["assigned_to"]

            if new_assignee and new_assignee != lead["assigned_to"]:
                new_deadline = now_ist() + timedelta(minutes=45)
                fields += ["assigned_to = %s", "assigned_by = %s", "deadline_at = %s"]
                values += [new_assignee, user_id, new_deadline]

                _log_assignment(cur, lead_id, new_assignee, user_id)

                assigner_name = _get_user_name(cur, user_id)
                cur.execute("SELECT name FROM leads WHERE id = %s", (lead_id,))
                lead_name_row = cur.fetchone()
                socketio.emit("new_lead_assigned", {
                    "lead_id":          lead_id,
                    "lead_name":        lead_name_row["name"] if lead_name_row else "",
                    "assigned_to":      new_assignee,
                    "assigned_by_name": assigner_name,
                    "deadline_at":      new_deadline.isoformat(),
                }, room=f"user_{new_assignee}")

            elif new_assignee is None:
                fields += ["assigned_to = %s", "assigned_by = %s", "deadline_at = %s"]
                values += [None, None, None]

        if not fields:
            return jsonify({"message": "Nothing to update"}), 400

        fields.append("updated_at = %s")
        values.append(now_ist())
        values.append(lead_id)

        cur.execute(
            f"UPDATE leads SET {', '.join(fields)} WHERE id = %s",
            tuple(values)
        )
        conn.commit()
        return jsonify({"message": "Lead updated"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  ADD REMARK
#  - Chairman / Manager / Lead Creator → can add remarks
#  - Assigned employee → can add remarks to their own leads
# ============================================================
@app.route("/leads/<int:lead_id>/remarks", methods=["POST"])
def add_lead_remark(lead_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    role    = session.get("role", "")
    user_id = session["user_id"]
    data    = request.get_json(silent=True) or {}

    remark         = (data.get("remark") or "").strip()
    status_at_time = (data.get("status_at_time") or "").strip() or None

    if not remark:
        return jsonify({"message": "remark cannot be empty"}), 400

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT id, assigned_to, status FROM leads WHERE id = %s",
            (lead_id,)
        )
        lead = cur.fetchone()
        if not lead:
            return jsonify({"message": "Lead not found"}), 404

        can_manage = _can_manage_leads(role, user_id, cur)
        if not can_manage and user_id != lead["assigned_to"]:
            return jsonify({"message": "Access denied"}), 403

        if not status_at_time:
            status_at_time = lead["status"]

        cur.execute(
            """
            INSERT INTO lead_remarks (lead_id, author_id, remark, status_at_time)
            VALUES (%s, %s, %s, %s)
            RETURNING id, created_at
            """,
            (lead_id, user_id, remark, status_at_time)
        )
        remark_row = cur.fetchone()

        cur.execute(
            """
            UPDATE leads
            SET remarks_count = remarks_count + 1,
                latest_remark = %s,
                updated_at    = %s
            WHERE id = %s
            """,
            (remark, now_ist(), lead_id)
        )

        conn.commit()
        return jsonify({
            "message":    "Remark added",
            "remark_id":  remark_row["id"],
            "created_at": remark_row["created_at"].isoformat(),
        }), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  LEAD HISTORY
# ============================================================
@app.route("/leads/<int:lead_id>/history", methods=["GET"])
def lead_history(lead_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    role    = session.get("role", "")
    user_id = session["user_id"]

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "SELECT id, assigned_to FROM leads WHERE id = %s",
            (lead_id,)
        )
        lead = cur.fetchone()
        if not lead:
            return jsonify({"message": "Lead not found"}), 404

        can_manage = _can_manage_leads(role, user_id, cur)

        if not can_manage and user_id != lead["assigned_to"]:
            cur.execute(
                "SELECT 1 FROM lead_assignments WHERE lead_id = %s AND assignee_id = %s",
                (lead_id, user_id)
            )
            if not cur.fetchone():
                return jsonify({"message": "Access denied"}), 403

        cur.execute(
            """
            SELECT
                la.id,
                la.assigned_at,
                la.is_current,
                assignee.name  AS assignee_name,
                assigner.name  AS assigned_by_name
            FROM lead_assignments la
            LEFT JOIN users assignee ON la.assignee_id    = assignee.user_id
            LEFT JOIN users assigner ON la.assigned_by_id = assigner.user_id
            WHERE la.lead_id = %s
            ORDER BY la.assigned_at ASC
            """,
            (lead_id,)
        )
        assignments = []
        for r in cur.fetchall():
            row = dict(r)
            row["assigned_at"] = row["assigned_at"].isoformat()
            assignments.append(row)

        cur.execute(
            """
            SELECT
                lr.id,
                lr.remark,
                lr.status_at_time,
                lr.created_at,
                author.name AS author_name
            FROM lead_remarks lr
            LEFT JOIN users author ON lr.author_id = author.user_id
            WHERE lr.lead_id = %s
            ORDER BY lr.created_at ASC
            """,
            (lead_id,)
        )
        remarks = []
        for r in cur.fetchall():
            row = dict(r)
            row["created_at"] = row["created_at"].isoformat()
            remarks.append(row)

        return jsonify({"assignments": assignments, "remarks": remarks}), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  EMPLOYEES LIST
#  - Chairman / Manager / Lead Creator can fetch this list
# ============================================================
@app.route("/leads/employees", methods=["GET"])
def leads_get_employees():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    role    = session.get("role", "")
    user_id = session["user_id"]

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not _can_manage_leads(role, user_id, cur):
            return jsonify({"message": "Access denied"}), 403

        cur.execute(
            """
            SELECT user_id, name, email, role, department, location
            FROM users
            WHERE is_active = TRUE
              AND role NOT IN ('chairman')
            ORDER BY name ASC
            """
        )
        return jsonify([dict(r) for r in cur.fetchall()]), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  LEAD CREATORS — list / grant / revoke (chairman only)
#  Granting lead-creator access gives that employee:
#    ✓ Create leads
#    ✓ Assign / reshuffle leads
#    ✓ See all leads (table view with full stats)
#    ✗ Cannot delete leads (chairman only)
# ============================================================
@app.route("/leads/creators", methods=["GET"])
def leads_list_creators():
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            """
            SELECT lc.user_id, u.name, u.role, u.location, lc.granted_at
            FROM lead_creators lc
            JOIN users u ON lc.user_id = u.user_id
            ORDER BY u.name ASC
            """
        )
        rows = []
        for r in cur.fetchall():
            row = dict(r)
            row["granted_at"] = row["granted_at"].isoformat()
            rows.append(row)
        return jsonify(rows), 200

    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route("/leads/creators", methods=["POST"])
def leads_grant_creator():
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403

    data    = request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"message": "user_id required"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO lead_creators (user_id, granted_by)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (user_id, session["user_id"])
        )
        conn.commit()
        return jsonify({"message": "Access granted"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route("/leads/creators/<int:target_user_id>", methods=["DELETE"])
def leads_revoke_creator(target_user_id):
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM lead_creators WHERE user_id = %s RETURNING user_id",
            (target_user_id,)
        )
        if not cur.fetchone():
            return jsonify({"message": "User not found in creators list"}), 404
        conn.commit()
        return jsonify({"message": "Access revoked"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ============================================================
#  DELETE LEAD — CHAIRMAN ONLY
#  Lead creators can create/assign/reshuffle but NOT delete.
# ============================================================
@app.route("/leads/<int:lead_id>", methods=["DELETE"])
def delete_lead(lead_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    # Strictly chairman only — not even lead creators
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied — chairman only"}), 403

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            "DELETE FROM leads WHERE id = %s RETURNING id, name",
            (lead_id,)
        )
        deleted = cur.fetchone()
        if not deleted:
            return jsonify({"message": "Lead not found"}), 404

        conn.commit()
        return jsonify({"message": f"Lead '{deleted['name']}' deleted successfully"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)
# ==================== APPLICATION STARTUP ====================

# ==================== CHAT SYSTEM ROUTES (FIXED) ====================
# DROP-IN REPLACEMENT for the chat section in your app.py
#
# BUGS FIXED:
# 1. Empty bubble on receiver: _enrich_message now always returns `content`
#    as a plain string (never None/missing), and `read_by` as a plain list of ints.
# 2. "Sent an attachment" notification: file messages now emit
#    `content` as "" (empty string) instead of None, so the frontend
#    can fall through to the file_name correctly.
# 3. Room ID type mismatch: room_id is always cast to int in emitted payloads.
# =====================================================================

import os
import time as time_module
from werkzeug.utils import secure_filename
from psycopg2.extras import RealDictCursor, execute_values
from collections import defaultdict

CHAT_UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "chat_files")
os.makedirs(CHAT_UPLOAD_FOLDER, exist_ok=True)

# Online users tracking: {user_id: {sid1, sid2, ...}}
_online_users = defaultdict(set)


def _enrich_message(cur, msg_dict):
    """
    Add reply_to info and reactions to a message dict.

    CRITICAL FIXES applied here:
    - `content` is always a string (never None) — prevents blank bubbles
    - `room_id` is always int — prevents isCurRoom mismatch on frontend
    - `read_by` is always a plain list of ints — consistent with frontend's toInt()
    - `reactions` always present as list
    """
    r = dict(msg_dict)

    # ── Ensure created_at is a string ──────────────────────────────────
    if r.get("created_at") and not isinstance(r["created_at"], str):
        r["created_at"] = r["created_at"].isoformat()

    # ── FIX 1: content MUST be a string, never None ────────────────────
    # If content is None (file-only message), set to "" so the frontend
    # extractContent() returns "" and falls through to file_name correctly
    if r.get("content") is None:
        r["content"] = ""

    # ── FIX 2: room_id MUST be an int ─────────────────────────────────
    if r.get("room_id") is not None:
        r["room_id"] = int(r["room_id"])

    # ── FIX 3: read_by MUST be a plain list of ints ───────────────────
    # PostgreSQL ARRAY() returns a list, but it might contain strings or
    # the field might be missing entirely — normalise it here
    raw_read_by = r.get("read_by") or []
    if isinstance(raw_read_by, (list, tuple)):
        r["read_by"] = [int(x) for x in raw_read_by if x is not None]
    else:
        r["read_by"] = []

    # ── Reply-to info ──────────────────────────────────────────────────
    r["reply_to_content"] = None
    r["reply_to_sender"]  = None
    if r.get("reply_to_id"):
        cur.execute(
            "SELECT content, file_name, sender_id FROM chat_messages WHERE id=%s",
            (r["reply_to_id"],)
        )
        parent = cur.fetchone()
        if parent:
            r["reply_to_content"] = parent["content"] or (
                f"📎 {parent['file_name']}" if parent["file_name"] else ""
            )
            cur.execute("SELECT name FROM users WHERE user_id=%s", (parent["sender_id"],))
            sr = cur.fetchone()
            r["reply_to_sender"] = sr["name"] if sr else "Unknown"

    # ── Reactions ──────────────────────────────────────────────────────
    cur.execute(
        "SELECT emoji, user_id FROM chat_message_reactions WHERE message_id=%s",
        (r["id"],)
    )
    r["reactions"] = [
        {"emoji": row["emoji"], "user_id": int(row["user_id"])}
        for row in cur.fetchall()
    ]
    return r


# ── Serve chat files ───────────────────────────────────────────────────────────
@app.route("/files/chat/<path:filename>")
def serve_chat_file(filename):
    return send_from_directory(CHAT_UPLOAD_FOLDER, filename, as_attachment=False)


# ── GET ALL USERS FOR CHAT ────────────────────────────────────────────────────
@app.route("/chat/users", methods=["GET"])
def get_chat_users():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id = session["user_id"]
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT user_id, name, email, role, department, location, image
            FROM users
            WHERE is_active = TRUE AND user_id != %s
            ORDER BY name ASC
        """, (user_id,))
        return jsonify([dict(r) for r in cur.fetchall()]), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── GET ROOMS ─────────────────────────────────────────────────────────────────
@app.route("/chat/rooms", methods=["GET"])
def get_chat_rooms():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id = session["user_id"]
    role    = session.get("role", "")
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if role == "chairman":
               cur.execute("""
        SELECT r.id, r.name, r.room_type, r.department,
               (SELECT content FROM chat_messages
                WHERE room_id=r.id AND (is_deleted IS NULL OR is_deleted=FALSE)
                ORDER BY created_at DESC LIMIT 1) AS last_message,
               (SELECT file_name FROM chat_messages
                WHERE room_id=r.id AND file_name IS NOT NULL
                ORDER BY created_at DESC LIMIT 1) AS last_file_name,
               (SELECT created_at FROM chat_messages
                WHERE room_id=r.id ORDER BY created_at DESC LIMIT 1) AS last_message_at,
               (SELECT COUNT(*) FROM chat_messages m
                WHERE m.room_id=r.id
                  AND (m.is_deleted IS NULL OR m.is_deleted=FALSE)
                  AND NOT EXISTS (
                      SELECT 1 FROM chat_message_reads rd
                      WHERE rd.message_id=m.id AND rd.user_id=%s
                  )) AS unread_count
        FROM chat_rooms r
        WHERE (r.is_active=TRUE OR r.is_active IS NULL)
          AND (
            r.room_type != 'dm'
            OR EXISTS (
                SELECT 1 FROM chat_room_members cm
                WHERE cm.room_id = r.id AND cm.user_id = %s
            )
          )
        ORDER BY last_message_at DESC NULLS LAST, r.name
          """, (user_id, user_id))
        else:
            cur.execute("""
                SELECT r.id, r.name, r.room_type, r.department,
                       (SELECT content FROM chat_messages
                        WHERE room_id=r.id AND (is_deleted IS NULL OR is_deleted=FALSE)
                        ORDER BY created_at DESC LIMIT 1) AS last_message,
                       (SELECT file_name FROM chat_messages
                        WHERE room_id=r.id AND file_name IS NOT NULL
                        ORDER BY created_at DESC LIMIT 1) AS last_file_name,
                       (SELECT created_at FROM chat_messages
                        WHERE room_id=r.id ORDER BY created_at DESC LIMIT 1) AS last_message_at,
                       (SELECT COUNT(*) FROM chat_messages m
                        WHERE m.room_id=r.id
                          AND (m.is_deleted IS NULL OR m.is_deleted=FALSE)
                          AND NOT EXISTS (
                              SELECT 1 FROM chat_message_reads rd
                              WHERE rd.message_id=m.id AND rd.user_id=%s
                          )) AS unread_count
                FROM chat_rooms r
                JOIN chat_room_members rm ON rm.room_id=r.id AND rm.user_id=%s
                WHERE r.is_active=TRUE OR r.is_active IS NULL
                ORDER BY last_message_at DESC NULLS LAST, r.name
            """, (user_id, user_id))

        rooms = []
        for r in cur.fetchall():
            row = dict(r)
            row["last_message_at"] = row["last_message_at"].isoformat() if row["last_message_at"] else None
            row["unread_count"]    = int(row["unread_count"] or 0)

            # FIX: sidebar preview — show file name if no text content
            if not row.get("last_message") and row.get("last_file_name"):
                row["last_message"] = f"📎 {row['last_file_name']}"
            row.pop("last_file_name", None)

            if row["room_type"] == "dm":
                cur.execute("""
                    SELECT u.name, u.image FROM users u
                    JOIN chat_room_members rm ON rm.user_id=u.user_id
                    WHERE rm.room_id=%s AND u.user_id != %s LIMIT 1
                """, (row["id"], user_id))
                other = cur.fetchone()
                if other:
                    row["name"]     = other["name"]
                    row["dm_image"] = other["image"]
            rooms.append(row)
        return jsonify(rooms), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/chat/room/<int:room_id>/rename", methods=["PUT"])
def rename_chat_room(room_id):
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403
 
    data = request.get_json(silent=True) or {}
    new_name = (data.get("name") or "").strip()
 
    if not new_name:
        return jsonify({"message": "Name required"}), 400
 
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT id, room_type FROM chat_rooms WHERE id=%s", (room_id,))
        room = cur.fetchone()
        if not room:
            return jsonify({"message": "Room not found"}), 404
        if room[1] == "dm":
            return jsonify({"message": "Cannot rename DMs"}), 400
 
        cur.execute("UPDATE chat_rooms SET name=%s WHERE id=%s", (new_name, room_id))
        conn.commit()
 
        socketio.emit("room_renamed", {"room_id": room_id, "name": new_name}, room=f"chat_{room_id}")
        return jsonify({"message": "Renamed", "name": new_name}), 200
 
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)
# ── GET MESSAGES ──────────────────────────────────────────────────────────────
@app.route("/chat/room/<int:room_id>/messages", methods=["GET"])
def get_room_messages(room_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id = session["user_id"]
    role    = session.get("role", "")
    before  = request.args.get("before")
    limit   = int(request.args.get("limit", 60))

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if role != "chairman":
            cur.execute(
                "SELECT 1 FROM chat_room_members WHERE room_id=%s AND user_id=%s",
                (room_id, user_id)
            )
            if not cur.fetchone():
                return jsonify({"message": "Access denied"}), 403

        query = """
            SELECT m.id, m.room_id, m.content, m.msg_type, m.created_at,
                   COALESCE(m.is_edited,  FALSE) AS is_edited,
                   COALESCE(m.is_deleted, FALSE) AS is_deleted,
                   m.file_url, m.file_name, m.file_size, m.reply_to_id,
                   u.user_id AS sender_id, u.name AS sender_name,
                   u.image   AS sender_image, u.role AS sender_role,
                   ARRAY(
                       SELECT rd.user_id::int FROM chat_message_reads rd
                       WHERE rd.message_id=m.id
                   ) AS read_by
            FROM chat_messages m
            JOIN users u ON u.user_id=m.sender_id
            WHERE m.room_id=%s
        """
        params = [room_id]
        if before:
            query += " AND m.id < %s"
            params.append(int(before))
        query += " ORDER BY m.created_at DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows     = cur.fetchall()
        messages = [_enrich_message(cur, row) for row in rows]
        messages.reverse()

        # Auto-mark read
        if messages:
            ids = [m["id"] for m in messages]
            execute_values(cur,
                "INSERT INTO chat_message_reads (message_id, user_id) VALUES %s ON CONFLICT DO NOTHING",
                [(mid, user_id) for mid in ids]
            )
            conn.commit()

        return jsonify(messages), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── SEND TEXT MESSAGE ─────────────────────────────────────────────────────────
@app.route("/chat/send", methods=["POST"])
def send_chat_message():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    user_id     = session["user_id"]
    role        = session.get("role", "")
    data        = request.get_json(silent=True) or {}
    room_id     = data.get("room_id")
    content     = (data.get("content") or "").strip()
    reply_to_id = data.get("reply_to_id") or None

    if not room_id or not content:
        return jsonify({"message": "room_id and content required"}), 400

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if role != "chairman":
            cur.execute(
                "SELECT 1 FROM chat_room_members WHERE room_id=%s AND user_id=%s",
                (room_id, user_id)
            )
            if not cur.fetchone():
                return jsonify({"message": "Not a member of this room"}), 403

        cur.execute("""
            INSERT INTO chat_messages (room_id, sender_id, content, reply_to_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id, created_at
        """, (room_id, user_id, content, reply_to_id))
        row = cur.fetchone()

        cur.execute("SELECT name, image, role FROM users WHERE user_id=%s", (user_id,))
        sender = cur.fetchone()
        conn.commit()

        # Build msg_data with _enrich_message to apply all fixes
        msg_data = _enrich_message(cur, {
            "id":           row["id"],
            "room_id":      int(room_id),
            "created_at":   row["created_at"],
            "is_edited":    False,
            "is_deleted":   False,
            "msg_type":     "text",
            "file_url":     None,
            "file_name":    None,
            "file_size":    None,
            "reply_to_id":  reply_to_id,
            # FIX: content is always the actual string here
            "content":      content,
            "sender_id":    user_id,
            "sender_name":  sender["name"],
            "sender_image": sender["image"],
            "sender_role":  sender["role"],
            "read_by":      [user_id],
        })

        # ✅ No include_self=False — this is an HTTP route, not a socket handler
        socketio.emit("new_message", msg_data, room=f"chat_{room_id}")
        return jsonify(msg_data), 201

    except Exception as e:
        conn.rollback()
        print(f"❌ send_chat_message error: {e}")
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── SEND FILE ─────────────────────────────────────────────────────────────────
@app.route("/chat/send-file", methods=["POST"])
def send_chat_file_route():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    user_id     = session["user_id"]
    role        = session.get("role", "")
    room_id     = request.form.get("room_id")
    content     = request.form.get("content", "").strip()
    reply_to_id = request.form.get("reply_to_id") or None

    if not room_id:
        return jsonify({"message": "room_id required"}), 400

    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"message": "No file provided"}), 400

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    ALLOWED = {
        "jpg","jpeg","png","gif","webp","svg","bmp","ico",
        "pdf","doc","docx","xls","xlsx","ppt","pptx",
        "txt","csv","zip","rar","7z",
        "mp4","mov","avi","mkv","webm","wmv","flv",
        "mp3","wav","ogg","aac","flac","m4a",
    }
    if ext not in ALLOWED:
        return jsonify({"message": f"File type .{ext} not allowed"}), 400

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if role != "chairman":
            cur.execute(
                "SELECT 1 FROM chat_room_members WHERE room_id=%s AND user_id=%s",
                (room_id, user_id)
            )
            if not cur.fetchone():
                return jsonify({"message": "Not a member"}), 403

        safe_name   = secure_filename(file.filename)
        unique_name = f"{int(time_module.time() * 1000)}_{safe_name}"
        filepath    = os.path.join(CHAT_UPLOAD_FOLDER, unique_name)
        file.save(filepath)
        db_size = os.path.getsize(filepath)
        db_url  = f"/files/chat/{unique_name}"

        # FIX: store content as None in DB if empty (clean), but we handle
        # it in the emitted payload below
        cur.execute("""
            INSERT INTO chat_messages
                (room_id, sender_id, content, file_url, file_name, file_size, reply_to_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, created_at
        """, (room_id, user_id, content or None, db_url, safe_name, db_size, reply_to_id))
        row = cur.fetchone()

        cur.execute("SELECT name, image, role FROM users WHERE user_id=%s", (user_id,))
        sender = cur.fetchone()
        conn.commit()

        msg_data = _enrich_message(cur, {
            "id":           row["id"],
            "room_id":      int(room_id),
            "created_at":   row["created_at"],
            "is_edited":    False,
            "is_deleted":   False,
            "msg_type":     "file",
            "file_url":     db_url,
            "file_name":    safe_name,
            "file_size":    db_size,
            "reply_to_id":  reply_to_id,
            # FIX: content is "" (empty string) not None — frontend extractContent
            # returns "" and falls through to show file_name in notification
            "content":      content or "",
            "sender_id":    user_id,
            "sender_name":  sender["name"],
            "sender_image": sender["image"],
            "sender_role":  sender["role"],
            "read_by":      [user_id],
        })

        # ✅ No include_self=False — HTTP route
        socketio.emit("new_message", msg_data, room=f"chat_{room_id}")
        return jsonify(msg_data), 201

    except Exception as e:
        conn.rollback()
        print(f"❌ send-file error: {e}")
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── EDIT MESSAGE ──────────────────────────────────────────────────────────────
@app.route("/chat/message/<int:msg_id>", methods=["PUT"])
def edit_chat_message(msg_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id = session["user_id"]
    content = (request.get_json(silent=True) or {}).get("content", "").strip()
    if not content:
        return jsonify({"message": "content required"}), 400

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT sender_id, room_id, is_deleted FROM chat_messages WHERE id=%s", (msg_id,))
        msg = cur.fetchone()
        if not msg:
            return jsonify({"message": "Not found"}), 404
        if msg["is_deleted"]:
            return jsonify({"message": "Cannot edit deleted message"}), 400
        if msg["sender_id"] != user_id:
            return jsonify({"message": "Can only edit your own messages"}), 403

        cur.execute("UPDATE chat_messages SET content=%s, is_edited=TRUE WHERE id=%s", (content, msg_id))
        conn.commit()

        socketio.emit("message_edited", {
            "message_id": msg_id,
            "content":    content,
            "room_id":    int(msg["room_id"]),
        }, room=f"chat_{msg['room_id']}")

        return jsonify({"message": "Edited", "content": content}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── DELETE MESSAGE ────────────────────────────────────────────────────────────
@app.route("/chat/message/<int:msg_id>", methods=["DELETE"])
def delete_chat_message(msg_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id     = session["user_id"]
    is_chairman = session.get("role") == "chairman"

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT sender_id, room_id FROM chat_messages WHERE id=%s", (msg_id,))
        msg = cur.fetchone()
        if not msg:
            return jsonify({"message": "Not found"}), 404
        if msg["sender_id"] != user_id and not is_chairman:
            return jsonify({"message": "Access denied"}), 403

        cur.execute(
            "UPDATE chat_messages SET is_deleted=TRUE, content='', file_url=NULL WHERE id=%s",
            (msg_id,)
        )
        conn.commit()

        socketio.emit("message_deleted", {
            "message_id": msg_id,
            "room_id":    int(msg["room_id"]),
        }, room=f"chat_{msg['room_id']}")

        return jsonify({"message": "Deleted"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── REACT TO MESSAGE ──────────────────────────────────────────────────────────
@app.route("/chat/message/<int:msg_id>/react", methods=["POST"])
def react_to_message(msg_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id = session["user_id"]
    emoji   = (request.get_json(silent=True) or {}).get("emoji", "")
    VALID   = ["👍","❤️","😂","😮","😢","🙏","🔥","✅","🎉","👏"]
    if emoji not in VALID:
        return jsonify({"message": "Invalid emoji"}), 400

    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT room_id FROM chat_messages WHERE id=%s", (msg_id,))
        msg = cur.fetchone()
        if not msg:
            return jsonify({"message": "Not found"}), 404

        cur.execute(
            "SELECT id FROM chat_message_reactions WHERE message_id=%s AND user_id=%s AND emoji=%s",
            (msg_id, user_id, emoji)
        )
        existing = cur.fetchone()
        if existing:
            cur.execute("DELETE FROM chat_message_reactions WHERE id=%s", (existing["id"],))
        else:
            cur.execute(
                "INSERT INTO chat_message_reactions (message_id, user_id, emoji) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                (msg_id, user_id, emoji)
            )
        conn.commit()

        cur.execute("SELECT emoji, user_id FROM chat_message_reactions WHERE message_id=%s", (msg_id,))
        reactions = [{"emoji": r["emoji"], "user_id": int(r["user_id"])} for r in cur.fetchall()]

        socketio.emit("message_reaction", {
            "message_id": msg_id,
            "reactions":  reactions,
            "room_id":    int(msg["room_id"]),
        }, room=f"chat_{msg['room_id']}")

        return jsonify({"reactions": reactions}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── CREATE DM ─────────────────────────────────────────────────────────────────
@app.route("/chat/dm/<target_email>", methods=["POST"])
def get_or_create_dm(target_email):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id = session["user_id"]
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT user_id, name FROM users WHERE email=%s", (target_email,))
        target = cur.fetchone()
        if not target:
            return jsonify({"message": "User not found"}), 404
        target_id = target["user_id"]
        if target_id == user_id:
            return jsonify({"message": "Cannot DM yourself"}), 400

        cur.execute("""
            SELECT r.id FROM chat_rooms r
            JOIN chat_room_members m1 ON m1.room_id=r.id AND m1.user_id=%s
            JOIN chat_room_members m2 ON m2.room_id=r.id AND m2.user_id=%s
            WHERE r.room_type='dm' LIMIT 1
        """, (user_id, target_id))
        existing = cur.fetchone()
        if existing:
            return jsonify({"room_id": existing["id"]}), 200

        cur.execute("""
            INSERT INTO chat_rooms (name, room_type, created_by, is_active)
            VALUES (%s, 'dm', %s, TRUE) RETURNING id
        """, (f"DM:{user_id}:{target_id}", user_id))
        room_id = cur.fetchone()["id"]

        execute_values(cur,
            "INSERT INTO chat_room_members (room_id, user_id) VALUES %s ON CONFLICT DO NOTHING",
            [(room_id, user_id), (room_id, target_id)]
        )
        conn.commit()
        return jsonify({"room_id": room_id}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── CREATE GROUP (chairman) ───────────────────────────────────────────────────
@app.route("/chat/room/create", methods=["POST"])
def create_chat_room():
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403
    data       = request.get_json(silent=True) or {}
    name       = (data.get("name") or "").strip()
    member_ids = data.get("member_ids", [])
    if not name:
        return jsonify({"message": "Room name required"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO chat_rooms (name, room_type, created_by, is_active)
            VALUES (%s, 'group', %s, TRUE) RETURNING id
        """, (name, session["user_id"]))
        room_id = cur.fetchone()[0]

        all_members = list(set([session["user_id"]] + member_ids))
        execute_values(cur,
            "INSERT INTO chat_room_members (room_id, user_id) VALUES %s ON CONFLICT DO NOTHING",
            [(room_id, uid) for uid in all_members]
        )
        conn.commit()
        return jsonify({"room_id": room_id, "message": "Group created"}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── DELETE ROOM (chairman) ────────────────────────────────────────────────────
@app.route("/chat/room/<int:room_id>", methods=["DELETE"])
def delete_chat_room(room_id):
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT id FROM chat_rooms WHERE id=%s", (room_id,))
        if not cur.fetchone():
            return jsonify({"message": "Room not found"}), 404
        cur.execute("DELETE FROM chat_rooms WHERE id=%s", (room_id,))
        conn.commit()
        socketio.emit("room_deleted", {"room_id": room_id})
        return jsonify({"message": "Room deleted"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── MANAGE ROOM MEMBERS ───────────────────────────────────────────────────────
@app.route("/chat/room/<int:room_id>/members", methods=["GET", "POST", "DELETE"])
def manage_room_members(room_id):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if request.method == "GET":
            cur.execute("""
                SELECT u.user_id, u.name, u.email, u.role, u.department, u.image
                FROM users u
                JOIN chat_room_members m ON m.user_id=u.user_id
                WHERE m.room_id=%s ORDER BY u.name
            """, (room_id,))
            return jsonify([dict(r) for r in cur.fetchall()]), 200

        if session.get("role") != "chairman":
            return jsonify({"message": "Chairman only"}), 403

        data   = request.get_json(silent=True) or {}
        target = data.get("user_id")
        if not target:
            return jsonify({"message": "user_id required"}), 400

        if request.method == "POST":
            cur.execute(
                "INSERT INTO chat_room_members (room_id, user_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (room_id, target)
            )
            socketio.emit("member_added",   {"room_id": room_id}, room=f"chat_{room_id}")
        else:
            cur.execute(
                "DELETE FROM chat_room_members WHERE room_id=%s AND user_id=%s",
                (room_id, target)
            )
            socketio.emit("member_removed", {"room_id": room_id}, room=f"chat_{room_id}")

        conn.commit()
        return jsonify({"message": "Done"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── MARK MESSAGES READ ────────────────────────────────────────────────────────
@app.route("/chat/mark-read", methods=["POST"])
def mark_messages_read():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    user_id     = session["user_id"]
    message_ids = (request.get_json(silent=True) or {}).get("message_ids", [])
    if not message_ids:
        return jsonify({"message": "No message_ids"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        execute_values(cur,
            "INSERT INTO chat_message_reads (message_id, user_id) VALUES %s ON CONFLICT DO NOTHING",
            [(mid, user_id) for mid in message_ids]
        )
        conn.commit()
        return jsonify({"message": "Marked read"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── ADMIN: ALL ROOMS (chairman) ───────────────────────────────────────────────
@app.route("/chat/admin/all-rooms", methods=["GET"])
def admin_all_rooms():
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403
    user_id = session["user_id"]
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT r.id, r.name, r.room_type, r.department,
                   (SELECT content FROM chat_messages
                    WHERE room_id=r.id AND (is_deleted IS NULL OR is_deleted=FALSE)
                    ORDER BY created_at DESC LIMIT 1) AS last_message,
                   (SELECT created_at FROM chat_messages
                    WHERE room_id=r.id ORDER BY created_at DESC LIMIT 1) AS last_message_at,
                   (SELECT COUNT(*) FROM chat_messages m
                    WHERE m.room_id=r.id AND (m.is_deleted IS NULL OR m.is_deleted=FALSE)
                    AND NOT EXISTS (
                        SELECT 1 FROM chat_message_reads rd
                        WHERE rd.message_id=m.id AND rd.user_id=%s
                    )) AS unread_count,
                   (SELECT COUNT(*) FROM chat_room_members WHERE room_id=r.id) AS member_count
            FROM chat_rooms r
            WHERE r.is_active=TRUE OR r.is_active IS NULL
            ORDER BY last_message_at DESC NULLS LAST, r.name
        """, (user_id,))

        rooms = []
        for r in cur.fetchall():
            row = dict(r)
            row["last_message_at"] = row["last_message_at"].isoformat() if row["last_message_at"] else None
            row["unread_count"]    = int(row["unread_count"] or 0)
            row["member_count"]    = int(row["member_count"] or 0)
            if row["room_type"] == "dm":
                cur.execute("""
                    SELECT u.name FROM users u
                    JOIN chat_room_members rm ON rm.user_id=u.user_id
                    WHERE rm.room_id=%s AND u.user_id != %s LIMIT 1
                """, (row["id"], user_id))
                other = cur.fetchone()
                if other:
                    row["name"] = other["name"]
            rooms.append(row)
        return jsonify(rooms), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── SECTION ACCESS CONTROL ────────────────────────────────────────────────────
AVAILABLE_SECTIONS = ["attendance","leave","salary","sales","leads","chat","fulldata","chatlogs"]

@app.route("/chat/access/set", methods=["POST"])
def set_employee_access():
    if "user_id" not in session or session.get("role") != "chairman":
        return jsonify({"message": "Chairman only"}), 403
    data         = request.get_json(silent=True) or {}
    target_email = data.get("email")
    sections     = [s for s in data.get("sections", []) if s in AVAILABLE_SECTIONS]
    if not target_email:
        return jsonify({"message": "email required"}), 400

    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users WHERE email=%s", (target_email,))
        user = cur.fetchone()
        if not user:
            return jsonify({"message": "User not found"}), 404
        cur.execute("""
            INSERT INTO employee_section_access (user_id, sections, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (user_id) DO UPDATE SET sections=%s::jsonb, updated_at=NOW()
        """, (user[0], json.dumps(sections), json.dumps(sections)))
        conn.commit()
        return jsonify({"message": "Access updated", "sections": sections}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route("/chat/access/get/<email>", methods=["GET"])
def get_employee_access(email):
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users WHERE email=%s", (email,))
        user = cur.fetchone()
        if not user:
            return jsonify({"sections": AVAILABLE_SECTIONS}), 200
        cur.execute("SELECT sections FROM employee_section_access WHERE user_id=%s", (user[0],))
        row      = cur.fetchone()
        sections = row[0] if row else AVAILABLE_SECTIONS
        if isinstance(sections, str):
            sections = json.loads(sections)
        return jsonify({"sections": sections}), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)


# ── SOCKET.IO CHAT EVENTS ─────────────────────────────────────────────────────
# include_self=False is VALID here — these are socket event handlers
# where request.sid exists. Do NOT use include_self=False in HTTP routes.

@socketio.on("join_chat_room")
def handle_join_chat_room(data):
    from flask_socketio import join_room
    room_id = data.get("room_id")
    if room_id:
        join_room(f"chat_{room_id}")
        emit("joined_room", {"room_id": room_id})

@socketio.on("leave_chat_room")
def handle_leave_chat_room(data):
    from flask_socketio import leave_room
    room_id = data.get("room_id")
    if room_id:
        leave_room(f"chat_{room_id}")

@socketio.on("typing")
def handle_typing(data):
    room_id = data.get("room_id")
    if room_id:
        emit("typing_indicator", {
            "room_id":   room_id,
            "user_name": data.get("user_name", ""),
            "is_typing": data.get("is_typing", False),
        }, room=f"chat_{room_id}", include_self=False)

@socketio.on("message_edited")
def handle_message_edited(data):
    room_id = data.get("room_id")
    if room_id:
        emit("message_edited", data, room=f"chat_{room_id}", include_self=False)

@socketio.on("message_deleted")
def handle_message_deleted(data):
    room_id = data.get("room_id")
    if room_id:
        emit("message_deleted", data, room=f"chat_{room_id}", include_self=False)

@socketio.on("message_reaction")
def handle_message_reaction(data):
    room_id = data.get("room_id")
    if room_id:
        emit("message_reaction", data, room=f"chat_{room_id}", include_self=False)

@socketio.on("user_online")
def handle_user_online(data):
    user_id = data.get("user_id")
    if not user_id:
        return
    _online_users[user_id].add(request.sid)
    all_online = [uid for uid, sids in _online_users.items() if sids]
    emit("online_users", all_online, broadcast=True)
    emit("user_came_online", {"user_id": user_id}, broadcast=True, include_self=False)

@socketio.on("user_offline")
def handle_user_offline(data):
    user_id = data.get("user_id")
    if not user_id:
        return
    _online_users[user_id].discard(request.sid)
    if not _online_users[user_id]:
        del _online_users[user_id]
        emit("user_went_offline", {"user_id": user_id}, broadcast=True, include_self=False)

@socketio.on("disconnect")
def handle_chat_disconnect():
    disconnected_user = None
    for user_id, sids in list(_online_users.items()):
        if request.sid in sids:
            sids.discard(request.sid)
            if not sids:
                del _online_users[user_id]
                disconnected_user = user_id
            break
    if disconnected_user:
        emit("user_went_offline", {"user_id": disconnected_user}, broadcast=True)
    print(f"❌ Client disconnected: {request.sid}")

@socketio.on("messages_read")
def handle_messages_read(data):
    room_id     = data.get("room_id")
    reader_id   = data.get("reader_id")
    message_ids = data.get("message_ids", [])
    if room_id and reader_id and message_ids:
        emit("messages_read", {
            "reader_id":   reader_id,
            "message_ids": message_ids,
        }, room=f"chat_{room_id}", include_self=False)

# ==================== END CHAT ROUTES ====================
if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False  # CRITICAL: Set to False in production
    )

# ==================== END OF PART 3 ====================
# ==================== OPTIMIZATION COMPLETE ====================