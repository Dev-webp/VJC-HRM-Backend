from flask import Flask, request, session, jsonify, redirect
from flask_cors import CORS, cross_origin
from datetime import datetime, date
from db import get_db_connection
from dotenv import load_dotenv
import os
from werkzeug.utils import secure_filename
import time
from flask import send_from_directory
from psycopg2.extras import RealDictCursor
from calendar import monthrange
from datetime import timedelta
from flask import request, session, jsonify
from flask_cors import cross_origin
from decimal import Decimal
from psycopg2.extras import RealDictCursor
from db import get_db_connection, put_db_connection
from flask import Flask, request, session, jsonify
from datetime import datetime, date, timezone, timedelta
from db import get_db_connection, put_db_connection
import pytz
from calendar import monthrange
from flask import Flask, request, jsonify, session, send_file
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal
from openpyxl import Workbook
from io import BytesIO
app = Flask(__name__)

IST = pytz.timezone('Asia/Kolkata')
OFFICE_IPS = [
    "171.76.84.77", 
    "152.57.107.135",
    "183.83.164.14",
    "49.43.216.190",
    "49.37.155.17",  
    # Add any other office IPs here if you have them
]
def now_ist():
    # Returns current time in India with timezone awareness
    return datetime.now(IST)

def today_ist():
    # Returns current date in India
    return now_ist().date()
# Load environment variables
def cleanup_orphaned_paid_leave_attendance():
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
        print("Orphaned paid leave attendance cleaned.")
    except Exception as e:
        conn.rollback()
        print("Cleanup error:", e)
    finally:
        cur.close()
        put_db_connection(conn)

load_dotenv()
UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "salary_slips")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET") or "fallback-secret-key"
OFFER_LETTER_FOLDER = os.path.join(os.getcwd(), "uploads", "offer_letters")
os.makedirs(OFFER_LETTER_FOLDER, exist_ok=True)
# CORS setup
CORS(app, supports_credentials=True, origins=[
    "http://hrm.vjcoverseas.com",
    "https://hrm.vjcoverseas.com",
    "http://localhost:3000"
])

app.config.update(
    SESSION_COOKIE_SAMESITE="None",
    SESSION_COOKIE_SECURE=True
)


# ---------------------- AUTH & SESSION ----------------------
PROFILE_UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "profile_images")
os.makedirs(PROFILE_UPLOAD_FOLDER, exist_ok=True)

SALARY_UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "salary_slips")
os.makedirs(SALARY_UPLOAD_FOLDER, exist_ok=True)


# Profile image upload route (POST)
@app.route("/upload-profile-image", methods=["POST"])
def upload_profile_image():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    file = request.files.get("image")  # React must send key 'imageFile'
    if not file:
        return jsonify({"message": "No file uploaded"}), 400

    safe_name = secure_filename(file.filename)
    unique_name = f"{int(time.time())}_{safe_name}"
    filepath = os.path.join(PROFILE_UPLOAD_FOLDER, unique_name)

    try:
        file.save(filepath)
        conn = get_db_connection()
        cur = conn.cursor()
        # Store relative path to serve via static route
        db_path = f"/files/profile_images/{unique_name}"
        cur.execute(
            "UPDATE users SET image = %s WHERE user_id = %s",
            (db_path, session["user_id"]),
        )
        conn.commit()
        cur.close()
        put_db_connection(conn)
        return jsonify({"message": "Profile image uploaded successfully", "image": db_path}), 200
    except Exception as e:
        return jsonify({"message": f"Error saving image: {str(e)}"}), 500
@app.route("/allowed-ips", methods=["GET"])
def get_allowed_ips():
    """Returns a list of public IP addresses permitted to access the service."""
    return jsonify({"allowed_ips": OFFICE_IPS})

@app.route("/upload-offer-letter", methods=["POST"])
def upload_offer_letter():
    if "user_id" not in session or session.get("role") != "chairman":
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

        # Check if the user exists
        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        if not user:
            return jsonify({"message": "User not found"}), 404

        # Save the file
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

        return jsonify({"message": "Offer letter uploaded successfully", "offerLetterUrl": db_path}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"message": f"Error saving file: {str(e)}"}), 500
    finally:
        if cur:
            cur.close()
        if conn:
            put_db_connection(conn)

# The existing route to serve the files remains the same.
@app.route("/files/offer_letters/<path:filename>")
def serve_offer_letter(filename):
    return send_from_directory(OFFER_LETTER_FOLDER, filename, as_attachment=False)
# Serve profile images static files
@app.route("/files/profile_images/<path:filename>")
def serve_profile_image(filename):
    return send_from_directory(PROFILE_UPLOAD_FOLDER, filename, as_attachment=False)
@app.route("/", methods=["GET", "POST"])
@cross_origin(supports_credentials=True)
def login():
    if request.method == "GET":
        return "✅ Backend running. Use POST to login."

    email = request.form.get("email")
    password = request.form.get("password")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, password, role FROM users WHERE email = %s", (email,))
    user = cur.fetchone()
    cur.close()
    put_db_connection(conn)

    if user and password == user[1]:
        session["user_id"] = user[0]
        session["role"] = user[2]
        session["email"] = email
        return redirect("/dashboard")
    
    return "❌ Invalid credentials", 401

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
    return jsonify({"redirect": "chairman" if session["role"] == "chairman" else "employee"})

# ---------------------- FILE UPLOADS (ANY TYPE) ----------------------
@app.route("/upload-salary-slip", methods=["POST"])
def upload_salary_slip():
    """
    Frontend should POST multipart/form-data with:
      - field 'email' (employee email)
      - field 'salarySlip' (the file)  <-- matches your SalarySlipUpload.jsx
    """
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    email = request.form.get("email")
    file = request.files.get("salarySlip")

    if not email or not file:
        return jsonify({"message": "Missing email or file"}), 400

    conn = None
    cur = None
    try:
        # Allow any file type; just sanitize filename to avoid path traversal
        original_name = file.filename or "upload.bin"
        safe_name = secure_filename(original_name)  # doesn't restrict types, just cleans the name
        unique_name = f"{int(time.time())}-{safe_name}"
        filepath = os.path.join(UPLOAD_FOLDER, unique_name)

        # Save to disk
        file.save(filepath)

        # Save DB record
        conn = get_db_connection()
        cur = conn.cursor()
        # Minimal schema: salary_slips(email TEXT, filename TEXT, path TEXT, uploaded_at TIMESTAMP DEFAULT NOW())
        cur.execute(
            """
            INSERT INTO salary_slips (email, filename, path)
            VALUES (%s, %s, %s)
            """,
            (email, unique_name, filepath),
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
    """
    Returns slips for the logged-in user's email.
    Used by your EmployeeDashboard: axios.get('https://backend.vjcoverseas.com/my-salary-slips')
    """
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    email = session.get("email")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Try uploaded_at (if your table has it); fall back to created_at; else omit date.
        try:
            cur.execute(
                """
                SELECT filename, path, uploaded_at
                FROM salary_slips
                WHERE email = %s
                ORDER BY uploaded_at DESC NULLS LAST, filename DESC
                """,
                (email,),
            )
            rows = cur.fetchall()
            items = [
                {
                    "filename": r[0],
                    "path": f"/files/salary_slips/{r[0]}",  # served by static route below
                    "uploadedAt": r[2].isoformat() if r[2] else None,
                }
                for r in rows
            ]
        except Exception:
            cur.execute(
                """
                SELECT filename, path
                FROM salary_slips
                WHERE email = %s
                ORDER BY filename DESC
                """,
                (email,),
            )
            rows = cur.fetchall()
            items = [{"filename": r[0], "path": f"/files/salary_slips/{r[0]}"} for r in rows]

        return jsonify(items), 200
    finally:
        cur.close()
        put_db_connection(conn)


# Serve uploaded files (so links work in the frontend list)
@app.route("/files/salary_slips/<path:filename>")
def serve_salary_slip(filename):
    return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=False)

# ---------------------- PROFILE ----------------------
# In your app.py file, locate the /me route

@app.route("/me", methods=["GET"])
def me():
    if "user_id" not in session:
        return jsonify({"message": "Unauthorized"}), 401

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT user_id, name, email, role, image, offer_letter_url, location,
                   employee_id, salary, bank_account, dob, doj, pan_no, ifsc_code, department
            FROM users
            WHERE user_id = %s
        """, (session["user_id"],))
        user = cur.fetchone()
        if not user:
            return jsonify({"message": "User not found"}), 404

        return jsonify({
            "id": user["user_id"],
            "name": user["name"],
            "email": user["email"],
            "role": user["role"],
            "image": user["image"],
            "offer_letter_url": user["offer_letter_url"],
            "location": user["location"],
            "employeeId": user["employee_id"],

            # --- Add these lines for the missing fields ---
            "salary": float(user["salary"]) if user["salary"] is not None else None,
            "bankAccount": user["bank_account"],
            "dob": user["dob"].isoformat() if user["dob"] else None,
            "doj": user["doj"].isoformat() if user["doj"] else None,
            "panNo": user["pan_no"],
            "ifscCode": user["ifsc_code"],
            "department": user["department"],
            # --- End add ---
        }), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/update-profile-image", methods=["POST"])
def update_profile_image():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    new_image = request.form.get("image")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET image = %s WHERE user_id = %s", (new_image, session["user_id"]))
    conn.commit()
    cur.close()
    put_db_connection(conn)
    return jsonify({"message": "Image updated"}), 200

@app.route("/register", methods=["POST"])
def register():
    name = request.form.get("name")
    email = request.form.get("email")
    password = request.form.get("password")

    if not email.endswith("@vjcoverseas.com"):
        return "❌ Only company emails allowed", 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (name, email, password, role) VALUES (%s, %s, %s, 'employee')",
            (name, email, password),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        return f"❌ Error: {str(e)}", 500
    finally:
        cur.close()
        put_db_connection(conn)

    return "✅ Registered", 200

# ---------------------- ATTENDANCE ----------------------
import json  # Make sure you have this at the top of your file

@app.route("/attendance", methods=["POST"])
def mark_attendance():
    if "user_id" not in session:
        return {"message": "Not logged in"}, 401

    user_id = session["user_id"]
    action = request.form.get("action")
    time_param = request.form.get("time")  # New param to send break timestamp

    now = now_ist().time()
    today = today_ist()

    valid_actions = [
        "office_in",
        "break_out",
        "break_in",
        "break_out_2",
        "break_in_2",
        "lunch_out",
        "lunch_in",
        "office_out",
        "extra_break_in",
        "extra_break_out"
    ]

    if action not in valid_actions:
        return {"message": "Invalid action"}, 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT extra_break_ins, extra_break_outs FROM attendance WHERE user_id = %s AND date = %s", (user_id, today))
        row = cur.fetchone()

        if action in ["extra_break_in", "extra_break_out"]:
            if not time_param:
                return {"message": "Missing time parameter for extra break"}, 400

            time_val = time_param  # expect HH:mm:ss string from frontend

            if row:
                # row[0] and row[1] are JSONB, so load as Python lists (jsonb => Python list)
                extra_break_ins = row[0] if row[0] else []
                extra_break_outs = row[1] if row[1] else []

                # Ensure they're Python lists (Postgres returns as list in psycopg3, but sometimes as string in psycopg2)
                if isinstance(extra_break_ins, str):
                    extra_break_ins = json.loads(extra_break_ins)
                if isinstance(extra_break_outs, str):
                    extra_break_outs = json.loads(extra_break_outs)

                if action == "extra_break_in":
                    extra_break_ins.append(time_val)
                else:
                    extra_break_outs.append(time_val)

                # Save using json.dumps and ::jsonb!
                cur.execute("""
                    UPDATE attendance
                    SET extra_break_ins = %s::jsonb, extra_break_outs = %s::jsonb
                    WHERE user_id = %s AND date = %s
                """, (
                    json.dumps(extra_break_ins),
                    json.dumps(extra_break_outs),
                    user_id, today
                ))
            else:
                extra_break_ins = [time_val] if action == "extra_break_in" else []
                extra_break_outs = [time_val] if action == "extra_break_out" else []
                cur.execute("""
                    INSERT INTO attendance (user_id, date, extra_break_ins, extra_break_outs)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb)
                """, (
                    user_id, today,
                    json.dumps(extra_break_ins),
                    json.dumps(extra_break_outs)
                ))

            conn.commit()
            return {"message": f"{action} recorded: {time_val}"}, 200

        else:
            if row:
                cur.execute(
                    f"UPDATE attendance SET {action} = %s WHERE user_id = %s AND date = %s",
                    (now, user_id, today)
                )
            else:
                columns = ['user_id', 'date', action]
                values = [user_id, today, now]
                query = f"INSERT INTO attendance ({', '.join(columns)}) VALUES (%s, %s, %s)"
                cur.execute(query, tuple(values))

            conn.commit()
            return {"message": f"{action} recorded"}, 200

    except Exception as e:
        conn.rollback()
        return {"message": f"❌ DB Error: {str(e)}"}, 500
    finally:
        cur.close()
        put_db_connection(conn)

import json

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
        base_query = """
            SELECT date, office_in, break_out, break_in, break_out_2, break_in_2, lunch_out, lunch_in, office_out, paid_leave_reason,
                   extra_break_ins, extra_break_outs
            FROM attendance
            WHERE user_id = %s
        """
        params = [user_id]

        if date_filter:
            base_query += " AND date = %s"
            params.append(date_filter)
        elif month_filter:
            base_query += " AND TO_CHAR(date, 'YYYY-MM') = %s"
            params.append(month_filter)

        base_query += " ORDER BY date DESC"
        cur.execute(base_query, params)

        rows = cur.fetchall()
        result = []

        for row in rows:
            # row[10] and row[11] (extra_break_ins/outs) might be list or JSON string
            extra_break_ins = row[10]
            extra_break_outs = row[11]
            if isinstance(extra_break_ins, str):
                try:
                    extra_break_ins = json.loads(extra_break_ins)
                except Exception:
                    extra_break_ins = []
            if extra_break_ins is None:
                extra_break_ins = []
            if isinstance(extra_break_outs, str):
                try:
                    extra_break_outs = json.loads(extra_break_outs)
                except Exception:
                    extra_break_outs = []
            if extra_break_outs is None:
                extra_break_outs = []

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

        return jsonify(result)

    finally:
        cur.close()
        put_db_connection(conn)
import json
from datetime import date
from calendar import monthrange

@app.route("/all-attendance")
def all_attendance():
    month = request.args.get("month")
    include_inactive = request.args.get("include_inactive") == "true"

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        query = """
            SELECT
                u.email, u.name, u.role, u.is_active,
                u.salary, u.location, u.employee_id, u.image,
                u.bank_account, u.dob, u.doj, u.pan_no, u.ifsc_code, u.department,
                a.date, a.office_in, a.break_out, a.break_in,
                a.break_out_2, a.break_in_2, a.lunch_out, a.lunch_in, a.office_out, a.paid_leave_reason,
                a.extra_break_ins, a.extra_break_outs
            FROM users u
            LEFT JOIN attendance a ON a.user_id = u.user_id
            WHERE u.is_active = %s
            ORDER BY u.email, a.date DESC
        """
        cur.execute(query, (not include_inactive,))
        rows = cur.fetchall()

        now_dt = now_ist()
        year, month_num = now_dt.year, now_dt.month
        if month:
            year, month_num = map(int, month.split('-'))
        total_days = monthrange(year, month_num)[1]
        all_dates = [date(year, month_num, d) for d in range(1, total_days + 1)]

        users = {}
        for r in rows:
            (
                email, name, role, is_active,
                salary, location, employee_id, image,
                bank_account, dob, doj, pan_no, ifsc_code, department,
                attend_date, office_in, break_out, break_in,
                break_out_2, break_in_2, lunch_out, lunch_in, office_out, paid_leave_reason,
                extra_break_ins, extra_break_outs
            ) = r

            # Ensure extra_break_ins and extra_break_outs are always lists
            if isinstance(extra_break_ins, str):
                try:
                    extra_break_ins = json.loads(extra_break_ins)
                except Exception:
                    extra_break_ins = []
            if extra_break_ins is None:
                extra_break_ins = []
            if isinstance(extra_break_outs, str):
                try:
                    extra_break_outs = json.loads(extra_break_outs)
                except Exception:
                    extra_break_outs = []
            if extra_break_outs is None:
                extra_break_outs = []

            if email not in users:
                users[email] = {
                    "name": name,
                    "role": role,
                    "is_active": is_active,
                    "salary": salary,
                    "location": location,
                    "employeeId": employee_id,
                    "image": image,
                    "bankAccount": bank_account,
                    "dob": dob.isoformat() if dob else None,
                    "doj": doj.isoformat() if doj else None,
                    "panNo": pan_no,
                    "ifscCode": ifsc_code,
                    "department": department,
                    "attendance": []
                }

            if attend_date and attend_date.year == year and attend_date.month == month_num:
                users[email]["attendance"].append({
                    "date": attend_date.isoformat(),
                    "office_in": office_in.isoformat() if office_in else None,
                    "office_out": office_out.isoformat() if office_out else None,
                    "break_out": break_out.isoformat() if break_out else None,
                    "break_in": break_in.isoformat() if break_in else None,
                    "break_out_2": break_out_2.isoformat() if break_out_2 else None,
                    "break_in_2": break_in_2.isoformat() if break_in_2 else None,
                    "lunch_out": lunch_out.isoformat() if lunch_out else None,
                    "lunch_in": lunch_in.isoformat() if lunch_in else None,
                    "paid_leave_reason": paid_leave_reason,
                    "extra_break_ins": extra_break_ins,
                    "extra_break_outs": extra_break_outs,
                })

        # Fill missing dates for attendance
        for user in users.values():
            existing_dates = {rec["date"] for rec in user["attendance"]}
            for d in all_dates:
                d_str = d.isoformat()
                if d_str not in existing_dates:
                    user["attendance"].append({
                        "date": d_str,
                        "office_in": None,
                        "office_out": None,
                        "break_out": None,
                        "break_in": None,
                        "break_out_2": None,
                        "break_in_2": None,
                        "lunch_out": None,
                        "lunch_in": None,
                        "paid_leave_reason": None,
                        "extra_break_ins": [],
                        "extra_break_outs": [],
                        "reason": "Sunday" if d.weekday() == 6 else None,
                        "present": True if d.weekday() == 6 else False
                    })
            user["attendance"].sort(key=lambda x: x["date"])

        return jsonify(users)

    finally:
        cur.close()
        put_db_connection(conn)
@app.route("/apply-leave", methods=["POST"])
def apply_leave():
    
    data = request.get_json()
    user_id = session["user_id"]
    leave_type = data.get("leave_type")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    reason = data.get("reason")

    if not all([leave_type, start_date, end_date, reason]):
        return jsonify({"message": "Missing required fields"}), 400

    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        if start_dt > end_dt:
            return jsonify({"message": "Start date cannot be after end date"}), 400
    except:
        return jsonify({"message": "Invalid date format"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Insert leave request with status "Pending"
        cur.execute("""
            INSERT INTO leave_requests (user_id, leave_type, start_date, end_date, reason, status)
            VALUES (%s, %s, %s, %s, %s, 'Pending')
        """, (user_id, leave_type, start_dt, end_dt, reason))
        conn.commit()
        return jsonify({"message": "Leave request submitted"}), 200
    except Exception as e:
        conn.rollback()
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
    cur.execute("""
        SELECT id, leave_type, start_date, end_date, reason, status, chairman_remarks
        FROM leave_requests
        WHERE user_id = %s
        ORDER BY start_date DESC
    """, (session["user_id"],))
    rows = cur.fetchall()
    cur.close()
    put_db_connection(conn)

    return jsonify([{
        "id": r[0],
        "leave_type": r[1],
        "start_date": r[2].strftime("%Y-%m-%d"),
        "end_date": r[3].strftime("%Y-%m-%d"),
        "reason": r[4] or "",
        "status": r[5],
        "chairman_remarks": r[6] or "",
    } for r in rows])


@app.route("/all-leave-requests")
def all_leave_requests():
   

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT lr.id, u.user_id, u.name, u.email,
                   lr.leave_type, lr.start_date, lr.end_date,
                   lr.reason, lr.status, lr.chairman_remarks
            FROM leave_requests lr
            JOIN users u ON lr.user_id = u.user_id
            ORDER BY lr.created_at DESC
        """)
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append({
                "id": r[0],
                "employee_id": r[1],
                "employee_name": r[2],
                "employee_email": r[3],
                "leave_type": r[4],
                "start_date": r[5].strftime("%Y-%m-%d") if r[5] else None,
                "end_date": r[6].strftime("%Y-%m-%d") if r[6] else None,
                "reason": r[7] or "",
                "status": r[8],
                "chairman_remarks": r[9] or "",
            })
        return jsonify(result)
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/leave-action", methods=["POST"])
def leave_action():
    data = request.get_json()
    leave_id = data.get("id")
    action = data.get("action")
    remarks = data.get("remarks", "")

    if not leave_id or action not in ("approve", "reject"):
        return jsonify({"message": "Invalid input"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Fetch leave record
        cur.execute("""
            SELECT user_id, leave_type, start_date, end_date, status
            FROM leave_requests WHERE id = %s
        """, (leave_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"message": "Leave request not found"}), 404

        user_id, leave_type, start_date, end_date, current_status = row

        if current_status.lower() != "pending":
            return jsonify({"message": "Leave request already processed"}), 400

        new_status = "Approved" if action == "approve" else "Rejected"

        # Convert dates if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        day = start_date
        while day <= end_date:
            if new_status == "Approved" and leave_type and "earned" in leave_type.lower():
                # Mark as paid leave day
                cur.execute("""
                    INSERT INTO attendance (user_id, date, present, paid_leave_reason, leave_type)
                    VALUES (%s, %s, TRUE, %s, %s)
                    ON CONFLICT (user_id, date)
                    DO UPDATE SET present = TRUE, paid_leave_reason = EXCLUDED.paid_leave_reason, leave_type = EXCLUDED.leave_type
                """, (user_id, day, "Earned Leave", leave_type))
            else:
                # Mark as absent (unpaid)
                cur.execute("""
                    INSERT INTO attendance (user_id, date, present)
                    VALUES (%s, %s, FALSE)
                    ON CONFLICT (user_id, date)
                    DO UPDATE SET present = FALSE
                """, (user_id, day))
            day += timedelta(days=1)

        # Update leave request status & remarks
        cur.execute("""
            UPDATE leave_requests
            SET status = %s, chairman_remarks = %s
            WHERE id = %s
        """, (new_status, remarks, leave_id))

        conn.commit()
        return jsonify({"message": f"Leave request {new_status.lower()}"}), 200

    except Exception as e:
        conn.rollback()
        print(f"Error in leave-action: {e}")
        return jsonify({"message": f"Server error: {str(e)}"}), 500

    finally:
        cur.close()
        put_db_connection(conn)


# ---------------------- CHAIRMAN DASHBOARD ----------------------
@app.route("/create-user", methods=["POST"])
def create_user():
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403

    data = request.get_json()
    name = data.get("name")
    email = data.get("email")
    password = data.get("password")
    role = data.get("role", "employee")
    image = data.get("image", "")
    location = data.get("location")
    employee_id = data.get("employee_id")  # changed to snake_case to match DB column naming convention
    salary = data.get("salary")
    bank_account = data.get("bank_account")
    dob = data.get("dob")
    doj = data.get("doj")
    pan_no = data.get("pan_no")
    ifsc_code = data.get("ifsc_code")
    department = data.get("department")

    if not all([name, email, password, role]):
        return jsonify({"message": "Missing required fields"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            return jsonify({"message": "User already exists"}), 409

        cur.execute("""
            INSERT INTO users 
            (name, email, password, role, image, location, employee_id, salary, bank_account, dob, doj, pan_no, ifsc_code, department) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, 
        (name, email, password, role, image, location, employee_id, salary, bank_account, dob, doj, pan_no, ifsc_code, department))
        conn.commit()
        return jsonify({"message": "✅ User created successfully"}), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"❌ DB Error: {str(e)}"}), 500
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
        cur.execute("SELECT user_id, leave_type, start_date, end_date, status FROM leave_requests WHERE id = %s", (leave_id,))
        leave = cur.fetchone()
        if not leave:
            return jsonify({"message": "Leave request not found"}), 404
        user_id, leave_type, start_date, end_date, status = leave
        if status.lower() == "approved" and leave_type and "earned" in leave_type.lower():
            cur.execute("""
                UPDATE attendance
                SET present = FALSE, paid_leave_reason = NULL, leave_type = NULL
                WHERE user_id = %s AND date >= %s AND date <= %s AND paid_leave_reason = 'Earned Leave'
            """, (user_id, start_date, end_date))
        cur.execute("DELETE FROM leave_requests WHERE id = %s", (leave_id,))
        conn.commit()
        # Call cleanup for other stale records
        cleanup_orphaned_paid_leave_attendance()
        return jsonify({"message": "Leave request deleted and paid leave attendance cleared"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Deletion error: {str(e)}"}), 500
    finally:
        cur.close()
        put_db_connection(conn)


app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",      # or "None" but Lax is safer for local
    SESSION_COOKIE_SECURE=False          # Must be False to send cookies over HTTP
)

# Paid Holidays Table (ensure in postgres: holidays(date, name, is_paid boolean))
@app.route("/mark-holiday", methods=["POST"])
def mark_holiday():
    if session.get("role") != "chairman":
        return jsonify({"message": "Unauthorized"}), 403
    data = request.get_json()
    date = data.get("date")
    name = data.get("name")
    is_paid = True  # Always mark as paid for office holidays

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO holidays (date, name, is_paid) VALUES (%s, %s, %s) ON CONFLICT (date) DO UPDATE SET name = %s, is_paid = %s",
        (date, name, is_paid, name, is_paid))
    conn.commit()
    cur.close()
    put_db_connection(conn)
    return jsonify({"message": "Holiday marked"}), 200
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
    month = request.args.get("month")  # "YYYY" or "YYYY-MM"
    max_attempts = 3

    for attempt in range(max_attempts):
        conn = None
        cur = None
        try:
            conn = get_db_connection()
            # Quick connection test to avoid stale connections
            try:
                with conn.cursor() as test_cur:
                    test_cur.execute("SELECT 1")
            except:
                put_db_connection(conn)
                conn = get_db_connection()
            cur = conn.cursor()

            if month and len(month) == 7:
                cur.execute("SELECT date, name, is_paid FROM holidays WHERE TO_CHAR(date, 'YYYY-MM') = %s", (month,))
            elif month and len(month) == 4:
                cur.execute("SELECT date, name, is_paid FROM holidays WHERE TO_CHAR(date, 'YYYY') = %s", (month,))
            else:
                cur.execute("SELECT date, name, is_paid FROM holidays ORDER BY date")

            rows = cur.fetchall()
            holidays = [
                {'date': r[0].strftime("%Y-%m-%d") if hasattr(r[0], 'strftime') else str(r[0]), 'name': r[1], 'is_paid': r[2]}
                for r in rows
            ]
            cur.close()
            put_db_connection(conn)
            return jsonify(holidays)

        except Exception as e:
            print(f"Attempt {attempt+1}/{max_attempts} failed: {e}")
            try:
                if cur:
                    cur.close()
                if conn:
                    put_db_connection(conn)
            except:
                pass

    # After retries fail, return empty list so frontend doesn't error.
    return jsonify([])
@app.route("/holidays-count")
def holidays_count():
    month = request.args.get("month")  # "YYYY-MM"
    if not month or len(month) != 7:
        return jsonify({"count": 0})
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM holidays WHERE TO_CHAR(date, 'YYYY-MM') = %s", (month,))
    count = cur.fetchone()[0]
    cur.close()
    put_db_connection(conn)
    return jsonify({"count": count})

from flask import request, jsonify, session
from datetime import datetime
from decimal import Decimal
from calendar import monthrange
from psycopg2.extras import RealDictCursor
from db import get_db_connection, put_db_connection
@app.route('/save-attendance-summary', methods=['POST'])
def save_attendance_summary():
    # 1. Check Authorization
    if 'user_id' not in session:
        return jsonify({"message": "Unauthorized"}), 401

    data = request.get_json()
    month = data.get('month')
    summary = data.get('summary', {})
    

    # Extract other summary fields
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
            (user_id, month, total_days, sundays, full_days, half_days, paid_leaves, absent_days, work_days, average_per_day)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            session['user_id'],
            month,
            total_days,
            sundays,
            full_days,
            half_days,
            paid_leaves,
            grace_absents,
            total_working_days,
            average_per_day,
          
        ))
        conn.commit()
        return jsonify({"message": "Summary and Net Payable saved"}), 200
    except Exception as e:
        conn.rollback()
        print(f"Error saving attendance summary: {e}")
        return jsonify({"message": "Error saving summary"}), 500
    finally:
        cur.close()
        put_db_connection(conn)


@app.route('/payroll/auto-generate-slip', methods=['POST'])
def auto_generate_payroll():
    if 'user_id' not in session:
        return jsonify({"message": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    month = data.get('month') or datetime.utcnow().strftime('%Y-%m')
    requested_email = data.get("email")  # optional

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if requested_email and session.get("role") == "chairman":
            cur.execute("SELECT user_id, name, salary FROM users WHERE email = %s", (requested_email,))
            user = cur.fetchone()
            if not user:
                return jsonify({"message": "User not found"}), 404
        else:
            cur.execute("SELECT user_id, name, salary FROM users WHERE user_id = %s", (session["user_id"],))
            user = cur.fetchone()
            if not user:
                return jsonify({"message": "User not found"}), 404

        if user['salary'] is None:
            return jsonify({"message": "Salary info unavailable"}), 400

        user_id = user['user_id']
        name = user['name']
        salary = float(user['salary'])

        cur.execute("""
            SELECT total_days, sundays, full_days, half_days, 
                   paid_leaves, absent_days, work_days
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
            work_days_raw = summary['work_days']
            work_days = float(work_days_raw) if isinstance(work_days_raw, Decimal) else float(work_days_raw)
        else:
            total_days = monthrange(int(month[:4]), int(month[5:]))[1]
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

        return jsonify(payroll_slip), 200

    finally:
        cur.close()
        put_db_connection(conn)

from flask import request, session, jsonify
from werkzeug.security import generate_password_hash
from db import get_db_connection

from urllib.parse import unquote
from flask import request, jsonify, session

from urllib.parse import unquote
from flask import request, jsonify, session

@app.route("/update-user/<email>", methods=["PUT", "POST"])
def update_user(email):
    email = unquote(email)  # Decode URL encoded email
    
    # Only chairman can update user details
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403
    
    data = request.get_json()
    if not data:
        return jsonify({"message": "No input data provided"}), 400
    
    allowed_fields = [
        "name",
        "role",
        "salary",
        "employee_id",
        "location",
        "password",
        "bank_account",
        "dob",
        "doj",
        "pan_no",
        "ifsc_code",
        "department",
        "image"
    ]

    fields = []
    values = []

    for field in allowed_fields:
        if field in data:
            value = data[field]
            # Convert empty string for dates to None
            if field in ("dob", "doj") and value == "":
                value = None

            db_field = "employee_id" if field == "employee_id" else field
            fields.append(f"{db_field} = %s")
            values.append(value)

    if not fields:
        return jsonify({"message": "No valid fields to update"}), 400

    values.append(email)  # for WHERE clause

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
        import traceback
        traceback.print_exc()
        return jsonify({"message": f"Database error: {str(e)}"}), 500

    finally:
        if cur:
            cur.close()
        if conn:
           conn.close()


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
            SELECT total_days, sundays, full_days, half_days,
                paid_leaves, absent_days, work_days, average_per_day, generated_at
            FROM attendance_summaries
            WHERE user_id = %s AND month = %s
            LIMIT 1
        """, (user_id, month))
        summary = cur.fetchone()
        if not summary:
            return jsonify({"message": "No summary found"}), 404

        work_days_val = summary[6]
        average_per_day_val = summary[7]
        work_days = float(work_days_val) if isinstance(work_days_val, Decimal) else work_days_val
        average_per_day = float(average_per_day_val) if isinstance(average_per_day_val, Decimal) else average_per_day_val

        data = {
            "totalDays": summary[0],
            "sundays": summary[1],
            "fullDays": summary[2],
            "halfDays": summary[3],
            "paidLeaves": summary[4],
            "absentDays": summary[5],
            "workDays": work_days,
            "averagePerDay": average_per_day,
            "generatedAt": str(summary[8]),
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
        # Join users and attendance_summaries for the designated month
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
        return send_file(bio, as_attachment=True, download_name=filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    finally:
        cur.close()
        put_db_connection(conn)

@app.route("/update-profile-name", methods=["POST"])
def update_profile_name():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    new_name = request.form.get("name")
    if not new_name:
        return jsonify({"message": "Name required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET name = %s WHERE user_id = %s", (new_name, session["user_id"]))
    conn.commit()
    cur.close()
    put_db_connection(conn)

    return jsonify({"message": "Name updated"}), 200
@app.route("/update-password", methods=["POST"])
def update_password():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    new_password = request.form.get("password")
    if not new_password:
        return jsonify({"message": "Password required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET password = %s WHERE user_id = %s", (new_password, session["user_id"]))
    conn.commit()
    cur.close()
    put_db_connection(conn)

    return jsonify({"message": "Password updated"}), 200


        
# Get approved leaves of type 'Earned' for a particular employee
# ---------------------- RUN ----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
