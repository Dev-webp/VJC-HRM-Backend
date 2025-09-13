import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
# app.py
from flask import Flask, request, session, jsonify, redirect, send_from_directory
from flask_cors import CORS, cross_origin
from datetime import datetime, date
from db import get_db_connection
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
import os
import time

# ---------------------- CONFIG ----------------------
load_dotenv()

# Where files are stored
UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads", "salary_slips")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET") or "fallback-secret-key"

# CORS
CORS(
    app,
    supports_credentials=True,
    origins=[
        "http://localhost:3000",
        "http://hrm.vjcoverseas.com",
        "https://postgres-frontend-attendance.onrender.com",
    ],
)

# Session cookie config (make SECURE False for localhost)
IS_LOCAL = os.getenv("ENV", "local").lower() == "local"
app.config.update(
    SESSION_COOKIE_SAMESITE="None",
    SESSION_COOKIE_SECURE=not IS_LOCAL,  # True on HTTPS, False on localhost
)

# ---------------------- AUTH & SESSION ----------------------
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
    conn.close()

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
        return jsonify(
            {"authenticated": True, "role": session.get("role"), "email": session.get("email")}
        ), 200
    return jsonify({"authenticated": False}), 401


@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return redirect("/")
    return jsonify({"redirect": "chairman" if session["role"] == "chairman" else "employee"})

# ---------------------- PROFILE ----------------------
@app.route("/me")
def me():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, name, email, image FROM users WHERE user_id = %s",
        (session["user_id"],),
    )
    user = cur.fetchone()
    cur.close()
    conn.close()

    if user:
        return jsonify({"id": user[0], "name": user[1], "email": user[2], "image": user[3] or ""})
    return jsonify({"message": "User not found"}), 404


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
    conn.close()
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
        conn.close()

    return "✅ Registered", 200

# ---------------------- ATTENDANCE ----------------------
@app.route("/attendance", methods=["POST"])
def mark_attendance():
    if "user_id" not in session:
        return {"message": "Not logged in"}, 401

    user_id = session["user_id"]
    action = request.form.get("action")
    now = datetime.now().time()
    today = date.today()

    valid_actions = [
        "office_in",
        "break_out",
        "break_in",
        "break_out_2",
        "break_in_2",
        "lunch_out",
        "lunch_in",
        "office_out",
    ]
    if action not in valid_actions:
        return {"message": "Invalid action"}, 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM attendance WHERE user_id = %s AND date = %s", (user_id, today))
        row = cur.fetchone()
        if row:
            cur.execute(
                f"UPDATE attendance SET {action} = %s WHERE user_id = %s AND date = %s",
                (now, user_id, today),
            )
        else:
            cur.execute(
                f"INSERT INTO attendance (user_id, date, {action}) VALUES (%s, %s, %s)",
                (user_id, today, now),
            )
        conn.commit()
        return {"message": f"{action} recorded"}, 200
    except Exception as e:
        conn.rollback()
        return {"message": f"❌ DB Error: {str(e)}"}, 500
    finally:
        cur.close()
        conn.close()


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
            SELECT date, office_in, break_out, break_in, break_out_2, break_in_2, lunch_out, lunch_in, office_out
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
        for r in rows:
            result.append(
                {
                    "date": r[0].strftime("%Y-%m-%d") if r[0] else "",
                    "office_in": r[1].strftime("%H:%M:%S") if r[1] else "",
                    "break_out": r[2].strftime("%H:%M:%S") if r[2] else "",
                    "break_in": r[3].strftime("%H:%M:%S") if r[3] else "",
                    "break_out_2": r[4].strftime("%H:%M:%S") if r[4] else "",
                    "break_in_2": r[5].strftime("%H:%M:%S") if r[5] else "",
                    "lunch_out": r[6].strftime("%H:%M:%S") if r[6] else "",
                    "lunch_in": r[7].strftime("%H:%M:%S") if r[7] else "",
                    "office_out": r[8].strftime("%H:%M:%S") if r[8] else "",
                }
            )
        return jsonify(result)
    finally:
        cur.close()
        conn.close()

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
            conn.close()


@app.route("/my-salary-slips", methods=["GET"])
def my_salary_slips():
    """
    Returns slips for the logged-in user's email.
    Used by your EmployeeDashboard: axios.get('http://localhost:5000/my-salary-slips')
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
        conn.close()


# Serve uploaded files (so links work in the frontend list)
@app.route("/files/salary_slips/<path:filename>")
def serve_salary_slip(filename):
    return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=False)

# ---------------------- LEAVE MANAGEMENT ----------------------
@app.route("/apply-leave", methods=["POST"])
def apply_leave():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    data = request.get_json()
    user_id = session["user_id"]
    leave_type = data.get("leave_type")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    reason = data.get("reason")

    if not all([leave_type, start_date, end_date, reason]):
        return jsonify({"message": "Missing fields"}), 400

    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        if start_dt > end_dt:
            return jsonify({"message": "Start date cannot be after end date"}), 400
    except ValueError:
        return jsonify({"message": "Invalid date format"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO leave_requests (user_id, leave_type, start_date, end_date, reason)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (user_id, leave_type, start_date, end_date, reason),
        )
        conn.commit()
        return jsonify({"message": "Leave request submitted"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/my-leave-requests")
def my_leave_requests():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, leave_type, start_date, end_date, reason, status, chairman_remarks
        FROM leave_requests
        WHERE user_id = %s
        ORDER BY start_date DESC
        """,
        (session["user_id"],),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify(
        [
            {
                "id": r[0],
                "leave_type": r[1],
                "start_date": r[2].strftime("%Y-%m-%d"),
                "end_date": r[3].strftime("%Y-%m-%d"),
                "reason": r[4] or "",
                "status": r[5],
                "chairman_remarks": r[6] or "",
            }
            for r in rows
        ]
    )


@app.route("/delete-leave-request/<int:leave_id>", methods=["DELETE"])
def delete_leave_request(leave_id):
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM leave_requests WHERE id = %s", (leave_id,))
        conn.commit()
        return jsonify({"message": "Leave request deleted"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Deletion error: {e}"}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/all-leave-requests")
def all_leave_requests():
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 
            lr.id, u.user_id, u.name, u.email,
            lr.leave_type, lr.start_date, lr.end_date,
            lr.reason, lr.status, lr.chairman_remarks
        FROM leave_requests lr
        JOIN users u ON lr.user_id = u.user_id
        ORDER BY lr.created_at DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify(
        [
            {
                "id": row[0],
                "employee_id": row[1],
                "employee_name": row[2],
                "employee_email": row[3],
                "leave_type": row[4],
                "start_date": row[5].strftime("%Y-%m-%d"),
                "end_date": row[6].strftime("%Y-%m-%d"),
                "reason": row[7] or "",
                "status": row[8],
                "chairman_remarks": row[9] or "",
            }
            for row in rows
        ]
    )


@app.route("/leave-action", methods=["POST"])
def leave_action():
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403

    data = request.get_json()
    leave_id = data.get("id")
    action = data.get("action")
    remarks = data.get("remarks", "")

    if not leave_id or action not in ["approve", "reject"]:
        return jsonify({"message": "Invalid request"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM leave_requests WHERE id = %s", (leave_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"message": "Leave request not found"}), 404
        if row[0].lower() != "pending":
            return jsonify({"message": "Leave request already processed"}), 400

        new_status = "Approved" if action == "approve" else "Rejected"
        cur.execute(
            """
            UPDATE leave_requests
            SET status = %s, chairman_remarks = %s
            WHERE id = %s
            """,
            (new_status, remarks, leave_id),
        )
        conn.commit()
        return jsonify({"message": f"Leave request {new_status.lower()}"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Server error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()

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

    if not all([name, email, password, role]):
        return jsonify({"message": "Missing fields"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        if cur.fetchone():
            return jsonify({"message": "User already exists"}), 409

        cur.execute(
            """
            INSERT INTO users (name, email, password, role, image)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (name, email, password, role, image),
        )
        conn.commit()
        return jsonify({"message": "✅ User created successfully"}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"❌ DB Error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/delete-user/<path:email>", methods=["DELETE", "OPTIONS"])
@cross_origin(supports_credentials=True)
def delete_user(email):
    if request.method == "OPTIONS":
        return "", 200

    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users WHERE email = %s", (email,))
        if not cur.fetchone():
            return jsonify({"message": "User not found"}), 404

        cur.execute("DELETE FROM users WHERE email = %s", (email,))
        conn.commit()
        return jsonify({"message": f"User {email} deleted successfully"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"message": f"Error deleting user: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/dashboard-data")
def dashboard_data():
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.name, u.email, a.date, a.office_in, a.break_out, a.break_in, a.lunch_out, a.lunch_in, a.office_out
        FROM attendance a
        JOIN users u ON u.user_id = a.user_id
        ORDER BY a.date DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify(
        [
            {
                "name": r[0],
                "email": r[1],
                "date": r[2].strftime("%Y-%m-%d"),
                "office_in": str(r[3]) if r[3] else "",
                "break_out": str(r[4]) if r[4] else "",
                "break_in": str(r[5]) if r[5] else "",
                "lunch_out": str(r[6]) if r[6] else "",
                "lunch_in": str(r[7]) if r[7] else "",
                "office_out": str(r[8]) if r[8] else "",
            }
            for r in rows
        ]
    )

# ---------------------- RUN ----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
