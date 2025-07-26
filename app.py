from flask import Flask, request, session, jsonify
from flask_cors import CORS
from datetime import datetime, date
from db import get_db_connection
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET") or "fallback-secret-key"

# ✅ Session cookie config for cross-origin requests
app.config.update(
    SESSION_COOKIE_SAMESITE="None",
    SESSION_COOKIE_SECURE=True
)

# ✅ Apply CORS to all routes, with frontend origin and credentials support

CORS(app, resources={r"/*": {"origins": "https://postgres-frontend-attendance.onrender.com"}}, supports_credentials=True)

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return "✅ Backend running. Use POST to login."

    email = request.form.get("email")
    password = request.form.get("password")
    print("Login attempt:", email, password)  # <- for Render logs


@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    return jsonify({
        "redirect": "chairman" if session["role"] == "chairman" else "employee"
    })


@app.route("/register", methods=["POST"])
def register():
    name = request.form.get("name")
    email = request.form.get("email")
    password = request.form.get("password")

    if not email.endswith("@vjcoverseas.com"):
        return jsonify({"message": "❌ Only company emails allowed"}), 400

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
        return jsonify({"message": f"❌ Error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()

    return jsonify({"message": "✅ Registered"}), 200


@app.route("/attendance", methods=["POST"])
def mark_attendance():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    user_id = session["user_id"]
    action = request.form.get("action")
    now = datetime.now().time()
    today = date.today()

    valid_actions = ["office_in", "break_out", "break_in", "lunch_out", "lunch_in", "office_out"]
    if action not in valid_actions:
        return jsonify({"message": "Invalid action"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM attendance WHERE user_id = %s AND date = %s", (user_id, today))
        row = cur.fetchone()

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
        return jsonify({"message": f"❌ DB Error: {str(e)}"}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/dashboard-data")
def dashboard_data():
    if session.get("role") != "chairman":
        return jsonify({"message": "Access denied"}), 403

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.name, u.email, a.date, a.office_in, a.break_out, a.break_in, a.lunch_out, a.lunch_in, a.office_out
        FROM attendance a
        JOIN users u ON u.user_id = a.user_id
        ORDER BY a.date DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = []
    for row in rows:
        result.append({
            "name": row[0],
            "email": row[1],
            "date": row[2].strftime("%Y-%m-%d"),
            "office_in": str(row[3]) if row[3] else "",
            "break_out": str(row[4]) if row[4] else "",
            "break_in": str(row[5]) if row[5] else "",
            "lunch_out": str(row[6]) if row[6] else "",
            "lunch_in": str(row[7]) if row[7] else "",
            "office_out": str(row[8]) if row[8] else "",
        })
    return jsonify(result)


@app.route("/me")
def me():
    if "user_id" not in session:
        return jsonify({"message": "Not logged in"}), 401

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, name, email, image FROM users WHERE user_id = %s", (session["user_id"],))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if user:
        return jsonify({
            "id": user[0],
            "name": user[1],
            "email": user[2],
            "image": user[3] or ""
        })
    else:
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


@app.route("/logout")
def logout():
    session.clear()
    return jsonify({"message": "Logged out"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
