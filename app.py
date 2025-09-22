# app.py
import os
import re
import sqlite3
import hashlib
import json
import random
import calendar
from datetime import date, datetime
from flask import Flask, request, jsonify, render_template, g, session

DB = "routelink.db"
HOL_JSON = "academic_holidays.json"
HOL_CSV = "academic_holidays.csv"

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.environ.get("ROUTELINK_SECRET", "dev-secret-change-me")  # change in production
app.config['JSON_SORT_KEYS'] = False

# ---------------- DB helpers ----------------
def ensure_column(table: str, column: str, col_type: str, default: str = None):
    conn = None
    try:
        conn = sqlite3.connect(DB)
        c = conn.cursor()
        c.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in c.fetchall()]
        if column not in cols:
            sql = f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"
            if default is not None:
                sql += f" DEFAULT {default}"
            c.execute(sql)
            conn.commit()
    except Exception:
        # best effort
        pass
    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass

def init_db():
    """Create/upgrade database schema (idempotent)."""
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    # users
    c.execute("""CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    email TEXT UNIQUE,
                    password_hash TEXT
                 )""")

    # routes
    c.execute("""CREATE TABLE IF NOT EXISTS routes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_no TEXT,
                    end_point TEXT,
                    major_stops TEXT,
                    time TEXT,
                    transport_type TEXT,
                    no_of_people INTEGER DEFAULT 0
                 )""")

    # links (link entries created when a person joins a route). We'll add user_id to map to users.
    c.execute("""CREATE TABLE IF NOT EXISTS links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER DEFAULT NULL,
                    name TEXT,
                    drop_point TEXT,
                    phone TEXT,
                    course_year TEXT,
                    branch TEXT
                 )""")

    # calendar (many-to-many date <-> route <-> link record)
    c.execute("""CREATE TABLE IF NOT EXISTS calendar (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    travel_date TEXT,
                    route_id INTEGER,
                    link_id INTEGER,
                    FOREIGN KEY(route_id) REFERENCES routes(id),
                    FOREIGN KEY(link_id) REFERENCES links(id)
                 )""")

    # Conversations (groups or 1:1), members and messages
    c.execute("""CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    is_group INTEGER DEFAULT 0,
                    route_id INTEGER DEFAULT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                 )""")
    c.execute("""CREATE TABLE IF NOT EXISTS conversation_members (
                    conversation_id INTEGER,
                    user_id INTEGER,
                    PRIMARY KEY(conversation_id, user_id),
                    FOREIGN KEY(conversation_id) REFERENCES conversations(id),
                    FOREIGN KEY(user_id) REFERENCES users(id)
                 )""")
    c.execute("""CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER,
                    sender_id INTEGER,
                    text TEXT,
                    ts TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY(conversation_id) REFERENCES conversations(id),
                    FOREIGN KEY(sender_id) REFERENCES users(id)
                 )""")

    conn.commit()
    # WAL for better concurrency
    try:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()
    except Exception:
        pass
    conn.close()

    # ensure optional columns exist for compatibility
    ensure_column("users", "gender", "TEXT")
    ensure_column("links", "gender", "TEXT")
    ensure_column("links", "user_id", "INTEGER", default="NULL")

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB, timeout=30, check_same_thread=False)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop('db', None)
    if db:
        try:
            db.close()
        except Exception:
            pass

def hash_pw(txt: str) -> str:
    return hashlib.sha256(txt.encode()).hexdigest()

def to_base36(n: int) -> str:
    digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if n == 0: return "0"
    out=[]
    while n:
        n, rem = divmod(n, 36)
        out.append(digits[rem])
    return "".join(reversed(out))

def generate_next_slot_no():
    try:
        conn = sqlite3.connect(DB)
        c = conn.cursor()
        c.execute("SELECT MAX(id) FROM routes")
        r = c.fetchone()
        conn.close()
        max_id = int(r[0]) if (r and r[0]) else 0
        seq = max_id + 1
    except Exception:
        seq = 1
    b36 = to_base36(seq).rjust(4, "0")
    return f"SL{b36}"

# ---------------- Holidays loader ----------------
def load_academic_holidays():
    if os.path.exists(HOL_JSON):
        try:
            with open(HOL_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            holidays=[]
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, str): holidays.append(item)
                    elif isinstance(item, dict) and "date" in item: holidays.append(item["date"])
            return sorted(set(holidays))
        except Exception:
            pass
    # fallback sample
    return generate_sample_holidays(date.today().year)

def generate_sample_holidays(year: int, seed: int = 123):
    random.seed(seed + year)
    fixed = [(year,1,26),(year,5,1),(year,8,15),(year,10,2),(year,12,25)]
    holidays=set()
    for y,m,d in fixed:
        try: holidays.add(date(y,m,d).isoformat())
        except Exception: pass
    while len(holidays) < 8:
        m=random.randint(1,12)
        d=random.randint(1,calendar.monthrange(year,m)[1])
        holidays.add(date(year,m,d).isoformat())
    return sorted(holidays)

# ---------------- Auth helper ----------------
def login_required(f):
    from functools import wraps
    @wraps(f)
    def wrapped(*args, **kwargs):
        if session.get("user_id") is None:
            return jsonify({"error":"Login required"}), 401
        return f(*args, **kwargs)
    return wrapped

# ---------------- Utility for messaging ----------------
def is_member(conn, convo_id, user_id):
    c = conn.cursor()
    c.execute("SELECT 1 FROM conversation_members WHERE conversation_id=? AND user_id=? LIMIT 1", (convo_id, user_id))
    return c.fetchone() is not None

# ---------------- HTTP API ----------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/me")
def api_me():
    if session.get("user_id"):
        return jsonify({"id": session.get("user_id"), "name": session.get("user_name")})
    return jsonify({"id": None})

@app.route("/holidays")
def api_holidays():
    return jsonify(load_academic_holidays())

@app.route("/next_slot")
def api_next_slot():
    return jsonify({"slot": generate_next_slot_no()})

@app.route("/calendar/<iso_date>")
def api_calendar_for_date(iso_date):
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("""
            SELECT DISTINCT r.id, r.slot_no, r.end_point, r.major_stops, r.time, r.transport_type
            FROM calendar cal LEFT JOIN routes r ON cal.route_id = r.id
            WHERE cal.travel_date = ? ORDER BY r.id DESC
        """, (iso_date,))
        rows = c.fetchall()
        out=[]
        for r in rows:
            out.append({k: r[k] for k in r.keys()})
        return jsonify(out)
    except Exception:
        return jsonify([]), 500

@app.route("/route_count")
def api_route_count():
    iso = request.args.get("date"); rid = request.args.get("route_id")
    if not iso or not rid: return jsonify({"count":0})
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM calendar WHERE travel_date=? AND route_id=? AND link_id IS NOT NULL", (iso,rid))
        r = c.fetchone()
        return jsonify({"count": int(r[0]) if r else 0})
    except Exception:
        return jsonify({"count":0})

@app.route("/routes", methods=["POST"])
@login_required
def api_create_route():
    data = request.get_json(force=True)
    d = data.get("date"); slot = data.get("slot_no"); endp = data.get("end_point")
    stops = data.get("major_stops"); ttime = data.get("time"); ttype = data.get("transport_type")
    if not all([d, slot, endp]):
        return "Missing required fields", 400
    try:
        sel = datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return "Invalid date", 400
    if sel < date.today():
        return "Cannot create route for past dates", 400
    if ttime:
        try: datetime.strptime(ttime, "%H:%M")
        except Exception: return "Invalid time", 400
    # duplicate check for same date
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("""
            SELECT r.id FROM routes r JOIN calendar cal ON cal.route_id = r.id
            WHERE cal.travel_date = ? AND LOWER(r.end_point)=LOWER(?) AND COALESCE(r.time,'')=? AND LOWER(COALESCE(r.transport_type,''))=LOWER(?)
            LIMIT 1
        """, (d, endp, ttime or "", ttype or ""))
        if c.fetchone(): return "Duplicate route", 409
    except Exception:
        pass
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("INSERT INTO routes (slot_no, end_point, major_stops, time, transport_type, no_of_people) VALUES (?, ?, ?, ?, ?, ?)",
                  (slot, endp, stops, ttime, ttype, 0))
        rid = c.lastrowid
        c.execute("INSERT INTO calendar (travel_date, route_id, link_id) VALUES (?, ?, NULL)", (d, rid))
        conn.commit()
        return jsonify({"route_id": rid}), 201
    except Exception as e:
        return str(e), 500

@app.route("/routes/<int:rid>/links", methods=["GET"])
@login_required
def api_routes_links(rid):
    # expects query param date=YYYY-MM-DD
    iso = request.args.get("date")
    if not iso: return jsonify([])
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("""
            SELECT l.id, l.user_id, l.name, l.gender, l.drop_point, l.phone, l.course_year, l.branch
            FROM links l JOIN calendar cal ON cal.link_id = l.id
            WHERE cal.route_id = ? AND cal.travel_date = ?
            ORDER BY l.id DESC
        """, (rid, iso))
        rows = c.fetchall()
        out=[{k:r[k] for k in r.keys()} for r in rows]
        return jsonify(out)
    except Exception:
        return jsonify([]), 500

@app.route("/routes/<int:rid>/join", methods=["POST"])
@login_required
def api_join_route(rid):
    data = request.get_json(force=True)
    d = data.get("date"); name = data.get("name"); gender = (data.get("gender") or "").upper()
    drop = data.get("drop"); phone = data.get("phone"); year = data.get("course_year"); branch = data.get("branch")
    if not all([d, name, gender, drop, phone, year, branch]): return "Missing fields", 400
    if gender not in ("M","F"): return "Invalid gender", 400
    if not phone.isdigit() or len(phone)<7: return "Invalid phone", 400
    try:
        sel = datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return "Invalid date", 400
    if sel < date.today(): return "Cannot join for past dates", 400
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT end_point FROM routes WHERE id=?", (rid,))
    rr = c.fetchone()
    if rr and rr["end_point"] and rr["end_point"].strip().lower() != drop.strip().lower():
        return f"Drop must match route endpoint '{rr['end_point']}'", 400
    # duplicate check (phone)
    c.execute("SELECT l.id FROM links l JOIN calendar cal ON cal.link_id = l.id WHERE cal.travel_date=? AND cal.route_id=? AND l.phone=?", (d, rid, phone))
    if c.fetchone(): return "Already joined", 409

    current_user_id = session.get("user_id")
    try:
        try:
            c.execute("INSERT INTO links (user_id, name, gender, drop_point, phone, course_year, branch) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (current_user_id, name, gender, drop, phone, year, branch))
        except Exception:
            # fallback if gender column absent or other schema issue
            c.execute("INSERT INTO links (user_id, name, drop_point, phone, course_year, branch) VALUES (?, ?, ?, ?, ?, ?)",
                      (current_user_id, name, drop, phone, year, branch))
        lid = c.lastrowid
        c.execute("INSERT INTO calendar (travel_date, route_id, link_id) VALUES (?, ?, ?)", (d, rid, lid))
        conn.commit()
        return jsonify({"link_id": lid}), 201
    except Exception as e:
        return str(e), 500

@app.route("/links", methods=["GET"])
@login_required
def api_links():
    gender = request.args.get("gender")
    try:
        conn = get_db(); c = conn.cursor()
        if gender and gender.upper() in ("M","F"):
            c.execute("SELECT id, user_id, name, gender, drop_point, phone, course_year, branch FROM links WHERE UPPER(gender)=? ORDER BY id DESC", (gender.upper(),))
        else:
            c.execute("SELECT id, user_id, name, gender, drop_point, phone, course_year, branch FROM links ORDER BY id DESC")
        rows = c.fetchall()
        return jsonify([{k:r[k] for k in r.keys()} for r in rows])
    except Exception:
        return jsonify([])

@app.route("/links/<int:lid>", methods=["DELETE","PUT","PATCH"])
@login_required
def api_links_modify(lid):
    if request.method == "DELETE":
        try:
            conn = get_db(); c = conn.cursor()
            c.execute("DELETE FROM calendar WHERE link_id=?", (lid,))
            c.execute("DELETE FROM links WHERE id=?", (lid,))
            conn.commit()
            return jsonify({"ok": True})
        except Exception as e:
            return str(e), 500
    else:
        data = request.get_json(force=True)
        allowed = {"name":"name","gender":"gender","drop":"drop_point","phone":"phone","course_year":"course_year","branch":"branch"}
        updates = {}
        for k,v in allowed.items():
            if k in data: updates[v] = data[k]
        if not updates: return "No fields", 400
        set_sql = ", ".join([f"{k}=?" for k in updates.keys()])
        vals = list(updates.values()); vals.append(lid)
        try:
            conn = get_db(); c = conn.cursor()
            c.execute(f"UPDATE links SET {set_sql} WHERE id=?", vals)
            conn.commit()
            return jsonify({"ok": True})
        except Exception as e:
            return str(e), 500

@app.route("/routes/<int:rid>", methods=["PUT","PATCH","DELETE"])
@login_required
def api_routes_modify(rid):
    if request.method == "DELETE":
        try:
            conn = get_db(); c = conn.cursor()
            c.execute("DELETE FROM calendar WHERE route_id=?", (rid,))
            c.execute("DELETE FROM routes WHERE id=?", (rid,))
            conn.commit()
            return jsonify({"ok": True})
        except Exception as e:
            return str(e), 500
    else:
        data = request.get_json(force=True)
        allowed = ["slot_no","end_point","major_stops","time","transport_type"]
        updates = {k: data[k] for k in allowed if k in data}
        if not updates: return "No fields", 400
        set_sql = ", ".join([f"{k}=?" for k in updates.keys()])
        vals = list(updates.values()); vals.append(rid)
        try:
            conn = get_db(); c = conn.cursor()
            c.execute(f"UPDATE routes SET {set_sql} WHERE id=?", vals)
            conn.commit()
            return jsonify({"ok": True})
        except Exception as e:
            return str(e), 500

# Register / Login
@app.route("/register", methods=["POST"])
def api_register():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    pw = data.get("password") or ""
    gender = (data.get("gender") or "").strip().upper()
    if not name or not email or not pw or gender not in ("M","F"):
        return jsonify({"error":"Missing fields"}), 400
    if not re.match(r"^[A-Za-z0-9._%+-]+@vitstudent\.ac\.in$", email):
        return jsonify({"error":"Use a VIT email"}), 400
    try:
        conn = get_db(); c = conn.cursor()
        try:
            c.execute("INSERT INTO users (name, email, password_hash, gender) VALUES (?, ?, ?, ?)", (name, email, hash_pw(pw), gender))
        except Exception:
            c.execute("INSERT INTO users (name, email, password_hash) VALUES (?, ?, ?)", (name, email, hash_pw(pw)))
        conn.commit()
        return jsonify({"ok": True})
    except sqlite3.IntegrityError:
        return jsonify({"error":"Email exists"}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/login", methods=["POST"])
def api_login():
    data = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()
    pw = data.get("password") or ""
    if not email or not pw: return jsonify({"error":"Missing fields"}), 400
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT id, name FROM users WHERE email=? AND password_hash=?", (email, hash_pw(pw)))
    r = c.fetchone()
    if r:
        session["user_id"] = r["id"]
        session["user_name"] = r["name"]
        return jsonify({"ok": True, "name": r["name"]})
    return jsonify({"error":"Invalid credentials"}), 401

@app.route("/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})

# ---------------- Messaging endpoints ----------------

@app.route("/conversations", methods=["GET"])
@login_required
def api_conversations():
    uid = session["user_id"]
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("""
            SELECT conv.id, conv.title, conv.is_group, conv.route_id,
                   (SELECT text FROM messages WHERE conversation_id=conv.id ORDER BY id DESC LIMIT 1) AS last_message,
                   (SELECT ts FROM messages WHERE conversation_id=conv.id ORDER BY id DESC LIMIT 1) AS last_ts
            FROM conversations conv
            JOIN conversation_members mem ON mem.conversation_id = conv.id
            WHERE mem.user_id = ?
            ORDER BY COALESCE(last_ts, conv.created_at) DESC
        """, (uid,))
        rows = c.fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r["id"],
                "title": r["title"],
                "is_group": bool(r["is_group"]),
                "route_id": r["route_id"],
                "last_message": r["last_message"]
            })
        return jsonify(out)
    except Exception as e:
        return jsonify([]), 500

@app.route("/conversations/ensure_dm/<int:peer_id>", methods=["POST"])
@login_required
def api_ensure_dm(peer_id):
    uid = session["user_id"]
    if peer_id == uid:
        return "Cannot DM yourself", 400
    try:
        conn = get_db(); c = conn.cursor()
        # ensure both users exist
        c.execute("SELECT id FROM users WHERE id=?", (peer_id,))
        if not c.fetchone(): return "Peer user not found", 404
        # Find existing 1:1 conversation
        c.execute("""
            SELECT conv.id FROM conversations conv
            JOIN conversation_members m1 ON m1.conversation_id=conv.id
            JOIN conversation_members m2 ON m2.conversation_id=conv.id
            WHERE conv.is_group=0 AND m1.user_id=? AND m2.user_id=? 
            GROUP BY conv.id
            HAVING COUNT(*)=2
            LIMIT 1
        """, (uid, peer_id))
        row = c.fetchone()
        if row:
            return jsonify({"conversation_id": row["id"]})
        # else create new conversation
        c.execute("INSERT INTO conversations (title,is_group) VALUES (?,0)", (None,))
        conv_id = c.lastrowid
        c.execute("INSERT INTO conversation_members (conversation_id,user_id) VALUES (?,?)", (conv_id, uid))
        c.execute("INSERT INTO conversation_members (conversation_id,user_id) VALUES (?,?)", (conv_id, peer_id))
        conn.commit()
        return jsonify({"conversation_id": conv_id})
    except Exception as e:
        return str(e), 500

@app.route("/conversations/ensure_group_for_route/<int:route_id>", methods=["POST"])
@login_required
def api_ensure_group_for_route(route_id):
    uid = session["user_id"]
    try:
        conn = get_db(); c = conn.cursor()
        # Ensure route exists
        c.execute("SELECT id, slot_no, end_point FROM routes WHERE id=?", (route_id,))
        rr = c.fetchone()
        if not rr: return "Route not found", 404
        # find existing group with this route_id
        c.execute("SELECT id FROM conversations WHERE is_group=1 AND route_id=? LIMIT 1", (route_id,))
        row = c.fetchone()
        if row:
            conv_id = row[0]
        else:
            title = f"Group: {rr['slot_no']} → {rr['end_point'] or ''}"
            c.execute("INSERT INTO conversations (title,is_group,route_id) VALUES (?,?,?)", (title, 1, route_id))
            conv_id = c.lastrowid
            # Add all members that are linked to that route/date (distinct user_id values from links for any date in calendar for this route)
            c.execute("""
                SELECT DISTINCT l.user_id FROM calendar cal
                JOIN links l ON l.id = cal.link_id
                WHERE cal.route_id = ? AND l.user_id IS NOT NULL
            """, (route_id,))
            rows = c.fetchall()
            member_ids = set([r[0] for r in rows if r[0]])
            member_ids.add(uid)  # ensure caller is member
            for mid in member_ids:
                try:
                    c.execute("INSERT OR IGNORE INTO conversation_members (conversation_id,user_id) VALUES (?,?)", (conv_id, mid))
                except Exception:
                    pass
            conn.commit()
        return jsonify({"conversation_id": conv_id})
    except Exception as e:
        return str(e), 500

@app.route("/conversations/<int:conv_id>/messages", methods=["GET","POST"])
@login_required
def api_conversation_messages(conv_id):
    uid = session["user_id"]
    conn = get_db(); c = conn.cursor()
    # check membership
    if not is_member(conn, conv_id, uid):
        return jsonify({"error":"Not a member"}), 403
    if request.method == "GET":
        c.execute("SELECT m.id, m.sender_id, u.name as sender_name, m.text, m.ts FROM messages m LEFT JOIN users u ON u.id=m.sender_id WHERE m.conversation_id=? ORDER BY m.id ASC", (conv_id,))
        rows = c.fetchall()
        out = [{"id":r["id"], "sender_id": r["sender_id"], "sender_name": r["sender_name"], "text": r["text"], "ts": r["ts"]} for r in rows]
        return jsonify(out)
    else:
        data = request.get_json(force=True)
        text = (data.get("text") or "").strip()
        if not text:
            return "Empty message", 400
        try:
            c.execute("INSERT INTO messages (conversation_id, sender_id, text) VALUES (?,?,?)", (conv_id, uid, text))
            conn.commit()
            return jsonify({"ok": True}), 201
        except Exception as e:
            return str(e), 500

# ---------------- Run ----------------
if __name__ == "__main__":
    init_db()
    print("Starting app on http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
