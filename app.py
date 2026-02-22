from flask import Flask, jsonify, g
from flask_cors import CORS
import sqlite3

DB_PATH = "oil_wells.db"

app = Flask(__name__)
CORS(app)

def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        db = g._db = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_db(_):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()

@app.get("/api/health")
def health():
    return jsonify({"ok": True})

@app.get("/api/wells")
def wells():
    db = get_db()
    rows = db.execute("""
        SELECT id, well_name, api_number, latitude, longitude, county
        FROM wells
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """).fetchall()
    return jsonify([dict(r) for r in rows])

@app.get("/api/wells/<int:well_id>")
def well_detail(well_id: int):
    db = get_db()
    well = db.execute("SELECT * FROM wells WHERE id = ?", (well_id,)).fetchone()
    if well is None:
        return jsonify({"error": "not found"}), 404

    stim = db.execute("""
        SELECT * FROM stimulation_data
        WHERE well_id = ?
        ORDER BY date_stimulated IS NULL, date_stimulated
    """, (well_id,)).fetchall()

    return jsonify({
        "well": dict(well),
        "stimulation": [dict(r) for r in stim],
    })

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)