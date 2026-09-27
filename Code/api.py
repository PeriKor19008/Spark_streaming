"""
Bonus — Απλό REST API που διαβάζει τα επεξεργασμένα δεδομένα από τη MongoDB
και εμφανίζει στατιστικά κυκλοφορίας.

Εκτέλεση:
    uvicorn api:app --reload --port 8000

Endpoints:
    GET /health                  έλεγχος σύνδεσης και πλήθος documents
    GET /links                   μέση/μέγιστη/ελάχιστη ταχύτητα ανά ακμή (συνολικά)
    GET /links/{link}            χρονοσειρά vcount/vspeed για μία ακμή
    GET /windows                 windowed aggregations των 30 δευτερολέπτων
    GET /congestion              οι πιο επιβαρυμένες ακμές (χαμηλότερη μέση ταχύτητα)
"""

import os

from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "traffic")

app = FastAPI(title="UXSIM Traffic Stats API", version="1.0")
db = MongoClient(MONGO_URI)[MONGO_DB]


@app.get("/health")
def health():
    return {
        "mongo": MONGO_URI,
        "database": MONGO_DB,
        "raw_data": db.raw_data.count_documents({}),
        "stats": db.get_collection("stats").count_documents({}),
        "window_stats": db.window_stats.count_documents({}),
    }


@app.get("/links")
def links():
    """Συγκεντρωτικά στατιστικά ανά ακμή για όλο το διάστημα της εξομοίωσης."""
    pipeline = [
        {"$group": {
            "_id": "$link",
            "avg_speed": {"$avg": "$vspeed"},
            "max_speed": {"$max": "$vmax"},
            "min_speed": {"$min": "$vmin"},
            "total_vehicles": {"$sum": "$vcount"},
            "samples": {"$sum": 1},
        }},
        {"$project": {"_id": 0, "link": "$_id", "avg_speed": 1, "max_speed": 1,
                      "min_speed": 1, "total_vehicles": 1, "samples": 1}},
        {"$sort": {"link": 1}},
    ]
    return list(db.get_collection("stats").aggregate(pipeline))


@app.get("/links/{link}")
def link_timeseries(link: str, limit: int = Query(200, ge=1, le=5000)):
    """Χρονοσειρά των μετρήσεων για μία συγκεκριμένη ακμή."""
    cursor = (
        db.get_collection("stats")
        .find({"link": link}, {"_id": 0})
        .sort("time", 1)
        .limit(limit)
    )
    rows = list(cursor)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Δεν βρέθηκαν δεδομένα για την ακμή '{link}'")
    return {"link": link, "points": len(rows), "series": rows}


@app.get("/windows")
def windows(link: str | None = None, limit: int = Query(200, ge=1, le=5000)):
    """Windowed aggregations των 30s, προαιρετικά φιλτραρισμένα ανά ακμή."""
    query = {"link": link} if link else {}
    cursor = (
        db.window_stats.find(query, {"_id": 0})
        .sort([("window_start", 1), ("link", 1)])
        .limit(limit)
    )
    return list(cursor)


@app.get("/congestion")
def congestion(top: int = Query(5, ge=1, le=50)):
    """Οι ακμές με τη χαμηλότερη μέση ταχύτητα — δείκτης συμφόρησης."""
    pipeline = [
        {"$group": {"_id": "$link",
                    "avg_speed": {"$avg": "$vspeed"},
                    "total_vehicles": {"$sum": "$vcount"}}},
        {"$project": {"_id": 0, "link": "$_id", "avg_speed": 1, "total_vehicles": 1}},
        {"$sort": {"avg_speed": 1}},
        {"$limit": top},
    ]
    return list(db.get_collection("stats").aggregate(pipeline))
