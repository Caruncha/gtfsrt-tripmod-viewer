# GTFS-rt TripModifications • Validator & Detour Viewer

Une app Streamlit pour valider des **TripModifications** GTFS‑realtime et **visualiser les détours** (segments remplacés, arrêts de remplacement) sur carte, en croisant avec un **GTFS statique**.

## Lancer localement

python -m venv .venv && source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
streamlit run streamlit_app.py
