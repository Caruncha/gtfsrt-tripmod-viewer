# -*- coding: utf-8 -*-
import io
import gzip
import zipfile
import streamlit as st
import pandas as pd
import pydeck as pdk
from google.protobuf.message import DecodeError
from google.protobuf import text_format

# 1) Binding GTFS-rt (installé depuis MobilityData via requirements.txt)
#    => inclut les champs expérimentaux (trip_modifications, shape, stop)
from google.transit import gtfs_realtime_pb2 as pb

# ---------- Utilitaires GTFS ----------
def load_gtfs_zip(gtfs_bytes: bytes) -> dict[str, pd.DataFrame]:
    zf = zipfile.ZipFile(io.BytesIO(gtfs_bytes))
    dfs = {}
    for name in zf.namelist():
        if name.lower().endswith(".txt"):
            with zf.open(name) as f:
                dfs[name.split("/")[-1].lower()] = pd.read_csv(f, dtype=str).fillna("")
    return dfs

def build_trip_shape(dfs: dict[str, pd.DataFrame], trip_id: str) -> list[tuple[float,float]]:
    trips = dfs.get("trips.txt"); stimes = dfs.get("stop_times.txt")
    stops = dfs.get("stops.txt"); shapes = dfs.get("shapes.txt")
    pts: list[tuple[float,float]] = []

    if trips is not None and shapes is not None and "shape_id" in trips.columns:
        trow = trips.loc[trips["trip_id"] == trip_id]
        if not trow.empty:
            shape_id = trow.iloc[0].get("shape_id", "")
            if shape_id and shape_id in shapes["shape_id"].values:
                shp = shapes.loc[shapes["shape_id"] == shape_id].copy()
                if "shape_pt_sequence" in shp.columns:
                    shp["shape_pt_sequence"] = pd.to_numeric(shp["shape_pt_sequence"], errors="coerce")
                    shp = shp.sort_values("shape_pt_sequence")
                for _, r in shp.iterrows():
                    try:
                        pts.append((float(r["shape_pt_lat"]), float(r["shape_pt_lon"])))
                    except Exception:
                        pass
                if pts:
                    return pts

    if stimes is not None and stops is not None:
        s = stimes.loc[stimes["trip_id"] == trip_id].copy()
        if s.empty:
            return pts
        s["stop_sequence"] = pd.to_numeric(s["stop_sequence"], errors="coerce")
        s = s.sort_values("stop_sequence")
        m = stops.set_index("stop_id")[["stop_lat","stop_lon"]].to_dict("index")
        for _, r in s.iterrows():
            sid = r["stop_id"]
            if sid in m:
                try:
                    pts.append((float(m[sid]["stop_lat"]), float(m[sid]["stop_lon"])))
                except Exception:
                    pass
    return pts

def decode_polyline(encoded: str) -> list[tuple[float,float]]:
    coords = []; index = 0; lat = 0; lng = 0
    while index < len(encoded):
        shift = result = 0
        while True:
            b = ord(encoded[index]) - 63; index += 1
            result |= (b & 0x1f) << shift; shift += 5
            if b < 0x20: break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1); lat += dlat
        shift = result = 0
        while True:
            b = ord(encoded[index]) - 63; index += 1
            result |= (b & 0x1f) << shift; shift += 5
            if b < 0x20: break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1); lng += dlng
        coords.append((lat / 1e5, lng / 1e5))
    return coords

# ---------- Parsing robuste du .pb ----------
def parse_tripmod_feed(raw: bytes) -> tuple[pb.FeedMessage, dict]:
    """
    Essaie successivement :
      1) binaire FeedMessage (avec détection gzip)
      2) textproto FeedMessage
      3) binaire TripModifications seul (on l’enveloppe dans un FeedMessage)
      4) textproto TripModifications seul
    Retourne (feed, meta) où meta décrit le mode de parsing.
    """
    meta = {"gzip": False, "mode": None}

    # GZIP ?
    if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
        raw = gzip.decompress(raw)
        meta["gzip"] = True

    # 1) FeedMessage binaire
    try:
        fm = pb.FeedMessage(); fm.ParseFromString(raw)
        meta["mode"] = "binary:FeedMessage"
        return fm, meta
    except DecodeError:
        pass

    # 2) FeedMessage textproto
    try:
        text = raw.decode("utf-8", errors="strict")
        fm = pb.FeedMessage()
        text_format.Parse(text, fm, allow_unknown_extension=True)
        meta["mode"] = "textproto:FeedMessage"
        return fm, meta
    except Exception:
        pass

    # 3) TripModifications seul (binaire)
    try:
        tm = pb.TripModifications(); tm.ParseFromString(raw)
        fm = pb.FeedMessage()
        ent = pb.FeedEntity(); ent.id = "tm-1"; ent.trip_modifications.CopyFrom(tm)
        fm.entity.extend([ent])
        meta["mode"] = "binary:TripModifications_wrapped"
        return fm, meta
    except Exception:
        pass

    # 4) TripModifications seul (textproto)
    try:
        text = raw.decode("utf-8", errors="strict")
        tm = pb.TripModifications()
        text_format.Parse(text, tm, allow_unknown_extension=True)
        fm = pb.FeedMessage()
        ent = pb.FeedEntity(); ent.id = "tm-1"; ent.trip_modifications.CopyFrom(tm)
        fm.entity.extend([ent])
        meta["mode"] = "textproto:TripModifications_wrapped"
        return fm, meta
    except Exception as e:
        raise DecodeError(f"Impossible de parser ce fichier comme GTFS-rt: {e}")

# ---------- UI ----------
st.set_page_config(page_title="TripModifications • Analyse & Carte", layout="wide")
st.title("GTFS‑rt TripModifications — Analyse & Visualisation")

col1, col2 = st.columns(2)
with col1:
    gtfs_file = st.file_uploader("GTFS statique (.zip)", type=["zip"])
with col2:
    rt_file = st.file_uploader("TripModification (.pb / .pb.gz / .pbtxt)", type=["pb","pbf","bin","pbtxt","txt","textproto","gz"])

if not (gtfs_file and rt_file):
    st.info("Charge un GTFS (.zip) et un TripModification (.pb / .pb.gz / .pbtxt).")
    st.stop()

# Charger GTFS
dfs = load_gtfs_zip(gtfs_file.read())
stops_df = dfs.get("stops.txt")

# Charger .pb avec tolérance
raw = rt_file.read()
try:
    feed, meta = parse_tripmod_feed(raw)
    st.caption(f"Décodage : mode={meta['mode']}, gzip={meta['gzip']}")
except DecodeError as e:
    st.error("Impossible de décoder le fichier fourni (ni FeedMessage, ni TripModifications).")
    st.exception(e)
    st.stop()

# Extraire entités utiles
tripmods = [(e.id, e.trip_modifications) for e in feed.entity if e.HasField("trip_modifications")]
shapes_rt = [e.shape for e in feed.entity if e.HasField("shape") and getattr(e.shape, "encoded_polyline", "")]

if not tripmods:
    st.warning("Aucune entité TripModifications détectée dans les données décodées.")
    st.stop()

sel = st.selectbox("Choisir une TripModifications", [f"{i+1}. {eid}" for i,(eid,_) in enumerate(tripmods)], index=0)
tm_idx = int(sel.split(".")[0]) - 1
eid, tm = tripmods[tm_idx]

# ---------- Analyse ----------
st.subheader("Analyse")
issues: list[str] = []
loc_type = {}
if stops_df is not None:
    if "location_type" not in stops_df.columns:
        stops_df["location_type"] = "0"
    loc_type = stops_df.set_index("stop_id")["location_type"].to_dict()

replacement_stop_ids: list[str] = []
for mod in tm.modifications:
    prev = None
    for rs in mod.replacement_stops:
        sid = rs.stop_id
        replacement_stop_ids.append(sid)
        if loc_type.get(sid, "0") != "0":
            issues.append(f"[Routabilité] '{sid}' n’est pas un arrêt routable (location_type != 0).")
        if prev is not None and rs.travel_time_to_stop < prev:
            issues.append("[Monotonicité] travel_time_to_stop non croissant.")
        prev = rs.travel_time_to_stop

if issues:
    st.error("Problèmes détectés :")
    for m in issues:
        st.write("•", m)
else:
    st.success("Aucun problème détecté (routabilité & monotonicité).")

# ---------- Carte ----------
st.subheader("Carte des détours")

# Trip de référence (pour shape d’origine)
trip_id_for_shape = None
if hasattr(tm, "selected_trips"):
    for sel_tm in tm.selected_trips:
        if hasattr(sel_tm, "trip_id") and sel_tm.trip_id:
            trip_id_for_shape = sel_tm.trip_id
            break

base_line = build_trip_shape(dfs, trip_id_for_shape) if trip_id_for_shape else []

# Détour: privilégier Shape temps réel (encoded polyline)
detour_paths: list[list[tuple[float,float]]] = []
for sh in shapes_rt:
    detour_paths.append(decode_polyline(sh.encoded_polyline))

# Fallback: relier les arrêts temporaires
if not detour_paths and stops_df is not None:
    m = stops_df.set_index("stop_id")[["stop_lat","stop_lon"]].to_dict("index")
    path = []
    for sid in replacement_stop_ids:
        if sid in m:
            try:
                path.append((float(m[sid]["stop_lat"]), float(m[sid]["stop_lon"])))
            except Exception:
                pass
    if len(path) >= 2:
        detour_paths.append(path)

layers: list[pdk.Layer] = []
if base_line:
    layers.append(
        pdk.Layer(
            "PathLayer",
            data=[{"path": [{"lat": lat, "lon": lon} for lat, lon in base_line]}],
            get_path="path",
            get_color=[128,128,128],
            width_min_pixels=2,
        )
    )
for path in detour_paths:
    layers.append(
        pdk.Layer(
            "PathLayer",
            data=[{"path": [{"lat": lat, "lon": lon} for lat, lon in path]}],
            get_path="path",
            get_color=[255,140,0],
            width_min_pixels=3,
        )
    )

rep_points = []
if stops_df is not None:
    m = stops_df.set_index("stop_id")[["stop_lat","stop_lon"]].to_dict("index")
    for sid in replacement_stop_ids:
        if sid in m:
            try:
                rep_points.append({"lat": float(m[sid]["stop_lat"]),
                                   "lon": float(m[sid]["stop_lon"]),
                                   "stop_id": sid})
            except Exception:
                pass
if rep_points:
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=rep_points,
            get_position='[lon, lat]',
            get_fill_color=[34,139,34],
            get_radius=30,
            pickable=True
        )
    )

# Vue initiale
if stops_df is not None and not stops_df.empty:
    try:
        lat0 = pd.to_numeric(stops_df["stop_lat"], errors="coerce").dropna().mean()
        lon0 = pd.to_numeric(stops_df["stop_lon"], errors="coerce").dropna().mean()
    except Exception:
        lat0, lon0 = 45.5017, -73.5673
else:
    lat0, lon0 = 45.5017, -73.5673

st.pydeck_chart(pdk.Deck(
    layers=layers,
    initial_view_state=pdk.ViewState(latitude=lat0, longitude=lon0, zoom=11),
    tooltip={"text": "{stop_id}"}
))
