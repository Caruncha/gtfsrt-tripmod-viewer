# -*- coding: utf-8 -*-
import io
import zipfile
import tempfile
import requests
import streamlit as st
import pandas as pd
import pydeck as pdk
from google.protobuf.message import DecodeError

# -----------------------------
# 1) Chargement Protobuf (bindings -> fallback auto)
# -----------------------------
@st.cache_resource(show_spinner=False)
def load_proto():
    # a) Essayer les bindings Python officiels
    try:
        from google.transit import gtfs_realtime_pb2 as pb
        if 'trip_modifications' in dir(pb.FeedEntity()):
            return pb, "bindings"
    except Exception:
        pass
    # b) Fallback : récupérer la dernière proto et compiler dans /tmp
    try:
        from grpc_tools import protoc
        PROTO_URL = ("https://raw.githubusercontent.com/google/transit/"
                     "master/gtfs-realtime/proto/gtfs-realtime.proto")
        r = requests.get(PROTO_URL, timeout=15)
        r.raise_for_status()
        tmpdir = tempfile.mkdtemp()
        proto_path = f"{tmpdir}/gtfs-realtime.proto"
        with open(proto_path, "wb") as f:
            f.write(r.content)
        ret = protoc.main(["protoc", f"-I{tmpdir}", f"--python_out={tmpdir}", proto_path])
        if ret != 0:
            raise RuntimeError("protoc failed")
        import sys
        sys.path.insert(0, tmpdir)
        import gtfs_realtime_pb2 as pb
        if 'trip_modifications' not in dir(pb.FeedEntity()):
            raise RuntimeError("proto chargée sans 'trip_modifications'")
        return pb, "fallback"
    except Exception as e:
        st.error("Impossible de charger la définition Protobuf (gtfs-realtime.proto).")
        st.exception(e)
        st.stop()

pb, proto_mode = load_proto()

# -----------------------------
# 2) Utilitaires GTFS
# -----------------------------
def load_gtfs_zip(gtfs_bytes: bytes) -> dict[str, pd.DataFrame]:
    zf = zipfile.ZipFile(io.BytesIO(gtfs_bytes))
    dfs: dict[str, pd.DataFrame] = {}
    for name in zf.namelist():
        if name.lower().endswith(".txt"):
            with zf.open(name) as f:
                dfs[name.lower()] = pd.read_csv(f, dtype=str).fillna("")
    return dfs

def build_trip_shape(dfs: dict[str, pd.DataFrame], trip_id: str) -> list[tuple[float,float]]:
    """Renvoie la polyline (lat,lon) d’un trip depuis shapes.txt, sinon fallback via la chaîne d’arrêts."""
    trips = dfs.get("trips.txt")
    stop_times = dfs.get("stop_times.txt")
    stops = dfs.get("stops.txt")
    shapes = dfs.get("shapes.txt")
    pts: list[tuple[float,float]] = []
    if trips is not None and shapes is not None and "shape_id" in trips.columns:
        trow = trips.loc[trips["trip_id"] == trip_id]
        if not trow.empty:
            shape_id = trow.iloc[0]["shape_id"]
            shp = shapes.loc[shapes["shape_id"] == shape_id]
            if "shape_pt_sequence" in shp.columns:
                shp = shp.sort_values(by="shape_pt_sequence", key=lambda s: pd.to_numeric(s, errors="coerce"))
            for _, r in shp.iterrows():
                try:
                    pts.append((float(r["shape_pt_lat"]), float(r["shape_pt_lon"])))
                except Exception:
                    pass
            if pts:
                return pts
    # Fallback : relier les arrêts du trip
    if stop_times is not None and stops is not None:
        stimes = stop_times.loc[stop_times["trip_id"] == trip_id].copy()
        if stimes.empty:
            return pts
        stimes["stop_sequence"] = pd.to_numeric(stimes["stop_sequence"], errors="coerce")
        stimes = stimes.sort_values("stop_sequence")
        s_map = stops.set_index("stop_id")[["stop_lat","stop_lon"]].to_dict("index")
        for _, r in stimes.iterrows():
            sid = r["stop_id"]
            if sid in s_map:
                try:
                    pts.append((float(s_map[sid]["stop_lat"]), float(s_map[sid]["stop_lon"])))
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

# -----------------------------
# 3) UI
# -----------------------------
st.set_page_config(page_title="TripModifications • Analyse & Carte", layout="wide")
st.title("GTFS‑rt TripModifications • Analyse & Visualisation des détours")

with st.expander("Informations"):
    st.write("- Cette app charge un **GTFS statique (.zip)** et un **TripModification (.pb)**.")
    st.write("- Statut proto :", "bindings officiels" if proto_mode=="bindings" else "fallback (proto GitHub)")
    st.caption("TripModifications/Shape/Stop sont des champs **expérimentaux** de la spec GTFS‑rt.")

col1, col2 = st.columns(2)
with col1:
    gtfs_file = st.file_uploader("GTFS statique (.zip)", type=["zip"])
with col2:
    rt_file = st.file_uploader("TripModification (.pb)", type=["pb","pbf","bin"])

if not (gtfs_file and rt_file):
    st.info("Charge les deux fichiers pour commencer.")
    st.stop()

# 4) Charger données
dfs = load_gtfs_zip(gtfs_file.read())
feed = pb.FeedMessage()
try:
    feed.ParseFromString(rt_file.read())
except DecodeError as e:
    st.error("Impossible de décoder le fichier .pb (Protobuf).")
    st.exception(e)
    st.stop()

tripmods = [(e.id, e.trip_modifications) for e in feed.entity if e.HasField("trip_modifications")]
shapes_rt = [e.shape for e in feed.entity if e.HasField("shape")]

if not tripmods:
    st.warning("Aucune entité TripModifications trouvée dans le .pb.")
    st.stop()

# Select TripModification
choice = st.selectbox("Choisir une TripModifications", [f"{i+1}. {eid}" for i, (eid, _) in enumerate(tripmods)], index=0)
idx = int(choice.split(".")[0]) - 1
eid, tm = tripmods[idx]

st.subheader("Analyse")
issues: list[str] = []
stops_df = dfs.get("stops.txt")
loc_type = {}
if stops_df is not None and "stop_id" in stops_df.columns:
    if "location_type" not in stops_df.columns:
        stops_df["location_type"] = "0"
    loc_type = stops_df.set_index("stop_id")["location_type"].to_dict()

# Vérifs minimales : routabilité + monotonicité travel_time_to_stop
replacement_stop_ids: list[str] = []
for mod in tm.modifications:
    prev_t = None
    for rs in mod.replacement_stops:
        sid = rs.stop_id
        replacement_stop_ids.append(sid)
        if loc_type.get(sid, "0") != "0":
            issues.append(f"[Routabilité] stop_id '{sid}' n'est pas routable (location_type != 0).")
        if prev_t is not None and rs.travel_time_to_stop < prev_t:
            issues.append("[Monotonicité] 'travel_time_to_stop' non croissant.")
        prev_t = rs.travel_time_to_stop

if issues:
    st.error("Problèmes détectés :")
    for msg in issues:
        st.write("•", msg)
else:
    st.success("Aucun problème détecté sur les règles implémentées.")

# -----------------------------
# 5) Carte (pydeck)
# -----------------------------
st.subheader("Carte des détours")

# Trip de référence (pour tracer la shape d’origine si possible)
trip_id_for_shape = None
if hasattr(tm, "selected_trips"):
    for sel in tm.selected_trips:
        if hasattr(sel, "trip_id") and sel.trip_id:
            trip_id_for_shape = sel.trip_id
            break

base_line = build_trip_shape(dfs, trip_id_for_shape) if trip_id_for_shape else []

# Détour : utiliser Shape temps réel si présent (encoded_polyline), sinon fallback (relier arrêts temporaires)
detour_paths: list[list[tuple[float,float]]] = []
for sh in shapes_rt:
    if getattr(sh, "encoded_polyline", ""):
        detour_paths.append(decode_polyline(sh.encoded_polyline))

stops_map = {}
if stops_df is not None and not stops_df.empty:
    stops_map = stops_df.set_index("stop_id")[["stop_lat","stop_lon"]].to_dict("index")

layers: list[pdk.Layer] = []

# shape d’origine (gris)
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

# détour (orange)
if not detour_paths and replacement_stop_ids:
    # fallback relier les arrêts temporaires
    fallback = []
    for sid in replacement_stop_ids:
        if sid in stops_map:
            try:
                fallback.append((float(stops_map[sid]["stop_lat"]), float(stops_map[sid]["stop_lon"])))
            except Exception:
                pass
    if len(fallback) >= 2:
        detour_paths.append(fallback)

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

# arrêts temporaires (verts)
rep_points = []
for sid in replacement_stop_ids:
    if sid in stops_map:
        try:
            rep_points.append({"lat": float(stops_map[sid]["stop_lat"]),
                               "lon": float(stops_map[sid]["stop_lon"]), "stop_id": sid})
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

# Vue initiale : barycentre des arrêts si possible
if stops_df is not None and not stops_df.empty:
    try:
        lat0 = pd.to_numeric(stops_df["stop_lat"], errors="coerce").dropna().mean()
        lon0 = pd.to_numeric(stops_df["stop_lon"], errors="coerce").dropna().mean()
    except Exception:
        lat0, lon0 = 45.5017, -73.5673
else:
    lat0, lon0 = 45.5017, -73.5673

view = pdk.ViewState(latitude=lat0, longitude=lon0, zoom=11)
st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view, tooltip={"text": "{stop_id}"}))
