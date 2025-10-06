import streamlit as st
import pandas as pd
import pydeck as pdk
import zipfile
import io
import tempfile
import requests
from google.protobuf import text_format
from google.protobuf.message import DecodeError

# Fallback pour charger la proto GTFS-rt si bindings manquants
def load_proto():
    try:
        from google.transit import gtfs_realtime_pb2 as pb
        if 'trip_modifications' in dir(pb.FeedEntity()):
            return pb
    except Exception:
        pass
    from grpc_tools import protoc
    PROTO_URL = "https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto"
    r = requests.get(PROTO_URL, timeout=15)
    tmpdir = tempfile.mkdtemp()
    proto_path = f"{tmpdir}/gtfs-realtime.proto"
    with open(proto_path, "wb") as f:
        f.write(r.content)
    protoc.main(["protoc", f"-I{tmpdir}", f"--python_out={tmpdir}", proto_path])
    import sys
    sys.path.insert(0, tmpdir)
    import gtfs_realtime_pb2 as pb
    return pb

pb = load_proto()

# Charger GTFS statique
def load_gtfs(gtfs_bytes):
    zf = zipfile.ZipFile(io.BytesIO(gtfs_bytes))
    dfs = {}
    for name in zf.namelist():
        if name.endswith(".txt"):
            with zf.open(name) as f:
                dfs[name.lower()] = pd.read_csv(f, dtype=str).fillna("")
    return dfs

# Shape d'origine
def build_shape(dfs, trip_id):
    trips = dfs.get("trips.txt")
    stop_times = dfs.get("stop_times.txt")
    stops = dfs.get("stops.txt")
    shapes = dfs.get("shapes.txt")
    pts = []
    if trips is not None and shapes is not None:
        row = trips.loc[trips["trip_id"] == trip_id]
        if not row.empty:
            shape_id = row.iloc[0]["shape_id"]
            shp = shapes.loc[shapes["shape_id"] == shape_id]
            shp = shp.sort_values(by="shape_pt_sequence", key=lambda s: pd.to_numeric(s, errors="coerce"))
            for _, r in shp.iterrows():
                pts.append((float(r["shape_pt_lat"]), float(r["shape_pt_lon"])))
    if not pts and stop_times is not None and stops is not None:
        stimes = stop_times.loc[stop_times["trip_id"] == trip_id].sort_values("stop_sequence")
        stops_map = stops.set_index("stop_id")[["stop_lat", "stop_lon"]].to_dict("index")
        for _, r in stimes.iterrows():
            sid = r["stop_id"]
            if sid in stops_map:
                pts.append((float(stops_map[sid]["stop_lat"]), float(stops_map[sid]["stop_lon"])))
    return pts

# Décodage polyline
def decode_polyline(encoded):
    coords, index, lat, lng = [], 0, 0, 0
    while index < len(encoded):
        shift, result = 0, 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat
        shift, result = 0, 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng
        coords.append((lat / 1e5, lng / 1e5))
    return coords

# UI
st.title("GTFS-rt TripModifications • Analyse & Carte")
gtfs_file = st.file_uploader("Uploader GTFS statique (.zip)", type=["zip"])
rt_file = st.file_uploader("Uploader TripModification (.pb)", type=["pb"])

if gtfs_file and rt_file:
    dfs = load_gtfs(gtfs_file.read())
    feed = pb.FeedMessage()
    try:
        feed.ParseFromString(rt_file.read())
    except DecodeError:
        st.error("Erreur de parsing du fichier .pb")
        st.stop()

    tripmods = [(e.id, e.trip_modifications) for e in feed.entity if e.HasField("trip_modifications")]
    shapes_rt = [e.shape for e in feed.entity if e.HasField("shape")]

    if not tripmods:
        st.warning("Aucune TripModification trouvée.")
        st.stop()

    choice = st.selectbox("Choisir une TripModification", [f"{i+1}. {eid}" for i, (eid, _) in enumerate(tripmods)])
    idx = int(choice.split(".")[0]) - 1
    eid, tm = tripmods[idx]

    st.subheader("Analyse")
    issues = []
    stops_df = dfs.get("stops.txt")
    loc_type = stops_df.set_index("stop_id")["location_type"].to_dict() if stops_df is not None else {}

    for mod in tm.modifications:
        prev_time = None
        for rs in mod.replacement_stops:
            sid = rs.stop_id
            if loc_type.get(sid, "0") != "0":
                issues.append(f"Stop {sid} n'est pas routable (location_type != 0)")
            if prev_time is not None and rs.travel_time_to_stop < prev_time:
                issues.append("travel_time_to_stop non monotone")
            prev_time = rs.travel_time_to_stop

    if issues:
        st.error("Problèmes détectés :")
        for i in issues:
            st.write("-", i)
    else:
        st.success("Aucun problème détecté.")

    st.subheader("Carte des détours")
    trip_id = None
    if hasattr(tm, "selected_trips"):
        for sel in tm.selected_trips:
            if hasattr(sel, "trip_id") and sel.trip_id:
                trip_id = sel.trip_id
                break

    base_shape = build_shape(dfs, trip_id) if trip_id else []
    detour_paths = []
    for sh in shapes_rt:
        if sh.encoded_polyline:
            detour_paths.append(decode_polyline(sh.encoded_polyline))

    replacement_ids = [rs.stop_id for mod in tm.modifications for rs in mod.replacement_stops]
    stops_map = stops_df.set_index("stop_id")[["stop_lat", "stop_lon"]].to_dict("index") if stops_df is not None else {}

    layers = []
    if base_shape:
        layers.append(pdk.Layer("PathLayer", data=[{"path": [{"lat": lat, "lon": lon} for lat, lon in base_shape]}],
                                get_path="path", get_color=[128, 128, 128], width_min_pixels=2))
    for path in detour_paths:
        layers.append(pdk.Layer("PathLayer", data=[{"path": [{"lat": lat, "lon": lon} for lat, lon in path]}],
                                get_path="path", get_color=[255, 140, 0], width_min_pixels=3))
    rep_points = [{"lat": float(stops_map[s]["stop_lat"]), "lon": float(stops_map[s]["stop_lon"]), "stop_id": s}
                  for s in replacement_ids if s in stops_map]
    if rep_points:
        layers.append(pdk.Layer("ScatterplotLayer", data=rep_points, get_position='[lon, lat]',
                                get_fill_color=[34, 139, 34], get_radius=30))

    view = pdk.ViewState(latitude=45.5017, longitude=-73.5673, zoom=11)
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view, tooltip={"text": "{stop_id}"}))
