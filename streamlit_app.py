# streamlit_app.py
# -*- coding: utf-8 -*-
import io
import os
import zipfile
import tempfile
import json
from typing import Dict, List, Tuple, Optional

import streamlit as st
import pandas as pd
import pydeck as pdk

from google.protobuf.message import DecodeError
from google.protobuf import text_format

# --- Import Protobuf: bindings -> fallback (proto récente) ---
PROTO_IMPORTED_VIA_BINDINGS = False
PROTO_IMPORTED_VIA_FALLBACK = False

def _import_proto():
    global PROTO_IMPORTED_VIA_BINDINGS, PROTO_IMPORTED_VIA_FALLBACK
    try:
        from google.transit import gtfs_realtime_pb2 as pb
        test = pb.FeedEntity()
        if 'trip_modifications' in dir(test):  # ok, champ présent
            PROTO_IMPORTED_VIA_BINDINGS = True
            return pb
    except Exception:
        pass

    # Fallback : télécharger la proto officielle et compiler dynamiquement
    try:
        import requests
        from grpc_tools import protoc
        PROTO_URL = "https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto"
        r = requests.get(PROTO_URL, timeout=15)
        r.raise_for_status()
        tmpdir = tempfile.mkdtemp()
        proto_path = os.path.join(tmpdir, "gtfs-realtime.proto")
        with open(proto_path, "wb") as f:
            f.write(r.content)
        protoc_ret = protoc.main(["protoc", f"-I{tmpdir}", f"--python_out={tmpdir}", proto_path])
        if protoc_ret != 0:
            raise RuntimeError("Echec compilation protoc")

        import sys
        sys.path.insert(0, tmpdir)
        import gtfs_realtime_pb2 as pb
        test = pb.FeedEntity()
        if 'trip_modifications' not in dir(test):
            raise RuntimeError("Bindings générés sans champ trip_modifications.")
        PROTO_IMPORTED_VIA_FALLBACK = True
        return pb
    except Exception as e:
        st.error("Impossible de charger la définition Protobuf (gtfs-realtime.proto). "
                 "Vérifie la connectivité outbound (pour récupérer la proto) ou mets à jour `gtfs-realtime-bindings`.")
        st.exception(e)
        st.stop()

pb = _import_proto()

# ------------------ Utilitaires GTFS statique ------------------

def load_gtfs_zip(gtfs_bytes: bytes) -> Dict[str, pd.DataFrame]:
    zf = zipfile.ZipFile(io.BytesIO(gtfs_bytes))
    dfs = {}
    for name in zf.namelist():
        if name.lower().endswith(".txt"):
            with zf.open(name) as f:
                dfs[os.path.basename(name).lower()] = pd.read_csv(f, dtype=str).fillna("")
    return dfs

def build_trip_shape(dfs: Dict[str, pd.DataFrame], trip_id: str) -> List[Tuple[float, float]]:
    trips = dfs.get("trips.txt")
    stop_times = dfs.get("stop_times.txt")
    stops = dfs.get("stops.txt")
    shapes = dfs.get("shapes.txt")
    if trips is None or stop_times is None or stops is None:
        return []
    trow = trips.loc[trips["trip_id"] == trip_id]
    pts: List[Tuple[float, float]] = []
    if not trow.empty and shapes is not None and "shape_id" in trow.columns:
        shape_id = trow.iloc[0]["shape_id"]
        if shape_id and shape_id in shapes["shape_id"].values:
            shp = shapes.loc[shapes["shape_id"] == shape_id]
            # tri par shape_pt_sequence (numérique)
            if "shape_pt_sequence" in shp.columns:
                shp = shp.sort_values(by="shape_pt_sequence", key=lambda s: pd.to_numeric(s, errors='coerce'))
            for _, r in shp.iterrows():
                try:
                    pts.append((float(r["shape_pt_lat"]), float(r["shape_pt_lon"])))
                except Exception:
                    pass
            if pts:
                return pts
    # Fallback: relier la chaîne d'arrêts du trip
    stimes = stop_times.loc[stop_times["trip_id"] == trip_id].copy()
    if stimes.empty:
        return []
    stimes["stop_sequence"] = pd.to_numeric(stimes["stop_sequence"], errors="coerce")
    stimes = stimes.sort_values("stop_sequence")
    stops_map = stops.set_index("stop_id")[["stop_lat", "stop_lon"]].to_dict("index")
    for _, r in stimes.iterrows():
        sid = r["stop_id"]
        if sid in stops_map:
            try:
                pts.append((float(stops_map[sid]["stop_lat"]), float(stops_map[sid]["stop_lon"])))
            except Exception:
                pass
    return pts

def decode_polyline(encoded: str) -> List[Tuple[float, float]]:
    coords = []
    index, lat, lng = 0, 0, 0
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

# ------------------ Validation TripModifications ------------------

class ValidationIssue:
    def __init__(self, level: str, code: str, msg: str, entity_id: str = "", context: dict = None):
        self.level = level  # "ERROR" | "WARN" | "INFO"
        self.code = code
        self.msg = msg
        self.entity_id = entity_id
        self.context = context or {}
    def to_dict(self):
        return {
            "niveau": self.level,
            "code": self.code,
            "entité": self.entity_id,
            "message": self.msg,
            "contexte": json.dumps(self.context, ensure_ascii=False),
        }

def validate_trip_mods(feed: "pb.FeedMessage", dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    issues: List[ValidationIssue] = []

    stops = dfs.get("stops.txt")
    loc_type = {}
    if stops is not None and "stop_id" in stops.columns:
        for _, r in stops.iterrows():
            lt = r.get("location_type", "")
            loc_type[r["stop_id"]] = int(lt) if str(lt).isdigit() else 0

    # collisions (trip selector, service_date)
    seen = set()

    for ent in feed.entity:
        if ent is None or not ent.HasField("trip_modifications"):
            continue
        tm = ent.trip_modifications
        entity_id = ent.id or ""

        # service_dates (liste de YYYYMMDD)
        service_dates = list(getattr(tm, "service_dates", []))

        # Construction de clés d’instance approximatives si selected_trips présent
        if hasattr(tm, "selected_trips"):
            for sel in tm.selected_trips:
                selector_str = str(sel)  # debug-friendly
                for d in service_dates or ["*"]:
                    key = (selector_str, d)
                    if key in seen:
                        issues.append(ValidationIssue(
                            "ERROR", "TM_DUPLICATE_ASSIGNMENT",
                            "Le même trip (sélecteur) est assigné à plus d’un TripModifications pour la même date de service.",
                            entity_id, {"selector": selector_str, "service_date": d}
                        ))
                    seen.add(key)

        # Vérifs sur les modifications
        for i, mod in enumerate(getattr(tm, "modifications", [])):
            tprev: Optional[int] = None
            r_stops = getattr(mod, "replacement_stops", [])
            for j, rs in enumerate(r_stops):
                t = None
                if hasattr(rs, "travel_time_to_stop"):
                    try:
                        t = int(rs.travel_time_to_stop)
                    except Exception:
                        pass
                sid = getattr(rs, "stop_id", "")
                if sid:
                    lt = loc_type.get(sid, 0)
                    if lt != 0:
                        issues.append(ValidationIssue(
                            "ERROR", "REPLACEMENT_STOP_NOT_ROUTABLE",
                            f"Le stop_id de remplacement '{sid}' n’est pas un arrêt routable (location_type=0).",
                            entity_id, {"replacement_stop_id": sid, "location_type": lt}
                        ))
                if t is not None:
                    if tprev is not None and t < tprev:
                        issues.append(ValidationIssue(
                            "ERROR", "TRAVEL_TIME_NOT_MONOTONIC",
                            "Les travel_time_to_stop ne sont pas strictement croissants.",
                            entity_id, {"mod_index": i, "pos": j, "prev": tprev, "curr": t}
                        ))
                    tprev = t

            # TODO : contrôle d’overlap entre spans si la proto expose start_stop_selector, etc.
            # issues.append(ValidationIssue("WARN","SPAN_OVERLAP_CHECK_SKIPPED","Contrôle des spans non implémenté.",entity_id))

    df = pd.DataFrame([x.to_dict() for x in issues]) if issues else pd.DataFrame(
        columns=["niveau", "code", "entité", "message", "contexte"]
    )
    return df

# ------------------ Carto (pydeck) ------------------

def make_layers_for_trip(
    dfs: Dict[str, pd.DataFrame],
    trip_id: str,
    detour_paths: List[List[Tuple[float, float]]],
    removed_stop_ids: List[str],
    replacement_stop_ids: List[str],
):
    base_line = build_trip_shape(dfs, trip_id) if trip_id else []
    layers = []

    if base_line:
        layers.append(
            pdk.Layer(
                "PathLayer",
                data=[{"path": [{"lat": lat, "lon": lon} for lat, lon in base_line]}],
                get_path="path",
                get_color=[128, 128, 128],
                width_scale=2,
                width_min_pixels=2,
                pickable=False,
            )
        )

    for path in detour_paths:
        if not path:
            continue
        layers.append(
            pdk.Layer(
                "PathLayer",
                data=[{"path": [{"lat": lat, "lon": lon} for lat, lon in path]}],
                get_path="path",
                get_color=[255, 140, 0],
                width_scale=3,
                width_min_pixels=3,
                pickable=False,
            )
        )

    stops = dfs.get("stops.txt")
    if stops is not None and not stops.empty:
        stops_map = stops.set_index("stop_id")[["stop_lat", "stop_lon"]].to_dict("index")

        def to_points(sids):
            pts = []
            for sid in sids:
                if sid in stops_map:
                    try:
                        pts.append({"lat": float(stops_map[sid]["stop_lat"]),
                                    "lon": float(stops_map[sid]["stop_lon"]),
                                    "stop_id": sid})
                    except Exception:
                        pass
            return pts

        rem_pts = to_points(removed_stop_ids)
        rep_pts = to_points(replacement_stop_ids)

        if rem_pts:
            layers.append(
                pdk.Layer(
                    "ScatterplotLayer", data=rem_pts,
                    get_position='[lon, lat]', get_fill_color=[220, 20, 60], get_radius=15,
                    pickable=True
                )
            )
        if rep_pts:
            layers.append(
                pdk.Layer(
                    "ScatterplotLayer", data=rep_pts,
                    get_position='[lon, lat]', get_fill_color=[34, 139, 34], get_radius=25,
                    pickable=True
                )
            )
    return layers

def guess_view_state(dfs: Dict[str, pd.DataFrame]) -> pdk.ViewState:
    stops = dfs.get("stops.txt")
    if stops is not None and not stops.empty:
        try:
            lat = pd.to_numeric(stops["stop_lat"], errors="coerce").dropna().mean()
            lon = pd.to_numeric(stops["stop_lon"], errors="coerce").dropna().mean()
            return pdk.ViewState(latitude=lat, longitude=lon, zoom=11)
        except Exception:
            pass
    return pdk.ViewState(latitude=45.5017, longitude=-73.5673, zoom=10)  # Montréal

# ------------------ UI Streamlit ------------------

st.set_page_config(page_title="GTFS-rt TripModifications • Validation & Carte (pydeck)", layout="wide")
st.title("GTFS-rt TripModifications • Validation & Carte (pydeck)")

expander = st.expander("À propos (statut expérimental & compatibilité proto)")
with expander:
    st.markdown(
        "- Les entités **TripModifications / Shape / Stop** sont **expérimentales** dans GTFS‑rt. "
        "En cas d’incompatibilités de bindings, l’app télécharge la dernière `gtfs-realtime.proto` et génère les bindings Python automatiquement."
    )
    st.markdown(
        f"- Chargement Protobuf : "
        f"{'bindings officiels' if PROTO_IMPORTED_VIA_BINDINGS else ('fallback (proto GitHub)' if PROTO_IMPORTED_VIA_FALLBACK else 'n/a')}."
    )

col1, col2 = st.columns(2)
with col1:
    rt_file = st.file_uploader("Fichier GTFS‑rt (.pb ou .textproto) contenant **TripModifications**",
                               type=["pb","bin","pbf","textproto","pbtxt","textpb","txtpb"])
with col2:
    gtfs_file = st.file_uploader("GTFS statique (.zip)", type=["zip"])

shape_entities = []
tripmods_entities = []

if rt_file and gtfs_file:
    dfs = load_gtfs_zip(gtfs_file.read())

    feed = pb.FeedMessage()
    uploaded_name = rt_file.name or ""
    try:
        if uploaded_name.lower().endswith((".textproto",".pbtxt",".textpb",".txtpb",".txt")):
            text = rt_file.read().decode("utf-8")
            text_format.Parse(text, feed, allow_unknown_extension=True)
        else:
            feed.ParseFromString(rt_file.read())
    except DecodeError as e:
        st.error("Impossible de décoder le buffer .pb (Protobuf).")
        st.exception(e)
        st.stop()
    except Exception as e:
        st.error("Impossible de parser le fichier fourni (pb ou textproto).")
        st.exception(e)
        st.stop()

    for ent in feed.entity:
        if ent is None:
            continue
        if ent.HasField("shape"):
            shape_entities.append(ent.shape)
        if ent.HasField("trip_modifications"):
            tripmods_entities.append((ent.id, ent.trip_modifications))

    st.subheader("Résumé du feed")
    st.write(f"- Entities total : {len(feed.entity)}")
    st.write(f"- TripModifications : {len(tripmods_entities)}")
    st.write(f"- Shapes (expérimental) : {len(shape_entities)}")

    # Validation
    st.subheader("Validation (règles clés)")
    issues_df = validate_trip_mods(feed, dfs)
    if issues_df.empty:
        st.success("Aucun problème détecté sur les règles implémentées.")
    else:
        st.dataframe(issues_df, use_container_width=True)

    # Carte
    st.subheader("Carte des détours (pydeck)")
    if tripmods_entities:
        choices = [f"{i+1}. entity_id={eid}" for i, (eid, _) in enumerate(tripmods_entities)]
        pick = st.selectbox("Choisir un TripModifications à visualiser", choices, index=0)
        idx = int(pick.split(".")[0]) - 1
        ent_id, tm = tripmods_entities[idx]

        st.markdown("**TripModifications (brut, debug)**")
        st.code(str(tm))

        # Trip de référence (si possible)
        trip_id_for_shape = None
        if hasattr(tm, "selected_trips"):
            for sel in tm.selected_trips:
                if hasattr(sel, "trip_id") and sel.trip_id:
                    trip_id_for_shape = sel.trip_id
                    break

        # Détour : shapes temps réel ?
        detour_paths: List[List[Tuple[float, float]]] = []
        for sh in shape_entities:
            enc = getattr(sh, "encoded_polyline", "")
            if enc:
                coords = decode_polyline(enc)
                detour_paths.append(coords)

        # Replacement stops
        replacement_stop_ids: List[str] = []
        removed_stop_ids: List[str] = []  # TODO si on calcule les spans exacts
        for mod in getattr(tm, "modifications", []):
            for rs in getattr(mod, "replacement_stops", []):
                sid = getattr(rs, "stop_id", "")
                if sid:
                    replacement_stop_ids.append(sid)

        # Fallback chemin détour: relier les replacement stops
        if not detour_paths and replacement_stop_ids:
            stops = dfs.get("stops.txt")
            if stops is not None and not stops.empty:
                m = stops.set_index("stop_id")[["stop_lat", "stop_lon"]].to_dict("index")
                path = []
                for sid in replacement_stop_ids:
                    if sid in m:
                        try:
                            path.append((float(m[sid]["stop_lat"]), float(m[sid]["stop_lon"])))
                        except Exception:
                            pass
                if len(path) >= 2:
                    detour_paths.append(path)

        layers = make_layers_for_trip(
            dfs,
            trip_id_for_shape or "",   # si None, on n’affiche que le détour
            detour_paths,
            removed_stop_ids,
            replacement_stop_ids,
        )
        view = guess_view_state(dfs)
        r = pdk.Deck(layers=layers, initial_view_state=view, tooltip={"text": "{stop_id}"})
        st.pydeck_chart(r)

        st.download_button(
            "Télécharger le rapport de validation (CSV)",
            data=issues_df.to_csv(index=False).encode("utf-8"),
            file_name="validation_tripmod.csv",
            mime="text/csv",
        )
    else:
        st.info("Aucun TripModifications détecté dans le feed. Fournis un .pb ou un .textproto contenant cette entité.")
else:
    st.info("Charge un fichier GTFS‑rt (TripModifications) et un GTFS .zip pour commencer.")
