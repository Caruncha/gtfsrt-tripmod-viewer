# scripts/generate_mini_gtfs.py
import os, zipfile, io, csv

OUT = os.path.join("data", "mini_gtfs.zip")
os.makedirs("data", exist_ok=True)

def writetxt(zf, name, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    for row in rows:
        w.writerow(row)
    zf.writestr(name, buf.getvalue())

with zipfile.ZipFile(OUT, "w", compression=zipfile.ZIP_DEFLATED) as z:
    writetxt(z, "agency.txt", [
        ["agency_id","agency_name","agency_url","agency_timezone","agency_lang"],
        ["STM","Société de transport de Montréal","https://stm.info","America/Toronto","fr"]
    ])
    writetxt(z, "routes.txt", [
        ["route_id","agency_id","route_short_name","route_long_name","route_type","route_color","route_text_color"],
        ["R1","STM","1","Exemple Détour","3","FF7F00","FFFFFF"]
    ])
    writetxt(z, "stops.txt", [
        ["stop_id","stop_name","stop_lat","stop_lon","location_type"],
        ["S1","Arrêt A", "45.5150","-73.5670","0"],
        ["S2","Arrêt B", "45.5155","-73.5600","0"],
        ["S3","Arrêt C", "45.5165","-73.5530","0"],
        ["R_TMP_1","Temporaire 1","45.5160","-73.5575","0"],
        ["R_TMP_2","Temporaire 2","45.5168","-73.5510","0"],
    ])
    writetxt(z, "trips.txt", [
        ["route_id","service_id","trip_id","trip_headsign","direction_id","shape_id"],
        ["R1","WEEK","T1","Vers C", "0","SH1"]
    ])
    writetxt(z, "calendar.txt", [
        ["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday","start_date","end_date"],
        ["WEEK","1","1","1","1","1","0","0","20250101","20251231"]
    ])
    writetxt(z, "stop_times.txt", [
        ["trip_id","arrival_time","departure_time","stop_id","stop_sequence"],
        ["T1","08:00:00","08:00:00","S1","1"],
        ["T1","08:05:00","08:05:00","S2","2"],
        ["T1","08:10:00","08:10:00","S3","3"],
    ])
    writetxt(z, "shapes.txt", [
        ["shape_id","shape_pt_lat","shape_pt_lon","shape_pt_sequence"],
        ["SH1","45.5150","-73.5670","1"],
        ["SH1","45.5155","-73.5600","2"],
        ["SH1","45.5165","-73.5530","3"],
    ])

print(f"OK: {OUT}")
