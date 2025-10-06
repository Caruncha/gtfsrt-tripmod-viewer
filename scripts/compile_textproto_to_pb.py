# scripts/compile_textproto_to_pb.py
import sys, os, tempfile, requests
from google.protobuf import text_format

def import_pb():
    # 1) Essaye les bindings Python (gtfs-realtime-bindings)
    try:
        from google.transit import gtfs_realtime_pb2 as pb
        if 'trip_modifications' in dir(pb.FeedEntity()):
            return pb
    except Exception:
        pass
    # 2) Fallback : télécharger la dernière proto et compiler
    from grpc_tools import protoc
    PROTO_URL = "https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto"
    r = requests.get(PROTO_URL, timeout=15); r.raise_for_status()
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "gtfs-realtime.proto")
    with open(path, "wb") as f: f.write(r.content)
    ret = protoc.main(["protoc", f"-I{tmp}", f"--python_out={tmp}", path])
    if ret != 0: raise RuntimeError("protoc failed")
    import sys as _sys; _sys.path.insert(0, tmp)
    import gtfs_realtime_pb2 as pb
    return pb

def main(in_path, out_path):
    pb = import_pb()
    feed = pb.FeedMessage()
    with open(in_path, "r", encoding="utf-8") as f:
        text_format.Parse(f.read(), feed, allow_unknown_extension=True)
    with open(out_path, "wb") as f:
        f.write(feed.SerializeToString())
    print("OK:", out_path)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/compile_textproto_to_pb.py samples/sample_tripmod.textproto samples/sample_tripmod.pb")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
