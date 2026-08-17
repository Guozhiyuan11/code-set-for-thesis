"""Small runnable check against the captured SMART Sleeper export."""

import json
from pathlib import Path

import decoder


data_path = Path(__file__).parents[1] / "data" / "smart_sleeper_data.json"
series = json.loads(data_path.read_text(encoding="utf-8"))["results"][0]["series"][0]
rows = [dict(zip(series["columns"], values)) for values in series["values"]]
frames = [decoder.SMARTSleeperFrame.decode(row) for row in rows]
environment_frames = [frame for frame in frames if frame.env_frame is not None]

assert len(rows) == 155
assert len(environment_frames) == 32
print(f"Decoded {len(environment_frames)} environment frames from {len(rows)} rows")
