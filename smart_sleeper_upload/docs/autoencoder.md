# SMART Sleeper Autoencoder v0

## What changed

`filters/model_rules.py` adds the immutable, single-record 7→4→2→4→7 model
layer after hard/context/adaptive checks and before event confirmation. It writes
model evidence without changing raw sensor values. Incomplete inputs receive
`model.autoencoder_not_evaluated`, never an anomaly candidate.

The seven fixed inputs are the five `*_t_x100` temperatures (converted to °C),
`moist_pc`, and `sleeper_rh`. `rain_mm`, `flood_flag`, identity, location and
timestamps are excluded from model input.

## Train and load

Install and run from the project root:

```powershell
python -m pip install -r requirements.txt
python train_autoencoder.py <labelled-data.csv> --output models/autoencoder-v0
```

Only explicitly labelled `isAnomaly = 0/false` rows enter the deterministic
70/15/15 train/validation/test normal split. Unlabelled records are skipped.
The model package contains weights, the explicit simulation-to-runtime mapping,
median/IQR scaler,
fixed feature order, seed, timestamps, and validation-derived thresholds
(overall p99; per field p99.5). The runtime validates every required package
component before inference; a load failure is recorded as evidence and leaves
the existing filter pipeline available.

## Modes

Set `--dynamic-mode` to `shadow`, `auto`, or `enforce`. It is the sole mode
source. Shadow records evidence only. Auto begins in
shadow and promotes only after the configured real-input volume, completeness,
candidate-rate and enabled event-confirmation requirements hold. Enforce still
requires the existing confirmed-event policy before setting `quality_state` to
`suspect`; a single model point only retains evidence. A changed model package
identity resets auto mode to shadow.

## Verification and limits

The committed package was trained on 163,312 complete labelled-normal rows.
Its held-out result did not meet the configured safety target (normal candidate
rate <=2%, target-autoencoder recall >=90%), so metadata marks it `failed` and
Auto is deliberately locked to Shadow. Python tests cover deterministic
inference and incomplete input; the existing Python and TypeScript suites still pass.

This v0 is intentionally a small pointwise model: no online updates, sequence
windows, LSTM/Transformer, SPOT, retraining schedule, new database table or
event system. Its published reconstruction threshold should be monitored in
shadow mode on production-quality inputs before enabling auto/enforce.
