"""Train the immutable SMART Sleeper 7→4→2→4→7 autoencoder package."""
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from filters.model_rules import FEATURES

TRAINING_FEATURE_MAPPING = {
    "rtd1_t_x100": "rail1Temp", "rtd2_t_x100": "rail2Temp",
    "rtd3_t_x100": "sleeperTemp", "rtd4_t_x100": "envMonIntTemp",
    "tmp102_t_x100": "ambiantTemp", "moist_pc": "moisture",
    "sleeper_rh": "envMonHumidity",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--output", default="models/autoencoder-v0")
    parser.add_argument("--seed", type=int, default=20250801)
    parser.add_argument("--epochs", type=int, default=100)
    return parser.parse_args()


def rows_from_path(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(payload, dict) and isinstance(payload.get("records"), list):
        return [row for row in payload["records"] if isinstance(row, dict)]
    raise ValueError(f"Unsupported labelled JSON layout: {path}")


def labelled_examples(rows: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray, list[str], list[str]]:
    features: list[list[float]] = []; labels: list[bool] = []; detectors: list[str] = []; anomaly_types: list[str] = []
    for row in rows:
        raw_label = row.get("isAnomaly", row.get("is_anomaly"))
        if raw_label is None:
            continue
        label = str(raw_label).strip().lower()
        if label not in {"0", "false", "1", "true"}:
            continue
        try:
            values = [float(row[source]) for source in TRAINING_FEATURE_MAPPING.values()]
        except (KeyError, TypeError, ValueError):
            continue
        if not np.isfinite(values).all() or not (0 <= values[5] <= 100 and 0 <= values[6] <= 100):
            continue
        features.append(values)
        labels.append(label in {"1", "true"})
        detectors.append(str(row.get("expectedDetector") or ""))
        anomaly_types.append(str(row.get("anomalyType") or "unknown"))
    if not features:
        raise ValueError("No labelled complete records found; unlabelled records are never treated as normal")
    return np.asarray(features, dtype=np.float64), np.asarray(labels, dtype=bool), detectors, anomaly_types


def infer(weights: dict[str, np.ndarray], scaled: np.ndarray) -> np.ndarray:
    a1 = np.tanh(scaled @ weights["w1"] + weights["b1"])
    a2 = np.tanh(a1 @ weights["w2"] + weights["b2"])
    a3 = np.tanh(a2 @ weights["w3"] + weights["b3"])
    return a3 @ weights["w4"] + weights["b4"]


def train(train_raw: np.ndarray, validation_raw: np.ndarray, seed: int, max_epochs: int) -> tuple[dict[str, np.ndarray], np.ndarray, np.ndarray, int, float]:
    rng = np.random.default_rng(seed)
    median = np.median(train_raw, axis=0)
    iqr = np.maximum(np.percentile(train_raw, 75, axis=0) - np.percentile(train_raw, 25, axis=0), 1e-6)
    train_x, validation_x = (train_raw - median) / iqr, (validation_raw - median) / iqr
    weights: dict[str, np.ndarray] = {}
    for name, left, right in (("w1", 7, 4), ("w2", 4, 2), ("w3", 2, 4), ("w4", 4, 7)):
        weights[name] = rng.normal(0, np.sqrt(1 / left), (left, right)); weights[f"b{name[1:]}"] = np.zeros(right)
    best = {name: value.copy() for name, value in weights.items()}; best_loss = float("inf"); patience = 0
    for epoch in range(max_epochs):
        indices = rng.permutation(len(train_x))
        for start in range(0, len(indices), 512):
            x = train_x[indices[start:start + 512]]
            z1 = x @ weights["w1"] + weights["b1"]; a1 = np.tanh(z1)
            z2 = a1 @ weights["w2"] + weights["b2"]; a2 = np.tanh(z2)
            z3 = a2 @ weights["w3"] + weights["b3"]; a3 = np.tanh(z3); out = a3 @ weights["w4"] + weights["b4"]
            grad = 2 * (out - x) / len(x)
            grads = {"w4": a3.T @ grad, "b4": grad.sum(0)}; grad = (grad @ weights["w4"].T) * (1 - a3 * a3)
            grads.update({"w3": a2.T @ grad, "b3": grad.sum(0)}); grad = (grad @ weights["w3"].T) * (1 - a2 * a2)
            grads.update({"w2": a1.T @ grad, "b2": grad.sum(0)}); grad = (grad @ weights["w2"].T) * (1 - a1 * a1)
            grads.update({"w1": x.T @ grad, "b1": grad.sum(0)})
            for name, grad_value in grads.items(): weights[name] -= .01 * np.clip(grad_value, -5, 5)
        loss = float(np.mean((infer(weights, validation_x) - validation_x) ** 2))
        if loss < best_loss - 1e-7: best_loss, best, patience = loss, {name: value.copy() for name, value in weights.items()}, 0
        else:
            patience += 1
            if patience >= 10: break
    return best, median, iqr, epoch + 1, best_loss


def main() -> int:
    args = parse_args(); rows = [row for item in args.inputs for row in rows_from_path(Path(item))]
    x, anomalous, detectors, anomaly_types = labelled_examples(rows); normal = x[~anomalous]
    if len(normal) < 100: raise ValueError("Need at least 100 complete labelled normal records")
    rng = np.random.default_rng(args.seed); normal = normal[rng.permutation(len(normal))]
    train_end, validation_end = int(len(normal) * .7), int(len(normal) * .85)
    train_raw, validation_raw, test_normal = normal[:train_end], normal[train_end:validation_end], normal[validation_end:]
    weights, median, iqr, epochs, validation_mse = train(train_raw, validation_raw, args.seed, args.epochs)
    validation_errors = np.square((validation_raw - median) / iqr - infer(weights, (validation_raw - median) / iqr))
    overall_threshold = float(np.quantile(validation_errors.mean(1), .99)); field_thresholds = {feature: float(np.quantile(validation_errors[:, index], .995)) for index, feature in enumerate(FEATURES)}
    test_anomaly = x[anomalous]
    test_detectors = [detectors[index] for index, is_bad in enumerate(anomalous) if is_bad]
    test_anomaly_types = [anomaly_types[index] for index, is_bad in enumerate(anomalous) if is_bad]
    def candidates(raw: np.ndarray) -> np.ndarray:
        errors = np.square((raw - median) / iqr - infer(weights, (raw - median) / iqr))
        return (errors.mean(1) > overall_threshold) | np.any(errors > np.asarray(list(field_thresholds.values())), axis=1)
    normal_rate = float(candidates(test_normal).mean()); anomaly_flags = candidates(test_anomaly)
    target = np.asarray([detector == "autoencoder" for detector in test_detectors], dtype=bool)
    recall = float(anomaly_flags[target].mean()) if target.any() else 0.0
    metrics = {"test_normal_candidate_rate": normal_rate, "autoencoder_anomaly_recall": recall, "recall_by_anomaly_type": {}}
    for anomaly_type, candidate in zip(test_anomaly_types, anomaly_flags):
        metrics["recall_by_anomaly_type"].setdefault(anomaly_type, []).append(bool(candidate))
    metrics["recall_by_anomaly_type"] = {key: sum(value) / len(value) for key, value in metrics["recall_by_anomaly_type"].items()}
    status = "passed" if normal_rate <= .02 and recall >= .90 else "failed"
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True); np.savez(out / "weights.npz", **weights)
    metadata = {"model_version": "smart-sleeper-ae-v0", "feature_order": list(FEATURES), "training_feature_mapping": TRAINING_FEATURE_MAPPING, "median": median.tolist(), "iqr": iqr.tolist(), "overall_threshold": overall_threshold, "field_thresholds": field_thresholds, "random_seed": args.seed, "trained_at_utc": datetime.now(timezone.utc).isoformat(), "validation_status": status, "metrics": metrics, "training": {"train_records": len(train_raw), "validation_records": len(validation_raw), "test_normal_records": len(test_normal), "test_anomaly_records": len(test_anomaly), "validation_mse": validation_mse, "epochs": epochs, "loss": "MSE", "scaler": "median/IQR"}}
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({**metadata["training"], **metrics, "validation_status": status}, indent=2)); return 0


if __name__ == "__main__": raise SystemExit(main())
