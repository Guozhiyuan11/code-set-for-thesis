import numpy as np

from train_autoencoder import labelled_examples


def test_labelled_examples_keeps_anomaly_types_aligned_after_skipping_invalid_rows():
    rows = [
        {"isAnomaly": "1", "expectedDetector": "autoencoder", "anomalyType": "skipped", "rail1Temp": "bad"},
        {"isAnomaly": "1", "expectedDetector": "autoencoder", "anomalyType": "kept", "rail1Temp": 1, "rail2Temp": 1, "sleeperTemp": 1, "envMonIntTemp": 1, "ambiantTemp": 1, "moisture": 1, "envMonHumidity": 1},
    ]
    matrix, labels, detectors, types = labelled_examples(rows)
    assert matrix.shape == (1, 7)
    assert np.array_equal(labels, np.array([True]))
    assert detectors == ["autoencoder"]
    assert types == ["kept"]
