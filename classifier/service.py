from __future__ import annotations
import numpy as np
import bentoml
from pydantic import BaseModel

class IncidentInput(BaseModel):
    alert_count: float
    p95_latency_ms: float
    production: int
    business_criticality: int
    exploitability: int
    data_exposure: int

FEATURES = ["alert_count", "p95_latency_ms", "production",
            "business_criticality", "exploitability", "data_exposure"]

@bentoml.service(name="db-incident-classifier")
class DBIncidentClassifier:
    model_ref = bentoml.models.BentoModel("db_incident_classifier:rt6gubv2nshldhwe")

    def __init__(self) -> None:
        self.model = self.model_ref.load_model()

    @bentoml.api
    def predict(self, incident: IncidentInput) -> dict:
        row = [float(incident.model_dump()[f]) for f in FEATURES]
        sample = np.asarray([row], dtype=np.float64)
        sev = self.model.predict(sample)[0]
        probs = self.model.predict_proba(sample)[0]
        return {
            "category": "Database",
            "severity": str(sev),
            "probabilities": {
                str(c): float(p) for c, p in zip(self.model.classes_, probs)
            },
        }
