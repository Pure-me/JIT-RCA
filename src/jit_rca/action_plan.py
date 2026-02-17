from __future__ import annotations
from datetime import datetime, timedelta, timezone

SUGGESTIONS = {
    "Transport/Dispatch": {
        "action": "Herbekijk cut-off tijden en herkalibreer transportplanning",
        "owner": "Transport",
        "expected_lift": 1.0
    },
    "Customer/Order Mgmt": {
        "action": "Herzie order lock-time afspraken met klant",
        "owner": "Account Mgmt",
        "expected_lift": 0.7
    },
    "IT/Integration": {
        "action": "Controleer WMS–TMS interfaces en corrigeer mapping",
        "owner": "IT",
        "expected_lift": 0.4
    },
    "Inventory": {
        "action": "Stockparameters en replenishment-cycli bijstellen",
        "owner": "Supply Chain",
        "expected_lift": 0.6
    },
    "Operations/Picking": {
        "action": "Capaciteitsplanning en pick-sequentie optimaliseren",
        "owner": "Operations",
        "expected_lift": 0.5
    },
    "Other": {
        "action": "Voer Gemba-walk en 5-Why analyse uit",
        "owner": "Quality",
        "expected_lift": 0.3
    },
    "Unknown": {
        "action": "Label data verbeteren en exceptions registreren",
        "owner": "Process Excellence",
        "expected_lift": 0.2
    }
}

def generate_action_plan(root_cause_result: dict, current_kpi_pct: float, target_sla: float = 97.0, horizon_days: int = 14) -> list[dict]:
    due = (datetime.now(timezone.utc) + timedelta(days=horizon_days)).date().isoformat()
    plan = []
    needed_lift = max(0.0, target_sla - current_kpi_pct)
    for rc in root_cause_result.get("pareto", []):
        bucket = rc["reason_bucket"]
        rec = SUGGESTIONS.get(bucket, SUGGESTIONS["Other"]).copy()
        impact = min(needed_lift, rec["expected_lift"] * rc["share_pct"] / 10.0)
        plan.append({
            "oorzaak_cluster": bucket,
            "impact_pct_pts": round(impact, 2),
            "aanbevolen_maatregel": rec["action"],
            "verantwoordelijke": rec["owner"],
            "deadline": due
        })
    return sorted(plan, key=lambda x: x["impact_pct_pts"], reverse=True)
