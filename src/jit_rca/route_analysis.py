from __future__ import annotations
import pandas as pd
import numpy as np

def analyze_routes(df: pd.DataFrame) -> pd.DataFrame:
    if not {'cnr_tour','planned_time','actual_time'}.issubset(df.columns):
        raise ValueError("Vereiste kolommen: cnr_tour, planned_time, actual_time")
    df = df.copy()
    df['planned_time'] = pd.to_datetime(df['planned_time'], errors='coerce', utc=True)
    df['actual_time']  = pd.to_datetime(df['actual_time'],  errors='coerce', utc=True)
    df = df.sort_values(['cnr_tour','planned_time'])
    df['delay_min'] = (df['actual_time'] - df['planned_time']).dt.total_seconds()/60.0
    route_summary = (
        df.groupby('cnr_tour')
        .agg(stops=('cnr_tour','count'),
             start_delay=('delay_min','first'),
             end_delay=('delay_min','last'),
             avg_delay=('delay_min','mean'),
             max_delay=('delay_min','max'))
        .reset_index()
    )
    conditions = [
        route_summary['start_delay'] > 10,
        (route_summary['start_delay'] <= 10) & (route_summary['max_delay'] > 15),
    ]
    choices = ['Planning/Dispatch', 'Sequencing / Route design']
    route_summary['root_cause_cluster'] = np.select(conditions, choices, default='Stable / On time')
    return route_summary
