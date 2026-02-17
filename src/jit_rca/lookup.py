# src/jit_rca/lookup.py
from __future__ import annotations
import pandas as pd

# Toegestane klanten + labels
CUSTOMER_LABELS = {
    "Z41102": "express",
    "Z41103": "hyper",
    "Z41104": "partner",
    "Z41105": "Super / MKTI",
    "Z41108": "B2B",
}

ALLOWED_CUSTOMERS = set(CUSTOMER_LABELS.keys())

def add_customer_label(df: pd.DataFrame, key_col: str = "customer") -> pd.DataFrame:
    """Voegt kolom 'customer_label' toe op basis van CUSTOMERS_LABELS."""
    df = df.copy()
    if key_col in df.columns:
        df["customer_label"] = df[key_col].map(CUSTOMER_LABELS).fillna("")
    else:
        df["customer_label"] = ""
    return df

def filter_allowed_customers(df: pd.DataFrame, key_col: str = "customer") -> pd.DataFrame:
    """Houdt enkel de klanten uit ALLOWED_CUSTOMERS over."""
    if key_col not in df.columns:
        return df.iloc[0:0].copy()  # leeg als kolom ontbreekt
    return df[df[key_col].isin(ALLOWED_CUSTOMERS)].copy()