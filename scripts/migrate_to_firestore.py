"""
migrate_to_firestore.py
Migra DatabaseInicial.xlsx completa a Cloud Firestore.

REQUISITOS:
  1. Descargar serviceAccountKey.json desde:
     Firebase Console > Proyecto > Configuración > Cuentas de servicio > Generar clave privada
  2. Colocarlo en: geoviewer-amva/serviceAccountKey.json
  3. Ejecutar: py scripts/migrate_to_firestore.py

Colecciones resultantes en Firestore:
  Sondeos, Proyectos, Estratigrafias, SPT, Triaxial_Ciclico,
  Consolidacion, Corte_Directo, Limites_Atterberg, DownHole,
  Lin_Sismicas, Gamma, Col_Resonante
"""

import os, sys, math
from pathlib import Path
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
EXCEL_PATH = r"C:\Users\User\OneDrive\Desktop\DatabaseInicial.xlsx"
KEY_PATH   = Path(__file__).parent.parent / "serviceAccountKey.json"
BATCH_SIZE = 400   # Firestore max 500 ops per batch

SHEETS = [
    "Sondeos",
    "Proyectos",
    "Estratigrafias",
    "SPT",
    "Triaxial_Ciclico",
    "Consolidacion",
    "Corte_Directo",
    "Limites_Atterberg",
    "DownHole",
    "Lin_Sismicas",
    "Gamma",
    "Col_Resonante",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def clean_value(v):
    """Convierte tipos Python a tipos Firestore-compatibles."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)

def df_to_docs(df: pd.DataFrame) -> list[dict]:
    docs = []
    for _, row in df.iterrows():
        doc = {k: clean_value(v) for k, v in row.items()}
        docs.append(doc)
    return docs

def upload_collection(db, collection_name: str, docs: list[dict], id_field="Id"):
    print(f"\n→ {collection_name}: {len(docs)} documentos", end="", flush=True)
    col_ref = db.collection(collection_name)
    batch   = db.batch()
    count   = 0

    for i, doc in enumerate(docs):
        doc_id  = str(doc.get(id_field, i))
        doc_ref = col_ref.document(doc_id)
        batch.set(doc_ref, doc)
        count += 1

        if count >= BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            count = 0
            print(".", end="", flush=True)

    if count:
        batch.commit()
        print(".", end="", flush=True)

    print(f" ✓")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not KEY_PATH.exists():
        print(f"ERROR: No se encontró {KEY_PATH}")
        print("Descarga la clave de servicio desde Firebase Console.")
        sys.exit(1)

    if not os.path.exists(EXCEL_PATH):
        print(f"ERROR: No se encontró {EXCEL_PATH}")
        sys.exit(1)

    print("Iniciando migración GeoViewer AMVA → Firestore")
    print(f"  Excel: {EXCEL_PATH}")
    print(f"  Clave: {KEY_PATH}")

    cred = credentials.Certificate(str(KEY_PATH))
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    xls = pd.ExcelFile(EXCEL_PATH)

    for sheet in SHEETS:
        if sheet not in xls.sheet_names:
            print(f"  ⚠ Hoja '{sheet}' no encontrada, omitiendo.")
            continue

        df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
        df = df.where(pd.notna(df), None)

        # Para hojas numéricas, re-convertir columnas numéricas
        numeric_sheets = {
            "Sondeos":       ["Cota","Nivel_freatico","Coordenada_Este","Coordenada_Norte"],
            "SPT":           ["Profundidad","Valor"],
            "DownHole":      ["Profundidad","Vp","Vs"],
            "Estratigrafias":["NumeroCapa","Espesor"],
            "Limites_Atterberg":["Profundidad","W","LL","LP"],
            "Gamma":         ["Profundidad","Peso_Unitario","Gravedad_Especifica","Pensiometro"],
            "Col_Resonante": ["Profundidad","Numero_Muestra","Y","G","Beta"],
            "Triaxial_Ciclico":["Profundidad","Numero_Ciclico","Beta","Epsilon","Modulo_Elastico","Modulo_Poisson"],
            "Consolidacion": ["Profundidad","e_value","Gs","Cc","Cs"],
            "Corte_Directo": ["Profundidad","Angulo_Friccion","Cohesion"],
            "Lin_Sismicas":  ["Coordenada_Norte_Ini","Coordenada_Este_Ini","Coordenada_Norte_Fin","Coordenada_Este_Fin"],
        }
        for col in numeric_sheets.get(sheet, []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        docs = df_to_docs(df)
        upload_collection(db, sheet, docs)

    print("\n✅ Migración completada.")
    print("   Colecciones creadas en: https://console.firebase.google.com/project/geoviewer-amva/firestore")

if __name__ == "__main__":
    main()
