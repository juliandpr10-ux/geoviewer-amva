"""
upload_images.py
Sube las 238 tomografías PNG a Firebase Storage y actualiza
los documentos Lin_Sismicas en Firestore con la URL pública.

Las imágenes están en formato ClickOnce (.png.deploy).
Son PNGs reales — solo tienen extensión extra por ClickOnce packaging.

Rutas en Storage: imagenes/1.png, imagenes/2.png, ..., imagenes/238.png

USO:
  py scripts/upload_images.py            # sube todas
  py scripts/upload_images.py --desde 50 # reanuda desde imagen 50
  py scripts/upload_images.py --dry-run  # simula sin subir

REQUISITOS:
  Storage activado en Firebase Console (ver instrucciones al pie)
  serviceAccountKey: detectado automáticamente (*adminsdk*.json)
"""

import sys, time, argparse
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, storage, firestore

# ── Config ────────────────────────────────────────────────────────────────────
IMAGES_DIR = Path(r"C:\Users\User\OneDrive\Desktop\BDManager\BDManager V2\InstalaBD"
                  r"\Application Files\GeotechnicBDManager_1_0_0_35\ImagenesLineas")
ROOT        = Path(__file__).parent.parent
KEY_MATCHES = list(ROOT.glob("*adminsdk*.json"))
KEY_PATH    = KEY_MATCHES[0] if KEY_MATCHES else ROOT / "serviceAccountKey.json"
BUCKET      = "geoviewer-amva.firebasestorage.app"
STORAGE_DIR = "imagenes"          # carpeta en Storage
URLS_OUT    = ROOT / "scripts" / "image_urls.json"

# ── Helpers ───────────────────────────────────────────────────────────────────
def find_deploy_files() -> list[tuple[int, Path]]:
    """
    Devuelve lista de (numero, path) ordenada numéricamente.
    Archivos: '1.png.deploy', '10.PNG.deploy', etc.
    """
    files = []
    for f in IMAGES_DIR.iterdir():
        if not f.suffix.lower() == ".deploy":
            continue
        stem = f.stem          # '1.png' o '10.PNG'
        base = Path(stem)
        if base.suffix.lower() == ".png" and base.stem.isdigit():
            files.append((int(base.stem), f))
    return sorted(files, key=lambda x: x[0])

def storage_name(num: int) -> str:
    """Nombre normalizado en Storage: imagenes/1.png"""
    return f"{STORAGE_DIR}/{num}.png"

def public_url(blob) -> str:
    return f"https://storage.googleapis.com/{BUCKET}/{blob.name}"

def bar(done: int, total: int, width: int = 25) -> str:
    filled = int(done / total * width)
    return "#" * filled + "-" * (width - filled)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--desde",   type=int, default=1,
                        help="Número de imagen desde donde empezar (para reanudar)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula la subida sin hacer nada real")
    args = parser.parse_args()

    # Validaciones
    if not IMAGES_DIR.exists():
        print(f"ERROR: Directorio no encontrado:\n  {IMAGES_DIR}")
        sys.exit(1)
    if not KEY_PATH.exists():
        print(f"ERROR: Service account key no encontrada:\n  {KEY_PATH}")
        sys.exit(1)

    files = find_deploy_files()
    if not files:
        print("ERROR: No se encontraron archivos .png.deploy en el directorio.")
        sys.exit(1)

    # Filtrar desde --desde
    files = [(n, p) for n, p in files if n >= args.desde]

    print("=" * 58)
    print("  GeoViewer AMVA — Subida de imagenes a Storage")
    print("=" * 58)
    print(f"  Bucket   : {BUCKET}")
    print(f"  Carpeta  : {STORAGE_DIR}/")
    print(f"  Imagenes : {len(files)} (desde #{args.desde})")
    if args.dry_run:
        print("  Modo     : DRY-RUN (no sube nada)")
    print("=" * 58)

    if args.dry_run:
        for n, p in files[:5]:
            print(f"  [DRY] {p.name} -> {storage_name(n)}")
        if len(files) > 5:
            print(f"  ... ({len(files) - 5} mas)")
        return

    # Inicializar Firebase
    cred = credentials.Certificate(str(KEY_PATH))
    firebase_admin.initialize_app(cred, {"storageBucket": BUCKET})
    bucket_ref = storage.bucket()
    db         = firestore.client()

    # Cargar URLs ya subidas (si se reanuda)
    import json
    existing_urls: dict = {}
    if URLS_OUT.exists():
        existing_urls = json.loads(URLS_OUT.read_text(encoding="utf-8"))

    urls   = dict(existing_urls)
    errors = []
    t0     = time.time()

    total = len(files)
    for i, (num, deploy_path) in enumerate(files, 1):
        blob_name = storage_name(num)
        label     = f"  [{i:>3}/{total}]  {num:>3}.png"

        try:
            blob = bucket_ref.blob(blob_name)
            blob.upload_from_filename(str(deploy_path), content_type="image/png")
            blob.make_public()
            url = public_url(blob)
            urls[str(num)] = url

            elapsed = time.time() - t0
            rate    = i / elapsed
            eta     = int((total - i) / rate) if rate > 0 else 0
            print(f"\r{label}  [{bar(i, total)}]  ETA {eta}s   ", end="", flush=True)

        except Exception as e:
            err_msg = str(e)
            # Detectar Storage no activado
            if "does not exist" in err_msg or "bucket" in err_msg.lower():
                print(f"\n\n  ERROR DE STORAGE: {err_msg}")
                print("\n  ── Activa Firebase Storage ──────────────────────────")
                print("  1. Ve a: https://console.firebase.google.com/project/geoviewer-amva/storage")
                print("  2. Haz clic en 'Comenzar'")
                print("  3. Elige 'Modo de producción' y haz clic en 'Siguiente'")
                print("  4. Selecciona una región (ej: us-central1) y haz clic en 'Listo'")
                print("  5. Vuelve a ejecutar: py scripts/upload_images.py")
                print("  ─────────────────────────────────────────────────────\n")
                # Guardar lo que llevamos antes de salir
                _save_urls(urls, URLS_OUT)
                sys.exit(1)
            print(f"\n{label}  ERROR: {err_msg[:60]}")
            errors.append((num, err_msg))
            continue

    print()   # nueva línea tras el \r

    # Guardar mapeo JSON
    _save_urls(urls, URLS_OUT)

    # Actualizar documentos Lin_Sismicas en Firestore con URL correcta
    updated = _update_firestore_urls(db, urls)

    elapsed = int(time.time() - t0)
    print("\n" + "=" * 58)
    print(f"  Subidas      : {len(urls) - len(existing_urls)} nuevas  ({len(urls)} total)")
    print(f"  Errores      : {len(errors)}")
    print(f"  Firestore    : {updated} docs Lin_Sismicas actualizados")
    print(f"  Tiempo       : {elapsed}s")
    print(f"  URLs en      : scripts/image_urls.json")
    print("=" * 58)
    if errors:
        print("\n  Fallos:")
        for num, msg in errors:
            print(f"    {num}.png — {msg[:70]}")


def _save_urls(urls: dict, path: Path):
    import json
    ordered = {str(k): urls[str(k)] for k in sorted(urls.keys(), key=int)}
    path.write_text(json.dumps(ordered, indent=2, ensure_ascii=False), encoding="utf-8")


def _update_firestore_urls(db, urls: dict) -> int:
    """
    Actualiza el campo 'ImagenURL' en cada documento de Lin_Sismicas
    que tenga RutaImagen = '{n}.png' con la URL pública de Storage.
    """
    col_ref = db.collection("Lin_Sismicas")
    docs    = col_ref.stream()
    batch   = db.batch()
    count   = 0

    for doc in docs:
        data  = doc.to_dict()
        ruta  = (data.get("RutaImagen") or "").strip()
        # Solo rutas numéricas: '65.png', '1.png', etc.
        stem  = Path(ruta).stem
        if not stem.isdigit():
            continue
        url = urls.get(stem)
        if url:
            batch.update(doc.reference, {"ImagenURL": url})
            count += 1
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()

    if count % 400 != 0:
        batch.commit()

    return count


if __name__ == "__main__":
    main()
