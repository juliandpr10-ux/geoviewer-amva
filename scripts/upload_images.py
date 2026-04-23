"""
upload_images.py
Sube las 238 tomografías PNG a Firebase Storage.

Las imágenes están en formato ClickOnce (.png.deploy).
Este script las lee directamente y las sube como PNG.

REQUISITOS:
  serviceAccountKey.json en raíz del proyecto
  py -m pip install firebase-admin  (ya instalado)

USO:
  py scripts/upload_images.py
"""

import os, sys
from pathlib import Path
import firebase_admin
from firebase_admin import credentials, storage

# ── Config ────────────────────────────────────────────────────────────────────
IMAGES_DIR  = Path(r"C:\Users\User\OneDrive\Desktop\BDManager\BDManager V2\InstalaBD\Application Files\GeotechnicBDManager_1_0_0_35\ImagenesLineas")
KEY_PATH    = Path(__file__).parent.parent / "serviceAccountKey.json"
BUCKET_NAME = "geoviewer-amva.firebasestorage.app"
STORAGE_PREFIX = "tomografias/"   # carpeta en Storage

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not KEY_PATH.exists():
        print(f"ERROR: No se encontró {KEY_PATH}")
        sys.exit(1)

    if not IMAGES_DIR.exists():
        print(f"ERROR: No se encontró directorio de imágenes: {IMAGES_DIR}")
        sys.exit(1)

    # Inicializar Firebase
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(KEY_PATH))
        firebase_admin.initialize_app(cred, {"storageBucket": BUCKET_NAME})

    bucket = storage.bucket()

    # Buscar archivos .deploy que son PNG
    deploy_files = sorted(IMAGES_DIR.glob("*.deploy"))
    png_files = [f for f in deploy_files if f.stem.lower().endswith('.png') or f.stem.lower().endswith('.PNG')]

    print(f"Subiendo {len(png_files)} imágenes PNG a Firebase Storage...")
    print(f"  Bucket: {BUCKET_NAME}")
    print(f"  Carpeta: {STORAGE_PREFIX}")
    print()

    urls = {}
    errors = []

    for i, deploy_path in enumerate(png_files, 1):
        # El nombre real es el stem (sin .deploy)
        png_name = deploy_path.stem  # ej: "1.png" o "10.PNG"
        storage_path = STORAGE_PREFIX + png_name.lower()

        try:
            blob = bucket.blob(storage_path)
            blob.upload_from_filename(
                str(deploy_path),
                content_type="image/png"
            )
            blob.make_public()
            url = blob.public_url
            img_id = png_name.lower().replace('.png', '')
            urls[img_id] = url

            print(f"  [{i:3d}/{len(png_files)}] {png_name} → {storage_path} ✓")

        except Exception as e:
            print(f"  [{i:3d}/{len(png_files)}] {png_name} ERROR: {e}")
            errors.append((png_name, str(e)))

    # Guardar mapeo de URLs
    output_path = Path(__file__).parent / "image_urls.py"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("# Generado automáticamente por upload_images.py\n")
        f.write("# Mapeo: ID numérico -> URL pública de Firebase Storage\n\n")
        f.write("IMAGE_URLS = {\n")
        for k, v in sorted(urls.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            f.write(f'    "{k}": "{v}",\n')
        f.write("}\n")

    print(f"\n✅ Subidas: {len(urls)}")
    if errors:
        print(f"❌ Errores: {len(errors)}")
        for name, err in errors:
            print(f"   {name}: {err}")

    print(f"\nMapa de URLs guardado en: {output_path}")
    print("Las URLs son públicas y se pueden usar en el dashboard.")

if __name__ == "__main__":
    main()
