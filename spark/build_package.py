"""Build spark/dist/ridespark.zip (the --py-files archive). Run from anywhere: python spark/build_package.py"""
import zipfile
from pathlib import Path

root = Path(__file__).resolve().parent
dist = root / "dist"
dist.mkdir(exist_ok=True)
target = dist / "ridespark.zip"
with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
    for path in sorted((root / "ridespark").rglob("*.py")):
        archive.write(path, path.relative_to(root).as_posix())
print(f"wrote {target}")
