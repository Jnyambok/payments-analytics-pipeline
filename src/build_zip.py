import os
import zipfile


INCLUDE_PATHS = [
    "README.md",
    "requirements.txt",
    "src",
    "sql",
    "presentation",
    "data",
]

EXCLUDE_DIRS = {"__pycache__", ".venv", ".git"}


def should_exclude(path: str) -> bool:
    parts = set(path.replace("\\", "/").split("/"))
    return any(p in EXCLUDE_DIRS for p in parts)


def add_path(zf: zipfile.ZipFile, path: str) -> None:
    if os.path.isfile(path):
        zf.write(path, arcname=path)
        return
    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            fp = os.path.join(root, f)
            if should_exclude(fp):
                continue
            zf.write(fp, arcname=fp)


def main() -> None:
    out = "deliverable.zip"
    if os.path.exists(out):
        os.remove(out)
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in INCLUDE_PATHS:
            if os.path.exists(p):
                add_path(zf, p)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()

