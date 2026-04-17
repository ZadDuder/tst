from pathlib import Path
from fastapi import UploadFile


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


async def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    content = await upload_file.read()
    destination.write_bytes(content)