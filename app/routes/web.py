from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.pipeline import run_pipeline
from app.utils.files import ensure_directory, save_upload_file

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

UPLOADS_DIR = Path("data/uploads")
OUTPUTS_DIR = Path("data/outputs")


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={}
    )


@router.post("/process", response_class=HTMLResponse)
async def process_reports(
    request: Request,
    report_1: UploadFile = File(...),
    report_2: UploadFile = File(...),
    report_3: UploadFile = File(...),
    report_4: UploadFile = File(...),
    a_threshold: int = Form(...),
    b_threshold: int = Form(...),
):
    if a_threshold >= b_threshold:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "error_message": "Порог A должен быть меньше порога B."
            }
        )

    job_id = uuid4().hex

    job_upload_dir = UPLOADS_DIR / job_id
    job_output_dir = OUTPUTS_DIR / job_id

    ensure_directory(job_upload_dir)
    ensure_directory(job_output_dir)

    uploaded_files = [report_1, report_2, report_3, report_4]
    saved_file_names = []
    saved_file_paths = []

    for upload in uploaded_files:
        safe_name = Path(upload.filename).name
        destination = job_upload_dir / safe_name
        await save_upload_file(upload, destination)
        saved_file_names.append(safe_name)
        saved_file_paths.append(destination)

    try:
        pipeline_result = run_pipeline(
            report_paths=saved_file_paths,
            output_dir=job_output_dir,
            a_threshold=a_threshold,
            b_threshold=b_threshold,
        )
    except Exception as exc:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "error_message": f"Ошибка обработки файлов: {str(exc)}"
            }
        )

    return templates.TemplateResponse(
        request=request,
        name="result.html",
        context={
            "job_id": job_id,
            "a_threshold": a_threshold,
            "b_threshold": b_threshold,
            "uploaded_files": saved_file_names,
            "merged_rows": pipeline_result["merged_rows"],
            "summary_rows": pipeline_result["summary_rows"],
            "preview_rows": pipeline_result["preview_rows"],
            "quality_report_path": str(pipeline_result["quality_report_path"]),
            "quality_report": pipeline_result["quality_report"],
        }
    )


@router.get("/download/{job_id}/{file_type}", name="download_file")
async def download_file(job_id: str, file_type: str):
    allowed_files = {
        "csv": "summary.csv",
        "xlsx": "summary.xlsx",
        "quality": "quality_report.json",
    }

    if file_type not in allowed_files:
        raise HTTPException(status_code=404, detail="Неизвестный тип файла")

    file_path = OUTPUTS_DIR / job_id / allowed_files[file_type]

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Файл не найден")

    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type="application/octet-stream",
    )