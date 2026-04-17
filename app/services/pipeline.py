from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd


logger = logging.getLogger(__name__)

REQUIRED_CANONICAL_COLUMNS = [
    "employee",
    "operation_type",
    "service_time",
    "processing_time",
    "date",
]

COLUMN_ALIASES = {
    "employee": [
        "employee",
        "employee_name",
        "user",
        "operator",
        "сотрудник",
        "сотрудник фио",
        "фио сотрудника",
        "оператор",
    ],
    "operation_type": [
        "operation_type",
        "operation",
        "service_type",
        "тип операции",
        "операция",
        "тип услуги",
    ],
    "service_time": [
        "service_time",
        "service_duration",
        "время обслуживания",
        "длительность обслуживания",
    ],
    "processing_time": [
        "processing_time",
        "processing_duration",
        "время обработки",
        "длительность обработки",
    ],
    "date": [
        "date",
        "report_date",
        "дата",
        "дата отчета",
        "дата отчёта",
    ],
}


def normalize_column_name(name: str) -> str:
    return (
        str(name)
        .strip()
        .lower()
        .replace("ё", "е")
        .replace("\n", " ")
        .replace("\t", " ")
    )


def read_report(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    logger.info("Reading report: %s", file_path)

    if suffix == ".csv":
        return pd.read_csv(file_path)

    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(file_path)

    raise ValueError(f"Unsupported file format: {file_path.name}")


def build_column_mapping(df: pd.DataFrame) -> dict[str, str]:
    normalized_columns = {col: normalize_column_name(col) for col in df.columns}
    mapping: dict[str, str] = {}

    for canonical_name, aliases in COLUMN_ALIASES.items():
        normalized_aliases = {normalize_column_name(alias) for alias in aliases}

        matched_source_column = None
        for original_col, normalized_col in normalized_columns.items():
            if normalized_col in normalized_aliases:
                matched_source_column = original_col
                break

        if matched_source_column is None:
            raise ValueError(
                f"Не найдена обязательная колонка '{canonical_name}'. "
                f"Доступные колонки: {list(df.columns)}"
            )

        mapping[matched_source_column] = canonical_name

    return mapping


def parse_duration_to_seconds(value) -> float:
    if pd.isna(value):
        return 0.0

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()

    if not text:
        return 0.0

    text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        pass

    parts = text.split(":")
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)

    if len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + float(seconds)

    raise ValueError(f"Не удалось распарсить время: '{value}'")


def classify(avg_total_time_sec: float, a_threshold: int, b_threshold: int) -> str:
    if avg_total_time_sec <= a_threshold:
        return "A"
    if avg_total_time_sec <= b_threshold:
        return "B"
    return "C"


def normalize_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    initial_rows = len(df)

    mapping = build_column_mapping(df)
    normalized_df = df.rename(columns=mapping).copy()
    normalized_df = normalized_df[REQUIRED_CANONICAL_COLUMNS].copy()

    normalized_df["employee"] = normalized_df["employee"].astype(str).str.strip()
    normalized_df["operation_type"] = (
        normalized_df["operation_type"].astype(str).str.strip()
    )

    normalized_df["date"] = pd.to_datetime(
        normalized_df["date"], errors="coerce"
    ).dt.date

    normalized_df["service_time_sec"] = normalized_df["service_time"].apply(
        parse_duration_to_seconds
    )
    normalized_df["processing_time_sec"] = normalized_df["processing_time"].apply(
        parse_duration_to_seconds
    )

    normalized_df["total_time_sec"] = (
        normalized_df["service_time_sec"] + normalized_df["processing_time_sec"]
    )

    before_drop_invalid_dates = len(normalized_df)
    normalized_df = normalized_df.dropna(subset=["date"])
    dropped_invalid_dates = before_drop_invalid_dates - len(normalized_df)

    quality = {
        "initial_rows": initial_rows,
        "rows_after_normalization": len(normalized_df),
        "dropped_invalid_dates": dropped_invalid_dates,
    }

    return normalized_df, quality


def process_single_report(file_path: Path) -> tuple[pd.DataFrame, dict]:
    raw_df = read_report(file_path)
    logger.info("Rows read from %s: %s", file_path.name, len(raw_df))

    normalized_df, quality = normalize_dataframe(raw_df)
    normalized_df["source_file"] = file_path.name

    file_quality = {
        "file_name": file_path.name,
        **quality,
    }

    logger.info(
        "Processed file %s | initial_rows=%s | rows_after_normalization=%s | dropped_invalid_dates=%s",
        file_path.name,
        file_quality["initial_rows"],
        file_quality["rows_after_normalization"],
        file_quality["dropped_invalid_dates"],
    )

    return normalized_df, file_quality


def build_summary(
    merged_df: pd.DataFrame,
    a_threshold: int,
    b_threshold: int,
) -> pd.DataFrame:
    summary_df = (
        merged_df.groupby(["employee", "operation_type"], as_index=False)
        .agg(
            clients_count=("total_time_sec", "count"),
            min_time_sec=("total_time_sec", "min"),
            max_time_sec=("total_time_sec", "max"),
            avg_time_sec=("total_time_sec", "mean"),
        )
        .sort_values(by=["employee", "operation_type"])
        .reset_index(drop=True)
    )

    summary_df["category"] = summary_df["avg_time_sec"].apply(
        lambda x: classify(x, a_threshold, b_threshold)
    )

    summary_df["min_time_sec"] = summary_df["min_time_sec"].round(2)
    summary_df["max_time_sec"] = summary_df["max_time_sec"].round(2)
    summary_df["avg_time_sec"] = summary_df["avg_time_sec"].round(2)

    return summary_df


def save_outputs(summary_df: pd.DataFrame, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "summary.csv"
    xlsx_path = output_dir / "summary.xlsx"

    summary_df.to_csv(csv_path, index=False)
    summary_df.to_excel(xlsx_path, index=False)

    return {
        "csv": csv_path,
        "xlsx": xlsx_path,
    }


def save_quality_report(quality_report: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    quality_path = output_dir / "quality_report.json"
    quality_path.write_text(
        json.dumps(quality_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return quality_path


def run_pipeline(
    report_paths: Iterable[Path],
    output_dir: Path,
    a_threshold: int,
    b_threshold: int,
) -> dict:
    logger.info("Pipeline started")

    dataframes = []
    file_reports = []

    for path in report_paths:
        df, file_quality = process_single_report(path)
        dataframes.append(df)
        file_reports.append(file_quality)

    merged_df = pd.concat(dataframes, ignore_index=True)
    logger.info("Merged dataframe rows: %s", len(merged_df))

    summary_df = build_summary(
        merged_df=merged_df,
        a_threshold=a_threshold,
        b_threshold=b_threshold,
    )
    logger.info("Summary rows: %s", len(summary_df))

    output_files = save_outputs(summary_df, output_dir)

    quality_report = {
        "files_processed": len(file_reports),
        "thresholds": {
            "a_threshold": a_threshold,
            "b_threshold": b_threshold,
        },
        "merged_rows": len(merged_df),
        "summary_rows": len(summary_df),
        "files": file_reports,
    }

    quality_report_path = save_quality_report(quality_report, output_dir)

    preview_rows = summary_df.head(20).to_dict(orient="records")

    logger.info("Pipeline completed successfully")

    return {
        "merged_rows": len(merged_df),
        "summary_rows": len(summary_df),
        "preview_rows": preview_rows,
        "output_files": output_files,
        "quality_report_path": quality_report_path,
        "quality_report": quality_report,
    }