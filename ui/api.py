from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from server import catalog as cat
from server import comparison as comp
from server import db
from server import jobs as job_svc
from server import profile as prof
from server.query_engine import connect, quote
from server.sql_guard import validate as sql_validate


class RefreshCatalogRequest(BaseModel):
    source_folder: Optional[str] = None
    target_folder: Optional[str] = None
    include_row_counts: bool = False


class PairOverrideRequest(BaseModel):
    source_dataset_id: str
    target_dataset_id: str
    enabled: bool = True
    key_mappings: Optional[List[Dict[str, str]]] = None
    compare_mappings: Optional[List[Dict[str, str]]] = None


class SaveKeyPresetRequest(BaseModel):
    name: str = Field(default="default")
    key_fields: List[str]


class SqlPreviewRequest(BaseModel):
    sql: str
    limit: int = Field(default=10, ge=1, le=100)


class FilteredPreviewRequest(BaseModel):
    filter_spec: Dict[str, Any]
    limit: int = Field(default=10, ge=1, le=100)


class ComboSummaryRequest(BaseModel):
    columns: List[str]
    top_n: int = Field(default=10, ge=1, le=100)


class StartCompareRequest(BaseModel):
    source_dataset_id: str
    target_dataset_id: str
    key_fields: List[str] = Field(default_factory=list)
    pair_id: Optional[str] = None
    compare_fields: Optional[List[str]] = None
    key_mappings: Optional[List[Dict[str, str]]] = None
    compare_mappings: Optional[List[Dict[str, str]]] = None


class QuickCompareRequest(BaseModel):
    source_dataset_id: str
    target_dataset_id: str
    key_fields: List[str] = Field(default_factory=list)
    compare_fields: Optional[List[str]] = None
    key_mappings: Optional[List[Dict[str, str]]] = None
    compare_mappings: Optional[List[Dict[str, str]]] = None
    sample_limit: int = Field(default=10, ge=1, le=100)


def _clean_field_mappings(mappings: Optional[List[Dict[str, str]]]) -> Optional[List[Dict[str, str]]]:
    if not mappings:
        return None
    cleaned: List[Dict[str, str]] = []
    for m in mappings:
        src = (m.get("source_field") or m.get("source") or "").strip()
        tgt = (m.get("target_field") or m.get("target") or "").strip()
        if src and tgt:
            cleaned.append({"source_field": src, "target_field": tgt})
    return cleaned or None


def _datasets_or_404() -> List[Dict[str, Any]]:
    conn = db.get_connection()
    datasets = db.list_datasets(conn)
    conn.close()
    if not datasets:
        raise HTTPException(status_code=400, detail="No datasets loaded. Run catalog refresh first.")
    return datasets


app = FastAPI(title="DM Helper Admin UI", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_STATIC_DIR = Path(__file__).resolve().parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/settings/folders")
def get_folders() -> Dict[str, str]:
    conn = db.get_connection()
    source = db.get_meta(conn, "source_folder", "") or ""
    target = db.get_meta(conn, "target_folder", "") or ""
    conn.close()
    return {"source_folder": source, "target_folder": target}


@app.get("/api/system/browse-folder")
def browse_folder(initial: Optional[str] = None) -> Dict[str, str]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Folder picker is unavailable: {exc}")

    initial_dir = initial if initial and os.path.isdir(initial) else str(Path.home())
    root = None
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        selected = filedialog.askdirectory(initialdir=initial_dir, mustexist=True) or ""
        return {"folder": selected}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to open folder picker: {exc}")
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:
                pass


@app.post("/api/catalog/refresh")
def refresh_catalog(req: RefreshCatalogRequest) -> Dict[str, Any]:
    return cat.refresh_catalog(
        source_folder=req.source_folder,
        target_folder=req.target_folder,
        include_row_counts=req.include_row_counts,
    )


@app.get("/api/datasets")
def list_datasets(side: Optional[str] = None, filter: Optional[str] = None) -> List[Dict[str, Any]]:
    return cat.get_datasets(side=side, filter_text=filter)


@app.get("/api/datasets/{dataset_id}")
def get_dataset(dataset_id: str) -> Dict[str, Any]:
    ds = cat.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")
    return ds


@app.get("/api/datasets/{dataset_id}/fields")
def get_fields(dataset_id: str) -> Dict[str, Any]:
    ds = cat.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")
    return {"dataset": dataset_id, "columns": ds["columns"], "column_count": len(ds["columns"])}


@app.get("/api/datasets/{dataset_id}/preview")
def preview_dataset(
    dataset_id: str,
    limit: int = 10,
    offset: int = 0,
    fields: Optional[str] = None,
) -> Dict[str, Any]:
    ds = cat.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found.")

    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    field_list = [f.strip() for f in fields.split(",")] if fields else None

    with connect([ds]) as duck:
        view = quote(dataset_id)
        sel = "*"
        selected_fields: Optional[List[str]] = None
        if field_list:
            selected_fields = [f for f in field_list if f in ds["columns"]]
            if selected_fields:
                sel = ", ".join(quote(f) for f in selected_fields)

        rows_cur = duck.execute(
            f"SELECT {sel} FROM {view} LIMIT {limit} OFFSET {offset}"
        )
        headers = [d[0] for d in rows_cur.description]
        rows = [list(r) for r in rows_cur.fetchall()]
        total = duck.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]

    return {
        "dataset": dataset_id,
        "headers": headers,
        "rows": rows,
        "total_rows": total,
        "limit": limit,
        "offset": offset,
        "selected_fields": selected_fields,
    }


@app.post("/api/sql/preview")
def sql_preview(req: SqlPreviewRequest) -> Dict[str, Any]:
    ok, err = sql_validate(req.sql)
    if not ok:
        raise HTTPException(status_code=400, detail=err)

    datasets = _datasets_or_404()
    clean = req.sql.strip().rstrip(";")
    sql_to_run = clean
    if "LIMIT" not in clean.upper():
        sql_to_run = f"{clean} LIMIT {req.limit}"

    with connect(datasets) as duck:
        try:
            total = duck.execute(f"SELECT COUNT(*) FROM ({clean}) _q").fetchone()[0]
        except Exception:
            total = None
        result = duck.execute(sql_to_run)
        headers = [d[0] for d in result.description]
        rows = [list(r) for r in result.fetchall()]

    return {
        "headers": headers,
        "rows": rows,
        "row_count": len(rows),
        "total_rows": total if total is not None else len(rows),
        "limit_applied": req.limit,
    }


@app.get("/api/profile/{dataset_id}")
def data_profile(dataset_id: str) -> Dict[str, Any]:
    return prof.data_profile(dataset_id)


@app.get("/api/summary/column/{dataset_id}")
def column_summary(
    dataset_id: str,
    column: Optional[str] = None,
    top_n: int = 10,
) -> Dict[str, Any]:
    top_n = max(1, min(int(top_n), 100))
    return prof.column_value_summary(dataset_id, column=column, top_n=top_n)


@app.post("/api/summary/combo/{dataset_id}")
def combo_summary(dataset_id: str, req: ComboSummaryRequest) -> Dict[str, Any]:
    return prof.combo_value_summary(dataset_id, req.columns, top_n=req.top_n)


@app.post("/api/preview/filtered/{dataset_id}")
def filtered_preview(dataset_id: str, req: FilteredPreviewRequest) -> Dict[str, Any]:
    return prof.preview_filtered_records(dataset_id, req.filter_spec, limit=req.limit)


@app.get("/api/duplicates/{dataset_id}")
def duplicates(dataset_id: str, key_fields: str, limit: int = 10) -> Dict[str, Any]:
    keys = [k.strip() for k in key_fields.split(",") if k.strip()]
    return prof.find_duplicates(dataset_id, key_columns=keys, limit=limit)


@app.get("/api/value-distribution/{dataset_id}")
def distribution(dataset_id: str, column: str, limit: int = 20) -> Dict[str, Any]:
    return prof.value_distribution(dataset_id, column=column, limit=limit)


@app.get("/api/pairs")
def list_pairs() -> List[Dict[str, Any]]:
    return cat.get_pairs()


@app.post("/api/pairs/override")
def upsert_pair_override(req: PairOverrideRequest) -> Dict[str, Any]:
    return cat.upsert_pair_override(
        source_id=req.source_dataset_id,
        target_id=req.target_dataset_id,
        enabled=req.enabled,
        key_mappings=_clean_field_mappings(req.key_mappings),
        compare_mappings=_clean_field_mappings(req.compare_mappings),
    )


@app.get("/api/pairs/resolve")
def resolve_pair(source_dataset_id: str, target_dataset_id: str) -> Dict[str, Any]:
    pair = cat.get_pair_by_datasets(source_dataset_id, target_dataset_id)
    return {"pair": pair}


@app.get("/api/pairs/quick-map")
def quick_map_pair(source_dataset_id: str, target_dataset_id: str) -> Dict[str, Any]:
    src = cat.get_dataset(source_dataset_id)
    tgt = cat.get_dataset(target_dataset_id)
    if not src:
        raise HTTPException(status_code=404, detail=f"Source dataset '{source_dataset_id}' not found.")
    if not tgt:
        raise HTTPException(status_code=404, detail=f"Target dataset '{target_dataset_id}' not found.")

    tgt_lookup = {c.lower(): c for c in tgt["columns"]}
    compare_mappings: List[Dict[str, str]] = []
    for s_col in src["columns"]:
        t_col = tgt_lookup.get(s_col.lower())
        if t_col:
            compare_mappings.append({"source_field": s_col, "target_field": t_col})

    return {
        "source_dataset_id": source_dataset_id,
        "target_dataset_id": target_dataset_id,
        "match_count": len(compare_mappings),
        "compare_mappings": compare_mappings,
    }


@app.get("/api/pairs/{pair_id}/suggest-keys")
def suggest_keys(pair_id: str) -> Dict[str, Any]:
    return prof.suggest_keys(pair_id)


@app.get("/api/pairs/{pair_id}/key-presets")
def list_key_presets(pair_id: str) -> List[Dict[str, Any]]:
    conn = db.get_connection()
    presets = db.list_key_presets(conn, pair_id)
    conn.close()
    return presets


@app.post("/api/pairs/{pair_id}/key-presets")
def save_key_preset(pair_id: str, req: SaveKeyPresetRequest) -> Dict[str, Any]:
    fields = [f.strip() for f in req.key_fields if f.strip()]
    if not fields:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    conn = db.get_connection()
    preset_id = db.save_key_preset(conn, pair_id, req.name, fields)
    conn.close()
    return {"preset_id": preset_id, "pair_id": pair_id, "name": req.name, "key_fields": fields}


@app.get("/api/schema-diff")
def schema_diff(source_dataset_id: str, target_dataset_id: str) -> Dict[str, Any]:
    return cat.schema_diff(source_dataset_id, target_dataset_id)


@app.post("/api/compare/start")
def start_compare(req: StartCompareRequest) -> Dict[str, Any]:
    key_mappings = _clean_field_mappings(req.key_mappings)
    compare_mappings = _clean_field_mappings(req.compare_mappings)
    keys = [k.strip() for k in req.key_fields if k.strip()]
    effective_keys = keys or [m["source_field"] for m in (key_mappings or [])]
    if not key_mappings and not keys:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    compare_fields = [c.strip() for c in (req.compare_fields or []) if c.strip()] or None
    return job_svc.start_comparison_job(
        source_id=req.source_dataset_id,
        target_id=req.target_dataset_id,
        key_columns=effective_keys,
        key_mappings=key_mappings,
        pair_id=req.pair_id,
        compare_columns=compare_fields,
        compare_mappings=compare_mappings,
        options={
            "key_mappings": key_mappings or [],
            "compare_mappings": compare_mappings or [],
        },
    )


@app.get("/api/compare/quick")
def quick_compare(
    source_dataset_id: str,
    target_dataset_id: str,
    key_fields: str,
    compare_fields: Optional[str] = None,
    sample_limit: int = 10,
) -> Dict[str, Any]:
    keys = [k.strip() for k in key_fields.split(",") if k.strip()]
    if not keys:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    comps = [c.strip() for c in compare_fields.split(",")] if compare_fields else None
    return comp.compare_datasets(
        source_id=source_dataset_id,
        target_id=target_dataset_id,
        key_columns=keys,
        compare_columns=comps,
        sample_limit=sample_limit,
    )


@app.post("/api/compare/quick")
def quick_compare_post(req: QuickCompareRequest) -> Dict[str, Any]:
    key_mappings = _clean_field_mappings(req.key_mappings)
    compare_mappings = _clean_field_mappings(req.compare_mappings)
    keys = [k.strip() for k in req.key_fields if k.strip()]
    effective_keys = keys or [m["source_field"] for m in (key_mappings or [])]
    compare_fields = [c.strip() for c in (req.compare_fields or []) if c.strip()] or None
    if not key_mappings and not keys:
        raise HTTPException(status_code=400, detail="At least one key field is required.")
    return comp.compare_datasets(
        source_id=req.source_dataset_id,
        target_id=req.target_dataset_id,
        key_columns=effective_keys,
        compare_columns=compare_fields,
        key_mappings=key_mappings,
        compare_mappings=compare_mappings,
        sample_limit=req.sample_limit,
    )


@app.get("/api/jobs")
def list_jobs(limit: int = 50) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    return job_svc.list_jobs(limit=limit)


@app.get("/api/jobs/{job_id}")
def get_job_status(job_id: str) -> Dict[str, Any]:
    result = job_svc.get_job_status(job_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.get("/api/jobs/{job_id}/summary")
def get_job_summary(job_id: str) -> Dict[str, Any]:
    result = job_svc.get_job_summary(job_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> Dict[str, Any]:
    result = job_svc.cancel_job(job_id)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.get("/api/reports")
def list_reports() -> List[Dict[str, Any]]:
    conn = db.get_connection()
    rows = db.list_reports(conn)
    conn.close()
    return rows


@app.get("/api/reports/{report_id}")
def report_metadata(report_id: str) -> Dict[str, Any]:
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    conn.close()
    if not report:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")
    return report


@app.get("/api/reports/{report_id}/download")
def download_report(report_id: str):
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    conn.close()
    if not report:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")
    path = report["file_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Report file not found: {path}")
    return FileResponse(path, filename=report["file_name"])


@app.delete("/api/reports/{report_id}")
def delete_report(report_id: str) -> Dict[str, Any]:
    conn = db.get_connection()
    report = db.get_report(conn, report_id)
    if not report:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")

    try:
        if os.path.exists(report["file_path"]):
            os.remove(report["file_path"])
    except Exception:
        pass

    db.delete_report(conn, report_id)
    conn.close()
    return {"deleted": report_id}


@app.get("/")
def serve_ui():
    index_path = _STATIC_DIR / "index.html"
    if not index_path.exists():
        return JSONResponse(
            status_code=200,
            content={
                "message": "UI static files not found.",
                "hint": "Create ui/static/index.html and related assets.",
            },
        )
    return FileResponse(str(index_path))
