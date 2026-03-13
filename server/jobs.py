"""
JobService.

Manages async/sync job lifecycle:
queued -> running -> succeeded / failed / canceled.
"""

from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor
import uuid
from typing import Any, Dict, List, Optional

from server import db
from server import reports as rpt
from server.comparison import compare_full
from server.reports import write_comparison_report
from server.query_engine import connect
from server.sql_guard import validate as sql_validate


def _job_workers() -> int:
    raw = (os.getenv("PROTOQUERY_JOB_WORKERS", "") or "").strip()
    if raw:
        try:
            value = int(raw)
            if value >= 1:
                return min(value, 8)
        except ValueError:
            pass
    return 2


_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=_job_workers(), thread_name_prefix="protoquery-job")
_JOB_FUTURES: Dict[str, Future] = {}


def _run_comparison_job(
    job_id: str,
    source_id: str,
    target_id: str,
    key_columns: List[str],
    key_mappings: Optional[List[Dict[str, str]]] = None,
    pair_id: Optional[str] = None,
    compare_columns: Optional[List[str]] = None,
    compare_mappings: Optional[List[Dict[str, str]]] = None,
) -> None:
    """Execute comparison/report generation for an existing queued job."""
    conn = db.get_connection()
    try:
        job = db.get_job(conn, job_id)
        if not job or job["state"] != "queued":
            return

        db.update_job_state(conn, job_id, "running")

        result = compare_full(
            source_id=source_id,
            target_id=target_id,
            key_columns=key_columns,
            compare_columns=compare_columns,
            key_mappings=key_mappings,
            compare_mappings=compare_mappings,
            conn=conn,
        )

        job = db.get_job(conn, job_id)
        if job and job["state"] == "canceled":
            return

        if "error" in result:
            db.update_job_state(conn, job_id, "failed", error_message=result["error"])
            return

        report_result = write_comparison_report(
            comparison_result=result,
            job_id=job_id,
            pair_id=pair_id,
            conn=conn,
        )
        if "error" in report_result:
            db.update_job_state(conn, job_id, "failed", error_message=report_result["error"])
            return

        progress = {
            "added": int(report_result.get("added", 0)),
            "removed": int(report_result.get("removed", 0)),
            "changed": int(report_result.get("changed", 0)),
        }

        job = db.get_job(conn, job_id)
        if job and job["state"] == "canceled":
            return

        db.update_job_state(conn, job_id, "succeeded", progress=progress)
    except Exception as exc:
        try:
            job = db.get_job(conn, job_id)
            if job and job["state"] != "canceled":
                db.update_job_state(conn, job_id, "failed", error_message=str(exc))
        except Exception:
            pass
    finally:
        conn.close()


def _execute_query_export(
    conn,
    sql: str,
    filename: Optional[str] = None,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    ok, err = sql_validate(sql)
    if not ok:
        return {"error": err}

    datasets = db.list_datasets(conn)
    if not datasets:
        return {"error": "No datasets loaded."}

    with connect(datasets) as duck:
        try:
            result = duck.execute(sql)
            headers = [d[0] for d in result.description]
        except Exception as exc:
            return {"error": str(exc)}

        return rpt.export_query_to_xlsx(
            headers=headers,
            rows=result,
            filename=filename,
            sql_query=sql,
            conn=conn,
            job_id=job_id,
        )


def _run_export_query_job(
    job_id: str,
    sql: str,
    filename: Optional[str] = None,
) -> None:
    """Execute query-export report generation for an existing queued job."""
    conn = db.get_connection()
    try:
        job = db.get_job(conn, job_id)
        if not job or job["state"] != "queued":
            return

        db.update_job_state(conn, job_id, "running")
        report_result = _execute_query_export(conn, sql, filename=filename, job_id=job_id)

        job = db.get_job(conn, job_id)
        if job and job["state"] == "canceled":
            return

        if "error" in report_result:
            db.update_job_state(conn, job_id, "failed", error_message=report_result["error"])
            return

        progress = {
            "row_count": int(report_result.get("row_count", 0)),
        }
        db.update_job_state(conn, job_id, "succeeded", progress=progress)
    except Exception as exc:
        try:
            job = db.get_job(conn, job_id)
            if job and job["state"] != "canceled":
                db.update_job_state(conn, job_id, "failed", error_message=str(exc))
        except Exception:
            pass
    finally:
        conn.close()


def start_comparison_job(
    source_id: str,
    target_id: str,
    key_columns: List[str],
    key_mappings: Optional[List[Dict[str, str]]] = None,
    pair_id: Optional[str] = None,
    compare_columns: Optional[List[str]] = None,
    compare_mappings: Optional[List[Dict[str, str]]] = None,
    options: Optional[Dict[str, Any]] = None,
    conn=None,
) -> Dict[str, Any]:
    """Create a comparison job and execute it.

    With external connections (mainly tests), execution stays synchronous.
    With normal app usage, execution runs in a background worker.
    """
    own = conn is None
    if own:
        conn = db.get_connection()

    job_id = f"job_{uuid.uuid4().hex[:8]}"

    # Create job record
    db.create_job(
        conn,
        job_id=job_id,
        source_dataset=source_id,
        target_dataset=target_id,
        key_fields=key_columns,
        pair_id=pair_id,
        options=options,
    )

    if own:
        conn.close()

    # Keep synchronous execution for externally supplied connections
    # (e.g. in-memory test DBs that cannot be shared across worker threads).
    if not own:
        db.update_job_state(conn, job_id, "running")
        try:
            result = compare_full(
                source_id=source_id,
                target_id=target_id,
                key_columns=key_columns,
                compare_columns=compare_columns,
                key_mappings=key_mappings,
                compare_mappings=compare_mappings,
                conn=conn,
            )
            if "error" in result:
                db.update_job_state(conn, job_id, "failed", error_message=result["error"])
                return {"job_id": job_id, "state": "failed", "error": result["error"]}
            report_result = write_comparison_report(
                comparison_result=result,
                job_id=job_id,
                pair_id=pair_id,
                conn=conn,
            )
            if "error" in report_result:
                db.update_job_state(conn, job_id, "failed", error_message=report_result["error"])
                return {"job_id": job_id, "state": "failed", "error": report_result["error"]}
            progress = {
                "added": int(report_result.get("added", 0)),
                "removed": int(report_result.get("removed", 0)),
                "changed": int(report_result.get("changed", 0)),
            }
            db.update_job_state(conn, job_id, "succeeded", progress=progress)
            return {"job_id": job_id, "state": "succeeded", "progress": progress, "report": report_result}
        except Exception as exc:
            db.update_job_state(conn, job_id, "failed", error_message=str(exc))
            return {"job_id": job_id, "state": "failed", "error": str(exc)}

    future = _JOB_EXECUTOR.submit(
        _run_comparison_job,
        job_id,
        source_id,
        target_id,
        key_columns,
        key_mappings,
        pair_id,
        compare_columns,
        compare_mappings,
    )
    _JOB_FUTURES[job_id] = future

    return {"job_id": job_id, "state": "queued"}


def start_export_query_job(
    sql: str,
    filename: Optional[str] = None,
    conn=None,
) -> Dict[str, Any]:
    """Create a query-export job and execute it."""
    own = conn is None
    if own:
        conn = db.get_connection()

    ok, err = sql_validate(sql)
    if not ok:
        if own:
            conn.close()
        return {"error": err}

    datasets = db.list_datasets(conn)
    if not datasets:
        if own:
            conn.close()
        return {"error": "No datasets loaded."}

    job_id = f"job_{uuid.uuid4().hex[:8]}"
    db.create_job(
        conn,
        job_id=job_id,
        source_dataset="query_export",
        target_dataset="query_export",
        key_fields=[],
        options={
            "type": "query_export",
            "filename": filename or "",
            "sql": sql,
        },
    )

    if own:
        conn.close()

    # Keep synchronous execution for externally supplied connections
    # (e.g. in-memory test DBs that cannot be shared across worker threads).
    if not own:
        db.update_job_state(conn, job_id, "running")
        try:
            report_result = _execute_query_export(conn, sql, filename=filename, job_id=job_id)
            if "error" in report_result:
                db.update_job_state(conn, job_id, "failed", error_message=report_result["error"])
                return {"job_id": job_id, "state": "failed", "error": report_result["error"]}
            progress = {"row_count": int(report_result.get("row_count", 0))}
            db.update_job_state(conn, job_id, "succeeded", progress=progress)
            return {"job_id": job_id, "state": "succeeded", "progress": progress, "report": report_result}
        except Exception as exc:
            db.update_job_state(conn, job_id, "failed", error_message=str(exc))
            return {"job_id": job_id, "state": "failed", "error": str(exc)}

    future = _JOB_EXECUTOR.submit(_run_export_query_job, job_id, sql, filename)
    _JOB_FUTURES[job_id] = future
    return {"job_id": job_id, "state": "queued"}


def get_job_status(job_id: str, conn=None) -> Dict[str, Any]:
    """Get current state and progress of a job."""
    own = conn is None
    if own:
        conn = db.get_connection()
    job = db.get_job(conn, job_id)
    if own:
        conn.close()

    if not job:
        return {"error": f"Job '{job_id}' not found."}
    return {
        "job_id": job["id"],
        "state": job["state"],
        "progress": job["progress"],
        "error_message": job.get("error_message"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "created_at": job["created_at"],
    }


def get_job_summary(job_id: str, conn=None) -> Dict[str, Any]:
    """Get summary of a completed job including report info."""
    own = conn is None
    if own:
        conn = db.get_connection()
    job = db.get_job(conn, job_id)
    if not job:
        if own:
            conn.close()
        return {"error": f"Job '{job_id}' not found."}

    report = db.get_report_by_job(conn, job_id)
    if own:
        conn.close()

    result = {
        "job_id": job["id"],
        "state": job["state"],
        "source": job["source_dataset"],
        "target": job["target_dataset"],
        "key_fields": job["key_fields"],
        "progress": job["progress"],
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
    }
    if report:
        result["report"] = {
            "id": report["id"],
            "file_path": report["file_path"],
            "file_name": report["file_name"],
            "summary": report.get("summary", {}),
        }
    return result


def cancel_job(job_id: str, conn=None) -> Dict[str, Any]:
    """Cancel a queued or running job."""
    own = conn is None
    if own:
        conn = db.get_connection()
    job = db.get_job(conn, job_id)
    if not job:
        if own:
            conn.close()
        return {"error": f"Job '{job_id}' not found."}

    if job["state"] in ("succeeded", "failed", "canceled"):
        if own:
            conn.close()
        return {"error": f"Job '{job_id}' already in terminal state: {job['state']}"}

    db.update_job_state(conn, job_id, "canceled")
    fut = _JOB_FUTURES.get(job_id)
    if fut is not None and not fut.running():
        try:
            fut.cancel()
        except Exception:
            pass
    if own:
        conn.close()
    return {"job_id": job_id, "state": "canceled"}


def list_jobs(limit: int = 50, conn=None) -> List[Dict[str, Any]]:
    """List recent jobs."""
    own = conn is None
    if own:
        conn = db.get_connection()
    jobs = db.list_jobs(conn, limit=limit)
    if own:
        conn.close()
    return jobs
