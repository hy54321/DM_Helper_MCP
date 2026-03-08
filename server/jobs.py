"""
JobService.

Manages comparison job lifecycle:
queued → running → succeeded / failed / canceled.

Phase 1 uses synchronous execution. The async wrapper (threading)
can be added later for large files.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from server import db
from server.comparison import compare_full
from server.reports import write_comparison_report


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
    """Create and immediately run a comparison job (synchronous).

    Returns job metadata + report info on success or error details on failure.
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

    # Run comparison
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
            if own:
                conn.close()
            return {"job_id": job_id, "state": "failed", "error": result["error"]}

        # Write report
        report_result = write_comparison_report(
            comparison_result=result,
            job_id=job_id,
            pair_id=pair_id,
            conn=conn,
        )

        # Summary for job progress
        progress = {
            "added": len(result.get("added", {}).get("data", [])),
            "removed": len(result.get("removed", {}).get("data", [])),
            "changed": len(result.get("changed", {}).get("data", [])),
        }
        db.update_job_state(conn, job_id, "succeeded", progress=progress)

        if own:
            conn.close()
        return {
            "job_id": job_id,
            "state": "succeeded",
            "progress": progress,
            "report": report_result,
        }

    except Exception as exc:
        db.update_job_state(conn, job_id, "failed", error_message=str(exc))
        if own:
            conn.close()
        return {"job_id": job_id, "state": "failed", "error": str(exc)}


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
