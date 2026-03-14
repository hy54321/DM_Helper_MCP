import importlib
from pathlib import Path

from fastapi.testclient import TestClient

from server import db


def _load_ui_api(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "protoquery.db"
    app_dir = tmp_path / "app_data"
    app_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("PROTOQUERY_DB_PATH", str(db_path))
    monkeypatch.setenv("PROTOQUERY_APP_BASE_DIR", str(app_dir))

    import ui.api as ui_api

    return importlib.reload(ui_api), db_path


def _register_csv_dataset(
    conn,
    dataset_id: str,
    side: str,
    csv_path: Path,
    columns: list[str],
) -> None:
    db.upsert_dataset(
        conn,
        {
            "id": dataset_id,
            "side": side,
            "file_name": csv_path.name,
            "file_path": str(csv_path),
            "sheet_name": "",
            "ext": ".csv",
            "columns": columns,
            "raw_columns": columns,
            "column_map": {c: c for c in columns},
            "row_count": None,
        },
    )


def test_create_relationship_remains_visible_after_cross_request_with_same_side_datasets(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    left_csv = tmp_path / "left_target.csv"
    right_csv = tmp_path / "right_target.csv"
    left_csv.write_text("ACCOUNT,COMPANY\n1,USMF\n2,USRT\n", encoding="utf-8")
    right_csv.write_text("ACCOUNTNUM,DATAAREAID\n1,USMF\n2,USRT\n", encoding="utf-8")

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "target_customer", "target", left_csv, ["ACCOUNT", "COMPANY"])
        _register_csv_dataset(conn, "target_sales", "target", right_csv, ["ACCOUNTNUM", "DATAAREAID"])
    finally:
        conn.close()

    create_response = client.post(
        "/api/relationships",
        json={
            "side": "cross",
            "left_dataset": "target_customer",
            "left_field": "ACCOUNT",
            "left_fields": ["ACCOUNT", "COMPANY"],
            "right_dataset": "target_sales",
            "right_field": "ACCOUNTNUM",
            "right_fields": ["ACCOUNTNUM", "DATAAREAID"],
            "confidence": 1.0,
            "method": "manual",
            "active": True,
        },
    )
    assert create_response.status_code == 200
    created = create_response.json()
    assert created["side"] == "target"
    assert created["left_fields"] == ["ACCOUNT", "COMPANY"]
    assert created["right_fields"] == ["ACCOUNTNUM", "DATAAREAID"]

    all_rows_response = client.get("/api/relationships", params={"limit": 200})
    assert all_rows_response.status_code == 200
    all_ids = {row["id"] for row in all_rows_response.json()}
    assert created["id"] in all_ids

    target_rows_response = client.get("/api/relationships", params={"side": "target", "limit": 200})
    assert target_rows_response.status_code == 200
    target_ids = {row["id"] for row in target_rows_response.json()}
    assert created["id"] in target_ids


def test_auto_link_relationships_creates_links_for_folder_scopes(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    cfg_csv = tmp_path / "config_groups.csv"
    tgt_csv = tmp_path / "target_customers.csv"
    cfg_csv.write_text(
        "Customer_group,Description\n"
        "C-DOM,Domestic\n"
        "C-EU,Europe\n"
        "C-IC-DOM,Intercompany Domestic\n"
        "C-IC-EU,Intercompany Europe\n",
        encoding="utf-8",
    )
    tgt_csv.write_text(
        "CUSTOMERACCOUNT,CUSTOMERGROUPID\n"
        "C0001,C-DOM\n"
        "C0002,C-DOM\n"
        "C0003,C-EU\n"
        "C0004,C-IC-DOM\n"
        "C0005,C-IC-EU\n",
        encoding="utf-8",
    )

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(
            conn,
            "configurations_customer_groups_Sheet1",
            "configurations",
            cfg_csv,
            ["Customer_group", "Description"],
        )
        _register_csv_dataset(
            conn,
            "target_SLS_Customers_V3_Customers_V3",
            "target",
            tgt_csv,
            ["CUSTOMERACCOUNT", "CUSTOMERGROUPID"],
        )
    finally:
        conn.close()

    create_response = client.post(
        "/api/relationships/auto-link",
        json={
            "left_side": "configurations",
            "right_side": "target",
            "min_confidence": 0.6,
            "max_links": 100,
        },
    )
    assert create_response.status_code == 200
    payload = create_response.json()
    assert payload["applied_count"] == 1
    assert payload["pairs_skipped_existing"] == 0

    rows_response = client.get("/api/relationships", params={"limit": 200})
    assert rows_response.status_code == 200
    rows = rows_response.json()
    created = next(
        r
        for r in rows
        if r["left_dataset"] == "configurations_customer_groups_Sheet1"
        and r["right_dataset"] == "target_SLS_Customers_V3_Customers_V3"
    )
    assert created["left_field"] == "Customer_group"
    assert created["right_field"] == "CUSTOMERGROUPID"

    second_response = client.post(
        "/api/relationships/auto-link",
        json={
            "left_side": "configurations",
            "right_side": "target",
            "min_confidence": 0.6,
            "max_links": 100,
        },
    )
    assert second_response.status_code == 200
    second_payload = second_response.json()
    assert second_payload["applied_count"] == 0
    assert second_payload["pairs_skipped_existing"] >= 1


def test_auto_link_relationships_supports_dataset_to_folder_scope(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    src_csv = tmp_path / "source_headers.csv"
    tgt_ok_csv = tmp_path / "target_lines_ok.csv"
    tgt_other_csv = tmp_path / "target_lines_other.csv"
    src_csv.write_text(
        "HEADER_ID,HEADER_NAME\n"
        "H1,A\n"
        "H2,B\n"
        "H3,C\n"
        "H4,D\n",
        encoding="utf-8",
    )
    tgt_ok_csv.write_text(
        "LINE_ID,HEADER_REF\n"
        "1,H1\n"
        "2,H1\n"
        "3,H2\n"
        "4,H3\n"
        "5,H4\n",
        encoding="utf-8",
    )
    tgt_other_csv.write_text(
        "LINE_ID,HEADER_REF\n"
        "1,Z1\n"
        "2,Z2\n"
        "3,Z3\n",
        encoding="utf-8",
    )

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_headers", "source", src_csv, ["HEADER_ID", "HEADER_NAME"])
        _register_csv_dataset(conn, "target_lines_ok", "target", tgt_ok_csv, ["LINE_ID", "HEADER_REF"])
        _register_csv_dataset(conn, "target_lines_other", "target", tgt_other_csv, ["LINE_ID", "HEADER_REF"])
    finally:
        conn.close()

    response = client.post(
        "/api/relationships/auto-link",
        json={
            "left_dataset": "source_headers",
            "right_side": "target",
            "min_confidence": 0.6,
            "max_links": 100,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["applied_count"] == 1

    rows_response = client.get("/api/relationships", params={"limit": 200})
    assert rows_response.status_code == 200
    rows = rows_response.json()
    created = next(r for r in rows if r["left_dataset"] == "source_headers")
    assert created["right_dataset"] == "target_lines_ok"
    assert created["left_field"] == "HEADER_ID"
    assert created["right_field"] == "HEADER_REF"


def test_auto_link_relationships_supports_name_mode(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    left_csv = tmp_path / "left_orders.csv"
    right_csv = tmp_path / "right_orders.csv"
    left_csv.write_text(
        "OrderId,Amount\n"
        "1,10\n"
        "2,20\n"
        "3,30\n",
        encoding="utf-8",
    )
    right_csv.write_text(
        "OrderId,Total\n"
        "1,10\n"
        "2,20\n"
        "3,30\n",
        encoding="utf-8",
    )

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_orders", "source", left_csv, ["OrderId", "Amount"])
        _register_csv_dataset(conn, "target_orders", "target", right_csv, ["OrderId", "Total"])
    finally:
        conn.close()

    response = client.post(
        "/api/relationships/auto-link",
        json={
            "left_side": "source",
            "right_side": "target",
            "mode": "name",
            "max_links": 100,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "name"
    assert payload["applied_count"] == 1

    rows_response = client.get("/api/relationships", params={"limit": 200})
    assert rows_response.status_code == 200
    rows = rows_response.json()
    created = next(
        r
        for r in rows
        if r["left_dataset"] == "source_orders" and r["right_dataset"] == "target_orders"
    )
    assert created["left_field"] == "OrderId"
    assert created["right_field"] == "OrderId"
    assert created["method"] == "name_auto_scope"
