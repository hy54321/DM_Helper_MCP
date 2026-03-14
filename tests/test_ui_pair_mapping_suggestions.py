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


def test_quick_map_content_mode_returns_confidence_and_key_candidates(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    source_csv = tmp_path / "source_customers.csv"
    target_csv = tmp_path / "target_customers.csv"
    source_csv.write_text(
        "SRC_ID,SRC_NAME,SRC_STATUS\n"
        "1,Alice,ACTIVE\n"
        "2,Bob,INACTIVE\n"
        "3,Cara,ACTIVE\n"
        "4,Dan,INACTIVE\n"
        "5,Eve,ACTIVE\n",
        encoding="utf-8",
    )
    target_csv.write_text(
        "ACCOUNTNUM,NAME,STATE\n"
        "1,Alice,ACTIVE\n"
        "2,Bob,INACTIVE\n"
        "3,Cara,ACTIVE\n"
        "4,Dan,INACTIVE\n"
        "5,Eve,ACTIVE\n",
        encoding="utf-8",
    )

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_customers", "source", source_csv, ["SRC_ID", "SRC_NAME", "SRC_STATUS"])
        _register_csv_dataset(conn, "target_customers", "target", target_csv, ["ACCOUNTNUM", "NAME", "STATE"])
    finally:
        conn.close()

    response = client.get(
        "/api/pairs/quick-map",
        params={
            "source_dataset_id": "source_customers",
            "target_dataset_id": "target_customers",
            "mode": "content",
            "min_confidence": 0.2,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    mappings = payload["compare_mappings"]
    by_source = {m["source_field"]: m for m in mappings}

    assert payload["mode"] == "content"
    assert set(by_source.keys()) == {"SRC_ID", "SRC_NAME", "SRC_STATUS"}
    assert by_source["SRC_ID"]["target_field"] == "ACCOUNTNUM"
    assert by_source["SRC_ID"]["origin_mode"] == "content"
    assert by_source["SRC_ID"]["confidence"] is not None
    assert by_source["SRC_ID"]["is_key_pair"] is True
    assert by_source["SRC_ID"]["use_key"] is True

    assert by_source["SRC_STATUS"]["target_field"] == "STATE"
    assert by_source["SRC_STATUS"]["low_cardinality"] is True
    assert by_source["SRC_STATUS"]["confidence"] < by_source["SRC_ID"]["confidence"]


def test_quick_map_content_mode_marks_one_to_many_key_candidate(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    source_csv = tmp_path / "source_headers.csv"
    target_csv = tmp_path / "target_lines.csv"
    source_csv.write_text(
        "HEADER_ID,HEADER_NAME\n"
        "H1,A\n"
        "H2,B\n"
        "H3,C\n"
        "H4,D\n"
        "H5,E\n",
        encoding="utf-8",
    )
    target_csv.write_text(
        "LINE_ID,HEADER_REF,AMOUNT\n"
        "1,H1,10\n"
        "2,H1,20\n"
        "3,H2,30\n"
        "4,H3,40\n"
        "5,H3,50\n"
        "6,H4,60\n"
        "7,H5,70\n",
        encoding="utf-8",
    )

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_headers", "source", source_csv, ["HEADER_ID", "HEADER_NAME"])
        _register_csv_dataset(conn, "target_lines", "target", target_csv, ["LINE_ID", "HEADER_REF", "AMOUNT"])
    finally:
        conn.close()

    response = client.get(
        "/api/pairs/quick-map",
        params={
            "source_dataset_id": "source_headers",
            "target_dataset_id": "target_lines",
            "mode": "content",
            "min_confidence": 0.2,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    mappings = payload["compare_mappings"]
    by_source = {m["source_field"]: m for m in mappings}

    assert by_source["HEADER_ID"]["target_field"] == "HEADER_REF"
    assert by_source["HEADER_ID"]["is_key_pair"] is True
    assert by_source["HEADER_ID"]["use_key"] is True


def test_quick_map_content_mode_marks_customer_group_fk_key_candidate_with_partial_overlap(
    monkeypatch, tmp_path: Path
) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    source_csv = tmp_path / "configurations_customer_groups.csv"
    target_csv = tmp_path / "target_customers.csv"
    source_csv.write_text(
        "Customer_group,Description\n"
        "C-DOM,Domestic\n"
        "C-EMP,Employee\n"
        "C-EU,Europe\n"
        "C-IC-DOM,Intercompany Domestic\n"
        "C-IC-EU,Intercompany Europe\n"
        "C-IC-TCY,Intercompany Turkey\n",
        encoding="utf-8",
    )
    # Intentionally includes C-TCY while source has no C-TCY entry.
    target_csv.write_text(
        "CUSTOMERACCOUNT,CUSTOMERGROUPID\n"
        "C0001,C-DOM\n"
        "C0002,C-DOM\n"
        "C0003,C-EU\n"
        "C0004,C-EU\n"
        "C0005,C-IC-DOM\n"
        "C0006,C-IC-DOM\n"
        "C0007,C-IC-EU\n"
        "C0008,C-TCY\n"
        "C0009,C-TCY\n"
        "C0010,C-TCY\n",
        encoding="utf-8",
    )

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(
            conn,
            "configurations_customer_groups_Sheet1",
            "configurations",
            source_csv,
            ["Customer_group", "Description"],
        )
        _register_csv_dataset(
            conn,
            "target_SLS_Customers_V3_Customers_V3",
            "target",
            target_csv,
            ["CUSTOMERACCOUNT", "CUSTOMERGROUPID"],
        )
    finally:
        conn.close()

    response = client.get(
        "/api/pairs/quick-map",
        params={
            "source_dataset_id": "configurations_customer_groups_Sheet1",
            "target_dataset_id": "target_SLS_Customers_V3_Customers_V3",
            "mode": "content",
            "min_confidence": 0.6,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    mappings = payload["compare_mappings"]

    group_mapping = next(
        m
        for m in mappings
        if m["source_field"] == "Customer_group" and m["target_field"] == "CUSTOMERGROUPID"
    )
    assert group_mapping["origin_mode"] == "content"
    assert group_mapping["is_key_pair"] is True
    assert group_mapping["use_key"] is True


def test_quick_map_name_mode_returns_dash_compatible_confidence(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    source_csv = tmp_path / "source_orders.csv"
    target_csv = tmp_path / "target_orders.csv"
    source_csv.write_text(
        "OrderId,Amount\n1,10\n2,20\n3,30\n",
        encoding="utf-8",
    )
    target_csv.write_text(
        "OrderId,Total\n1,10\n2,20\n3,30\n",
        encoding="utf-8",
    )

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_orders", "source", source_csv, ["OrderId", "Amount"])
        _register_csv_dataset(conn, "target_orders", "target", target_csv, ["OrderId", "Total"])
    finally:
        conn.close()

    response = client.get(
        "/api/pairs/quick-map",
        params={
            "source_dataset_id": "source_orders",
            "target_dataset_id": "target_orders",
            "mode": "name",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "name"
    assert payload["match_count"] == 1
    mapping = payload["compare_mappings"][0]
    assert mapping["source_field"] == "OrderId"
    assert mapping["target_field"] == "OrderId"
    assert mapping["origin_mode"] == "name"
    assert mapping["confidence"] is None


def test_pair_override_preserves_mapping_metadata(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    source_csv = tmp_path / "source_meta.csv"
    target_csv = tmp_path / "target_meta.csv"
    source_csv.write_text("A\n1\n", encoding="utf-8")
    target_csv.write_text("B\n1\n", encoding="utf-8")

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_meta", "source", source_csv, ["A"])
        _register_csv_dataset(conn, "target_meta", "target", target_csv, ["B"])
    finally:
        conn.close()

    response = client.post(
        "/api/pairs/override",
        json={
            "source_dataset_id": "source_meta",
            "target_dataset_id": "target_meta",
            "enabled": True,
            "key_mappings": [
                {
                    "source_field": "A",
                    "target_field": "B",
                    "origin_mode": "content",
                    "confidence": 0.91,
                    "is_key_pair": True,
                    "low_cardinality": False,
                }
            ],
            "compare_mappings": [
                {
                    "source_field": "A",
                    "target_field": "B",
                    "origin_mode": "content",
                    "confidence": 0.91,
                    "is_key_pair": True,
                    "low_cardinality": False,
                }
            ],
        },
    )
    assert response.status_code == 200

    conn = db.get_connection(path=str(db_path))
    try:
        pair = db.get_pair_by_datasets(conn, "source_meta", "target_meta")
    finally:
        conn.close()

    assert pair is not None
    assert pair["compare_mappings"][0]["origin_mode"] == "content"
    assert pair["compare_mappings"][0]["confidence"] == 0.91
    assert pair["compare_mappings"][0]["is_key_pair"] is True


def test_delete_pair_key_mappings_endpoint_clears_only_key_mappings(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    source_csv = tmp_path / "source_pair_delete.csv"
    target_csv = tmp_path / "target_pair_delete.csv"
    source_csv.write_text("SRC_ID\n1\n2\n", encoding="utf-8")
    target_csv.write_text("TGT_ID\n1\n2\n", encoding="utf-8")

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_pair_delete", "source", source_csv, ["SRC_ID"])
        _register_csv_dataset(conn, "target_pair_delete", "target", target_csv, ["TGT_ID"])
    finally:
        conn.close()

    create_response = client.post(
        "/api/pairs/override",
        json={
            "source_dataset_id": "source_pair_delete",
            "target_dataset_id": "target_pair_delete",
            "enabled": True,
            "key_mappings": [{"source_field": "SRC_ID", "target_field": "TGT_ID"}],
            "compare_mappings": [{"source_field": "SRC_ID", "target_field": "TGT_ID"}],
        },
    )
    assert create_response.status_code == 200
    pair_id = create_response.json()["pair_id"]

    delete_response = client.delete(f"/api/pairs/{pair_id}/key-mappings")
    assert delete_response.status_code == 200
    payload = delete_response.json()
    assert payload["pair_id"] == pair_id
    assert payload["key_mapping_count"] == 0
    assert payload["compare_mapping_count"] == 1

    pairs_response = client.get("/api/pairs")
    assert pairs_response.status_code == 200
    pair = next(p for p in pairs_response.json() if p["id"] == pair_id)
    assert pair["key_mappings"] == []
    assert len(pair["compare_mappings"]) == 1
    compare_mapping = pair["compare_mappings"][0]
    assert compare_mapping["source_field"] == "SRC_ID"
    assert compare_mapping["target_field"] == "TGT_ID"


def test_delete_pair_endpoint_removes_pair_row(monkeypatch, tmp_path: Path) -> None:
    ui_api, db_path = _load_ui_api(monkeypatch, tmp_path)
    client = TestClient(ui_api.app)

    source_csv = tmp_path / "source_pair_remove.csv"
    target_csv = tmp_path / "target_pair_remove.csv"
    source_csv.write_text("SRC_KEY\n1\n", encoding="utf-8")
    target_csv.write_text("TGT_KEY\n1\n", encoding="utf-8")

    conn = db.get_connection(path=str(db_path))
    try:
        _register_csv_dataset(conn, "source_pair_remove", "source", source_csv, ["SRC_KEY"])
        _register_csv_dataset(conn, "target_pair_remove", "target", target_csv, ["TGT_KEY"])
    finally:
        conn.close()

    create_response = client.post(
        "/api/pairs/override",
        json={
            "source_dataset_id": "source_pair_remove",
            "target_dataset_id": "target_pair_remove",
            "enabled": True,
            "key_mappings": [{"source_field": "SRC_KEY", "target_field": "TGT_KEY"}],
            "compare_mappings": [{"source_field": "SRC_KEY", "target_field": "TGT_KEY"}],
        },
    )
    assert create_response.status_code == 200
    pair_id = create_response.json()["pair_id"]

    delete_response = client.delete(f"/api/pairs/{pair_id}")
    assert delete_response.status_code == 200
    payload = delete_response.json()
    assert payload["pair_id"] == pair_id
    assert payload["deleted"] is True

    pairs_response = client.get("/api/pairs")
    assert pairs_response.status_code == 200
    assert all(p["id"] != pair_id for p in pairs_response.json())
