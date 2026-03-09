const { useEffect, useState } = React;

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  let data = null;
  try {
    data = await res.json();
  } catch (_err) {
    data = null;
  }

  if (!res.ok) {
    const detail = data?.detail || data?.error || `HTTP ${res.status}`;
    throw new Error(detail);
  }
  return data;
}

function JsonView({ value }) {
  return <pre>{JSON.stringify(value, null, 2)}</pre>;
}

function App() {
  const [tab, setTab] = useState("catalog");
  const [status, setStatus] = useState("Ready.");
  const [error, setError] = useState("");

  const [sourceFolder, setSourceFolder] = useState("");
  const [targetFolder, setTargetFolder] = useState("");
  const [reportFolder, setReportFolder] = useState("");
  const [includeRowCounts, setIncludeRowCounts] = useState(false);

  const [datasets, setDatasets] = useState([]);
  const [pairs, setPairs] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [reports, setReports] = useState([]);
  const [relationships, setRelationships] = useState([]);

  const [profileDataset, setProfileDataset] = useState("");
  const [profileColumn, setProfileColumn] = useState("");
  const [topN, setTopN] = useState(10);
  const [profileResult, setProfileResult] = useState(null);
  const [columnSummaryResult, setColumnSummaryResult] = useState(null);
  const [filteredResult, setFilteredResult] = useState(null);
  const [filterColumn, setFilterColumn] = useState("");
  const [filterValue, setFilterValue] = useState("");
  const [filterBlanks, setFilterBlanks] = useState(false);

  const [sourceDataset, setSourceDataset] = useState("");
  const [targetDataset, setTargetDataset] = useState("");
  const [pairId, setPairId] = useState("");
  const [fieldMappings, setFieldMappings] = useState([]);
  const [mappingSearch, setMappingSearch] = useState("");
  const [compareResult, setCompareResult] = useState(null);
  const [jobSummary, setJobSummary] = useState(null);
  const [relationshipSide, setRelationshipSide] = useState("target");
  const [relationshipId, setRelationshipId] = useState("");
  const [leftDatasetId, setLeftDatasetId] = useState("");
  const [leftField, setLeftField] = useState("");
  const [leftExtraFields, setLeftExtraFields] = useState("");
  const [rightDatasetId, setRightDatasetId] = useState("");
  const [rightField, setRightField] = useState("");
  const [rightExtraFields, setRightExtraFields] = useState("");
  const [relationshipConfidence, setRelationshipConfidence] = useState(0.95);
  const [relationshipMethod, setRelationshipMethod] = useState("manual");
  const [relationshipActive, setRelationshipActive] = useState(true);
  const [autoLinkConfidence, setAutoLinkConfidence] = useState(0.9);
  const [autoLinkSuggestOnly, setAutoLinkSuggestOnly] = useState(false);

  async function refreshBootstrap() {
    setError("");
    try {
      const [folders, ds, pr, jb, rp, rels] = await Promise.all([
        api("/api/settings/folders"),
        api("/api/datasets"),
        api("/api/pairs"),
        api("/api/jobs"),
        api("/api/reports"),
        api("/api/relationships?limit=500"),
      ]);
      setSourceFolder(folders.source_folder || "");
      setTargetFolder(folders.target_folder || "");
      setReportFolder(folders.report_folder || "");
      setDatasets(ds || []);
      setPairs(pr || []);
      setJobs(jb || []);
      setReports(rp || []);
      setRelationships(rels || []);
      if (ds?.length && !profileDataset) {
        setProfileDataset(ds[0].id);
      }
      if (ds?.length && !sourceDataset) {
        const src = ds.find((x) => x.side === "source") || ds[0];
        const tgt = ds.find((x) => x.side === "target") || ds[0];
        setSourceDataset(src.id);
        setTargetDataset(tgt.id);
      }
      setStatus("Loaded latest metadata.");
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    refreshBootstrap();
  }, []);

  useEffect(() => {
    if (!sourceDataset || !targetDataset) return;
    const exactPair = pairs.find((p) => p.source_dataset === sourceDataset && p.target_dataset === targetDataset);
    if (exactPair && !pairId) {
      setPairId(exactPair.id);
    }
    if (!exactPair && pairId) {
      const selected = pairs.find((p) => p.id === pairId);
      if (selected && (selected.source_dataset !== sourceDataset || selected.target_dataset !== targetDataset)) {
        setPairId("");
        setFieldMappings([]);
        setMappingSearch("");
      }
    }
  }, [sourceDataset, targetDataset, pairs, pairId]);

  useEffect(() => {
    const sideDatasetIds = new Set(datasets.filter((d) => d.side === relationshipSide).map((d) => d.id));
    if (leftDatasetId && !sideDatasetIds.has(leftDatasetId)) {
      setLeftDatasetId("");
      setLeftField("");
      setLeftExtraFields("");
    }
    if (rightDatasetId && !sideDatasetIds.has(rightDatasetId)) {
      setRightDatasetId("");
      setRightField("");
      setRightExtraFields("");
    }
  }, [relationshipSide, leftDatasetId, rightDatasetId, datasets]);

  useEffect(() => {
    if (!pairId) return;
    const pair = pairs.find((p) => p.id === pairId);
    if (!pair) return;
    if (pair.source_dataset !== sourceDataset) setSourceDataset(pair.source_dataset);
    if (pair.target_dataset !== targetDataset) setTargetDataset(pair.target_dataset);

    const keySig = new Set(
      (pair.key_mappings || []).map((m) => `${m.source_field || m.source}|||${m.target_field || m.target}`)
    );
    const rows = [];
    for (const m of pair.compare_mappings || []) {
      const sourceField = m.source_field || m.source;
      const targetField = m.target_field || m.target;
      if (!sourceField || !targetField) continue;
      rows.push({
        source_field: sourceField,
        target_field: targetField,
        use_key: keySig.has(`${sourceField}|||${targetField}`),
        use_compare: true,
      });
    }
    for (const m of pair.key_mappings || []) {
      const sourceField = m.source_field || m.source;
      const targetField = m.target_field || m.target;
      if (!sourceField || !targetField) continue;
      const exists = rows.some((r) => r.source_field === sourceField && r.target_field === targetField);
      if (!exists) {
        rows.push({ source_field: sourceField, target_field: targetField, use_key: true, use_compare: false });
      }
    }
    setFieldMappings(rows);
    setMappingSearch("");
  }, [pairId, pairs]);

  function onSourceDatasetChange(value) {
    setSourceDataset(value);
    setPairId("");
    setFieldMappings([]);
    setMappingSearch("");
  }

  function onTargetDatasetChange(value) {
    setTargetDataset(value);
    setPairId("");
    setFieldMappings([]);
    setMappingSearch("");
  }

  async function onRefreshCatalog() {
    setError("");
    setStatus("Refreshing catalog...");
    try {
      const res = await api("/api/catalog/refresh", {
        method: "POST",
        body: JSON.stringify({
          source_folder: sourceFolder || null,
          target_folder: targetFolder || null,
          report_folder: reportFolder,
          include_row_counts: includeRowCounts,
        }),
      });
      setStatus(
        `Catalog refreshed. Source=${res.source_datasets}, Target=${res.target_datasets}, Pairs=${res.total_pairs}, RowCounts=${res.row_counts_included ? "on" : "off"}`
      );
      await refreshBootstrap();
    } catch (err) {
      setError(err.message);
      setStatus("Catalog refresh failed.");
    }
  }

  async function onSaveFolders() {
    setError("");
    setStatus("Saving folder settings...");
    try {
      const saved = await api("/api/settings/folders", {
        method: "POST",
        body: JSON.stringify({
          source_folder: sourceFolder,
          target_folder: targetFolder,
          report_folder: reportFolder,
        }),
      });
      setSourceFolder(saved.source_folder || "");
      setTargetFolder(saved.target_folder || "");
      setReportFolder(saved.report_folder || "");
      setStatus("Folder settings saved.");
    } catch (err) {
      setError(err.message);
      setStatus("Saving folder settings failed.");
    }
  }

  async function onBrowseFolder(kind) {
    setError("");
    const current = kind === "source" ? sourceFolder : kind === "target" ? targetFolder : reportFolder;
    try {
      const path = current ? `?initial=${encodeURIComponent(current)}` : "";
      const res = await api(`/api/system/browse-folder${path}`);
      if (!res?.folder) {
        return;
      }
      const nextSource = kind === "source" ? res.folder : sourceFolder;
      const nextTarget = kind === "target" ? res.folder : targetFolder;
      const nextReport = kind === "report" ? res.folder : reportFolder;
      const saved = await api("/api/settings/folders", {
        method: "POST",
        body: JSON.stringify({
          source_folder: nextSource,
          target_folder: nextTarget,
          report_folder: nextReport,
        }),
      });
      setSourceFolder(saved.source_folder || "");
      setTargetFolder(saved.target_folder || "");
      setReportFolder(saved.report_folder || "");
      const label = kind === "source" ? "Source" : kind === "target" ? "Target" : "Report";
      setStatus(`${label} folder selected.`);
    } catch (err) {
      setError(err.message);
    }
  }

  async function loadProfile() {
    if (!profileDataset) return;
    setError("");
    setStatus("Loading profile...");
    try {
      const [profile, summary] = await Promise.all([
        api(`/api/profile/${encodeURIComponent(profileDataset)}`),
        api(
          `/api/summary/column/${encodeURIComponent(profileDataset)}?top_n=${encodeURIComponent(
            topN
          )}${profileColumn ? `&column=${encodeURIComponent(profileColumn)}` : ""}`
        ),
      ]);
      setProfileResult(profile);
      setColumnSummaryResult(summary);
      setStatus("Profile loaded.");
    } catch (err) {
      setError(err.message);
    }
  }

  async function loadFilteredPreview() {
    if (!profileDataset || !filterColumn) return;
    setError("");
    setStatus("Loading filtered preview...");
    try {
      const body = {
        filter_spec: {
          column: filterColumn,
          blanks_only: filterBlanks,
        },
        limit: 10,
      };
      if (!filterBlanks) {
        body.filter_spec.value = filterValue;
      }
      const result = await api(`/api/preview/filtered/${encodeURIComponent(profileDataset)}`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      setFilteredResult(result);
      setStatus("Filtered preview loaded.");
    } catch (err) {
      setError(err.message);
    }
  }

  function normalizeFieldName(name) {
    return String(name || "").trim().toLowerCase();
  }

  function buildQuickMappings(sourceCols, targetCols) {
    const targetLookup = new Map(targetCols.map((c) => [normalizeFieldName(c), c]));
    const results = [];
    for (const sourceField of sourceCols) {
      const targetField = targetLookup.get(normalizeFieldName(sourceField));
      if (!targetField) continue;
      results.push({
        source_field: sourceField,
        target_field: targetField,
        use_key: false,
        use_compare: true,
      });
    }
    return results;
  }

  function applyQuickMappings() {
    const src = datasets.find((d) => d.id === sourceDataset);
    const tgt = datasets.find((d) => d.id === targetDataset);
    if (!src || !tgt) {
      setError("Select source and target datasets first.");
      return;
    }
    const mapped = buildQuickMappings(src.columns || [], tgt.columns || []);
    setFieldMappings(mapped);
    setStatus(`Quick-mapped ${mapped.length} same-name field(s). Mark key fields as needed.`);
  }

  function addMappingRow() {
    setFieldMappings((prev) => [...prev, { source_field: "", target_field: "", use_key: false, use_compare: true }]);
  }

  function removeMappingRow(index) {
    setFieldMappings((prev) => prev.filter((_, i) => i !== index));
  }

  function updateMappingRow(index, patch) {
    setFieldMappings((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  }

  async function savePairMappings() {
    if (!sourceDataset || !targetDataset) return;
    const keyMappings = fieldMappings
      .filter((m) => m.use_key && m.source_field && m.target_field)
      .map((m) => ({ source_field: m.source_field, target_field: m.target_field }));
    const compareMappings = fieldMappings
      .filter((m) => m.use_compare && m.source_field && m.target_field)
      .map((m) => ({ source_field: m.source_field, target_field: m.target_field }));
    if (!keyMappings.length) {
      setError("Select at least one key mapping before saving.");
      return;
    }
    setError("");
    setStatus("Saving pair mappings...");
    try {
      const res = await api("/api/pairs/override", {
        method: "POST",
        body: JSON.stringify({
          source_dataset_id: sourceDataset,
          target_dataset_id: targetDataset,
          enabled: true,
          key_mappings: keyMappings,
          compare_mappings: compareMappings,
        }),
      });
      await refreshBootstrap();
      setPairId(res.pair_id || "");
      setStatus(`Saved pair mappings as ${res.pair_id}.`);
    } catch (err) {
      setError(err.message);
      setStatus("Saving pair mappings failed.");
    }
  }

  async function onQuickCompare() {
    if (!sourceDataset || !targetDataset) return;
    const keyMappings = fieldMappings
      .filter((m) => m.use_key && m.source_field && m.target_field)
      .map((m) => ({ source_field: m.source_field, target_field: m.target_field }));
    const compareMappings = fieldMappings
      .filter((m) => m.use_compare && m.source_field && m.target_field)
      .map((m) => ({ source_field: m.source_field, target_field: m.target_field }));
    if (!keyMappings.length) {
      setError("Select at least one key mapping.");
      return;
    }
    setError("");
    setStatus("Running quick compare...");
    try {
      const result = await api("/api/compare/quick", {
        method: "POST",
        body: JSON.stringify({
          source_dataset_id: sourceDataset,
          target_dataset_id: targetDataset,
          key_mappings: keyMappings,
          compare_mappings: compareMappings,
          sample_limit: 10,
        }),
      });
      setCompareResult(result);
      setStatus("Quick compare complete.");
    } catch (err) {
      setError(err.message);
    }
  }

  async function onStartJob() {
    if (!sourceDataset || !targetDataset) return;
    const keyMappings = fieldMappings
      .filter((m) => m.use_key && m.source_field && m.target_field)
      .map((m) => ({ source_field: m.source_field, target_field: m.target_field }));
    const compareMappings = fieldMappings
      .filter((m) => m.use_compare && m.source_field && m.target_field)
      .map((m) => ({ source_field: m.source_field, target_field: m.target_field }));
    if (!keyMappings.length) {
      setError("Select at least one key mapping.");
      return;
    }
    setError("");
    setStatus("Starting comparison job...");
    try {
      const result = await api("/api/compare/start", {
        method: "POST",
        body: JSON.stringify({
          source_dataset_id: sourceDataset,
          target_dataset_id: targetDataset,
          key_mappings: keyMappings,
          pair_id: pairId || null,
          compare_mappings: compareMappings.length ? compareMappings : null,
        }),
      });
      setCompareResult(result);
      await refreshBootstrap();
      setStatus(`Job ${result.job_id} finished with state ${result.state}.`);
    } catch (err) {
      setError(err.message);
    }
  }

  async function loadJobSummary(jobId) {
    setError("");
    try {
      const result = await api(`/api/jobs/${encodeURIComponent(jobId)}/summary`);
      setJobSummary(result);
      setStatus(`Loaded summary for ${jobId}.`);
    } catch (err) {
      setError(err.message);
    }
  }

  async function onDeleteReport(reportId) {
    if (!confirm(`Delete report ${reportId}?`)) {
      return;
    }
    setError("");
    try {
      await api(`/api/reports/${encodeURIComponent(reportId)}`, { method: "DELETE" });
      await refreshBootstrap();
      setStatus(`Deleted report ${reportId}.`);
    } catch (err) {
      setError(err.message);
    }
  }

  function clearRelationshipForm() {
    setRelationshipId("");
    setLeftDatasetId("");
    setLeftField("");
    setLeftExtraFields("");
    setRightDatasetId("");
    setRightField("");
    setRightExtraFields("");
    setRelationshipConfidence(0.95);
    setRelationshipMethod("manual");
    setRelationshipActive(true);
  }

  function parseCommaFields(value) {
    return (value || "")
      .split(",")
      .map((x) => x.trim())
      .filter((x) => x);
  }

  function relationshipFieldLabel(row, side) {
    const list =
      side === "left"
        ? row.left_fields || (row.left_field ? [row.left_field] : [])
        : row.right_fields || (row.right_field ? [row.right_field] : []);
    return list.length ? list.join(" + ") : "";
  }

  function editRelationship(row) {
    setRelationshipId(String(row.id || ""));
    setRelationshipSide(row.side || "target");
    setLeftDatasetId(row.left_dataset || "");
    const lf = row.left_fields || (row.left_field ? [row.left_field] : []);
    setLeftField(lf[0] || "");
    setLeftExtraFields(lf.slice(1).join(", "));
    setRightDatasetId(row.right_dataset || "");
    const rf = row.right_fields || (row.right_field ? [row.right_field] : []);
    setRightField(rf[0] || "");
    setRightExtraFields(rf.slice(1).join(", "));
    setRelationshipConfidence(Number(row.confidence || 0.95));
    setRelationshipMethod(row.method || "manual");
    setRelationshipActive(!!row.active);
    setTab("relationships");
  }

  async function saveRelationship() {
    const leftFields = [leftField, ...parseCommaFields(leftExtraFields)].filter((x) => x);
    const rightFields = [rightField, ...parseCommaFields(rightExtraFields)].filter((x) => x);
    if (!leftDatasetId || !rightDatasetId || !leftFields.length || !rightFields.length) {
      setError("Select left/right datasets and fields.");
      return;
    }
    if (leftFields.length !== rightFields.length) {
      setError("Left and right field counts must match for composite relationships.");
      return;
    }
    setError("");
    setStatus(relationshipId ? "Updating relationship..." : "Creating relationship...");
    const payload = {
      side: relationshipSide,
      left_dataset: leftDatasetId,
      left_field: leftFields[0],
      left_fields: leftFields,
      right_dataset: rightDatasetId,
      right_field: rightFields[0],
      right_fields: rightFields,
      confidence: Number(relationshipConfidence),
      method: relationshipMethod || "manual",
      active: !!relationshipActive,
    };
    try {
      if (relationshipId) {
        await api(`/api/relationships/${encodeURIComponent(relationshipId)}`, {
          method: "PUT",
          body: JSON.stringify(payload),
        });
      } else {
        await api("/api/relationships", {
          method: "POST",
          body: JSON.stringify(payload),
        });
      }
      await refreshBootstrap();
      clearRelationshipForm();
      setStatus("Relationship saved.");
    } catch (err) {
      setError(err.message);
      setStatus("Saving relationship failed.");
    }
  }

  async function removeRelationship(id) {
    if (!confirm(`Delete relationship ${id}?`)) return;
    setError("");
    setStatus("Deleting relationship...");
    try {
      await api(`/api/relationships/${encodeURIComponent(id)}`, { method: "DELETE" });
      await refreshBootstrap();
      setStatus(`Deleted relationship ${id}.`);
    } catch (err) {
      setError(err.message);
      setStatus("Delete relationship failed.");
    }
  }

  async function runAutoLink() {
    setError("");
    setStatus(autoLinkSuggestOnly ? "Suggesting related table links..." : "Auto-linking related tables...");
    try {
      const res = await api("/api/relationships/link-related", {
        method: "POST",
        body: JSON.stringify({
          side: relationshipSide,
          min_confidence: Number(autoLinkConfidence),
          suggest_only: autoLinkSuggestOnly,
        }),
      });
      if (!autoLinkSuggestOnly) {
        await refreshBootstrap();
      }
      setStatus(
        `${autoLinkSuggestOnly ? "Suggestion run complete" : "Auto-link complete"}. Suggested=${res.suggested_count}, Applied=${res.applied_count}.`
      );
    } catch (err) {
      setError(err.message);
      setStatus("Auto-link failed.");
    }
  }

  const sourceOptions = datasets.filter((d) => d.side === "source");
  const targetOptions = datasets.filter((d) => d.side === "target");
  const relationshipDatasets = datasets.filter((d) => d.side === relationshipSide);
  const leftDatasetObj = relationshipDatasets.find((d) => d.id === leftDatasetId);
  const rightDatasetObj = relationshipDatasets.find((d) => d.id === rightDatasetId);
  const leftFieldOptions = leftDatasetObj?.columns || [];
  const rightFieldOptions = rightDatasetObj?.columns || [];
  const filteredRelationships = relationships.filter((r) => r.side === relationshipSide);
  const selectedSource = sourceOptions.find((d) => d.id === sourceDataset);
  const selectedTarget = targetOptions.find((d) => d.id === targetDataset);
  const pairOptions = pairs.filter(
    (p) =>
      (!sourceDataset || p.source_dataset === sourceDataset) &&
      (!targetDataset || p.target_dataset === targetDataset)
  );
  const mappingQuery = mappingSearch.trim().toLowerCase();
  const filteredMappingRows = fieldMappings
    .map((m, idx) => ({ m, idx }))
    .filter(({ m }) => {
      if (!mappingQuery) return true;
      return `${m.source_field || ""} ${m.target_field || ""}`.toLowerCase().includes(mappingQuery);
    });

  return (
    <div className="app">
      <div className="header">
        <h1>DM Helper Admin</h1>
        <div className="sub">FastAPI + React control panel for catalog, profiling, compare jobs, and reports.</div>
      </div>

      <div className="layout">
        <aside className="sidebar">
          <div className="tabs">
            <button className={`tab ${tab === "catalog" ? "active" : ""}`} onClick={() => setTab("catalog")}>
              Catalog
            </button>
            <button className={`tab ${tab === "profile" ? "active" : ""}`} onClick={() => setTab("profile")}>
              Profiling
            </button>
            <button className={`tab ${tab === "compare" ? "active" : ""}`} onClick={() => setTab("compare")}>
              Compare & Jobs
            </button>
            <button className={`tab ${tab === "reports" ? "active" : ""}`} onClick={() => setTab("reports")}>
              Reports
            </button>
            <button className={`tab ${tab === "relationships" ? "active" : ""}`} onClick={() => setTab("relationships")}>
              Relationships
            </button>
          </div>
        </aside>
        <main className="content">
          <div className="status">
            <strong>Status:</strong> {status}
            {error ? (
              <>
                {" "}
                <span style={{ color: "#b42318" }}>| Error: {error}</span>
              </>
            ) : null}
          </div>

      {tab === "catalog" ? (
        <>
          <div className="card">
            <div className="row">
              <div className="col-6">
                <label>Source folder</label>
                <div className="field-with-action">
                  <input value={sourceFolder} onChange={(e) => setSourceFolder(e.target.value)} placeholder="C:\data\source" />
                  <button type="button" className="secondary browse-btn" onClick={() => onBrowseFolder("source")}>
                    Browse
                  </button>
                </div>
              </div>
              <div className="col-6">
                <label>Target folder</label>
                <div className="field-with-action">
                  <input value={targetFolder} onChange={(e) => setTargetFolder(e.target.value)} placeholder="C:\data\target" />
                  <button type="button" className="secondary browse-btn" onClick={() => onBrowseFolder("target")}>
                    Browse
                  </button>
                </div>
              </div>
              <div className="col-6">
                <label>Report folder</label>
                <div className="field-with-action">
                  <input value={reportFolder} onChange={(e) => setReportFolder(e.target.value)} placeholder="C:\data\reports" />
                  <button type="button" className="secondary browse-btn" onClick={() => onBrowseFolder("report")}>
                    Browse
                  </button>
                </div>
              </div>
              <div className="col-3">
                <label>Options</label>
                <div className="toggle-inline">
                  <input
                    type="checkbox"
                    className="check-input"
                    checked={includeRowCounts}
                    onChange={(e) => setIncludeRowCounts(e.target.checked)}
                  />
                  <span>Include row counts (slow)</span>
                </div>
              </div>
              <div className="col-3">
                <button className="refresh-catalog-btn" onClick={onRefreshCatalog}>
                  Refresh Catalog
                </button>
              </div>
              <div className="col-3">
                <button className="secondary" onClick={onSaveFolders}>
                  Save Folder Settings
                </button>
              </div>
              <div className="col-3">
                <button className="secondary" onClick={refreshBootstrap}>
                  Reload Metadata
                </button>
              </div>
            </div>
          </div>

          <div className="card">
            <h3>Datasets ({datasets.length})</h3>
            <div className="scroll">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Side</th>
                    <th>File</th>
                    <th>Sheet</th>
                    <th>Columns</th>
                    <th>Rows</th>
                  </tr>
                </thead>
                <tbody>
                  {datasets.map((d) => (
                    <tr key={d.id}>
                      <td>{d.id}</td>
                      <td>{d.side}</td>
                      <td>{d.file_name}</td>
                      <td>{d.sheet_name || "-"}</td>
                      <td>{(d.columns || []).length}</td>
                      <td>{d.row_count == null ? "-" : d.row_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="card">
            <h3>Pairs ({pairs.length})</h3>
            <div className="scroll">
              <table>
                <thead>
                  <tr>
                    <th>Pair ID</th>
                    <th>Source Dataset</th>
                    <th>Target Dataset</th>
                    <th>Auto</th>
                    <th>Enabled</th>
                  </tr>
                </thead>
                <tbody>
                  {pairs.map((p) => (
                    <tr key={p.id}>
                      <td>{p.id}</td>
                      <td>{p.source_dataset}</td>
                      <td>{p.target_dataset}</td>
                      <td>{String(p.auto_matched)}</td>
                      <td>{String(p.enabled)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}

      {tab === "profile" ? (
        <>
          <div className="card">
            <div className="row">
              <div className="col-4">
                <label>Dataset</label>
                <select value={profileDataset} onChange={(e) => setProfileDataset(e.target.value)}>
                  <option value="">Select...</option>
                  {datasets.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-4">
                <label>Single column (optional)</label>
                <input value={profileColumn} onChange={(e) => setProfileColumn(e.target.value)} placeholder="customer_id" />
              </div>
              <div className="col-2">
                <label>Top N</label>
                <input type="number" value={topN} onChange={(e) => setTopN(Number(e.target.value || 10))} />
              </div>
              <div className="col-2">
                <label>&nbsp;</label>
                <button onClick={loadProfile}>Load Profile</button>
              </div>
            </div>
          </div>

          <div className="card">
            <h3>Filtered Preview</h3>
            <div className="row">
              <div className="col-4">
                <label>Column</label>
                <input value={filterColumn} onChange={(e) => setFilterColumn(e.target.value)} placeholder="status" />
              </div>
              <div className="col-4">
                <label>Value</label>
                <input value={filterValue} onChange={(e) => setFilterValue(e.target.value)} disabled={filterBlanks} placeholder="Open" />
              </div>
              <div className="col-2">
                <label>Blanks only</label>
                <select value={filterBlanks ? "1" : "0"} onChange={(e) => setFilterBlanks(e.target.value === "1")}>
                  <option value="0">No</option>
                  <option value="1">Yes</option>
                </select>
              </div>
              <div className="col-2">
                <label>&nbsp;</label>
                <button onClick={loadFilteredPreview}>Load Rows</button>
              </div>
            </div>
          </div>

          <div className="card">
            <h3>Profile Result</h3>
            <JsonView value={profileResult || { note: "Run profile to view result." }} />
          </div>

          <div className="card">
            <h3>Column Summary Result</h3>
            <JsonView value={columnSummaryResult || { note: "Run profile to view summary." }} />
          </div>

          <div className="card">
            <h3>Filtered Preview Result</h3>
            <JsonView value={filteredResult || { note: "Run filtered preview to view rows." }} />
          </div>
        </>
      ) : null}

      {tab === "compare" ? (
        <>
          <div className="card">
            <div className="row">
              <div className="col-4">
                <label>Source dataset</label>
                <select value={sourceDataset} onChange={(e) => onSourceDatasetChange(e.target.value)}>
                  <option value="">Select...</option>
                  {sourceOptions.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-4">
                <label>Target dataset</label>
                <select value={targetDataset} onChange={(e) => onTargetDatasetChange(e.target.value)}>
                  <option value="">Select...</option>
                  {targetOptions.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-4">
                <label>Pair ID (optional)</label>
                <select value={pairId} onChange={(e) => setPairId(e.target.value)}>
                  <option value="">None</option>
                  {pairOptions.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-12">
                <label>Field mappings (mark key and compare usage)</label>
                <div className="actions">
                  <button type="button" className="secondary" onClick={applyQuickMappings}>
                    Quick Map Matching Names
                  </button>
                  <button type="button" className="secondary" onClick={addMappingRow}>
                    Add Mapping Row
                  </button>
                  <button type="button" className="secondary" onClick={savePairMappings}>
                    Save Pair Mappings
                  </button>
                </div>
                <div className="mapping-toolbar">
                  <input
                    type="text"
                    value={mappingSearch}
                    onChange={(e) => setMappingSearch(e.target.value)}
                    placeholder="Search mapping rows (source/target field)..."
                  />
                  <div className="hint">
                    Showing {filteredMappingRows.length} of {fieldMappings.length}
                  </div>
                </div>
              </div>
              <div className="col-12">
                <div className="scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Source field</th>
                        <th>Target field</th>
                        <th>Use as key</th>
                        <th>Use in compare</th>
                        <th>Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredMappingRows.length ? (
                        filteredMappingRows.map(({ m, idx }) => (
                          <tr key={`${m.source_field}-${m.target_field}-${idx}`}>
                            <td>
                              <select
                                value={m.source_field}
                                onChange={(e) => updateMappingRow(idx, { source_field: e.target.value })}
                              >
                                <option value="">Select source field...</option>
                                {(selectedSource?.columns || []).map((c) => (
                                  <option key={c} value={c}>
                                    {c}
                                  </option>
                                ))}
                              </select>
                            </td>
                            <td>
                              <select
                                value={m.target_field}
                                onChange={(e) => updateMappingRow(idx, { target_field: e.target.value })}
                              >
                                <option value="">Select target field...</option>
                                {(selectedTarget?.columns || []).map((c) => (
                                  <option key={c} value={c}>
                                    {c}
                                  </option>
                                ))}
                              </select>
                            </td>
                            <td>
                              <input
                                type="checkbox"
                                className="check-input"
                                checked={!!m.use_key}
                                onChange={(e) => updateMappingRow(idx, { use_key: e.target.checked })}
                              />
                            </td>
                            <td>
                              <input
                                type="checkbox"
                                className="check-input"
                                checked={!!m.use_compare}
                                onChange={(e) => updateMappingRow(idx, { use_compare: e.target.checked })}
                              />
                            </td>
                            <td>
                              <button type="button" className="secondary" onClick={() => removeMappingRow(idx)}>
                                Remove
                              </button>
                            </td>
                          </tr>
                        ))
                      ) : (
                        <tr>
                          <td colSpan={5}>
                            {fieldMappings.length
                              ? "No rows match your search."
                              : "No mappings yet. Use quick map or add rows manually."}
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
              <div className="col-3">
                <button className="secondary" onClick={onQuickCompare}>
                  Quick Compare
                </button>
              </div>
              <div className="col-3">
                <button onClick={onStartJob}>Start Job</button>
              </div>
              <div className="col-3">
                <button className="secondary" onClick={refreshBootstrap}>
                  Refresh Jobs
                </button>
              </div>
            </div>
          </div>

          <div className="card">
            <h3>Compare Result</h3>
            <JsonView value={compareResult || { note: "Run a quick compare or job." }} />
          </div>

          <div className="card">
            <h3>Jobs ({jobs.length})</h3>
            <div className="scroll">
              <table>
                <thead>
                  <tr>
                    <th>Job ID</th>
                    <th>State</th>
                    <th>Source</th>
                    <th>Target</th>
                    <th>Created</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.map((j) => (
                    <tr key={j.id}>
                      <td>{j.id}</td>
                      <td>{j.state}</td>
                      <td>{j.source_dataset}</td>
                      <td>{j.target_dataset}</td>
                      <td>{j.created_at}</td>
                      <td>
                        <div className="actions">
                          <button className="secondary" onClick={() => loadJobSummary(j.id)}>
                            Summary
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="card">
            <h3>Selected Job Summary</h3>
            <JsonView value={jobSummary || { note: "Click Summary on a job row." }} />
          </div>
        </>
      ) : null}

      {tab === "reports" ? (
        <>
          <div className="card">
            <div className="row">
              <div className="col-3">
                <button className="secondary" onClick={refreshBootstrap}>
                  Refresh Reports
                </button>
              </div>
            </div>
          </div>
          <div className="card">
            <h3>Reports ({reports.length})</h3>
            <div className="scroll">
              <table>
                <thead>
                  <tr>
                    <th>Report ID</th>
                    <th>File</th>
                    <th>Source</th>
                    <th>Target</th>
                    <th>Created</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {reports.map((r) => (
                    <tr key={r.id}>
                      <td>{r.id}</td>
                      <td>{r.file_name}</td>
                      <td>{r.source_dataset}</td>
                      <td>{r.target_dataset}</td>
                      <td>{r.created_at}</td>
                      <td>
                        <div className="actions">
                          <a href={`/api/reports/${encodeURIComponent(r.id)}/download`} target="_blank" rel="noreferrer">
                            <button className="secondary">Download</button>
                          </a>
                          <button className="danger" onClick={() => onDeleteReport(r.id)}>
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}

      {tab === "relationships" ? (
        <>
          <div className="card">
            <div className="row">
              <div className="col-2">
                <label>Side</label>
                <select value={relationshipSide} onChange={(e) => setRelationshipSide(e.target.value)}>
                  <option value="source">source</option>
                  <option value="target">target</option>
                </select>
              </div>
              <div className="col-3">
                <label>Auto-link min confidence</label>
                <input
                  type="number"
                  min="0"
                  max="1"
                  step="0.01"
                  value={autoLinkConfidence}
                  onChange={(e) => setAutoLinkConfidence(e.target.value)}
                />
              </div>
              <div className="col-2">
                <label>Mode</label>
                <label className="toggle-inline">
                  <input
                    className="check-input"
                    type="checkbox"
                    checked={autoLinkSuggestOnly}
                    onChange={(e) => setAutoLinkSuggestOnly(e.target.checked)}
                  />
                  <span>Suggest only</span>
                </label>
              </div>
              <div className="col-2">
                <label>&nbsp;</label>
                <button className="auto-link-btn" onClick={runAutoLink}>
                  Auto-link
                </button>
              </div>
              <div className="col-2">
                <label>&nbsp;</label>
                <button className="secondary" onClick={refreshBootstrap}>
                  Refresh
                </button>
              </div>
            </div>
          </div>

          <div className="card">
            <h3>{relationshipId ? `Edit Relationship #${relationshipId}` : "Create Relationship"}</h3>
            <div className="row">
              <div className="col-3">
                <label>Left dataset</label>
                <select
                  value={leftDatasetId}
                  onChange={(e) => {
                    setLeftDatasetId(e.target.value);
                    setLeftField("");
                    setLeftExtraFields("");
                  }}
                >
                  <option value="">Select...</option>
                  {relationshipDatasets.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-3">
                <label>Left field</label>
                <select value={leftField} onChange={(e) => setLeftField(e.target.value)}>
                  <option value="">Select...</option>
                  {leftFieldOptions.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-3">
                <label>Right dataset</label>
                <select
                  value={rightDatasetId}
                  onChange={(e) => {
                    setRightDatasetId(e.target.value);
                    setRightField("");
                    setRightExtraFields("");
                  }}
                >
                  <option value="">Select...</option>
                  {relationshipDatasets.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-3">
                <label>Right field</label>
                <select value={rightField} onChange={(e) => setRightField(e.target.value)}>
                  <option value="">Select...</option>
                  {rightFieldOptions.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
              </div>
            </div>
            <div className="row">
              <div className="col-6">
                <label>Additional left fields (comma-separated, same order as right)</label>
                <input
                  placeholder="e.g. LINENUM, INVENTDIMID"
                  value={leftExtraFields}
                  onChange={(e) => setLeftExtraFields(e.target.value)}
                />
              </div>
              <div className="col-6">
                <label>Additional right fields (comma-separated, same order as left)</label>
                <input
                  placeholder="e.g. LINENUM, INVENTDIMID"
                  value={rightExtraFields}
                  onChange={(e) => setRightExtraFields(e.target.value)}
                />
              </div>
            </div>
            <div className="row">
              <div className="col-2">
                <label>Confidence</label>
                <input
                  type="number"
                  min="0"
                  max="1"
                  step="0.001"
                  value={relationshipConfidence}
                  onChange={(e) => setRelationshipConfidence(e.target.value)}
                />
              </div>
              <div className="col-3">
                <label>Method</label>
                <input value={relationshipMethod} onChange={(e) => setRelationshipMethod(e.target.value)} />
              </div>
              <div className="col-2">
                <label>Active</label>
                <select value={relationshipActive ? "1" : "0"} onChange={(e) => setRelationshipActive(e.target.value === "1")}>
                  <option value="1">Yes</option>
                  <option value="0">No</option>
                </select>
              </div>
              <div className="col-2">
                <label>&nbsp;</label>
                <button onClick={saveRelationship}>{relationshipId ? "Update" : "Create"}</button>
              </div>
              <div className="col-2">
                <label>&nbsp;</label>
                <button className="secondary" onClick={clearRelationshipForm}>
                  Clear
                </button>
              </div>
            </div>
          </div>

          <div className="card">
            <h3>Relationships ({filteredRelationships.length})</h3>
            <div className="scroll">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Left</th>
                    <th>Right</th>
                    <th>Confidence</th>
                    <th>Method</th>
                    <th>Active</th>
                    <th>Updated</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRelationships.map((r) => (
                    <tr key={r.id}>
                      <td>{r.id}</td>
                      <td>{`${r.left_dataset}.${relationshipFieldLabel(r, "left")}`}</td>
                      <td>{`${r.right_dataset}.${relationshipFieldLabel(r, "right")}`}</td>
                      <td>{r.confidence}</td>
                      <td>{r.method}</td>
                      <td>{r.active ? "Yes" : "No"}</td>
                      <td>{r.updated_at}</td>
                      <td>
                        <div className="actions">
                          <button className="secondary" onClick={() => editRelationship(r)}>
                            Edit
                          </button>
                          <button className="danger" onClick={() => removeRelationship(r.id)}>
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}
        </main>
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
