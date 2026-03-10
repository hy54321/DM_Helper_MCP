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

function DataGrid({ headers, rows, emptyMessage = "No rows." }) {
  const safeHeaders = Array.isArray(headers) ? headers : [];
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeHeaders.length) {
    return <div style={{ color: "#5b6470" }}>{emptyMessage}</div>;
  }
  return (
    <div className="scroll">
      <table>
        <thead>
          <tr>
            {safeHeaders.map((header, idx) => (
              <th key={`grid-head-${idx}`}>{displayValue(header)}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {safeRows.length ? (
            safeRows.map((row, rowIdx) => (
              <tr key={`grid-row-${rowIdx}`}>
                {(Array.isArray(row) ? row : []).map((cell, colIdx) => (
                  <td key={`grid-cell-${rowIdx}-${colIdx}`}>{displayValue(cell)}</td>
                ))}
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={Math.max(1, safeHeaders.length)}>{emptyMessage}</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function objectRowsToGrid(rows) {
  const sourceRows = Array.isArray(rows) ? rows.filter((r) => r && typeof r === "object") : [];
  if (!sourceRows.length) return { headers: [], rows: [] };

  const headers = [];
  sourceRows.forEach((row) => {
    Object.keys(row).forEach((key) => {
      if (!headers.includes(key)) headers.push(key);
    });
  });

  const data = sourceRows.map((row) =>
    headers.map((key) => {
      const value = row[key];
      return value && typeof value === "object" ? JSON.stringify(value) : value;
    })
  );
  return { headers, rows: data };
}

function changedSampleToGrid(changedSample) {
  if (!Array.isArray(changedSample) || !changedSample.length) {
    return { headers: [], rows: [] };
  }
  const rows = [];
  changedSample.forEach((item) => {
    const keyText =
      item?.keys && typeof item.keys === "object"
        ? Object.entries(item.keys)
            .map(([k, v]) => `${k}=${displayValue(v)}`)
            .join(", ")
        : "-";
    const changes = Array.isArray(item?.changes) ? item.changes : [];
    if (!changes.length) {
      rows.push({ keys: keyText, field: "-", source: "-", target: "-" });
      return;
    }
    changes.forEach((chg) => {
      rows.push({
        keys: keyText,
        field: chg?.field || `${displayValue(chg?.source_field)} -> ${displayValue(chg?.target_field)}`,
        source: chg?.source,
        target: chg?.target,
      });
    });
  });
  return objectRowsToGrid(rows);
}

function displayValue(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function buildInspectorEmbedUrl(baseUrl, mcpPort) {
  const fallbackBase = "http://localhost:6274";
  const fallbackPort = Number(mcpPort) > 0 ? Number(mcpPort) : 8000;
  try {
    const url = new URL(baseUrl || fallbackBase);
    if (!url.searchParams.get("transport")) {
      url.searchParams.set("transport", "streamable-http");
    }
    if (!url.searchParams.get("serverUrl")) {
      url.searchParams.set("serverUrl", `http://127.0.0.1:${fallbackPort}/mcp`);
    }
    return url.toString();
  } catch (_err) {
    return `${fallbackBase}/?transport=streamable-http&serverUrl=${encodeURIComponent(
      `http://127.0.0.1:${fallbackPort}/mcp`
    )}`;
  }
}

const SUMMARY_TOP_N = 10;
const DEFAULT_CLAUDE_INSTRUCTIONS = `You are a Data Migration Assistant using MCP server

Mission:
Reconcile source vs target datasets with tool-based evidence only.

Operating policy:
1. Prefer MCP tools over assumptions.
2. If datasets are missing/stale, call \`refresh_catalog\`.
3. Before analysis, confirm IDs via \`list_datasets\`; confirm columns via \`list_fields\`.
4. Before comparison, always run \`schema_diff\`.
5. For keys, use \`suggest_keys\` when a pair exists; state key confidence/risk.
6. When comparison is requested use:
   - \`compare_tables\` to respond to user with summary of findings.
   - \`start_comparison_job\` -> \`get_job_status\` -> \`get_job_summary\` to trigger creation of comparison excel report.
7. Never claim source/target data was modified.
8. Only run \`upsert_pair_override\`, \`save_key_preset\`, or \`delete_report\` if user explicitly asks.
9. If a cross join query is required checl links using 'get_dataset_links'.

Default workflow:
- Discovery: \`refresh_catalog\`, \`list_datasets\`, use \`list_table_pairs\` to find out which datasets are paired, use \`list_field_pairs\` to find out field and unique index pairings with the pair_id from 'list_table_pairs', \`row_count_summary\`
- Profiling: \`list_fields\`, \`data_profile\`, \`column_value_summary\`
- Optional profiling: \`find_duplicates\`, \`value_distribution\`, \`preview_filtered_records\`
- Comparison: \`schema_diff\`, \`suggest_keys\`, \`compare_tables\` or async job flow, \`compare_field\`
- Reporting:
  - If user asks a report on a single table/dataset/file then use \`export_column_value_summary\` for Excel-style Top N + Blanks
  - \`export_query\` for custom SQL results exports only.
  - \`list_reports\`, \`get_report_metadata\`

SQL rules:
- Use \`run_sql_preview\` for read-only exploration.
- Keep SQL minimal and purpose-driven.
- Use \`export_query\` for full export requests.
- On SQL error, explain cause and provide corrected next call.

Response format (always):
1) Summary: objective, dataset IDs, key fields, main result
2) Evidence: tool calls (name + key args), counts, sample mismatches
3) Interpretation: likely root causes, confidence/risks
4) Next actions: concrete MCP calls

Error recovery:
- Dataset not found -> \`list_datasets\`, ask for exact ID
- Column not found -> \`list_fields\`, propose valid columns
- Schema drift issue -> report drift, recommend compare field subset/mapping
- Long job -> poll \`get_job_status\` and report progress`;

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
  const [profileResult, setProfileResult] = useState(null);
  const [columnSummaryResult, setColumnSummaryResult] = useState(null);
  const [comboSummaryRows, setComboSummaryRows] = useState([]);
  const [selectedSummaryColumns, setSelectedSummaryColumns] = useState([]);
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
  const [compareSampleTab, setCompareSampleTab] = useState("added");
  const [quickMapChoiceOpen, setQuickMapChoiceOpen] = useState(false);
  const [quickMapPendingMappings, setQuickMapPendingMappings] = useState([]);
  const [jobSummary, setJobSummary] = useState(null);
  const [relationshipSide, setRelationshipSide] = useState("target");
  const [relationshipId, setRelationshipId] = useState("");
  const [leftDatasetId, setLeftDatasetId] = useState("");
  const [rightDatasetId, setRightDatasetId] = useState("");
  const [relationshipMappings, setRelationshipMappings] = useState([{ left_field: "", right_field: "" }]);
  const [relationshipConfidence, setRelationshipConfidence] = useState(0.95);
  const [relationshipMethod, setRelationshipMethod] = useState("manual");
  const [relationshipActive, setRelationshipActive] = useState(true);
  const [autoLinkConfidence, setAutoLinkConfidence] = useState(0.9);
  const [serviceState, setServiceState] = useState({
    desktop_mode: false,
    services: {},
    ui: { running: true, host: "127.0.0.1", port: "8001" },
  });
  const [serviceBusy, setServiceBusy] = useState({});
  const [settingsTheme, setSettingsTheme] = useState("light");
  const [settingsApiKeyInput, setSettingsApiKeyInput] = useState("");
  const [settingsApiKeyMasked, setSettingsApiKeyMasked] = useState("");
  const [settingsApiKeySet, setSettingsApiKeySet] = useState(false);
  const [settingsApiKeyNeedsReset, setSettingsApiKeyNeedsReset] = useState(false);
  const [settingsApiKeyActivated, setSettingsApiKeyActivated] = useState(false);
  const [settingsModel, setSettingsModel] = useState("");
  const [settingsModels, setSettingsModels] = useState([]);
  const [settingsClaudeInstructions, setSettingsClaudeInstructions] = useState("");
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsValidating, setSettingsValidating] = useState(false);
  const [settingsLoadingModels, setSettingsLoadingModels] = useState(false);
  const [claudeMessages, setClaudeMessages] = useState([]);
  const [claudeInput, setClaudeInput] = useState("");
  const [claudeSending, setClaudeSending] = useState(false);
  const [claudeInlineError, setClaudeInlineError] = useState("");

  function applyAppSettings(payload) {
    const safe = payload && typeof payload === "object" ? payload : {};
    setSettingsTheme(safe.theme === "dark" ? "dark" : "light");
    const cachedModels = Array.isArray(safe.models)
      ? safe.models
          .filter((m) => m && typeof m === "object" && String(m.id || "").trim())
          .map((m) => ({
            id: String(m.id || "").trim(),
            display_name: String(m.display_name || m.id || "").trim(),
          }))
      : [];
    setSettingsModels(cachedModels);
    setSettingsApiKeySet(!!safe.anthropic_api_key_set);
    setSettingsApiKeyMasked(String(safe.anthropic_api_key_masked || ""));
    setSettingsApiKeyNeedsReset(!!safe.anthropic_api_key_needs_reset);
    setSettingsApiKeyActivated(!!safe.anthropic_api_key_activated);
    setSettingsModel(String(safe.model || ""));
    setSettingsClaudeInstructions(String(safe.claude_instructions || ""));
  }

  async function loadServices({ silent = false } = {}) {
    try {
      const state = await api("/api/system/services");
      const nextState =
        state || {
          desktop_mode: false,
          services: {},
          ui: { running: true, host: "127.0.0.1", port: "8001" },
        };
      setServiceState(nextState);
      if (!silent) {
        setStatus("Service status loaded.");
      }
      return nextState;
    } catch (err) {
      if (!silent) {
        setError(err.message);
        setStatus("Service status unavailable.");
      }
      return null;
    }
  }

  async function loadAppSettings({ silent = false } = {}) {
    try {
      const data = await api("/api/settings/app");
      applyAppSettings(data);
      if (!silent) {
        setStatus("Settings loaded.");
      }
    } catch (err) {
      if (!silent) {
        setError(err.message);
      }
    }
  }

  async function refreshBootstrap() {
    setError("");
    try {
      const [folders, ds, pr, jb, rp, rels] = await Promise.all([
        api("/api/settings/folders"),
        api("/api/datasets"),
        api("/api/pairs"),
        api("/api/jobs"),
        api("/api/reports?limit=0"),
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
      await loadServices({ silent: true });
      setStatus("Loaded latest metadata.");
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    refreshBootstrap();
    loadAppSettings({ silent: true });
  }, []);

  useEffect(() => {
    const theme = settingsTheme === "dark" ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", theme);
  }, [settingsTheme]);

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
      setRelationshipMappings([{ left_field: "", right_field: "" }]);
    }
    if (rightDatasetId && !sideDatasetIds.has(rightDatasetId)) {
      setRightDatasetId("");
      setRelationshipMappings([{ left_field: "", right_field: "" }]);
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

  useEffect(() => {
    const inspectorRunning = !!serviceState.services?.mcp_inspector?.running;
    if (tab === "inspector" && !inspectorRunning) {
      setTab("settings");
    }
  }, [tab, serviceState]);

  useEffect(() => {
    if (tab === "claude" && !settingsApiKeyActivated) {
      setTab("settings");
    }
  }, [tab, settingsApiKeyActivated]);

  useEffect(() => {
    const ds = datasets.find((d) => d.id === profileDataset);
    const cols = new Set((ds?.columns || []).map((c) => String(c)));
    if (profileColumn && !cols.has(profileColumn)) {
      setProfileColumn("");
    }
    if (filterColumn && !cols.has(filterColumn)) {
      setFilterColumn("");
    }
  }, [profileDataset, datasets]);

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

  async function onValidateAnthropicKey() {
    const key = String(settingsApiKeyInput || "").trim();
    if (!key && !settingsApiKeySet) {
      setError("Enter and save an Anthropic API key before validating.");
      return;
    }
    setError("");
    setSettingsValidating(true);
    setStatus("Validating Anthropic API key...");
    try {
      const res = await api("/api/settings/anthropic/validate", {
        method: "POST",
        body: JSON.stringify({ api_key: key }),
      });
      if (res?.app_settings) {
        applyAppSettings(res.app_settings);
      }
      setStatus(res?.message || "Anthropic API key is valid.");
    } catch (err) {
      setError(err.message);
      setStatus("Anthropic API key validation failed.");
    } finally {
      setSettingsValidating(false);
    }
  }

  async function onLookupAnthropicModels() {
    const inputKey = String(settingsApiKeyInput || "").trim();
    if (!inputKey && !settingsApiKeySet) {
      setError("Enter an API key or save one before looking up models.");
      return;
    }
    setError("");
    setSettingsLoadingModels(true);
    setStatus("Loading Anthropic models...");
    try {
      const res = await api("/api/settings/anthropic/models", {
        method: "POST",
        body: JSON.stringify({ api_key: inputKey }),
      });
      const models = Array.isArray(res?.models) ? res.models : [];
      setSettingsModels(models);
      if (!settingsModel) {
        if (res?.selected_model) {
          setSettingsModel(res.selected_model);
        } else if (models.length) {
          setSettingsModel(models[0].id);
        }
      }
      setStatus(`Loaded ${models.length} model(s) from Anthropic.`);
    } catch (err) {
      setError(err.message);
      setStatus("Model lookup failed.");
    } finally {
      setSettingsLoadingModels(false);
    }
  }

  async function onSaveAppSettings() {
    setError("");
    setSettingsSaving(true);
    setStatus("Saving app settings...");
    try {
      const payload = {
        theme: settingsTheme,
        model: String(settingsModel || "").trim(),
        anthropic_api_key: String(settingsApiKeyInput || "").trim() || null,
        claude_instructions: String(settingsClaudeInstructions || "").trim(),
      };
      const saved = await api("/api/settings/app", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      applyAppSettings(saved);
      setSettingsApiKeyInput("");
      setStatus("Settings saved.");
    } catch (err) {
      setError(err.message);
      setStatus("Saving settings failed.");
    } finally {
      setSettingsSaving(false);
    }
  }

  async function onSendClaudeMessage() {
    const message = String(claudeInput || "").trim();
    if (!message || claudeSending) {
      return;
    }
    setClaudeInlineError("");
    if (!settingsApiKeyActivated) {
      setClaudeInlineError("Validate your saved Anthropic API key in Settings first.");
      setTab("settings");
      return;
    }
    if (!String(settingsModel || "").trim()) {
      setClaudeInlineError("Select and save an Anthropic model in Settings first.");
      setTab("settings");
      return;
    }
    const liveState = await loadServices({ silent: true });
    const effectiveState = liveState || serviceState;
    const mcpServerRunning = !!effectiveState?.services?.mcp_server?.running;
    if (!mcpServerRunning) {
      setClaudeInlineError("MCP server is not started. Start MCP server in Settings before sending a message.");
      setStatus("Claude message not sent.");
      return;
    }

    const history = claudeMessages.map((item) => ({
      role: String(item.role || ""),
      content: String(item.content || ""),
    }));
    const nextUserMessage = { role: "user", content: message };

    setError("");
    setClaudeSending(true);
    setClaudeMessages((prev) => [...prev, nextUserMessage]);
    setClaudeInput("");
    setStatus("Sending message to Claude...");
    try {
      const res = await api("/api/claude/chat", {
        method: "POST",
        body: JSON.stringify({
          message,
          history,
        }),
      });
      const assistantMessage = {
        role: "assistant",
        content: String(res?.message?.content || "").trim() || "(No text response)",
      };
      setClaudeMessages((prev) => [...prev, assistantMessage]);
      setStatus("Claude response received.");
    } catch (err) {
      setClaudeInlineError(String(err.message || "Failed to send Claude message."));
      setError(err.message);
      setStatus("Claude chat failed.");
    } finally {
      setClaudeSending(false);
    }
  }

  function onClearClaudeChat() {
    setClaudeMessages([]);
    setClaudeInput("");
    setClaudeInlineError("");
  }

  function onLoadDefaultClaudeInstructions() {
    setSettingsClaudeInstructions(DEFAULT_CLAUDE_INSTRUCTIONS);
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
        api(`/api/summary/column/${encodeURIComponent(profileDataset)}${profileColumn ? `?column=${encodeURIComponent(profileColumn)}` : ""}`),
      ]);
      setProfileResult(profile);
      setColumnSummaryResult(summary);
      setComboSummaryRows([]);
      setSelectedSummaryColumns([]);
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

  function toggleSummaryColumnSelection(column) {
    if (!column) return;
    setSelectedSummaryColumns((prev) =>
      prev.includes(column) ? prev.filter((c) => c !== column) : [...prev, column]
    );
  }

  async function addMultiFieldSummary() {
    if (!profileDataset) return;
    const uniqueColumns = [...new Set(selectedSummaryColumns.filter(Boolean))];
    if (uniqueColumns.length < 2) {
      setError("Select at least two base columns to add a multi-field summary.");
      return;
    }
    const signature = uniqueColumns.join("|||");
    const exists = comboSummaryRows.some((r) => (r.columns || []).join("|||") === signature);
    if (exists) {
      setStatus("That multi-field combination is already in the summary.");
      return;
    }

    setError("");
    setStatus("Adding multi-field summary...");
    try {
      const combo = await api(`/api/summary/combo/${encodeURIComponent(profileDataset)}`, {
        method: "POST",
        body: JSON.stringify({
          columns: uniqueColumns,
        }),
      });
      if (combo?.error) {
        throw new Error(combo.error);
      }
      const normalized = {
        column: combo.column || uniqueColumns.join(" - "),
        columns: uniqueColumns,
        top_values: Array.isArray(combo.top_values) ? combo.top_values : [],
        blank_or_null_count:
          combo.blank_or_null_count === null || combo.blank_or_null_count === undefined
            ? "-"
            : combo.blank_or_null_count,
      };
      setComboSummaryRows((prev) => [...prev, normalized]);
      setSelectedSummaryColumns([]);
      setStatus(`Added multi-field summary for ${normalized.column}.`);
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

  function hasExistingMappings(rows = fieldMappings) {
    return (Array.isArray(rows) ? rows : []).some(
      (m) => String(m?.source_field || "").trim() && String(m?.target_field || "").trim()
    );
  }

  function mergeQuickMappings(existingRows, mappedRows) {
    const existing = Array.isArray(existingRows) ? existingRows : [];
    const merged = [...existing];
    const signatures = new Set(
      existing
        .filter((m) => String(m?.source_field || "").trim() && String(m?.target_field || "").trim())
        .map((m) => `${normalizeFieldName(m.source_field)}|||${normalizeFieldName(m.target_field)}`)
    );
    let addedCount = 0;
    for (const row of Array.isArray(mappedRows) ? mappedRows : []) {
      const sig = `${normalizeFieldName(row.source_field)}|||${normalizeFieldName(row.target_field)}`;
      if (signatures.has(sig)) continue;
      merged.push(row);
      signatures.add(sig);
      addedCount += 1;
    }
    return { merged, addedCount, existingCount: existing.length };
  }

  function applyQuickMappingsChoice(mode, mappedRows) {
    setError("");
    const mapped = Array.isArray(mappedRows) ? mappedRows : quickMapPendingMappings;
    if (mode === "cancel") {
      setQuickMapChoiceOpen(false);
      setQuickMapPendingMappings([]);
      setStatus("Quick map cancelled.");
      return;
    }
    if (mode === "override") {
      setFieldMappings(mapped);
      setQuickMapChoiceOpen(false);
      setQuickMapPendingMappings([]);
      setStatus(`Quick-mapped ${mapped.length} same-name field(s). Existing mappings were overridden.`);
      return;
    }
    const { merged, addedCount, existingCount } = mergeQuickMappings(fieldMappings, mapped);
    setFieldMappings(merged);
    setQuickMapChoiceOpen(false);
    setQuickMapPendingMappings([]);
    setStatus(
      `Quick-mapped ${mapped.length} same-name field(s). Added ${addedCount} new mapping(s); kept ${existingCount} existing row(s).`
    );
  }

  function applyQuickMappings() {
    setError("");
    const src = datasets.find((d) => d.id === sourceDataset);
    const tgt = datasets.find((d) => d.id === targetDataset);
    if (!src || !tgt) {
      setError("Select source and target datasets first.");
      return;
    }
    const mapped = buildQuickMappings(src.columns || [], tgt.columns || []);
    if (!mapped.length) {
      setStatus("No same-name fields found to quick-map.");
      return;
    }
    if (pairId || hasExistingMappings()) {
      setQuickMapPendingMappings(mapped);
      setQuickMapChoiceOpen(true);
      return;
    }
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

  async function onOpenReport(reportId) {
    setError("");
    setStatus(`Opening report ${reportId}...`);
    try {
      await api(`/api/reports/${encodeURIComponent(reportId)}/open`, { method: "POST" });
      setStatus(`Opened report ${reportId}.`);
    } catch (err) {
      setError(err.message);
      setStatus("Open report failed.");
    }
  }

  function clearRelationshipForm() {
    setRelationshipId("");
    setLeftDatasetId("");
    setRightDatasetId("");
    setRelationshipMappings([{ left_field: "", right_field: "" }]);
    setRelationshipConfidence(0.95);
    setRelationshipMethod("manual");
    setRelationshipActive(true);
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
    setRightDatasetId(row.right_dataset || "");
    const rf = row.right_fields || (row.right_field ? [row.right_field] : []);
    const rowCount = Math.max(lf.length, rf.length, 1);
    const rows = [];
    for (let i = 0; i < rowCount; i += 1) {
      rows.push({
        left_field: lf[i] || "",
        right_field: rf[i] || "",
      });
    }
    setRelationshipMappings(rows);
    setRelationshipConfidence(Number(row.confidence || 0.95));
    setRelationshipMethod(row.method || "manual");
    setRelationshipActive(!!row.active);
    setTab("relationships");
  }

  function addRelationshipMappingRow() {
    setRelationshipMappings((prev) => [...prev, { left_field: "", right_field: "" }]);
  }

  function removeRelationshipMappingRow(index) {
    setRelationshipMappings((prev) => {
      const next = prev.filter((_, i) => i !== index);
      return next.length ? next : [{ left_field: "", right_field: "" }];
    });
  }

  function updateRelationshipMappingRow(index, patch) {
    setRelationshipMappings((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  }

  async function saveRelationship() {
    if (!leftDatasetId || !rightDatasetId) {
      setError("Select left and right datasets first.");
      return;
    }

    const nonEmptyRows = relationshipMappings.filter((m) => (m.left_field || "").trim() || (m.right_field || "").trim());
    if (!nonEmptyRows.length) {
      setError("Select left/right datasets and fields.");
      return;
    }

    if (nonEmptyRows.some((m) => !(m.left_field || "").trim() || !(m.right_field || "").trim())) {
      setError("Each mapping row must have both left and right fields.");
      return;
    }

    const leftFields = nonEmptyRows.map((m) => m.left_field.trim());
    const rightFields = nonEmptyRows.map((m) => m.right_field.trim());

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
    setStatus("Auto-linking related tables...");
    try {
      const res = await api("/api/relationships/link-related", {
        method: "POST",
        body: JSON.stringify({
          side: relationshipSide,
          min_confidence: Number(autoLinkConfidence),
          suggest_only: false,
        }),
      });
      await refreshBootstrap();
      setStatus(`Auto-link complete. Suggested=${res.suggested_count}, Applied=${res.applied_count}.`);
    } catch (err) {
      setError(err.message);
      setStatus("Auto-link failed.");
    }
  }

  async function onToggleService(serviceName, nextEnabled) {
    const labels = {
      mcp_server: "MCP server",
      mcp_inspector: "MCP inspector",
      ngrok: "ngrok",
    };
    const label = labels[serviceName] || serviceName;
    const requiresFolders = serviceName === "mcp_server" || serviceName === "mcp_inspector";
    const src = String(sourceFolder || "").trim();
    const tgt = String(targetFolder || "").trim();
    const rpt = String(reportFolder || "").trim();
    const foldersConfigured = !!src && !!tgt && !!rpt;

    if (nextEnabled && requiresFolders) {
      if (!foldersConfigured) {
        const msg = "Select source, target, and report folders first in Catalog before starting MCP server or inspector.";
        setError(msg);
        setStatus("Service start blocked.");
        return;
      }
      try {
        await api("/api/settings/folders", {
          method: "POST",
          body: JSON.stringify({
            source_folder: src,
            target_folder: tgt,
            report_folder: rpt,
          }),
        });
      } catch (err) {
        setError(err.message);
        setStatus("Could not save folders before starting service.");
        return;
      }
    }

    setError("");
    setStatus(`${nextEnabled ? "Starting" : "Stopping"} ${label}...`);
    setServiceBusy((prev) => ({ ...prev, [serviceName]: true }));
    try {
      await api(`/api/system/services/${encodeURIComponent(serviceName)}/${nextEnabled ? "start" : "stop"}`, {
        method: "POST",
      });
      await loadServices({ silent: true });
      setStatus(`${label} ${nextEnabled ? "started" : "stopped"}.`);
    } catch (err) {
      setError(err.message);
      setStatus(`${label} ${nextEnabled ? "start" : "stop"} failed.`);
    } finally {
      setServiceBusy((prev) => ({ ...prev, [serviceName]: false }));
    }
  }

  async function onForceStopService(serviceName) {
    const labels = {
      mcp_server: "MCP server",
      mcp_inspector: "MCP inspector",
    };
    const label = labels[serviceName] || serviceName;
    if (
      !confirm(
        `Force stop ${label}? This will kill any process listening on its port(s), even if it was started outside this app.`
      )
    ) {
      return;
    }

    setError("");
    setStatus(`Force stopping ${label}...`);
    setServiceBusy((prev) => ({ ...prev, [serviceName]: true }));
    try {
      const result = await api(`/api/system/services/${encodeURIComponent(serviceName)}/force-stop`, {
        method: "POST",
      });
      await loadServices({ silent: true });
      const killed = Array.isArray(result?.killed_pids) ? result.killed_pids.length : 0;
      const checkedPorts = Array.isArray(result?.checked_ports) ? result.checked_ports.filter((p) => Number.isFinite(p)) : [];
      const checkedText = checkedPorts.length ? ` Checked ports: ${checkedPorts.join(", ")}.` : "";
      setStatus(
        killed > 0
          ? `${label} force-stopped. Killed ${killed} process(es).`
          : `${label} force-stop completed. No external listener process was killed.${checkedText}`
      );
    } catch (err) {
      setError(err.message);
      setStatus(`Force stop ${label} failed.`);
    } finally {
      setServiceBusy((prev) => ({ ...prev, [serviceName]: false }));
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
  const profileDatasetObj = datasets.find((d) => d.id === profileDataset);
  const profileFieldOptions = profileDatasetObj?.columns || [];
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
  const foldersConfigured =
    String(sourceFolder || "").trim().length > 0 &&
    String(targetFolder || "").trim().length > 0 &&
    String(reportFolder || "").trim().length > 0;
  const canValidateAnthropicKey = String(settingsApiKeyInput || "").trim().length > 0 || settingsApiKeySet;
  const canLookupAnthropicModels = canValidateAnthropicKey || settingsApiKeySet;
  const settingsBusy = settingsSaving || settingsValidating || settingsLoadingModels;
  const claudeTabEnabled = settingsApiKeyActivated;
  const claudeCanSend =
    claudeTabEnabled &&
    String(settingsModel || "").trim().length > 0 &&
    String(claudeInput || "").trim().length > 0 &&
    !claudeSending;
  const desktopMode = !!serviceState.desktop_mode;
  const inspectorRunning = !!serviceState.services?.mcp_inspector?.running;
  const inspectorUrl = buildInspectorEmbedUrl(
    serviceState.services?.mcp_inspector?.service_url || "http://localhost:6274",
    serviceState.services?.mcp_server?.port || 8000
  );
  const managedServices = [
    {
      key: "mcp_server",
      label: "MCP Server",
      description: "Runs streamable-http MCP endpoint for external tool calls.",
    },
    {
      key: "mcp_inspector",
      label: "MCP Inspector",
      description: "Runs inspector web app against the local MCP server.",
    },
    {
      key: "ngrok",
      label: "ngrok",
      description: "Publishes MCP port externally (requires ngrok installed).",
    },
  ];
  const baseColumnSummaries = Array.isArray(columnSummaryResult?.summaries)
    ? columnSummaryResult.summaries
    : [];
  const mergedColumnSummaries = [...baseColumnSummaries, ...comboSummaryRows];
  const topHeaders = Array.from({ length: SUMMARY_TOP_N }, (_, idx) => `Top ${idx + 1}`);
  const quickAddedGrid = objectRowsToGrid(compareResult?.added_sample);
  const quickRemovedGrid = objectRowsToGrid(compareResult?.removed_sample);
  const quickChangedGrid = changedSampleToGrid(compareResult?.changed_sample);
  const hasLegacyCompareResult = !!(compareResult?.added && compareResult?.removed && compareResult?.changed);
  const hasQuickCompareResult = !!(
    compareResult &&
    (Object.prototype.hasOwnProperty.call(compareResult, "added_count") ||
      Object.prototype.hasOwnProperty.call(compareResult, "removed_count") ||
      Object.prototype.hasOwnProperty.call(compareResult, "changed_count") ||
      Array.isArray(compareResult.added_sample) ||
      Array.isArray(compareResult.removed_sample) ||
      Array.isArray(compareResult.changed_sample))
  );
  const hasJobCompareResult = !!compareResult?.job_id;
  const compareSampleTabs = [
    { key: "added", label: "Added", emptyMessage: "No added rows." },
    { key: "removed", label: "Removed", emptyMessage: "No removed rows." },
    { key: "changed", label: "Changed", emptyMessage: "No changed rows." },
  ];
  const compareSampleGridByTab = hasLegacyCompareResult
    ? {
        added: { headers: compareResult?.added?.headers, rows: compareResult?.added?.data },
        removed: { headers: compareResult?.removed?.headers, rows: compareResult?.removed?.data },
        changed: { headers: compareResult?.changed?.headers, rows: compareResult?.changed?.data },
      }
    : {
        added: quickAddedGrid,
        removed: quickRemovedGrid,
        changed: quickChangedGrid,
      };
  const activeCompareSampleKey = compareSampleTabs.some((tabDef) => tabDef.key === compareSampleTab)
    ? compareSampleTab
    : "added";
  const activeCompareSampleDef = compareSampleTabs.find((tabDef) => tabDef.key === activeCompareSampleKey) || compareSampleTabs[0];
  const activeCompareSampleGrid = compareSampleGridByTab[activeCompareSampleKey] || { headers: [], rows: [] };

  return (
    <div className="app">
      <div className="header">
        <h1>DM Helper Admin</h1>
      </div>

      <div className="layout">
        <aside className="sidebar">
          <div className="tabs">
            <button className={`tab ${tab === "catalog" ? "active" : ""}`} onClick={() => setTab("catalog")}>
              Catalog
            </button>
            <button className={`tab ${tab === "relationships" ? "active" : ""}`} onClick={() => setTab("relationships")}>
              Relationships
            </button>
            <button className={`tab ${tab === "profile" ? "active" : ""}`} onClick={() => setTab("profile")}>
              Profiling
            </button>
            <button className={`tab ${tab === "compare" ? "active" : ""}`} onClick={() => setTab("compare")}>
              Comparison
            </button>
            <button className={`tab ${tab === "reports" ? "active" : ""}`} onClick={() => setTab("reports")}>
              Reports
            </button>
            <button
              className={`tab ${tab === "claude" ? "active" : ""}`}
              onClick={() => setTab("claude")}
              disabled={!claudeTabEnabled}
              title={!claudeTabEnabled ? "Validate your saved Anthropic API key in Settings first." : "Open Claude chat"}
            >
              Claude
            </button>
            <button
              className={`tab ${tab === "inspector" ? "active" : ""}`}
              onClick={() => setTab("inspector")}
              disabled={!inspectorRunning}
              title={!inspectorRunning ? "Start MCP Inspector in Settings first." : "Open MCP Inspector"}
            >
              MCP Inspector
            </button>
            <button className={`tab ${tab === "settings" ? "active" : ""}`} onClick={() => setTab("settings")}>
              Settings
            </button>
          </div>
        </aside>
        <main className="content">
          <div className="status-row">
            <div className="status">
              <strong>Status:</strong> {status}
              {error ? (
                <>
                  {" "}
                  <span style={{ color: "#b42318" }}>| Error: {error}</span>
                </>
              ) : null}
            </div>
            <div className="status-actions">
              {tab === "settings" ? (
                <button
                  className="secondary status-icon-btn"
                  onClick={() => loadAppSettings()}
                  disabled={settingsBusy}
                  title="Reload saved settings"
                  aria-label="Reload saved settings"
                >
                  {"\u21bb"}
                </button>
              ) : null}
              {tab === "inspector" && inspectorRunning ? (
                <button className="secondary inspector-refresh-btn" onClick={() => loadServices()}>
                  Refresh Inspector Status
                </button>
              ) : null}
            </div>
          </div>

      {tab === "settings" ? (
        <>
          <div className="card">
            <div className="row">
              <div className="col-8">
                <h3>Desktop Services</h3>
                <p className="sub" style={{ margin: 0 }}>
                  UI backend is always running. Use sliders below to start/stop MCP server and ngrok.
                </p>
              </div>
              <div className="col-4">
                <label>&nbsp;</label>
                <button className="secondary" onClick={() => loadServices()}>
                  Refresh Service Status
                </button>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="row">
              <div className="col-12">
                <table className="services-table">
                  <colgroup>
                    <col className="svc-col-service" />
                    <col className="svc-col-enabled" />
                    <col className="svc-col-status" />
                    <col className="svc-col-details" />
                    <col className="svc-col-force" />
                  </colgroup>
                  <thead>
                    <tr>
                      <th>Service</th>
                      <th>Enabled</th>
                      <th>Status</th>
                      <th>Details</th>
                      <th className="force-col">Force</th>
                    </tr>
                  </thead>
                  <tbody>
                    {managedServices.map((svc) => {
                      const info = serviceState.services?.[svc.key] || {};
                      const running = !!info.running;
                      const busy = !!serviceBusy[svc.key];
                      const requiresFolders = svc.key === "mcp_server" || svc.key === "mcp_inspector";
                      const blockedByFolders = requiresFolders && !running && !foldersConfigured;
                      const disabled = !desktopMode || busy || blockedByFolders;
                      const canForceStop =
                        desktopMode &&
                        (svc.key === "ngrok" || (running && (svc.key === "mcp_server" || svc.key === "mcp_inspector")));
                      const toggleTitle = !desktopMode
                        ? "Desktop mode only"
                        : blockedByFolders
                          ? "Set source, target, and report folders in Catalog first"
                          : "Toggle service";
                      return (
                        <tr key={svc.key}>
                          <td>
                            <strong>{svc.label}</strong>
                            <div style={{ fontSize: 12, color: "#5b6470" }}>{svc.description}</div>
                          </td>
                          <td>
                            <label className="switch" title={toggleTitle}>
                              <input
                                type="checkbox"
                                checked={running}
                                disabled={disabled}
                                onChange={(e) => onToggleService(svc.key, e.target.checked)}
                              />
                              <span className="slider" />
                            </label>
                          </td>
                          <td>{running ? "Running" : "Stopped"}</td>
                          <td>
                            {info.pid ? `PID ${info.pid}` : "-"} | {info.port ? `Port ${info.port}` : "Port -"}
                            {info.service_url ? ` | URL: ${info.service_url}` : ""}
                            {info.log_file ? ` | Log: ${info.log_file}` : ""}
                            {info.last_error ? <div style={{ color: "#b42318" }}>{info.last_error}</div> : null}
                          </td>
                          <td className="force-col">
                            {canForceStop ? (
                              <button
                                className="danger force-stop-btn"
                                onClick={() => onForceStopService(svc.key)}
                                disabled={busy}
                                title={
                                  svc.key === "ngrok"
                                    ? "Kill ngrok process(es) including external local instances"
                                    : "Kill process listening on service port"
                                }
                              >
                                Force Stop
                              </button>
                            ) : (
                              "-"
                            )}
                          </td>
                        </tr>
                      );
                    })}
                    <tr>
                      <td>
                        <strong>UI Backend</strong>
                        <div style={{ fontSize: 12, color: "#5b6470" }}>This desktop app API and web UI process.</div>
                      </td>
                      <td>-</td>
                      <td>{serviceState.ui?.running ? "Running" : "Stopped"}</td>
                      <td>
                        {serviceState.ui?.host || "127.0.0.1"}:{serviceState.ui?.port || "8001"}
                      </td>
                      <td className="force-col">-</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
            {!desktopMode ? (
              <div style={{ marginTop: 10, color: "#5b6470" }}>
                Service toggles are available only in desktop mode (`DMH_DESKTOP_MODE=1`).
              </div>
            ) : null}
            {desktopMode && !foldersConfigured ? (
              <div style={{ marginTop: 6, color: "#5b6470" }}>
                To start MCP server/inspector, set source, target, and report folders in Catalog.
              </div>
            ) : null}
          </div>

          <div className="card">
            <h3>Theme</h3>
            <div className="row">
              <div className="col-4">
                <label>Theme</label>
                <select value={settingsTheme} onChange={(e) => setSettingsTheme(e.target.value)}>
                  <option value="light">Light</option>
                  <option value="dark">Dark</option>
                </select>
                <div className="sub">Choose app theme for the admin UI.</div>
              </div>
            </div>
          </div>

          <div className="card anthropic-card">
            <h3>Anthropic</h3>
            <div className="sub anthropic-subtitle">Configure API key and model used by the Claude chat tab.</div>
            <div className="anthropic-grid">
              <div className="anthropic-key-block">
                <label>Anthropic API Key</label>
                <input
                  className="anthropic-key-input"
                  type="password"
                  value={settingsApiKeyInput}
                  onChange={(e) => setSettingsApiKeyInput(e.target.value)}
                  placeholder="sk-ant-..."
                  autoComplete="off"
                />
                <div className="anthropic-meta">
                  <span className="anthropic-meta-pill">
                    Stored key:{" "}
                    {settingsApiKeySet
                      ? settingsApiKeyMasked
                        ? settingsApiKeyMasked
                        : "configured"
                      : "not set"}
                  </span>
                  <span className={`anthropic-meta-pill ${settingsApiKeyActivated ? "is-active" : "is-inactive"}`}>
                    Activation: {settingsApiKeyActivated ? "active" : "inactive"}
                  </span>
                </div>
                {settingsApiKeyNeedsReset ? (
                  <div className="anthropic-warning">
                    Stored API key cannot be decrypted. Enter a new key and save.
                  </div>
                ) : null}
              </div>

              <div className="anthropic-actions">
                <button
                  className="secondary anthropic-action-btn"
                  onClick={onValidateAnthropicKey}
                  disabled={!canValidateAnthropicKey || settingsValidating || settingsSaving}
                >
                  {settingsValidating ? "Validating..." : "Validate Key"}
                </button>
                <button
                  className="secondary anthropic-action-btn"
                  onClick={onLookupAnthropicModels}
                  disabled={!canLookupAnthropicModels || settingsLoadingModels || settingsSaving}
                >
                  {settingsLoadingModels ? "Loading..." : "Lookup Models"}
                </button>
              </div>

              <div className="anthropic-model-block">
                <label>Model</label>
                <select className="anthropic-model-select" value={settingsModel} onChange={(e) => setSettingsModel(e.target.value)}>
                  <option value="">Select a model...</option>
                  {settingsModel && !settingsModels.some((m) => m.id === settingsModel) ? (
                    <option value={settingsModel}>{settingsModel} (saved)</option>
                  ) : null}
                  {settingsModels.map((m) => (
                    <option key={m.id} value={m.id}>
                      {m.id}
                    </option>
                  ))}
                </select>
                <div className="sub">Selected model is used by the Claude tab chat.</div>
              </div>

              <div className="anthropic-instructions-block">
                <details className="settings-collapsible anthropic-collapsible">
                  <summary>Claude System Instructions</summary>
                  <div className="settings-collapsible-body">
                    <label>Instructions sent as system prompt for Claude chat</label>
                    <textarea
                      className="instructions-input"
                      value={settingsClaudeInstructions}
                      onChange={(e) => setSettingsClaudeInstructions(e.target.value)}
                      placeholder="Add task-specific instructions for Claude..."
                    />
                    <div className="actions">
                      <button className="secondary" type="button" onClick={onLoadDefaultClaudeInstructions} disabled={settingsBusy}>
                        Load DM Assistant Template
                      </button>
                    </div>
                    <div className="sub">
                      These instructions are saved and applied automatically to every message in the Claude tab.
                    </div>
                  </div>
                </details>
              </div>

              <div className="anthropic-save-row">
                <button className="anthropic-save-btn" onClick={onSaveAppSettings} disabled={settingsBusy}>
                  {settingsSaving ? "Saving..." : "Save Settings"}
                </button>
              </div>
            </div>
          </div>
        </>
      ) : null}

      {tab === "claude" ? (
        <>
          <div className="card">
            <h3>Claude</h3>
            <div className="sub">Chat using your saved Anthropic key and selected model ({settingsModel || "not selected"}).</div>
            <div className="claude-chat-window">
              {claudeMessages.length ? (
                claudeMessages.map((msg, idx) => (
                  <div
                    key={`claude-message-${idx}`}
                    className={`claude-message ${msg.role === "assistant" ? "assistant" : "user"}`}
                  >
                    <div className="claude-message-role">{msg.role === "assistant" ? "Claude" : "You"}</div>
                    <div className="claude-message-text">{displayValue(msg.content)}</div>
                  </div>
                ))
              ) : (
                <div className="claude-empty">No messages yet. Ask Claude something to start.</div>
              )}
            </div>
            <div className="row">
              <div className="col-12">
                <label>Message</label>
                <textarea
                  className="claude-input"
                  value={claudeInput}
                  onChange={(e) => {
                    setClaudeInput(e.target.value);
                    if (claudeInlineError) setClaudeInlineError("");
                  }}
                  placeholder="Type your message for Claude..."
                />
              </div>
              <div className="col-12">
                <div className="actions">
                  <button onClick={onSendClaudeMessage} disabled={!claudeCanSend}>
                    {claudeSending ? "Sending..." : "Send"}
                  </button>
                  <button className="secondary" onClick={onClearClaudeChat} disabled={claudeSending || !claudeMessages.length}>
                    Clear Chat
                  </button>
                </div>
                {claudeInlineError ? <div className="claude-inline-error">{claudeInlineError}</div> : null}
              </div>
            </div>
          </div>
        </>
      ) : null}

      {inspectorRunning ? (
        <div style={{ display: tab === "inspector" ? "block" : "none" }}>
          <div className="card inspector-card">
            <iframe className="inspector-frame" title="MCP Inspector" src={inspectorUrl} />
          </div>
        </div>
      ) : null}

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
                <select value={profileColumn} onChange={(e) => setProfileColumn(e.target.value)} disabled={!profileDataset}>
                  <option value="">All columns</option>
                  {profileFieldOptions.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-4">
                <label>&nbsp;</label>
                <button onClick={loadProfile}>Load Profile</button>
              </div>
            </div>
          </div>

          <div className="card">
            <h3>Profile Result</h3>
            {profileResult?.error ? (
              <div style={{ color: "#b42318" }}>{profileResult.error}</div>
            ) : profileResult?.columns?.length ? (
              <div className="scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Distinct</th>
                      <th>Blank/Null</th>
                      <th>Min</th>
                      <th>Max</th>
                    </tr>
                  </thead>
                  <tbody>
                    {profileResult.columns.map((row, idx) => (
                      <tr key={`profile-col-${row.column || idx}`}>
                        <td>{displayValue(row.column)}</td>
                        <td>{displayValue(row.distinct)}</td>
                        <td>{displayValue(row.blank_count)}</td>
                        <td>{displayValue(row.min)}</td>
                        <td>{displayValue(row.max)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div style={{ color: "#5b6470" }}>Run profile to view result.</div>
            )}
          </div>

          <div className="card">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 10, flexWrap: "wrap" }}>
              <h3 style={{ margin: 0 }}>Column Summary Result</h3>
              {baseColumnSummaries.length && !columnSummaryResult?.error ? (
                <div className="sub" style={{ margin: 0 }}>
                  Dataset: {displayValue(columnSummaryResult.dataset)} | Columns summarized:{" "}
                  {displayValue(baseColumnSummaries.length)}
                </div>
              ) : null}
            </div>
            {columnSummaryResult?.error ? (
              <div style={{ color: "#b42318" }}>{columnSummaryResult.error}</div>
            ) : mergedColumnSummaries.length ? (
              <>
                <div className="scroll summary-matrix-wrap">
                  <table className="summary-matrix-table">
                    <thead>
                      <tr>
                        <th style={{ width: 34 }}>&nbsp;</th>
                        <th>Column</th>
                        {topHeaders.map((header) => (
                          <th key={`summary-top-head-${header}`}>{header}</th>
                        ))}
                        <th>Blanks</th>
                      </tr>
                    </thead>
                    <tbody>
                      {mergedColumnSummaries.map((summary, idx) => {
                        const topValues = Array.isArray(summary?.top_values) ? summary.top_values : [];
                        const isBaseSummary = idx < baseColumnSummaries.length;
                        const colName = String(summary?.column || "");
                        return (
                          <tr key={`summary-matrix-row-${colName || "col"}-${idx}`}>
                            <td>
                              {isBaseSummary ? (
                                <input
                                  type="checkbox"
                                  className="check-input"
                                  checked={selectedSummaryColumns.includes(colName)}
                                  onChange={() => toggleSummaryColumnSelection(colName)}
                                />
                              ) : null}
                            </td>
                            <td>{displayValue(summary?.column)}</td>
                            {topHeaders.map((_, topIdx) => {
                              const topEntry = topValues[topIdx];
                              const text =
                                topEntry && topEntry.value !== undefined
                                  ? `${displayValue(topEntry.value)} (${displayValue(topEntry.count)})`
                                  : "-";
                              return <td key={`summary-top-${idx}-${topIdx}`}>{text}</td>;
                            })}
                            <td>{displayValue(summary?.blank_or_null_count)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                <div className="actions actions-right" style={{ marginTop: 8 }}>
                  <button className="secondary" onClick={addMultiFieldSummary}>
                    Add multi-field count
                  </button>
                </div>
              </>
            ) : (
              <div style={{ color: "#5b6470" }}>Run profile to view summary.</div>
            )}
          </div>

          <div className="card">
            <h3>Filtered Preview</h3>
            <div className="row">
              <div className="col-4">
                <label>Column</label>
                <select value={filterColumn} onChange={(e) => setFilterColumn(e.target.value)} disabled={!profileDataset}>
                  <option value="">Select...</option>
                  {profileFieldOptions.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
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
            <h3>Filtered Preview Result</h3>
            {filteredResult?.error ? (
              <div style={{ color: "#b42318" }}>{filteredResult.error}</div>
            ) : Array.isArray(filteredResult?.headers) && Array.isArray(filteredResult?.rows) ? (
              <>
                <div className="sub" style={{ marginBottom: 8 }}>
                  Dataset: {displayValue(filteredResult.dataset)} | Rows: {displayValue(filteredResult.row_count)} | Filter:{" "}
                  {displayValue(filteredResult.filter)}
                </div>
                <div className="scroll">
                  <table>
                    <thead>
                      <tr>
                        {filteredResult.headers.map((header, idx) => (
                          <th key={`filtered-head-${idx}`}>{displayValue(header)}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {filteredResult.rows.length ? (
                        filteredResult.rows.map((row, rowIdx) => (
                          <tr key={`filtered-row-${rowIdx}`}>
                            {row.map((cell, colIdx) => (
                              <td key={`filtered-cell-${rowIdx}-${colIdx}`}>{displayValue(cell)}</td>
                            ))}
                          </tr>
                        ))
                      ) : (
                        <tr>
                          <td colSpan={Math.max(1, filteredResult.headers.length)}>No rows matched the filter.</td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <div style={{ color: "#5b6470" }}>Run filtered preview to view rows.</div>
            )}
          </div>
        </>
      ) : null}

      {tab === "compare" ? (
        <>
          <div className="card">
            <h3 style={{ margin: "0 0 8px" }}>Mapping</h3>
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
              <div className="col-12">
                <div className="actions actions-right">
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
              </div>
            </div>
          </div>

          <div className="card compare-result-card">
            <div className="card-header-row compare-result-header">
              <h3 style={{ margin: 0 }}>Compare Result</h3>
              <div className="actions actions-right compare-result-header-actions">
                <button className="secondary" onClick={onQuickCompare}>
                  Quick Compare
                </button>
                <button onClick={onStartJob}>Start Job</button>
              </div>
            </div>
            {compareResult?.error ? (
              <div style={{ color: "#b42318" }}>{compareResult.error}</div>
            ) : hasLegacyCompareResult ? (
              <>
                <div className="sub" style={{ marginBottom: 8 }}>
                  Source: {displayValue(compareResult.source)} | Target: {displayValue(compareResult.target)} | Keys:{" "}
                  {Array.isArray(compareResult.key_columns) && compareResult.key_columns.length
                    ? compareResult.key_columns.join(", ")
                    : "-"}
                </div>
                <div className="result-metrics">
                  <div className="metric-card">
                    <div className="metric-label">Added</div>
                    <div className="metric-value">{displayValue(compareResult.added?.data?.length || 0)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Removed</div>
                    <div className="metric-value">{displayValue(compareResult.removed?.data?.length || 0)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Changed</div>
                    <div className="metric-value">{displayValue(compareResult.changed?.data?.length || 0)}</div>
                  </div>
                </div>
                <div className="row">
                  <div className="col-6">
                    <label>Schema drift (source only)</label>
                    <div>{Array.isArray(compareResult.schema_drift?.source_only) && compareResult.schema_drift.source_only.length ? compareResult.schema_drift.source_only.join(", ") : "-"}</div>
                  </div>
                  <div className="col-6">
                    <label>Schema drift (target only)</label>
                    <div>{Array.isArray(compareResult.schema_drift?.target_only) && compareResult.schema_drift.target_only.length ? compareResult.schema_drift.target_only.join(", ") : "-"}</div>
                  </div>
                </div>
                <div className="compare-result-tabs">
                  {compareSampleTabs.map((tabDef) => (
                    <button
                      key={tabDef.key}
                      type="button"
                      className={activeCompareSampleKey === tabDef.key ? "compare-result-tab-btn" : "secondary compare-result-tab-btn"}
                      onClick={() => setCompareSampleTab(tabDef.key)}
                    >
                      {tabDef.label}
                    </button>
                  ))}
                </div>
                <h4>{activeCompareSampleDef.label} Sample</h4>
                <DataGrid
                  headers={activeCompareSampleGrid.headers}
                  rows={activeCompareSampleGrid.rows}
                  emptyMessage={activeCompareSampleDef.emptyMessage}
                />
              </>
            ) : hasQuickCompareResult ? (
              <>
                <div className="sub" style={{ marginBottom: 8 }}>
                  Source: {displayValue(compareResult.source)} | Target: {displayValue(compareResult.target)} | Keys:{" "}
                  {Array.isArray(compareResult.key_columns) && compareResult.key_columns.length
                    ? compareResult.key_columns.join(", ")
                    : "-"}
                </div>
                <div className="result-metrics">
                  <div className="metric-card">
                    <div className="metric-label">Added</div>
                    <div className="metric-value">{displayValue(compareResult.added_count || 0)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Removed</div>
                    <div className="metric-value">{displayValue(compareResult.removed_count || 0)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Changed</div>
                    <div className="metric-value">{displayValue(compareResult.changed_count || 0)}</div>
                  </div>
                </div>
                <div className="row">
                  <div className="col-6">
                    <label>Schema drift (source only)</label>
                    <div>
                      {Array.isArray(compareResult.schema_drift?.source_only_columns) &&
                      compareResult.schema_drift.source_only_columns.length
                        ? compareResult.schema_drift.source_only_columns.join(", ")
                        : "-"}
                    </div>
                  </div>
                  <div className="col-6">
                    <label>Schema drift (target only)</label>
                    <div>
                      {Array.isArray(compareResult.schema_drift?.target_only_columns) &&
                      compareResult.schema_drift.target_only_columns.length
                        ? compareResult.schema_drift.target_only_columns.join(", ")
                        : "-"}
                    </div>
                  </div>
                </div>
                <div className="compare-result-tabs">
                  {compareSampleTabs.map((tabDef) => (
                    <button
                      key={tabDef.key}
                      type="button"
                      className={activeCompareSampleKey === tabDef.key ? "compare-result-tab-btn" : "secondary compare-result-tab-btn"}
                      onClick={() => setCompareSampleTab(tabDef.key)}
                    >
                      {tabDef.label}
                    </button>
                  ))}
                </div>
                <h4>{activeCompareSampleDef.label} Sample</h4>
                <DataGrid
                  headers={activeCompareSampleGrid.headers}
                  rows={activeCompareSampleGrid.rows}
                  emptyMessage={activeCompareSampleDef.emptyMessage}
                />
              </>
            ) : hasJobCompareResult ? (
              <>
                <div className="result-metrics">
                  <div className="metric-card">
                    <div className="metric-label">Job ID</div>
                    <div className="metric-value">{displayValue(compareResult.job_id)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">State</div>
                    <div className="metric-value">{displayValue(compareResult.state)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Added / Removed / Changed</div>
                    <div className="metric-value">
                      {displayValue(compareResult.progress?.added || 0)} / {displayValue(compareResult.progress?.removed || 0)} /{" "}
                      {displayValue(compareResult.progress?.changed || 0)}
                    </div>
                  </div>
                </div>
                {compareResult.report ? (
                  <div className="sub">
                    Report: {displayValue(compareResult.report.file_name)} ({displayValue(compareResult.report.report_id)})
                  </div>
                ) : null}
              </>
            ) : (
              <div style={{ color: "#5b6470" }}>Run a quick compare or start a job to see results.</div>
            )}
          </div>

          <div className="card">
            <div className="card-header-row">
              <h3 style={{ margin: 0 }}>Jobs ({jobs.length})</h3>
              <button className="secondary header-action-btn" onClick={refreshBootstrap}>
                Refresh Jobs
              </button>
            </div>
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
            {jobSummary?.error ? (
              <div style={{ color: "#b42318" }}>{jobSummary.error}</div>
            ) : jobSummary?.job_id ? (
              <>
                <div className="result-metrics">
                  <div className="metric-card">
                    <div className="metric-label">Job ID</div>
                    <div className="metric-value">{displayValue(jobSummary.job_id)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">State</div>
                    <div className="metric-value">{displayValue(jobSummary.state)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Source -> Target</div>
                    <div className="metric-value">
                      {displayValue(jobSummary.source)} -> {displayValue(jobSummary.target)}
                    </div>
                  </div>
                </div>
                <div className="sub" style={{ marginBottom: 8 }}>
                  Keys: {Array.isArray(jobSummary.key_fields) && jobSummary.key_fields.length ? jobSummary.key_fields.join(", ") : "-"}{" "}
                  | Started: {displayValue(jobSummary.started_at)} | Finished: {displayValue(jobSummary.finished_at)}
                </div>
                <div className="result-metrics">
                  <div className="metric-card">
                    <div className="metric-label">Added</div>
                    <div className="metric-value">{displayValue(jobSummary.progress?.added || 0)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Removed</div>
                    <div className="metric-value">{displayValue(jobSummary.progress?.removed || 0)}</div>
                  </div>
                  <div className="metric-card">
                    <div className="metric-label">Changed</div>
                    <div className="metric-value">{displayValue(jobSummary.progress?.changed || 0)}</div>
                  </div>
                </div>
                {jobSummary.report ? (
                  <>
                    <h4>Report</h4>
                    <div className="sub" style={{ marginBottom: 8 }}>
                      ID: {displayValue(jobSummary.report.id)} | File: {displayValue(jobSummary.report.file_name)}
                    </div>
                    {jobSummary.report.summary ? (
                      <div className="scroll">
                        <table>
                          <thead>
                            <tr>
                              <th>Metric</th>
                              <th>Value</th>
                            </tr>
                          </thead>
                          <tbody>
                            {Object.entries(jobSummary.report.summary).map(([key, val]) => (
                              <tr key={`job-summary-${key}`}>
                                <td>{displayValue(key)}</td>
                                <td>{Array.isArray(val) ? val.join(", ") || "-" : displayValue(val)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : null}
                    <div className="actions actions-right" style={{ marginTop: 8 }}>
                      <button className="secondary" onClick={() => onOpenReport(jobSummary.report.id)}>
                        Open Report
                      </button>
                    </div>
                  </>
                ) : null}
              </>
            ) : (
              <div style={{ color: "#5b6470" }}>Click Summary on a job row.</div>
            )}
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
              <table className="reports-table">
                <colgroup>
                  <col style={{ width: "11%" }} />
                  <col style={{ width: "47%" }} />
                  <col style={{ width: "16%" }} />
                  <col style={{ width: "16%" }} />
                  <col style={{ width: "10%" }} />
                  <col style={{ width: "220px" }} />
                </colgroup>
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
                        <div className="actions report-actions">
                          <button className="secondary" onClick={() => onOpenReport(r.id)}>
                            Open
                          </button>
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
              <div className="col-6">
                <label>Left dataset</label>
                <select
                  value={leftDatasetId}
                  onChange={(e) => {
                    setLeftDatasetId(e.target.value);
                    setRelationshipMappings([{ left_field: "", right_field: "" }]);
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
              <div className="col-6">
                <label>Right dataset</label>
                <select
                  value={rightDatasetId}
                  onChange={(e) => {
                    setRightDatasetId(e.target.value);
                    setRelationshipMappings([{ left_field: "", right_field: "" }]);
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
              <div className="col-12">
                <label>Field mappings</label>
                <div className="actions" style={{ marginBottom: 8 }}>
                  <button type="button" className="secondary" onClick={addRelationshipMappingRow}>
                    Add Mapping Row
                  </button>
                </div>
                <div className="scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Left field</th>
                        <th>Right field</th>
                        <th>Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {relationshipMappings.map((m, idx) => (
                        <tr key={`rel-map-${idx}`}>
                          <td>
                            <select
                              value={m.left_field}
                              onChange={(e) => updateRelationshipMappingRow(idx, { left_field: e.target.value })}
                            >
                              <option value="">Select...</option>
                              {leftFieldOptions.map((c) => (
                                <option key={c} value={c}>
                                  {c}
                                </option>
                              ))}
                            </select>
                          </td>
                          <td>
                            <select
                              value={m.right_field}
                              onChange={(e) => updateRelationshipMappingRow(idx, { right_field: e.target.value })}
                            >
                              <option value="">Select...</option>
                              {rightFieldOptions.map((c) => (
                                <option key={c} value={c}>
                                  {c}
                                </option>
                              ))}
                            </select>
                          </td>
                          <td>
                            <button type="button" className="secondary" onClick={() => removeRelationshipMappingRow(idx)}>
                              Remove
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
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
      {quickMapChoiceOpen ? (
        <div className="modal-backdrop" onClick={() => applyQuickMappingsChoice("cancel")}>
          <div className="modal-card" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <h4 style={{ margin: "0 0 6px" }}>Existing mappings detected</h4>
            <div className="sub" style={{ margin: 0 }}>
              Quick map found {quickMapPendingMappings.length} same-name field(s). Choose how to apply these mappings.
            </div>
            <div className="actions actions-right modal-actions">
              <button type="button" className="secondary" onClick={() => applyQuickMappingsChoice("cancel")}>
                Cancel
              </button>
              <button type="button" className="secondary" onClick={() => applyQuickMappingsChoice("merge")}>
                Create Without Overriding
              </button>
              <button type="button" onClick={() => applyQuickMappingsChoice("override")}>
                Override Existing Values
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
