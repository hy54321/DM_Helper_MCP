const { useEffect, useRef, useState } = React;
const THEME_STORAGE_KEY = "protoquery_theme";
const NEW_FOLDER_CONFIG_OPTION_ID = "__new__";

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

function DataGrid({ headers, rows, emptyMessage = "No rows.", className = "" }) {
  const safeHeaders = Array.isArray(headers) ? headers : [];
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeHeaders.length) {
    return <div style={{ color: "#5b6470" }}>{emptyMessage}</div>;
  }
  const wrapperClass = className ? `scroll ${className}` : "scroll";
  return (
    <div className={wrapperClass}>
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

const ISO_DATE_TIME_SECONDS_PATTERN = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$/;

function formatDateTimeToSeconds(value) {
  if (typeof value !== "string") return value;
  const text = value.trim();
  const match = text.match(ISO_DATE_TIME_SECONDS_PATTERN);
  if (!match) return value;
  return `${match[1]}T${match[2]}`;
}

function formatDateTimeForBadge(value) {
  if (!value) return "Never";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "Never";
  return `${dt.toLocaleDateString()} ${dt.toLocaleTimeString()}`;
}

function getStoredTheme() {
  try {
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === "dark" || stored === "light") {
      return stored;
    }
  } catch (_err) {
    // Ignore localStorage access issues and fallback.
  }
  return "dark";
}

function displayValue(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(formatDateTimeToSeconds(value));
}

function formatPrettyJson(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "string") {
    const text = value.trim();
    if (!text) return "-";
    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch (_err) {
      return text;
    }
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch (_err) {
    return String(value);
  }
}

function getDatasetStatus(dataset) {
  const safeColumns = Array.isArray(dataset?.columns) ? dataset.columns : [];
  const rawColumns = Array.isArray(dataset?.raw_columns) ? dataset.raw_columns : [];

  if (!safeColumns.length) {
    return {
      level: "error",
      icon: "✖",
      label: "Error",
      message: "File could not be read or no header row was detected. Check file content/encoding, then refresh catalog.",
    };
  }

  if (!rawColumns.length) {
    return {
      level: "ok",
      icon: "✔",
      label: "OK",
      message: "File loaded successfully.",
    };
  }

  let hasBlankNames = false;
  let hasDuplicateNames = false;
  const seen = new Set();

  rawColumns.forEach((raw, idx) => {
    const text = raw == null ? "" : String(raw);
    const trimmed = text.trim();
    const key = trimmed.toLowerCase();

    if (!trimmed) {
      hasBlankNames = true;
    } else if (seen.has(key)) {
      hasDuplicateNames = true;
    } else {
      seen.add(key);
    }
  });

  if (hasBlankNames || hasDuplicateNames) {
    const details = [];
    if (hasBlankNames) details.push("empty column names");
    if (hasDuplicateNames) details.push("duplicate column names");
    return {
      level: "warning",
      icon: "⚠",
      label: "Warning",
      message: `File loaded with column sanitization (${details.join(", ")}).`,
    };
  }

  return {
    level: "ok",
    icon: "✔",
    label: "OK",
    message: "File loaded successfully.",
  };
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
8. Only run \`upsert_pair_override\` if user explicitly asks.
9. If a cross join query is required checl links using 'get_dataset_links'.

Default workflow:
- Discovery: \`refresh_catalog\`, \`list_datasets\`, use \`list_table_pairs\` to find out which datasets are paired, use \`list_field_pairs\` to find out field and unique index pairings with the pair_id from 'list_table_pairs', \`row_count_summary\` (per dataset_id)
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
  const [configurationsFolder, setConfigurationsFolder] = useState("");
  const [translationsFolder, setTranslationsFolder] = useState("");
  const [rulesFolder, setRulesFolder] = useState("");
  const [exposeSourceToTools, setExposeSourceToTools] = useState(true);
  const [exposeTargetToTools, setExposeTargetToTools] = useState(true);
  const [exposeConfigurationsToTools, setExposeConfigurationsToTools] = useState(false);
  const [exposeTranslationsToTools, setExposeTranslationsToTools] = useState(false);
  const [exposeRulesToTools, setExposeRulesToTools] = useState(false);
  const [reportFolder, setReportFolder] = useState("");
  const [savedFoldersSnapshot, setSavedFoldersSnapshot] = useState({
    source_folder: "",
    target_folder: "",
    configurations_folder: "",
    translations_folder: "",
    rules_folder: "",
    report_folder: "",
    expose_source_to_tools: true,
    expose_target_to_tools: true,
    expose_configurations_to_tools: false,
    expose_translations_to_tools: false,
    expose_rules_to_tools: false,
  });
  const [folderConfigs, setFolderConfigs] = useState([]);
  const [activeFolderConfigId, setActiveFolderConfigId] = useState("");
  const [selectedFolderConfigId, setSelectedFolderConfigId] = useState("");
  const [folderConfigNameInput, setFolderConfigNameInput] = useState("");
  const [folderConfigBusy, setFolderConfigBusy] = useState(false);
  const [folderConfigModal, setFolderConfigModal] = useState({
    open: false,
    mode: "save",
    name: "",
    configId: "",
    configName: "",
  });
  const [includeRowCounts, setIncludeRowCounts] = useState(false);
  const [catalogRefreshing, setCatalogRefreshing] = useState(false);
  const [catalogReloading, setCatalogReloading] = useState(false);
  const [lastCatalogRefreshAt, setLastCatalogRefreshAt] = useState("");
  const [catalogSetupCollapsed, setCatalogSetupCollapsed] = useState(false);

  const [datasets, setDatasets] = useState([]);
  const [catalogDatasetIssue, setCatalogDatasetIssue] = useState(null);
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
  const [compareSuggestBusy, setCompareSuggestBusy] = useState(false);
  const [compareSuggestMessage, setCompareSuggestMessage] = useState("");
  const [mappingSearch, setMappingSearch] = useState("");
  const [compareResult, setCompareResult] = useState(null);
  const [compareSampleTab, setCompareSampleTab] = useState("added");
  const [quickMapChoiceOpen, setQuickMapChoiceOpen] = useState(false);
  const [quickMapPendingMappings, setQuickMapPendingMappings] = useState([]);
  const [quickMapPendingLabel, setQuickMapPendingLabel] = useState("suggested");
  const [pairKeyDeleteModal, setPairKeyDeleteModal] = useState({
    open: false,
    pairId: "",
    sourceDataset: "",
    targetDataset: "",
    keyCount: 0,
    busy: false,
  });
  const [pairDeleteModal, setPairDeleteModal] = useState({
    open: false,
    pairId: "",
    sourceDataset: "",
    targetDataset: "",
    busy: false,
  });
  const [jobSummary, setJobSummary] = useState(null);
  const [sqlText, setSqlText] = useState("");
  const [sqlLimit, setSqlLimit] = useState(100);
  const [sqlIncludeTotal, setSqlIncludeTotal] = useState(false);
  const [sqlOutputMode, setSqlOutputMode] = useState("grid");
  const [sqlExportFilename, setSqlExportFilename] = useState("");
  const [sqlResult, setSqlResult] = useState(null);
  const [sqlExportJob, setSqlExportJob] = useState(null);
  const [sqlBusy, setSqlBusy] = useState(false);
  const [relationshipLeftFolderFilter, setRelationshipLeftFolderFilter] = useState("any");
  const [relationshipRightFolderFilter, setRelationshipRightFolderFilter] = useState("any");
  const [relationshipId, setRelationshipId] = useState("");
  const [leftDatasetId, setLeftDatasetId] = useState("");
  const [rightDatasetId, setRightDatasetId] = useState("");
  const [relationshipMappings, setRelationshipMappings] = useState([{ left_field: "", right_field: "" }]);
  const [relationshipSuggestBusy, setRelationshipSuggestBusy] = useState(false);
  const [relationshipSuggestStopRequested, setRelationshipSuggestStopRequested] = useState(false);
  const [relationshipSuggestMessage, setRelationshipSuggestMessage] = useState("");
  const [relationshipConfidence, setRelationshipConfidence] = useState(0.95);
  const [relationshipMethod, setRelationshipMethod] = useState("manual");
  const [relationshipActive, setRelationshipActive] = useState(true);
  const [relationshipDeleteModal, setRelationshipDeleteModal] = useState({
    open: false,
    relationshipId: "",
    leftLabel: "",
    rightLabel: "",
    busy: false,
  });
  const [reportDeleteModal, setReportDeleteModal] = useState({
    open: false,
    reportId: "",
    reportFile: "",
    busy: false,
  });
  const [forceStopModal, setForceStopModal] = useState({
    open: false,
    serviceName: "",
    serviceLabel: "",
    busy: false,
  });
  const [serviceState, setServiceState] = useState({
    desktop_mode: false,
    services: {},
    ui: { running: true, host: "127.0.0.1", port: "8001" },
  });
  const [serviceBusy, setServiceBusy] = useState({});
  const [settingsTheme, setSettingsTheme] = useState(getStoredTheme);
  const [settingsToolLoggingEnabled, setSettingsToolLoggingEnabled] = useState(true);
  const [settingsApiKeyInput, setSettingsApiKeyInput] = useState("");
  const [settingsApiKeyMasked, setSettingsApiKeyMasked] = useState("");
  const [settingsApiKeySet, setSettingsApiKeySet] = useState(false);
  const [settingsApiKeyNeedsReset, setSettingsApiKeyNeedsReset] = useState(false);
  const [settingsApiKeyActivated, setSettingsApiKeyActivated] = useState(false);
  const [settingsNgrokTokenInput, setSettingsNgrokTokenInput] = useState("");
  const [settingsNgrokTokenMasked, setSettingsNgrokTokenMasked] = useState("");
  const [settingsNgrokTokenSet, setSettingsNgrokTokenSet] = useState(false);
  const [settingsNgrokTokenNeedsReset, setSettingsNgrokTokenNeedsReset] = useState(false);
  const [settingsMcpAuthMode, setSettingsMcpAuthMode] = useState("none");
  const [settingsMcpApiKeyHeaderName, setSettingsMcpApiKeyHeaderName] = useState("x-api-key");
  const [settingsMcpApiKeyMasked, setSettingsMcpApiKeyMasked] = useState("");
  const [settingsMcpApiKeySet, setSettingsMcpApiKeySet] = useState(false);
  const [settingsMcpApiKeyNeedsReset, setSettingsMcpApiKeyNeedsReset] = useState(false);
  const [settingsMcpGeneratedApiKey, setSettingsMcpGeneratedApiKey] = useState("");
  const [settingsModel, setSettingsModel] = useState("");
  const [settingsModels, setSettingsModels] = useState([]);
  const [settingsClaudeInstructions, setSettingsClaudeInstructions] = useState("");
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsValidating, setSettingsValidating] = useState(false);
  const [settingsLoadingModels, setSettingsLoadingModels] = useState(false);
  const [settingsGeneratingMcpApiKey, setSettingsGeneratingMcpApiKey] = useState(false);
  const [claudeMessages, setClaudeMessages] = useState([]);
  const [claudeInput, setClaudeInput] = useState("");
  const [claudeSending, setClaudeSending] = useState(false);
  const [claudeInlineError, setClaudeInlineError] = useState("");
  const [toolLogs, setToolLogs] = useState([]);
  const [toolLogTotal, setToolLogTotal] = useState(0);
  const [toolLogNames, setToolLogNames] = useState([]);
  const [toolLogStatusFilter, setToolLogStatusFilter] = useState("all");
  const [toolLogNameFilter, setToolLogNameFilter] = useState("");
  const [toolLogTextFilter, setToolLogTextFilter] = useState("");
  const [toolLogSinceDays, setToolLogSinceDays] = useState(7);
  const [toolLogCleanupDays, setToolLogCleanupDays] = useState(7);
  const [toolLogLoading, setToolLogLoading] = useState(false);
  const [toolLogCleaning, setToolLogCleaning] = useState(false);
  const relationshipSuggestStopRef = useRef(false);
  const relationshipSuggestAbortRef = useRef(null);

  function applyAppSettings(payload) {
    const safe = payload && typeof payload === "object" ? payload : {};
    setSettingsTheme(safe.theme === "dark" ? "dark" : "light");
    setSettingsToolLoggingEnabled(safe.tool_logging_enabled !== false);
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
    setSettingsNgrokTokenSet(!!safe.ngrok_authtoken_set);
    setSettingsNgrokTokenMasked(String(safe.ngrok_authtoken_masked || ""));
    setSettingsNgrokTokenNeedsReset(!!safe.ngrok_authtoken_needs_reset);
    setSettingsMcpAuthMode(safe.mcp_auth_mode === "api" ? "api" : "none");
    setSettingsMcpApiKeyHeaderName(String(safe.mcp_api_key_header_name || "x-api-key"));
    setSettingsMcpApiKeySet(!!safe.mcp_api_key_set);
    setSettingsMcpApiKeyMasked(String(safe.mcp_api_key_masked || ""));
    setSettingsMcpApiKeyNeedsReset(!!safe.mcp_api_key_needs_reset);
    setSettingsModel(String(safe.model || ""));
    setSettingsClaudeInstructions(String(safe.claude_instructions || ""));
  }

  function applyFolderConfigs(payload) {
    const safe = payload && typeof payload === "object" ? payload : {};
    const rawConfigs = Array.isArray(safe.configs) ? safe.configs : [];
    const nextConfigs = rawConfigs
      .map((item) => ({
        id: String(item?.id || "").trim(),
        name: String(item?.name || "").trim(),
        source_folder: String(item?.source_folder || "").trim(),
        target_folder: String(item?.target_folder || "").trim(),
        configurations_folder: String(item?.configurations_folder || "").trim(),
        translations_folder: String(item?.translations_folder || "").trim(),
        rules_folder: String(item?.rules_folder || "").trim(),
        report_folder: String(item?.report_folder || "").trim(),
        expose_source_to_tools: item?.expose_source_to_tools !== false,
        expose_target_to_tools: item?.expose_target_to_tools !== false,
        expose_configurations_to_tools: !!item?.expose_configurations_to_tools,
        expose_translations_to_tools: !!item?.expose_translations_to_tools,
        expose_rules_to_tools: !!item?.expose_rules_to_tools,
        updated_at: String(item?.updated_at || ""),
      }))
      .filter((item) => item.id && item.name);
    const validIds = new Set(nextConfigs.map((item) => item.id));
    const nextActive = String(safe.active_id || "").trim();
    const activeId = validIds.has(nextActive) ? nextActive : "";
    setFolderConfigs(nextConfigs);
    setActiveFolderConfigId(activeId);
    setSelectedFolderConfigId((prev) => {
      if (prev === NEW_FOLDER_CONFIG_OPTION_ID) return NEW_FOLDER_CONFIG_OPTION_ID;
      if (prev && validIds.has(prev)) return prev;
      if (activeId) return activeId;
      return nextConfigs[0]?.id || NEW_FOLDER_CONFIG_OPTION_ID;
    });
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

  async function loadToolLogs({ silent = false, overrides = null } = {}) {
    setError("");
    setToolLogLoading(true);
    try {
      const nextFilters = overrides && typeof overrides === "object" ? overrides : {};
      const statusValue = Object.prototype.hasOwnProperty.call(nextFilters, "status")
        ? nextFilters.status
        : toolLogStatusFilter;
      const nameValue = Object.prototype.hasOwnProperty.call(nextFilters, "tool_name")
        ? nextFilters.tool_name
        : toolLogNameFilter;
      const containsValue = Object.prototype.hasOwnProperty.call(nextFilters, "contains")
        ? nextFilters.contains
        : toolLogTextFilter;
      const sinceDaysValue = Object.prototype.hasOwnProperty.call(nextFilters, "since_days")
        ? nextFilters.since_days
        : toolLogSinceDays;

      const safeStatus = ["all", "ok", "error"].includes(statusValue) ? statusValue : "all";
      const safeDays = Math.max(0, Number.parseInt(String(sinceDaysValue || "0"), 10) || 0);
      const params = new URLSearchParams({
        limit: "300",
        offset: "0",
        status: safeStatus,
        since_days: String(safeDays),
      });
      if (String(nameValue || "").trim()) {
        params.set("tool_name", String(nameValue || "").trim());
      }
      if (String(containsValue || "").trim()) {
        params.set("contains", String(containsValue || "").trim());
      }
      const payload = await api(`/api/tool-logs?${params.toString()}`);
      setToolLogs(Array.isArray(payload?.items) ? payload.items : []);
      setToolLogTotal(Number(payload?.total || 0));
      setToolLogNames(Array.isArray(payload?.tool_names) ? payload.tool_names : []);
      if (!silent) {
        setStatus(`Loaded ${Array.isArray(payload?.items) ? payload.items.length : 0} tool log entr${payload?.items?.length === 1 ? "y" : "ies"}.`);
      }
    } catch (err) {
      setError(err.message);
      if (!silent) {
        setStatus("Loading tool logs failed.");
      }
    } finally {
      setToolLogLoading(false);
    }
  }

  async function refreshBootstrap() {
    setError("");
    try {
      const [folders, folderConfigState, ds, pr, jb, rp, rels] = await Promise.all([
        api("/api/settings/folders"),
        api("/api/settings/folder-configs"),
        api("/api/datasets"),
        api("/api/pairs"),
        api("/api/jobs"),
        api("/api/reports?limit=200"),
        api("/api/relationships?limit=500"),
      ]);
      setSourceFolder(folders.source_folder || "");
      setTargetFolder(folders.target_folder || "");
      setConfigurationsFolder(folders.configurations_folder || "");
      setTranslationsFolder(folders.translations_folder || "");
      setRulesFolder(folders.rules_folder || "");
      setReportFolder(folders.report_folder || "");
      setExposeSourceToTools(folders.expose_source_to_tools !== false);
      setExposeTargetToTools(folders.expose_target_to_tools !== false);
      setExposeConfigurationsToTools(!!folders.expose_configurations_to_tools);
      setExposeTranslationsToTools(!!folders.expose_translations_to_tools);
      setExposeRulesToTools(!!folders.expose_rules_to_tools);
      setSavedFoldersSnapshot({
        source_folder: folders.source_folder || "",
        target_folder: folders.target_folder || "",
        configurations_folder: folders.configurations_folder || "",
        translations_folder: folders.translations_folder || "",
        rules_folder: folders.rules_folder || "",
        report_folder: folders.report_folder || "",
        expose_source_to_tools: folders.expose_source_to_tools !== false,
        expose_target_to_tools: folders.expose_target_to_tools !== false,
        expose_configurations_to_tools: !!folders.expose_configurations_to_tools,
        expose_translations_to_tools: !!folders.expose_translations_to_tools,
        expose_rules_to_tools: !!folders.expose_rules_to_tools,
      });
      applyFolderConfigs(folderConfigState);
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
    const bg = theme === "dark" ? "#040b19" : "#f4f6f8";
    document.documentElement.setAttribute("data-theme", theme);
    document.documentElement.style.backgroundColor = bg;
    document.body.style.backgroundColor = bg;
    const root = document.getElementById("root");
    if (root) {
      root.style.backgroundColor = bg;
    }
    try {
      localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch (_err) {
      // Ignore localStorage write issues.
    }
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
    const leftScoped = datasets.filter(
      (d) => relationshipLeftFolderFilter === "any" || d.side === relationshipLeftFolderFilter
    );
    const rightScoped = datasets.filter(
      (d) => relationshipRightFolderFilter === "any" || d.side === relationshipRightFolderFilter
    );
    const leftDatasetIds = new Set(leftScoped.map((d) => d.id));
    const rightDatasetIds = new Set(rightScoped.map((d) => d.id));
    if (leftDatasetId && !leftDatasetIds.has(leftDatasetId)) {
      setLeftDatasetId("");
      setRelationshipMappings([{ left_field: "", right_field: "" }]);
    }
    if (rightDatasetId && !rightDatasetIds.has(rightDatasetId)) {
      setRightDatasetId("");
      setRelationshipMappings([{ left_field: "", right_field: "" }]);
    }
  }, [relationshipLeftFolderFilter, relationshipRightFolderFilter, leftDatasetId, rightDatasetId, datasets]);

  useEffect(() => {
    if (!catalogDatasetIssue?.datasetId) return;
    const current = datasets.find((d) => d.id === catalogDatasetIssue.datasetId);
    if (!current) {
      setCatalogDatasetIssue(null);
      return;
    }
    const statusInfo = getDatasetStatus(current);
    if (statusInfo.level === "ok") {
      setCatalogDatasetIssue(null);
    }
  }, [datasets, catalogDatasetIssue?.datasetId]);

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
      rows.push(
        mappingRowFromPayload(
          {
            ...m,
            source_field: sourceField,
            target_field: targetField,
          },
          {
            use_key: keySig.has(`${sourceField}|||${targetField}`),
            use_compare: true,
          }
        )
      );
    }
    for (const m of pair.key_mappings || []) {
      const sourceField = m.source_field || m.source;
      const targetField = m.target_field || m.target;
      if (!sourceField || !targetField) continue;
      const exists = rows.some((r) => r.source_field === sourceField && r.target_field === targetField);
      if (!exists) {
        rows.push(
          mappingRowFromPayload(
            {
              ...m,
              source_field: sourceField,
              target_field: targetField,
            },
            {
              use_key: true,
              use_compare: false,
            }
          )
        );
      }
    }
    setFieldMappings(rows);
    setMappingSearch("");
  }, [pairId, pairs]);

  useEffect(() => {
    if (!sourceDataset || !targetDataset) return;
    if (pairId) return;
    const exactPair = pairs.find((p) => p.source_dataset === sourceDataset && p.target_dataset === targetDataset);
    if (exactPair) return;
    if (hasExistingMappings(fieldMappings)) return;
    const relationshipRows = relationshipMappingsForDatasetPair(sourceDataset, targetDataset);
    if (!relationshipRows.length) return;
    setFieldMappings(relationshipRows);
    setStatus(`Loaded ${relationshipRows.length} key mapping(s) from Relationships.`);
  }, [sourceDataset, targetDataset, pairId, pairs, relationships, fieldMappings]);

  useEffect(() => {
    if (!leftDatasetId || !rightDatasetId) return;
    if (relationshipId) return;
    const leftSide = datasets.find((d) => d.id === leftDatasetId)?.side || "";
    const rightSide = datasets.find((d) => d.id === rightDatasetId)?.side || "";
    const sourceTargetPair =
      (leftSide === "source" && rightSide === "target") ||
      (leftSide === "target" && rightSide === "source");
    if (!sourceTargetPair) return;
    const hasRows = relationshipMappings.some((m) => String(m.left_field || "").trim() || String(m.right_field || "").trim());
    if (hasRows) return;
    const keyRows = pairKeyMappingsForRelationshipPair(leftDatasetId, rightDatasetId);
    if (!keyRows.length) return;
    setRelationshipMappings(keyRows);
  }, [leftDatasetId, rightDatasetId, relationshipId, relationshipMappings, pairs, datasets]);

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
    if (tab !== "logs") return;
    loadToolLogs({ silent: true });
  }, [tab]);

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
    setCompareSuggestMessage("");
  }

  function onTargetDatasetChange(value) {
    setTargetDataset(value);
    setPairId("");
    setFieldMappings([]);
    setMappingSearch("");
    setCompareSuggestMessage("");
  }

  async function onRefreshCatalog() {
    setError("");
    setCatalogRefreshing(true);
    setStatus("Refreshing catalog...");
    try {
      const savedFolders = await api("/api/settings/folders", {
        method: "POST",
        body: JSON.stringify({
          source_folder: sourceFolder,
          target_folder: targetFolder,
          configurations_folder: configurationsFolder,
          translations_folder: translationsFolder,
          rules_folder: rulesFolder,
          report_folder: reportFolder,
          expose_source_to_tools: !!exposeSourceToTools,
          expose_target_to_tools: !!exposeTargetToTools,
          expose_configurations_to_tools: !!exposeConfigurationsToTools,
          expose_translations_to_tools: !!exposeTranslationsToTools,
          expose_rules_to_tools: !!exposeRulesToTools,
        }),
      });
      setSavedFoldersSnapshot({
        source_folder: savedFolders.source_folder || "",
        target_folder: savedFolders.target_folder || "",
        configurations_folder: savedFolders.configurations_folder || "",
        translations_folder: savedFolders.translations_folder || "",
        rules_folder: savedFolders.rules_folder || "",
        report_folder: savedFolders.report_folder || "",
        expose_source_to_tools: savedFolders.expose_source_to_tools !== false,
        expose_target_to_tools: savedFolders.expose_target_to_tools !== false,
        expose_configurations_to_tools: !!savedFolders.expose_configurations_to_tools,
        expose_translations_to_tools: !!savedFolders.expose_translations_to_tools,
        expose_rules_to_tools: !!savedFolders.expose_rules_to_tools,
      });
      const res = await api("/api/catalog/refresh", {
        method: "POST",
        body: JSON.stringify({
          source_folder: sourceFolder || null,
          target_folder: targetFolder || null,
          configurations_folder: configurationsFolder || null,
          translations_folder: translationsFolder || null,
          rules_folder: rulesFolder || null,
          report_folder: reportFolder,
          include_row_counts: includeRowCounts,
        }),
      });
      setStatus(
        `Catalog refreshed. Source=${res.source_datasets}, Target=${res.target_datasets}, Pairs=${res.total_pairs}, RowCounts=${res.row_counts_included ? "on" : "off"}`
      );
      await refreshBootstrap();
      setLastCatalogRefreshAt(new Date().toISOString());
    } catch (err) {
      setError(err.message);
      setStatus("Catalog refresh failed.");
    } finally {
      setCatalogRefreshing(false);
    }
  }

  async function saveFolderConfigurationByName(name, { closeModal = true } = {}) {
    const normalizedName = String(name || "").trim();
    if (!normalizedName) {
      setError("Configuration name is required.");
      return null;
    }
    setError("");
    setFolderConfigBusy(true);
    setStatus("Saving folder configuration...");
    try {
      const res = await api("/api/settings/folder-configs", {
        method: "POST",
        body: JSON.stringify({
          name: normalizedName,
          source_folder: sourceFolder,
          target_folder: targetFolder,
          configurations_folder: configurationsFolder,
          translations_folder: translationsFolder,
          rules_folder: rulesFolder,
          report_folder: reportFolder,
          expose_source_to_tools: !!exposeSourceToTools,
          expose_target_to_tools: !!exposeTargetToTools,
          expose_configurations_to_tools: !!exposeConfigurationsToTools,
          expose_translations_to_tools: !!exposeTranslationsToTools,
          expose_rules_to_tools: !!exposeRulesToTools,
          set_active: true,
        }),
      });
      const folders = res?.folders || {};
      setSourceFolder(folders.source_folder || "");
      setTargetFolder(folders.target_folder || "");
      setConfigurationsFolder(folders.configurations_folder || "");
      setTranslationsFolder(folders.translations_folder || "");
      setRulesFolder(folders.rules_folder || "");
      setReportFolder(folders.report_folder || "");
      setExposeSourceToTools(folders.expose_source_to_tools !== false);
      setExposeTargetToTools(folders.expose_target_to_tools !== false);
      setExposeConfigurationsToTools(!!folders.expose_configurations_to_tools);
      setExposeTranslationsToTools(!!folders.expose_translations_to_tools);
      setExposeRulesToTools(!!folders.expose_rules_to_tools);
      setSavedFoldersSnapshot({
        source_folder: folders.source_folder || "",
        target_folder: folders.target_folder || "",
        configurations_folder: folders.configurations_folder || "",
        translations_folder: folders.translations_folder || "",
        rules_folder: folders.rules_folder || "",
        report_folder: folders.report_folder || "",
        expose_source_to_tools: folders.expose_source_to_tools !== false,
        expose_target_to_tools: folders.expose_target_to_tools !== false,
        expose_configurations_to_tools: !!folders.expose_configurations_to_tools,
        expose_translations_to_tools: !!folders.expose_translations_to_tools,
        expose_rules_to_tools: !!folders.expose_rules_to_tools,
      });
      applyFolderConfigs(res);
      if (res?.saved_id) {
        setSelectedFolderConfigId(String(res.saved_id));
      }
      setFolderConfigNameInput(String(res?.saved_name || normalizedName));
      setStatus(res?.created ? `Configuration "${res.saved_name}" created.` : `Configuration "${res.saved_name}" updated.`);
      if (closeModal) {
        setFolderConfigModal((prev) => ({ ...prev, open: false }));
      }
      return res;
    } catch (err) {
      setError(err.message);
      setStatus("Saving configuration failed.");
      return null;
    } finally {
      setFolderConfigBusy(false);
    }
  }

  function onSaveFolderConfig() {
    const selectedId = String(selectedFolderConfigId || "").trim();
    if (!selectedId || selectedId === NEW_FOLDER_CONFIG_OPTION_ID) {
      setFolderConfigModal({
        open: true,
        mode: "save",
        name: "",
        configId: "",
        configName: "",
      });
      return;
    }
    const selectedConfig = folderConfigs.find((cfg) => cfg.id === selectedId);
    if (!selectedConfig) {
      setError("Select a configuration first.");
      return;
    }
    void saveFolderConfigurationByName(selectedConfig.name, { closeModal: false });
  }

  function onRenameFolderConfig() {
    const selectedId = String(selectedFolderConfigId || "").trim();
    if (!selectedId || selectedId === NEW_FOLDER_CONFIG_OPTION_ID) {
      setError("Select an existing configuration to rename.");
      return;
    }
    const selectedConfig = folderConfigs.find((cfg) => cfg.id === selectedId);
    if (!selectedConfig) {
      setError("Select an existing configuration to rename.");
      return;
    }
    setFolderConfigModal({
      open: true,
      mode: "rename",
      name: selectedConfig.name,
      configId: selectedConfig.id,
      configName: selectedConfig.name,
    });
  }

  function closeFolderConfigModal() {
    if (folderConfigBusy) return;
    setFolderConfigModal((prev) => ({
      ...prev,
      open: false,
    }));
  }

  async function onConfirmSaveFolderConfig() {
    await saveFolderConfigurationByName(folderConfigModal.name, { closeModal: true });
  }

  async function onConfirmRenameFolderConfig() {
    const configId = String(folderConfigModal.configId || "").trim();
    const oldName = String(folderConfigModal.configName || "").trim();
    const nextName = String(folderConfigModal.name || "").trim();
    if (!configId || !oldName) {
      setError("Select a configuration to rename.");
      return;
    }
    if (!nextName) {
      setError("Configuration name is required.");
      return;
    }
    if (nextName.toLowerCase() === oldName.toLowerCase()) {
      setFolderConfigModal((prev) => ({ ...prev, open: false }));
      return;
    }
    const saveRes = await saveFolderConfigurationByName(nextName, { closeModal: false });
    if (!saveRes) return;
    const savedId = String(saveRes.saved_id || "").trim();
    if (savedId && savedId !== configId) {
      setFolderConfigBusy(true);
      setStatus(`Renaming "${oldName}" to "${nextName}"...`);
      try {
        const deleteRes = await api(`/api/settings/folder-configs/${encodeURIComponent(configId)}`, {
          method: "DELETE",
        });
        applyFolderConfigs(deleteRes);
        setSelectedFolderConfigId(savedId);
      } catch (err) {
        setError(err.message);
        setStatus("Rename partially completed. New configuration saved, but old configuration could not be deleted.");
        setFolderConfigBusy(false);
        return;
      } finally {
        setFolderConfigBusy(false);
      }
    }
    setFolderConfigModal((prev) => ({ ...prev, open: false }));
    setStatus(`Configuration renamed to "${nextName}".`);
  }

  async function onSelectFolderConfig(nextConfigId) {
    const configId = String(nextConfigId || "").trim();
    if (!configId || configId === NEW_FOLDER_CONFIG_OPTION_ID) {
      setStatus("New configuration mode selected. Click save to create a named configuration.");
      return;
    }
    setError("");
    setFolderConfigBusy(true);
    setStatus("Applying folder configuration...");
    try {
      const res = await api(`/api/settings/folder-configs/${encodeURIComponent(configId)}/apply`, {
        method: "POST",
      });
      const folders = res?.folders || {};
      setSourceFolder(folders.source_folder || "");
      setTargetFolder(folders.target_folder || "");
      setConfigurationsFolder(folders.configurations_folder || "");
      setTranslationsFolder(folders.translations_folder || "");
      setRulesFolder(folders.rules_folder || "");
      setReportFolder(folders.report_folder || "");
      setExposeSourceToTools(folders.expose_source_to_tools !== false);
      setExposeTargetToTools(folders.expose_target_to_tools !== false);
      setExposeConfigurationsToTools(!!folders.expose_configurations_to_tools);
      setExposeTranslationsToTools(!!folders.expose_translations_to_tools);
      setExposeRulesToTools(!!folders.expose_rules_to_tools);
      setSavedFoldersSnapshot({
        source_folder: folders.source_folder || "",
        target_folder: folders.target_folder || "",
        configurations_folder: folders.configurations_folder || "",
        translations_folder: folders.translations_folder || "",
        rules_folder: folders.rules_folder || "",
        report_folder: folders.report_folder || "",
        expose_source_to_tools: folders.expose_source_to_tools !== false,
        expose_target_to_tools: folders.expose_target_to_tools !== false,
        expose_configurations_to_tools: !!folders.expose_configurations_to_tools,
        expose_translations_to_tools: !!folders.expose_translations_to_tools,
        expose_rules_to_tools: !!folders.expose_rules_to_tools,
      });
      applyFolderConfigs(res);
      setStatus(`Configuration "${res?.applied_name || ""}" applied.`);
    } catch (err) {
      setError(err.message);
      setStatus("Applying configuration failed.");
    } finally {
      setFolderConfigBusy(false);
    }
  }

  function onDeleteFolderConfig() {
    const configId = String(selectedFolderConfigId || "").trim();
    if (!configId || configId === NEW_FOLDER_CONFIG_OPTION_ID) {
      setError("Select a configuration to delete.");
      return;
    }
    const selectedConfig = folderConfigs.find((cfg) => cfg.id === configId);
    const selectedName = selectedConfig?.name || configId;
    setFolderConfigModal({
      open: true,
      mode: "delete",
      name: "",
      configId,
      configName: selectedName,
    });
  }

  async function onConfirmDeleteFolderConfig() {
    const configId = String(folderConfigModal.configId || "").trim();
    const selectedName = String(folderConfigModal.configName || configId).trim();
    if (!configId) {
      setError("Select a configuration to delete.");
      return;
    }
    setError("");
    setFolderConfigBusy(true);
    setStatus("Deleting folder configuration...");
    try {
      const res = await api(`/api/settings/folder-configs/${encodeURIComponent(configId)}`, {
        method: "DELETE",
      });
      applyFolderConfigs(res);
      if (
        String(folderConfigNameInput || "").trim().toLowerCase() === String(selectedName).trim().toLowerCase()
      ) {
        setFolderConfigNameInput("");
      }
      setStatus(`Configuration "${res?.deleted_name || selectedName}" deleted.`);
      setFolderConfigModal((prev) => ({ ...prev, open: false }));
    } catch (err) {
      setError(err.message);
      setStatus("Deleting configuration failed.");
    } finally {
      setFolderConfigBusy(false);
    }
  }

  async function onReloadMetadata() {
    setError("");
    setCatalogReloading(true);
    setStatus("Reloading metadata...");
    try {
      await refreshBootstrap();
      setStatus("Metadata reloaded.");
    } catch (err) {
      setError(err.message);
      setStatus("Metadata reload failed.");
    } finally {
      setCatalogReloading(false);
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

  async function onGenerateMcpApiKey() {
    if (settingsMcpAuthMode !== "api") {
      setStatus("Switch MCP authentication mode to API first.");
      return;
    }
    setError("");
    setSettingsGeneratingMcpApiKey(true);
    setStatus("Generating MCP API key...");
    try {
      const res = await api("/api/settings/mcp-auth/generate", { method: "POST" });
      const generated = String(res?.api_key || "").trim();
      if (!generated) {
        throw new Error("API key generation returned an empty key.");
      }
      setSettingsMcpGeneratedApiKey(generated);
      if (res?.app_settings) {
        applyAppSettings(res.app_settings);
      } else {
        await loadAppSettings({ silent: true });
      }
      setStatus("MCP API key generated. Copy it now; it will only be shown once.");
    } catch (err) {
      setError(err.message);
      setStatus("MCP API key generation failed.");
    } finally {
      setSettingsGeneratingMcpApiKey(false);
    }
  }

  async function onCopyGeneratedMcpApiKey() {
    const key = String(settingsMcpGeneratedApiKey || "").trim();
    if (!key) {
      setStatus("Generate an MCP API key first.");
      return;
    }
    setError("");
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(key);
      } else {
        const temp = document.createElement("textarea");
        temp.value = key;
        temp.setAttribute("readonly", "true");
        temp.style.position = "fixed";
        temp.style.opacity = "0";
        document.body.appendChild(temp);
        temp.focus();
        temp.select();
        document.execCommand("copy");
        document.body.removeChild(temp);
      }
      setStatus("MCP API key copied to clipboard.");
    } catch (err) {
      setError(err.message || "Copy failed.");
      setStatus("Copying MCP API key failed.");
    }
  }

  async function onSaveAppSettings() {
    setError("");
    setSettingsSaving(true);
    setStatus("Saving app settings...");
    try {
      const payload = {
        theme: settingsTheme,
        mcp_auth_mode: settingsMcpAuthMode,
        tool_logging_enabled: !!settingsToolLoggingEnabled,
        model: String(settingsModel || "").trim(),
        anthropic_api_key: String(settingsApiKeyInput || "").trim() || null,
        ngrok_authtoken: String(settingsNgrokTokenInput || "").trim() || null,
        claude_instructions: String(settingsClaudeInstructions || "").trim(),
      };
      const saved = await api("/api/settings/app", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      applyAppSettings(saved);
      setSettingsApiKeyInput("");
      setSettingsNgrokTokenInput("");
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
      if (tab === "logs") {
        await loadToolLogs({ silent: true });
      }
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

  async function onClearAllToolLogs() {
    if (toolLogCleaning) return;
    const confirmed = window.confirm("Delete all tool call logs?");
    if (!confirmed) return;
    setError("");
    setToolLogCleaning(true);
    setStatus("Deleting all tool logs...");
    try {
      const res = await api("/api/tool-logs", { method: "DELETE" });
      setStatus(`Deleted ${Number(res?.deleted || 0)} tool log entr${Number(res?.deleted || 0) === 1 ? "y" : "ies"}.`);
      await loadToolLogs({ silent: true });
    } catch (err) {
      setError(err.message);
      setStatus("Deleting tool logs failed.");
    } finally {
      setToolLogCleaning(false);
    }
  }

  async function onClearToolLogsOlderThanDays() {
    if (toolLogCleaning) return;
    const days = Math.max(1, Number.parseInt(String(toolLogCleanupDays || "1"), 10) || 1);
    setError("");
    setToolLogCleaning(true);
    setStatus(`Deleting tool logs older than ${days} day(s)...`);
    try {
      const res = await api("/api/tool-logs/cleanup-older-than", {
        method: "POST",
        body: JSON.stringify({ days }),
      });
      setStatus(`Deleted ${Number(res?.deleted || 0)} log entr${Number(res?.deleted || 0) === 1 ? "y" : "ies"} older than ${days} day(s).`);
      await loadToolLogs({ silent: true });
    } catch (err) {
      setError(err.message);
      setStatus("Deleting old tool logs failed.");
    } finally {
      setToolLogCleaning(false);
    }
  }

  async function onResetToolLogFilters() {
    const defaults = {
      status: "all",
      tool_name: "",
      contains: "",
      since_days: 7,
    };
    setToolLogStatusFilter(defaults.status);
    setToolLogNameFilter(defaults.tool_name);
    setToolLogTextFilter(defaults.contains);
    setToolLogSinceDays(defaults.since_days);
    await loadToolLogs({ silent: true, overrides: defaults });
    setStatus("Tool log filters reset.");
  }

  function onLoadDefaultClaudeInstructions() {
    setSettingsClaudeInstructions(DEFAULT_CLAUDE_INSTRUCTIONS);
  }

  async function onBrowseFolder(kind) {
    setError("");
    const current =
      kind === "source"
        ? sourceFolder
        : kind === "target"
          ? targetFolder
          : kind === "configurations"
            ? configurationsFolder
            : kind === "translations"
              ? translationsFolder
              : kind === "rules"
                ? rulesFolder
                : reportFolder;
    try {
      const path = current ? `?initial=${encodeURIComponent(current)}` : "";
      const res = await api(`/api/system/browse-folder${path}`);
      if (!res?.folder) {
        return;
      }
      const nextSource = kind === "source" ? res.folder : sourceFolder;
      const nextTarget = kind === "target" ? res.folder : targetFolder;
      const nextConfigurations = kind === "configurations" ? res.folder : configurationsFolder;
      const nextTranslations = kind === "translations" ? res.folder : translationsFolder;
      const nextRules = kind === "rules" ? res.folder : rulesFolder;
      const nextReport = kind === "report" ? res.folder : reportFolder;
      const saved = await api("/api/settings/folders", {
        method: "POST",
        body: JSON.stringify({
          source_folder: nextSource,
          target_folder: nextTarget,
          configurations_folder: nextConfigurations,
          translations_folder: nextTranslations,
          rules_folder: nextRules,
          report_folder: nextReport,
          expose_source_to_tools: !!exposeSourceToTools,
          expose_target_to_tools: !!exposeTargetToTools,
          expose_configurations_to_tools: !!exposeConfigurationsToTools,
          expose_translations_to_tools: !!exposeTranslationsToTools,
          expose_rules_to_tools: !!exposeRulesToTools,
        }),
      });
      setSourceFolder(saved.source_folder || "");
      setTargetFolder(saved.target_folder || "");
      setConfigurationsFolder(saved.configurations_folder || "");
      setTranslationsFolder(saved.translations_folder || "");
      setRulesFolder(saved.rules_folder || "");
      setReportFolder(saved.report_folder || "");
      setExposeSourceToTools(saved.expose_source_to_tools !== false);
      setExposeTargetToTools(saved.expose_target_to_tools !== false);
      setExposeConfigurationsToTools(!!saved.expose_configurations_to_tools);
      setExposeTranslationsToTools(!!saved.expose_translations_to_tools);
      setExposeRulesToTools(!!saved.expose_rules_to_tools);
      setSavedFoldersSnapshot({
        source_folder: saved.source_folder || "",
        target_folder: saved.target_folder || "",
        configurations_folder: saved.configurations_folder || "",
        translations_folder: saved.translations_folder || "",
        rules_folder: saved.rules_folder || "",
        report_folder: saved.report_folder || "",
        expose_source_to_tools: saved.expose_source_to_tools !== false,
        expose_target_to_tools: saved.expose_target_to_tools !== false,
        expose_configurations_to_tools: !!saved.expose_configurations_to_tools,
        expose_translations_to_tools: !!saved.expose_translations_to_tools,
        expose_rules_to_tools: !!saved.expose_rules_to_tools,
      });
      applyFolderConfigs(await api("/api/settings/folder-configs"));
      const label =
        kind === "source"
          ? "Source"
          : kind === "target"
            ? "Target"
            : kind === "configurations"
              ? "Configurations"
              : kind === "translations"
                ? "Translations"
                : kind === "rules"
                  ? "Rules"
                  : "Report";
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

  function normalizeOriginMode(value) {
    const mode = String(value || "").trim().toLowerCase();
    if (mode === "name" || mode === "content" || mode === "manual") {
      return mode;
    }
    return "manual";
  }

  function normalizeConfidence(value) {
    if (value === null || value === undefined || value === "") return null;
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    return Math.max(0, Math.min(1, n));
  }

  function mappingRowFromPayload(mapping, defaults = {}) {
    const sourceField = mapping?.source_field || mapping?.source || "";
    const targetField = mapping?.target_field || mapping?.target || "";
    const originMode = normalizeOriginMode(mapping?.origin_mode || defaults.origin_mode || "manual");
    const confidence = originMode === "content" ? normalizeConfidence(mapping?.confidence) : null;
    const keyHint = Boolean(mapping?.is_key_pair || mapping?.key_pair || mapping?.key_candidate);
    const useKeyDefault = defaults.use_key !== undefined ? Boolean(defaults.use_key) : false;
    const useCompareDefault = defaults.use_compare !== undefined ? Boolean(defaults.use_compare) : true;
    return {
      source_field: sourceField,
      target_field: targetField,
      use_key: mapping?.use_key === undefined ? useKeyDefault || keyHint : Boolean(mapping.use_key),
      use_compare: mapping?.use_compare === undefined ? useCompareDefault : Boolean(mapping.use_compare),
      origin_mode: originMode,
      confidence,
      is_key_pair: keyHint,
      low_cardinality: Boolean(mapping?.low_cardinality),
    };
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
      setQuickMapPendingLabel("suggested");
      setStatus("Mapping suggestion cancelled.");
      return;
    }
    if (mode === "override") {
      setFieldMappings(mapped);
      setQuickMapChoiceOpen(false);
      setQuickMapPendingMappings([]);
      setQuickMapPendingLabel("suggested");
      setStatus(`Applied ${mapped.length} ${quickMapPendingLabel} mapping(s). Existing mappings were overridden.`);
      return;
    }
    const { merged, addedCount, existingCount } = mergeQuickMappings(fieldMappings, mapped);
    setFieldMappings(merged);
    setQuickMapChoiceOpen(false);
    setQuickMapPendingMappings([]);
    setQuickMapPendingLabel("suggested");
    setStatus(
      `Applied ${mapped.length} ${quickMapPendingLabel} mapping(s). Added ${addedCount} new mapping(s); kept ${existingCount} existing row(s).`
    );
  }

  function relationshipMappingsForDatasetPair(sourceId, targetId) {
    if (!sourceId || !targetId) return [];
    const rows = [];
    const seen = new Set();
    const activeRelationships = Array.isArray(relationships)
      ? relationships.filter((r) => r && r.active !== false)
      : [];
    for (const relRow of activeRelationships) {
      const direct = relRow.left_dataset === sourceId && relRow.right_dataset === targetId;
      const reverse = relRow.left_dataset === targetId && relRow.right_dataset === sourceId;
      if (!direct && !reverse) continue;
      const pairs =
        Array.isArray(relRow.field_pairs) && relRow.field_pairs.length
          ? relRow.field_pairs
          : (() => {
              const leftFields = Array.isArray(relRow.left_fields)
                ? relRow.left_fields
                : relRow.left_field
                  ? [relRow.left_field]
                  : [];
              const rightFields = Array.isArray(relRow.right_fields)
                ? relRow.right_fields
                : relRow.right_field
                  ? [relRow.right_field]
                  : [];
              const count = Math.min(leftFields.length, rightFields.length);
              const fallback = [];
              for (let i = 0; i < count; i += 1) {
                fallback.push({ left_field: leftFields[i], right_field: rightFields[i] });
              }
              return fallback;
            })();
      for (const pair of pairs) {
        const sourceField = direct ? pair.left_field : pair.right_field;
        const targetField = direct ? pair.right_field : pair.left_field;
        if (!sourceField || !targetField) continue;
        const sig = `${normalizeFieldName(sourceField)}|||${normalizeFieldName(targetField)}`;
        if (seen.has(sig)) continue;
        seen.add(sig);
        rows.push(
          mappingRowFromPayload(
            {
              source_field: sourceField,
              target_field: targetField,
              origin_mode: relRow.method || "manual",
            },
            { use_key: true, use_compare: true }
          )
        );
      }
    }
    return rows;
  }

  function pairKeyMappingsForRelationshipPair(leftId, rightId) {
    if (!leftId || !rightId) return [];
    const leftSide = datasets.find((d) => d.id === leftId)?.side || "";
    const rightSide = datasets.find((d) => d.id === rightId)?.side || "";
    const sourceTargetPair =
      (leftSide === "source" && rightSide === "target") ||
      (leftSide === "target" && rightSide === "source");
    if (!sourceTargetPair) return [];
    const directPair = pairs.find((p) => p.source_dataset === leftId && p.target_dataset === rightId);
    const reversePair = !directPair
      ? pairs.find((p) => p.source_dataset === rightId && p.target_dataset === leftId)
      : null;
    const pair = directPair || reversePair;
    if (!pair) return [];
    const reverse = !!reversePair;
    const keyMappings = Array.isArray(pair.key_mappings) ? pair.key_mappings : [];
    return keyMappings
      .map((m) => ({
        left_field: reverse ? m.target_field || m.target : m.source_field || m.source,
        right_field: reverse ? m.source_field || m.source : m.target_field || m.target,
      }))
      .filter((m) => String(m.left_field || "").trim() && String(m.right_field || "").trim());
  }

  function inferRelationshipSide(leftId, rightId) {
    const leftSide = datasets.find((d) => d.id === leftId)?.side || "";
    const rightSide = datasets.find((d) => d.id === rightId)?.side || "";
    if (!leftSide || !rightSide) return "cross";
    if (leftSide === rightSide) return leftSide;
    return "cross";
  }

  async function applyNameMappings() {
    setError("");
    setCompareSuggestMessage("");
    if (!sourceDataset || !targetDataset) {
      setError("Select source and target datasets first.");
      return;
    }
    setStatus("Suggesting name-based mappings...");
    try {
      const result = await api(
        `/api/pairs/quick-map?source_dataset_id=${encodeURIComponent(sourceDataset)}&target_dataset_id=${encodeURIComponent(
          targetDataset
        )}&mode=name`
      );
      const mapped = Array.isArray(result?.compare_mappings)
        ? result.compare_mappings.map((m) => mappingRowFromPayload(m, { use_compare: true }))
        : [];
      if (!mapped.length) {
        setStatus("No same-name fields found.");
        return;
      }
      if (pairId || hasExistingMappings()) {
        setQuickMapPendingMappings(mapped);
        setQuickMapPendingLabel("name-based");
        setQuickMapChoiceOpen(true);
        return;
      }
      setFieldMappings(mapped);
      const keyMarked = mapped.filter((m) => m.use_key).length;
      setStatus(`Suggested ${mapped.length} name-based mapping(s). Key-marked: ${keyMarked}.`);
    } catch (err) {
      setError(err.message);
      setStatus("Name-based mapping suggestion failed.");
    }
  }

  async function applyContentAwareMappings() {
    setError("");
    if (!sourceDataset || !targetDataset) {
      setError("Select source and target datasets first.");
      return;
    }
    setCompareSuggestBusy(true);
    setCompareSuggestMessage("");
    setStatus("Suggesting content-aware mappings...");
    try {
      const result = await api(
        `/api/pairs/quick-map?source_dataset_id=${encodeURIComponent(sourceDataset)}&target_dataset_id=${encodeURIComponent(
          targetDataset
        )}&mode=content&min_confidence=0.6`
      );
      const mapped = Array.isArray(result?.compare_mappings)
        ? result.compare_mappings.map((m) => mappingRowFromPayload(m, { use_compare: true }))
        : [];
      if (!mapped.length) {
        setCompareSuggestMessage("No content-based mapping candidates were found for the selected datasets.");
        setStatus("No high-confidence content-aware mappings found.");
        return;
      }
      if (pairId || hasExistingMappings()) {
        setQuickMapPendingMappings(mapped);
        setQuickMapPendingLabel("content-aware");
        setQuickMapChoiceOpen(true);
        setCompareSuggestMessage(`Found ${mapped.length} content-based mapping candidate(s). Choose how to apply them.`);
        return;
      }
      setFieldMappings(mapped);
      const keyMarked = mapped.filter((m) => m.use_key).length;
      setCompareSuggestMessage(`Found ${mapped.length} content-based mapping candidate(s).`);
      setStatus(`Suggested ${mapped.length} content-aware mapping(s). Key-marked: ${keyMarked}.`);
    } catch (err) {
      setError(err.message);
      setCompareSuggestMessage("Content-based mapping suggestion failed.");
      setStatus("Content-aware mapping suggestion failed.");
    } finally {
      setCompareSuggestBusy(false);
    }
  }

  function addMappingRow() {
    setFieldMappings((prev) => [
      ...prev,
      {
        source_field: "",
        target_field: "",
        use_key: false,
        use_compare: true,
        origin_mode: "manual",
        confidence: null,
        is_key_pair: false,
        low_cardinality: false,
      },
    ]);
  }

  function removeMappingRow(index) {
    setFieldMappings((prev) => prev.filter((_, i) => i !== index));
  }

  function updateMappingRow(index, patch) {
    const normalizedPatch = { ...patch };
    if (Object.prototype.hasOwnProperty.call(normalizedPatch, "origin_mode")) {
      normalizedPatch.origin_mode = normalizeOriginMode(normalizedPatch.origin_mode);
      if (normalizedPatch.origin_mode !== "content") {
        normalizedPatch.confidence = null;
      }
    }
    if (Object.prototype.hasOwnProperty.call(normalizedPatch, "confidence")) {
      normalizedPatch.confidence = normalizeConfidence(normalizedPatch.confidence);
    }
    setFieldMappings((prev) =>
      prev.map((row, i) => {
        if (i !== index) return row;
        return { ...row, ...normalizedPatch };
      })
    );
  }

  function closePairKeyDeleteModal() {
    setPairKeyDeleteModal((prev) => {
      if (prev.busy) return prev;
      return {
        open: false,
        pairId: "",
        sourceDataset: "",
        targetDataset: "",
        keyCount: 0,
        busy: false,
      };
    });
  }

  function onDeletePairKeyMappings(pair) {
    if (!pair?.id) return;
    const keyCount = Array.isArray(pair.key_mappings) ? pair.key_mappings.length : 0;
    if (!keyCount) {
      setStatus(`Pair ${pair.id} has no saved key mappings to clear.`);
      return;
    }
    setPairKeyDeleteModal({
      open: true,
      pairId: pair.id,
      sourceDataset: pair.source_dataset || "",
      targetDataset: pair.target_dataset || "",
      keyCount,
      busy: false,
    });
  }

  async function confirmDeletePairKeyMappings() {
    if (!pairKeyDeleteModal.pairId || pairKeyDeleteModal.busy) return;
    setPairKeyDeleteModal((prev) => ({ ...prev, busy: true }));
    setError("");
    setStatus(`Clearing key mappings from ${pairKeyDeleteModal.pairId}...`);
    try {
      const result = await api(`/api/pairs/${encodeURIComponent(pairKeyDeleteModal.pairId)}/key-mappings`, {
        method: "DELETE",
      });
      await refreshBootstrap();
      setStatus(`Cleared key mappings for ${result.pair_id}.`);
      setPairKeyDeleteModal({
        open: false,
        pairId: "",
        sourceDataset: "",
        targetDataset: "",
        keyCount: 0,
        busy: false,
      });
    } catch (err) {
      setError(err.message);
      setStatus("Clearing key mappings failed.");
      setPairKeyDeleteModal((prev) => ({ ...prev, busy: false }));
    }
  }

  function closePairDeleteModal() {
    setPairDeleteModal((prev) => {
      if (prev.busy) return prev;
      return {
        open: false,
        pairId: "",
        sourceDataset: "",
        targetDataset: "",
        busy: false,
      };
    });
  }

  function onDeletePair(pair) {
    if (!pair?.id) return;
    setPairDeleteModal({
      open: true,
      pairId: pair.id,
      sourceDataset: pair.source_dataset || "",
      targetDataset: pair.target_dataset || "",
      busy: false,
    });
  }

  async function confirmDeletePair() {
    if (!pairDeleteModal.pairId || pairDeleteModal.busy) return;
    setPairDeleteModal((prev) => ({ ...prev, busy: true }));
    setError("");
    setStatus(`Deleting pair ${pairDeleteModal.pairId}...`);
    try {
      const result = await api(`/api/pairs/${encodeURIComponent(pairDeleteModal.pairId)}`, {
        method: "DELETE",
      });
      await refreshBootstrap();
      if (pairId === result.pair_id) {
        setPairId("");
      }
      setStatus(`Deleted pair ${result.pair_id}.`);
      setPairDeleteModal({
        open: false,
        pairId: "",
        sourceDataset: "",
        targetDataset: "",
        busy: false,
      });
    } catch (err) {
      setError(err.message);
      setStatus("Deleting pair failed.");
      setPairDeleteModal((prev) => ({ ...prev, busy: false }));
    }
  }

  async function savePairMappings() {
    if (!sourceDataset || !targetDataset) return;
    const keyMappings = fieldMappings
      .filter((m) => m.use_key && m.source_field && m.target_field)
      .map((m) => ({
        source_field: m.source_field,
        target_field: m.target_field,
        origin_mode: normalizeOriginMode(m.origin_mode),
        confidence: normalizeOriginMode(m.origin_mode) === "content" ? normalizeConfidence(m.confidence) : null,
        is_key_pair: !!m.is_key_pair,
        low_cardinality: !!m.low_cardinality,
        use_key: !!m.use_key,
        use_compare: !!m.use_compare,
      }));
    const compareMappings = fieldMappings
      .filter((m) => m.use_compare && m.source_field && m.target_field)
      .map((m) => ({
        source_field: m.source_field,
        target_field: m.target_field,
        origin_mode: normalizeOriginMode(m.origin_mode),
        confidence: normalizeOriginMode(m.origin_mode) === "content" ? normalizeConfidence(m.confidence) : null,
        is_key_pair: !!m.is_key_pair,
        low_cardinality: !!m.low_cardinality,
        use_key: !!m.use_key,
        use_compare: !!m.use_compare,
      }));
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

  function closeReportDeleteModal() {
    setReportDeleteModal((prev) => {
      if (prev.busy) return prev;
      return { open: false, reportId: "", reportFile: "", busy: false };
    });
  }

  function onDeleteReport(reportId) {
    const report = reports.find((r) => String(r.id) === String(reportId));
    setReportDeleteModal({
      open: true,
      reportId: String(reportId || ""),
      reportFile: String(report?.file_name || ""),
      busy: false,
    });
  }

  async function confirmDeleteReport() {
    const reportId = String(reportDeleteModal.reportId || "").trim();
    if (!reportId || reportDeleteModal.busy) return;
    setReportDeleteModal((prev) => ({ ...prev, busy: true }));
    setError("");
    setStatus(`Deleting report ${reportId}...`);
    try {
      await api(`/api/reports/${encodeURIComponent(reportId)}`, { method: "DELETE" });
      await refreshBootstrap();
      setStatus(`Deleted report ${reportId}.`);
      setReportDeleteModal({ open: false, reportId: "", reportFile: "", busy: false });
    } catch (err) {
      setError(err.message);
      setStatus("Deleting report failed.");
      setReportDeleteModal((prev) => ({ ...prev, busy: false }));
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

  async function runSqlPreview() {
    const clean = String(sqlText || "").trim();
    if (!clean) {
      setError("Enter a SQL query first.");
      return;
    }
    setError("");
    setSqlBusy(true);
    setStatus("Running SQL preview...");
    try {
      const result = await api("/api/sql/preview", {
        method: "POST",
        body: JSON.stringify({
          sql: clean,
          limit: Math.max(1, Math.min(100, Number(sqlLimit) || 100)),
          include_total: !!sqlIncludeTotal,
        }),
      });
      setSqlResult(result);
      setStatus("SQL preview completed.");
    } catch (err) {
      setError(err.message);
      setStatus("SQL preview failed.");
    } finally {
      setSqlBusy(false);
    }
  }

  async function startSqlExport() {
    const clean = String(sqlText || "").trim();
    if (!clean) {
      setError("Enter a SQL query first.");
      return;
    }
    setError("");
    setSqlBusy(true);
    setStatus("Starting SQL export job...");
    try {
      const result = await api("/api/sql/export", {
        method: "POST",
        body: JSON.stringify({
          sql: clean,
          filename: String(sqlExportFilename || "").trim() || null,
          async_job: true,
        }),
      });
      setSqlExportJob(result);
      await refreshBootstrap();
      setStatus(result?.message || `Export job queued: ${result.job_id}`);
    } catch (err) {
      setError(err.message);
      setStatus("SQL export failed to start.");
    } finally {
      setSqlBusy(false);
    }
  }

  async function refreshSqlExportJob() {
    const jobId = String(sqlExportJob?.job_id || "").trim();
    if (!jobId) {
      return;
    }
    setError("");
    setSqlBusy(true);
    try {
      const summary = await api(`/api/jobs/${encodeURIComponent(jobId)}/summary`);
      setSqlExportJob(summary);
      if (summary?.state === "succeeded") {
        await refreshBootstrap();
        setStatus(`Export job ${jobId} completed.`);
      } else {
        setStatus(`Export job ${jobId}: ${summary?.state || "unknown"}.`);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setSqlBusy(false);
    }
  }

  async function onRunSqlAction() {
    if (sqlOutputMode === "export") {
      await startSqlExport();
      return;
    }
    await runSqlPreview();
  }

  async function onOpenSqlExportReport() {
    const reportId = String(sqlExportJob?.report?.id || "").trim();
    if (!reportId) {
      return;
    }
    await onOpenReport(reportId);
  }

  function clearRelationshipForm({ keepDatasets = false } = {}) {
    setRelationshipId("");
    if (!keepDatasets) {
      setLeftDatasetId("");
      setRightDatasetId("");
    }
    setRelationshipMappings([{ left_field: "", right_field: "" }]);
    setRelationshipSuggestMessage("");
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
    setLeftDatasetId(row.left_dataset || "");
    const leftSide = datasets.find((d) => d.id === row.left_dataset)?.side || "any";
    setRelationshipLeftFolderFilter(leftSide);
    const lf = row.left_fields || (row.left_field ? [row.left_field] : []);
    setRightDatasetId(row.right_dataset || "");
    const rightSide = datasets.find((d) => d.id === row.right_dataset)?.side || "any";
    setRelationshipRightFolderFilter(rightSide);
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
    // Editing is always a manual action; keep confidence read-only ("-") until user re-suggests.
    setRelationshipConfidence(0.95);
    setRelationshipMethod("manual");
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

  function stopRelationshipSuggestion() {
    if (!relationshipSuggestBusy) return;
    relationshipSuggestStopRef.current = true;
    setRelationshipSuggestStopRequested(true);
    try {
      relationshipSuggestAbortRef.current?.abort();
    } catch (_err) {}
    setStatus("Stopping relationship suggestion...");
  }

  async function suggestRelationshipMappings(mode) {
    relationshipSuggestStopRef.current = false;
    setRelationshipSuggestStopRequested(false);
    const requestedMode = mode === "content" ? "content" : "name";
    const contentMode = requestedMode === "content";
    const leftScopeSelected = !!leftDatasetId || relationshipLeftFolderFilter !== "any";
    const rightScopeSelected = !!rightDatasetId || relationshipRightFolderFilter !== "any";
    if (!leftDatasetId || !rightDatasetId) {
      if (relationshipId) {
        setError("Select left and right datasets first.");
        return;
      }
      if (!leftScopeSelected || !rightScopeSelected) {
        setError("Select a left and right dataset or folder prefilter scope first.");
        return;
      }
      setError("");
      setRelationshipSuggestMessage("");
      setRelationshipSuggestBusy(true);
      setStatus(`Auto-matching relationships by ${requestedMode} for selected scope...`);
      try {
        const leftScopeDatasets = leftDatasetId
          ? datasets.filter((d) => d.id === leftDatasetId)
          : datasets.filter((d) => d.side === relationshipLeftFolderFilter);
        const rightScopeDatasets = rightDatasetId
          ? datasets.filter((d) => d.id === rightDatasetId)
          : datasets.filter((d) => d.side === relationshipRightFolderFilter);

        const pairQueue = [];
        const seenPairSigs = new Set();
        for (const leftDs of leftScopeDatasets) {
          for (const rightDs of rightScopeDatasets) {
            if (!leftDs?.id || !rightDs?.id) continue;
            if (leftDs.id === rightDs.id) continue;
            const sig = [leftDs.id, rightDs.id].sort().join("|||");
            if (seenPairSigs.has(sig)) continue;
            seenPairSigs.add(sig);
            pairQueue.push({ left_dataset: leftDs.id, right_dataset: rightDs.id });
          }
        }

        if (!pairQueue.length) {
          setRelationshipSuggestMessage("No dataset pairs found for the selected folder scope.");
          setStatus("No dataset pairs available for scoped matching.");
          return;
        }

        let totalApplied = 0;
        let totalSkippedExisting = 0;
        let failedPairs = 0;
        let lastFailureMessage = "";

        for (let i = 0; i < pairQueue.length; i += 1) {
          if (relationshipSuggestStopRef.current) {
            break;
          }
          const pair = pairQueue[i];
          const progressLabel = `${i + 1}/${pairQueue.length}`;
          setStatus(`Auto-matching by ${requestedMode}: ${progressLabel} pairs processed...`);
          setRelationshipSuggestMessage(
            `Processing pair ${progressLabel}: ${pair.left_dataset} -> ${pair.right_dataset}`
          );
          try {
            const controller = new AbortController();
            relationshipSuggestAbortRef.current = controller;
            const result = await api("/api/relationships/auto-link", {
              method: "POST",
              signal: controller.signal,
              body: JSON.stringify({
                left_side: relationshipLeftFolderFilter || "any",
                right_side: relationshipRightFolderFilter || "any",
                left_dataset: pair.left_dataset,
                right_dataset: pair.right_dataset,
                mode: requestedMode,
                min_confidence: requestedMode === "content" ? 0.6 : 0.75,
                suggest_only: false,
                max_links: 1,
              }),
            });
            totalApplied += Number(result?.applied_count || 0);
            totalSkippedExisting += Number(result?.pairs_skipped_existing || 0);
          } catch (err) {
            if (relationshipSuggestStopRef.current) {
              break;
            }
            failedPairs += 1;
            lastFailureMessage = String(err?.message || err || "");
          } finally {
            relationshipSuggestAbortRef.current = null;
          }
        }

        await refreshBootstrap();
        const unresolvedPairs = Math.max(0, pairQueue.length - totalApplied - totalSkippedExisting - failedPairs);
        const skippedNoMatch = unresolvedPairs;
        const stopped = relationshipSuggestStopRef.current;
        if (failedPairs > 0) {
          setError(
            `Scoped matching completed with ${failedPairs} failed pair(s).${lastFailureMessage ? ` Last error: ${lastFailureMessage}` : ""}`
          );
        }
        setRelationshipSuggestMessage(
          `Processed ${pairQueue.length}/${pairQueue.length} pairs. Created: ${totalApplied}. Skipped existing: ${totalSkippedExisting}. No match: ${skippedNoMatch}. Failed: ${failedPairs}.`
        );
        setStatus(
          stopped
            ? `Scoped ${requestedMode} matching stopped. Created ${totalApplied} relationship(s).`
            : `Scoped ${requestedMode} matching finished. Created ${totalApplied} relationship(s) across ${pairQueue.length} pair(s).`
        );
        return;
      } catch (err) {
        if (relationshipSuggestStopRef.current) {
          setStatus(`Scoped ${requestedMode} matching stopped.`);
        } else {
          setError(err.message);
          setStatus(`Auto-matching by ${requestedMode} failed.`);
        }
        return;
      } finally {
        relationshipSuggestAbortRef.current = null;
        relationshipSuggestStopRef.current = false;
        setRelationshipSuggestStopRequested(false);
        setRelationshipSuggestBusy(false);
      }
    }

    setError("");
    setRelationshipSuggestMessage("");
    setRelationshipSuggestBusy(true);
    setStatus(`Suggesting relationship mappings by ${requestedMode}...`);
    try {
      const controller = new AbortController();
      relationshipSuggestAbortRef.current = controller;
      const res = await api(
        `/api/pairs/quick-map?source_dataset_id=${encodeURIComponent(leftDatasetId)}&target_dataset_id=${encodeURIComponent(
          rightDatasetId
        )}&mode=${requestedMode}${requestedMode === "content" ? "&min_confidence=0.6" : ""}`,
        { signal: controller.signal }
      );
      const keyCandidates = Array.isArray(res?.compare_mappings)
        ? res.compare_mappings.filter((m) => !!m && (m.is_key_pair === true || m.use_key === true))
        : [];
      const contentConfidences = contentMode
        ? keyCandidates
            .map((m) => Number(m?.confidence))
            .filter((v) => Number.isFinite(v))
        : [];
      const rows = keyCandidates
            .map((m) => ({
              left_field: String(m?.source_field || "").trim(),
              right_field: String(m?.target_field || "").trim(),
            }))
            .filter((m) => m.left_field && m.right_field)
      if (!rows.length) {
        if (contentMode) {
          setRelationshipSuggestMessage("No content-based key mapping candidates were found for relationship creation.");
        }
        setStatus(`No ${requestedMode}-based key mappings found for relationship creation.`);
        return;
      }
      const merged = relationshipMappings.filter(
        (m) => String(m.left_field || "").trim() || String(m.right_field || "").trim()
      );
      const seen = new Set(
        merged
          .filter((m) => (m.left_field || "").trim() && (m.right_field || "").trim())
          .map((m) => `${normalizeFieldName(m.left_field)}|||${normalizeFieldName(m.right_field)}`)
      );
      let added = 0;
      rows.forEach((row) => {
        const sig = `${normalizeFieldName(row.left_field)}|||${normalizeFieldName(row.right_field)}`;
        if (seen.has(sig)) return;
        seen.add(sig);
        merged.push(row);
        added += 1;
      });
      if (!merged.length && !added) {
        merged.push({ left_field: "", right_field: "" });
      }
      setRelationshipMappings(merged);
      if (contentMode) {
        if (contentConfidences.length) {
          const avgConfidence = contentConfidences.reduce((sum, v) => sum + v, 0) / contentConfidences.length;
          const boundedConfidence = Math.max(0, Math.min(1, avgConfidence));
          setRelationshipMethod("content");
          setRelationshipConfidence(boundedConfidence.toFixed(3));
          setRelationshipSuggestMessage(
            `Found ${rows.length} content-based key mapping candidate(s). Added ${added} row(s). Confidence set to ${boundedConfidence.toFixed(3)}.`
          );
        } else {
          setRelationshipSuggestMessage(`Found ${rows.length} content-based key mapping candidate(s). Added ${added} row(s).`);
        }
      }
      setStatus(`Suggested ${rows.length} ${requestedMode}-based key mapping(s). Added ${added} new row(s).`);
    } catch (err) {
      if (relationshipSuggestStopRef.current) {
        setStatus(`Suggestion by ${requestedMode} stopped.`);
      } else {
        setError(err.message);
        if (contentMode) {
          setRelationshipSuggestMessage("Content-based relationship mapping suggestion failed.");
        }
        setStatus("Relationship mapping suggestion failed.");
      }
    } finally {
      relationshipSuggestAbortRef.current = null;
      relationshipSuggestStopRef.current = false;
      setRelationshipSuggestStopRequested(false);
      setRelationshipSuggestBusy(false);
    }
  }

  async function saveRelationship() {
    const leftScopeSelected = !!leftDatasetId || relationshipLeftFolderFilter !== "any";
    const rightScopeSelected = !!rightDatasetId || relationshipRightFolderFilter !== "any";
    if (!leftDatasetId || !rightDatasetId) {
      if (relationshipId) {
        setError("Select left and right datasets first.");
        return;
      }
      if (!leftScopeSelected || !rightScopeSelected) {
        setError("Select a left and right dataset or folder prefilter scope first.");
        return;
      }
      setError("");
      setStatus("Auto-creating relationships for selected folder/dataset scope...");
      try {
        const result = await api("/api/relationships/auto-link", {
          method: "POST",
          body: JSON.stringify({
            left_side: relationshipLeftFolderFilter || "any",
            right_side: relationshipRightFolderFilter || "any",
            left_dataset: leftDatasetId || "",
            right_dataset: rightDatasetId || "",
            min_confidence: 0.6,
            suggest_only: false,
            max_links: 500,
          }),
        });
        await refreshBootstrap();
        const appliedCount = Number(result?.applied_count || 0);
        const skippedExisting = Number(result?.pairs_skipped_existing || 0);
        if (appliedCount > 0) {
          setStatus(`Auto-created ${appliedCount} relationship(s). Skipped existing dataset pairs: ${skippedExisting}.`);
        } else {
          setStatus(`No new relationships were created. Existing pairs skipped: ${skippedExisting}.`);
        }
        return;
      } catch (err) {
        setError(err.message);
        setStatus("Auto-creating relationships failed.");
        return;
      }
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
    const normalizedMethod = String(relationshipMethod || "manual").trim().toLowerCase() || "manual";

    setError("");
    setStatus(relationshipId ? "Updating relationship..." : "Creating relationship...");
    const resolvedSide = inferRelationshipSide(leftDatasetId, rightDatasetId);
    const payload = {
      side: resolvedSide,
      left_dataset: leftDatasetId,
      left_field: leftFields[0],
      left_fields: leftFields,
      right_dataset: rightDatasetId,
      right_field: rightFields[0],
      right_fields: rightFields,
      confidence: normalizedMethod === "manual" ? 1 : Number(relationshipConfidence),
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
      clearRelationshipForm({ keepDatasets: true });
      setStatus("Relationship saved.");
    } catch (err) {
      setError(err.message);
      setStatus("Saving relationship failed.");
    }
  }

  function closeRelationshipDeleteModal() {
    setRelationshipDeleteModal((prev) => {
      if (prev.busy) return prev;
      return { open: false, relationshipId: "", leftLabel: "", rightLabel: "", busy: false };
    });
  }

  function removeRelationship(row) {
    setRelationshipDeleteModal({
      open: true,
      relationshipId: String(row?.id || ""),
      leftLabel: row ? `${row.left_dataset}.${relationshipFieldLabel(row, "left")}` : "",
      rightLabel: row ? `${row.right_dataset}.${relationshipFieldLabel(row, "right")}` : "",
      busy: false,
    });
  }

  async function confirmDeleteRelationship() {
    const relationshipId = String(relationshipDeleteModal.relationshipId || "").trim();
    if (!relationshipId || relationshipDeleteModal.busy) return;
    setRelationshipDeleteModal((prev) => ({ ...prev, busy: true }));
    setError("");
    setStatus("Deleting relationship...");
    try {
      await api(`/api/relationships/${encodeURIComponent(relationshipId)}`, { method: "DELETE" });
      await refreshBootstrap();
      setRelationshipDeleteModal({ open: false, relationshipId: "", leftLabel: "", rightLabel: "", busy: false });
      setStatus(`Deleted relationship ${relationshipId}.`);
    } catch (err) {
      setError(err.message);
      setStatus("Delete relationship failed.");
      setRelationshipDeleteModal((prev) => ({ ...prev, busy: false }));
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
            configurations_folder: configurationsFolder,
            translations_folder: translationsFolder,
            rules_folder: rulesFolder,
            report_folder: rpt,
            expose_source_to_tools: !!exposeSourceToTools,
            expose_target_to_tools: !!exposeTargetToTools,
            expose_configurations_to_tools: !!exposeConfigurationsToTools,
            expose_translations_to_tools: !!exposeTranslationsToTools,
            expose_rules_to_tools: !!exposeRulesToTools,
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

  function closeForceStopModal() {
    setForceStopModal((prev) => {
      if (prev.busy) return prev;
      return { open: false, serviceName: "", serviceLabel: "", busy: false };
    });
  }

  function onForceStopService(serviceName) {
    const labels = {
      mcp_server: "MCP server",
      mcp_inspector: "MCP inspector",
    };
    const label = labels[serviceName] || serviceName;
    setForceStopModal({ open: true, serviceName, serviceLabel: label, busy: false });
  }

  async function confirmForceStopService() {
    const serviceName = String(forceStopModal.serviceName || "").trim();
    const label = String(forceStopModal.serviceLabel || serviceName).trim();
    if (!serviceName || forceStopModal.busy) return;
    setForceStopModal((prev) => ({ ...prev, busy: true }));
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
      setForceStopModal({ open: false, serviceName: "", serviceLabel: "", busy: false });
    } catch (err) {
      setError(err.message);
      setStatus(`Force stop ${label} failed.`);
      setForceStopModal((prev) => ({ ...prev, busy: false }));
    } finally {
      setServiceBusy((prev) => ({ ...prev, [serviceName]: false }));
    }
  }

  const sourceOptions = datasets.filter((d) => d.side === "source");
  const targetOptions = datasets.filter((d) => d.side === "target");
  const relationshipLeftDatasets = datasets.filter(
    (d) => relationshipLeftFolderFilter === "any" || d.side === relationshipLeftFolderFilter
  );
  const relationshipRightDatasets = datasets.filter(
    (d) => relationshipRightFolderFilter === "any" || d.side === relationshipRightFolderFilter
  );
  const leftDatasetObj = relationshipLeftDatasets.find((d) => d.id === leftDatasetId);
  const rightDatasetObj = relationshipRightDatasets.find((d) => d.id === rightDatasetId);
  const leftFieldOptions = leftDatasetObj?.columns || [];
  const rightFieldOptions = rightDatasetObj?.columns || [];
  const relationshipDatasetSideById = new Map(datasets.map((d) => [d.id, d.side]));
  const filteredRelationships = relationships.filter((r) => {
    if (relationshipLeftFolderFilter !== "any") {
      const leftSide = relationshipDatasetSideById.get(r.left_dataset) || "";
      if (leftSide !== relationshipLeftFolderFilter) return false;
    }
    if (relationshipRightFolderFilter !== "any") {
      const rightSide = relationshipDatasetSideById.get(r.right_dataset) || "";
      if (rightSide !== relationshipRightFolderFilter) return false;
    }
    return true;
  });
  const relationshipPairKeyRows = pairKeyMappingsForRelationshipPair(leftDatasetId, rightDatasetId);
  const relationshipMethodNormalized = String(relationshipMethod || "").trim().toLowerCase() || "manual";
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
      return `${m.source_field || ""} ${m.target_field || ""} ${m.origin_mode || ""}`.toLowerCase().includes(mappingQuery);
    });
  const sourceFolderTrimmed = String(sourceFolder || "").trim();
  const targetFolderTrimmed = String(targetFolder || "").trim();
  const configurationsFolderTrimmed = String(configurationsFolder || "").trim();
  const translationsFolderTrimmed = String(translationsFolder || "").trim();
  const rulesFolderTrimmed = String(rulesFolder || "").trim();
  const reportFolderTrimmed = String(reportFolder || "").trim();
  const activeFolderConfig =
    folderConfigs.find((cfg) => cfg.id === activeFolderConfigId) ||
    folderConfigs.find((cfg) => cfg.id === selectedFolderConfigId) ||
    null;
  const foldersConfigured =
    sourceFolderTrimmed.length > 0 &&
    targetFolderTrimmed.length > 0 &&
    reportFolderTrimmed.length > 0;
  const foldersDirty =
    sourceFolderTrimmed !== String(savedFoldersSnapshot.source_folder || "").trim() ||
    targetFolderTrimmed !== String(savedFoldersSnapshot.target_folder || "").trim() ||
    configurationsFolderTrimmed !== String(savedFoldersSnapshot.configurations_folder || "").trim() ||
    translationsFolderTrimmed !== String(savedFoldersSnapshot.translations_folder || "").trim() ||
    rulesFolderTrimmed !== String(savedFoldersSnapshot.rules_folder || "").trim() ||
    reportFolderTrimmed !== String(savedFoldersSnapshot.report_folder || "").trim() ||
    !!exposeSourceToTools !== !!savedFoldersSnapshot.expose_source_to_tools ||
    !!exposeTargetToTools !== !!savedFoldersSnapshot.expose_target_to_tools ||
    !!exposeConfigurationsToTools !== !!savedFoldersSnapshot.expose_configurations_to_tools ||
    !!exposeTranslationsToTools !== !!savedFoldersSnapshot.expose_translations_to_tools ||
    !!exposeRulesToTools !== !!savedFoldersSnapshot.expose_rules_to_tools;
  const lastCatalogRefreshLabel = formatDateTimeForBadge(lastCatalogRefreshAt);
  const canValidateAnthropicKey = String(settingsApiKeyInput || "").trim().length > 0 || settingsApiKeySet;
  const canLookupAnthropicModels = canValidateAnthropicKey || settingsApiKeySet;
  const settingsBusy = settingsSaving || settingsValidating || settingsLoadingModels || settingsGeneratingMcpApiKey;
  const mcpApiAuthEnabled = settingsMcpAuthMode === "api";
  const canCopyMcpApiKey = String(settingsMcpGeneratedApiKey || "").trim().length > 0;
  const claudeTabEnabled = settingsApiKeyActivated;
  const claudeCanSend =
    claudeTabEnabled &&
    String(settingsModel || "").trim().length > 0 &&
    String(claudeInput || "").trim().length > 0 &&
    !claudeSending;
  const sqlCanExecute = String(sqlText || "").trim().length > 0 && !sqlBusy;
  const sqlGridHeaders = Array.isArray(sqlResult?.headers) ? sqlResult.headers : [];
  const sqlGridRows = Array.isArray(sqlResult?.rows) ? sqlResult.rows : [];
  const sqlExportState = String(sqlExportJob?.state || "");
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
      description: "Publishes MCP port externally using embedded SDK (requires saved auth token).",
    },
  ];
  const topServiceIndicators = managedServices.map((svc) => ({
    key: svc.key,
    label: svc.label,
    running: !!serviceState.services?.[svc.key]?.running,
  }));
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
      <div className="header app-header">
        <h1 className="app-title">ProtoQuery</h1>
        <div className="header-right">
          <div className="service-status-list" role="status" aria-live="polite" aria-label="Service status" title={status}>
            {topServiceIndicators.map((svc) => (
              <span key={svc.key} className="service-status-item">
                <span
                  className={`service-status-dot ${svc.running ? "running" : "stopped"}`}
                  aria-hidden="true"
                />
                <span>{svc.label}</span>
              </span>
            ))}
            {error ? <span className="service-status-error">Error: {error}</span> : null}
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
            <button className={`tab ${tab === "sql" ? "active" : ""}`} onClick={() => setTab("sql")}>
              SQL
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
            <button className={`tab ${tab === "logs" ? "active" : ""}`} onClick={() => setTab("logs")}>
              Tool Logs
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
        <main className={tab === "sql" ? "content content-sql" : "content"}>
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
                Service toggles are available only in desktop mode (`PROTOQUERY_DESKTOP_MODE=1`).
              </div>
            ) : null}
            {desktopMode && !foldersConfigured ? (
              <div style={{ marginTop: 6, color: "#5b6470" }}>
                To start MCP server/inspector, set source, target, and report folders in Catalog.
              </div>
            ) : null}
          </div>

          <div className="card settings-compact-card">
            <div className="settings-top-grid">
              <div className="settings-left-stack">
                <div className="settings-panel">
                  <h3>Theme</h3>
                  <label>UI Theme</label>
                  <select value={settingsTheme} onChange={(e) => setSettingsTheme(e.target.value)}>
                    <option value="light">Light</option>
                    <option value="dark">Dark</option>
                  </select>
                  <div className="sub">Choose app theme for the admin UI.</div>
                  <label className="catalog-check-row" style={{ marginTop: 8 }}>
                    <input
                      type="checkbox"
                      className="check-input"
                      checked={!!settingsToolLoggingEnabled}
                      onChange={(e) => setSettingsToolLoggingEnabled(e.target.checked)}
                    />
                    <span>Enable tool call logging (Claude + external MCP + Inspector)</span>
                  </label>
                  <div className="sub">Turn off to stop creating new entries in the Tool Logs tab.</div>
                </div>
                <div className="settings-panel">
                  <h3>ngrok</h3>
                  <div className="sub">Configure your ngrok authtoken to start tunnels.</div>
                  <label>Auth Token</label>
                  <input
                    type="password"
                    value={settingsNgrokTokenInput}
                    onChange={(e) => setSettingsNgrokTokenInput(e.target.value)}
                    placeholder="Paste token from dashboard.ngrok.com"
                    autoComplete="off"
                  />
                  <div className="anthropic-meta settings-inline-meta">
                    <span className="anthropic-meta-pill">
                      Stored token:{" "}
                      {settingsNgrokTokenSet
                        ? settingsNgrokTokenMasked
                          ? settingsNgrokTokenMasked
                          : "configured"
                        : "not set"}
                    </span>
                    <span className={`anthropic-meta-pill ${desktopMode ? "is-active" : "is-inactive"}`}>
                      Desktop mode: {desktopMode ? "on" : "off"}
                    </span>
                  </div>
                  {settingsNgrokTokenNeedsReset ? (
                    <div className="anthropic-warning">
                      Stored ngrok authtoken cannot be decrypted. Enter a new token and save.
                    </div>
                  ) : null}
                </div>
              </div>
              <div className="settings-panel settings-mcp-panel">
                <h3>MCP Authentication</h3>
                <div className="sub">Protect `/mcp` with no auth or API key authentication.</div>
                <label>Mode</label>
                <select value={settingsMcpAuthMode} onChange={(e) => setSettingsMcpAuthMode(e.target.value)}>
                  <option value="none">No authentication</option>
                  <option value="api">API key</option>
                </select>
                <div className="anthropic-meta settings-inline-meta">
                  <span className={`anthropic-meta-pill ${mcpApiAuthEnabled ? "is-active" : "is-inactive"}`}>
                    Current mode: {mcpApiAuthEnabled ? "API key" : "none"}
                  </span>
                </div>
                {mcpApiAuthEnabled ? (
                  <div className="mcp-auth-controls">
                    <label>API Key Header</label>
                    <input value={settingsMcpApiKeyHeaderName} readOnly />
                    <label>Generated API Key</label>
                    <div className="mcp-auth-row">
                      <input
                        type="text"
                        value={settingsMcpGeneratedApiKey}
                        readOnly
                        placeholder="Generate a key, then copy it into Copilot Studio."
                      />
                      <button
                        className="secondary"
                        type="button"
                        onClick={onCopyGeneratedMcpApiKey}
                        disabled={!canCopyMcpApiKey}
                      >
                        Copy Key
                      </button>
                    </div>
                    <div className="mcp-auth-row">
                      <button
                        className="secondary"
                        type="button"
                        onClick={onGenerateMcpApiKey}
                        disabled={settingsGeneratingMcpApiKey || settingsSaving}
                      >
                        {settingsGeneratingMcpApiKey ? "Generating..." : "Generate / Rotate Key"}
                      </button>
                      <button className="secondary" type="button" onClick={onSaveAppSettings} disabled={settingsBusy}>
                        Save Auth Mode
                      </button>
                    </div>
                    <div className="anthropic-meta settings-inline-meta">
                      <span className="anthropic-meta-pill">
                        Stored key:{" "}
                        {settingsMcpApiKeySet
                          ? settingsMcpApiKeyMasked
                            ? settingsMcpApiKeyMasked
                            : "configured"
                          : "not set"}
                      </span>
                    </div>
                    {settingsMcpApiKeyNeedsReset ? (
                      <div className="anthropic-warning">
                        Stored MCP API key cannot be decrypted. Generate a new key and save.
                      </div>
                    ) : null}
                    <div className="sub">After changing this mode or key, restart MCP Server to apply it.</div>
                  </div>
                ) : (
                  <div className="sub">Endpoint accepts requests without authentication.</div>
                )}
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
              </div>

              <div className="anthropic-model-block">
                <label>Model</label>
                <div className="anthropic-model-row">
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
                  <button
                    className="secondary anthropic-action-btn anthropic-model-lookup-btn"
                    onClick={onLookupAnthropicModels}
                    disabled={!canLookupAnthropicModels || settingsLoadingModels || settingsSaving}
                  >
                    {settingsLoadingModels ? "Loading..." : "Lookup Models"}
                  </button>
                </div>
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
                        Load Instructions
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

      {tab === "logs" ? (
        <>
          <div className="card">
            <h3>Tool Call Logs</h3>
            <div className="sub">
              Shows request and response payloads for MCP tool calls executed through the Claude tab.
            </div>

            <div className="tool-logs-filter-grid">
              <div>
                <label>Status</label>
                <select value={toolLogStatusFilter} onChange={(e) => setToolLogStatusFilter(e.target.value)}>
                  <option value="all">All</option>
                  <option value="ok">Success</option>
                  <option value="error">Error</option>
                </select>
              </div>
              <div>
                <label>Tool</label>
                <select value={toolLogNameFilter} onChange={(e) => setToolLogNameFilter(e.target.value)}>
                  <option value="">All tools</option>
                  {toolLogNames.map((name) => (
                    <option key={name} value={name}>
                      {name}
                    </option>
                  ))}
                </select>
              </div>
              <div>
                <label>Contains Text</label>
                <input
                  value={toolLogTextFilter}
                  onChange={(e) => setToolLogTextFilter(e.target.value)}
                  placeholder="Find payload/error text..."
                />
              </div>
              <div>
                <label>Since (days)</label>
                <input
                  type="number"
                  min="0"
                  max="3650"
                  value={toolLogSinceDays}
                  onChange={(e) => setToolLogSinceDays(e.target.value)}
                />
              </div>
            </div>

            <div className="actions">
              <button className="secondary" onClick={() => loadToolLogs()} disabled={toolLogLoading || toolLogCleaning}>
                {toolLogLoading ? "Loading..." : "Apply / Refresh"}
              </button>
              <button className="secondary" onClick={onResetToolLogFilters} disabled={toolLogLoading || toolLogCleaning}>
                Reset Filters
              </button>
            </div>

            <div className="tool-logs-cleanup">
              <div className="tool-logs-cleanup-days">
                <label>Cleanup: older than X days</label>
                <input
                  type="number"
                  min="1"
                  max="3650"
                  value={toolLogCleanupDays}
                  onChange={(e) => setToolLogCleanupDays(e.target.value)}
                />
              </div>
              <div className="actions">
                <button
                  className="secondary"
                  onClick={onClearToolLogsOlderThanDays}
                  disabled={toolLogCleaning || toolLogLoading}
                >
                  {toolLogCleaning ? "Cleaning..." : "Delete Older Than X Day(s)"}
                </button>
                <button className="danger" onClick={onClearAllToolLogs} disabled={toolLogCleaning || toolLogLoading}>
                  Delete Everything
                </button>
              </div>
            </div>

            <div className="sub">
              Showing {toolLogs.length} of {toolLogTotal} entr{toolLogTotal === 1 ? "y" : "ies"}.
            </div>

            <div className="scroll tool-logs-table-wrap">
              <table className="tool-logs-table">
                <thead>
                  <tr>
                    <th>Status</th>
                    <th>Tool</th>
                    <th>Called</th>
                    <th>Responded</th>
                    <th>Duration (ms)</th>
                    <th>Request</th>
                    <th>Response</th>
                    <th>Error</th>
                  </tr>
                </thead>
                <tbody>
                  {toolLogs.length ? (
                    toolLogs.map((item) => (
                      <tr key={`tool-log-${item.id}`}>
                        <td>
                          <span className={`tool-log-status-pill ${item.status === "error" ? "error" : "ok"}`}>
                            {item.status === "error" ? "Error" : "OK"}
                          </span>
                        </td>
                        <td>{displayValue(item.tool_name)}</td>
                        <td>{formatDateTimeForBadge(item.called_at)}</td>
                        <td>{formatDateTimeForBadge(item.responded_at)}</td>
                        <td>{displayValue(item.duration_ms)}</td>
                        <td>
                          <pre className="tool-log-json">{formatPrettyJson(item.request_payload)}</pre>
                        </td>
                        <td>
                          <pre className="tool-log-json">{formatPrettyJson(item.response_payload)}</pre>
                        </td>
                        <td>
                          <pre className="tool-log-json">{formatPrettyJson(item.error_message)}</pre>
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={8}>No tool log entries found for the current filters.</td>
                    </tr>
                  )}
                </tbody>
              </table>
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
          <div className={`card catalog-setup-card ${catalogSetupCollapsed ? "collapsed" : ""}`}>
            <div
              className="catalog-setup-bar"
              role="button"
              tabIndex={0}
              aria-expanded={!catalogSetupCollapsed}
              onClick={() => setCatalogSetupCollapsed((prev) => !prev)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  setCatalogSetupCollapsed((prev) => !prev);
                }
              }}
            >
              <div className="catalog-setup-title">
                <span
                  className={`catalog-setup-caret ${catalogSetupCollapsed ? "collapsed" : "expanded"}`}
                  aria-hidden="true"
                >
                  {">"}
                </span>
                <h3>Catalog Setup</h3>
                <span className={`catalog-state-badge ${foldersDirty ? "dirty" : "saved"}`}>
                  {foldersDirty ? "Unsaved changes" : "Saved"}
                </span>
              </div>
              {catalogSetupCollapsed ? (
                <div
                  className="catalog-setup-inline-controls"
                  onClick={(e) => e.stopPropagation()}
                  onKeyDown={(e) => e.stopPropagation()}
                >
                  <label className="catalog-check-row compact">
                    <input
                      type="checkbox"
                      className="check-input"
                      checked={includeRowCounts}
                      onChange={(e) => setIncludeRowCounts(e.target.checked)}
                    />
                    <span>Include row counts (slow)</span>
                  </label>
                  <button
                    className="refresh-catalog-btn catalog-refresh-inline"
                    onClick={onRefreshCatalog}
                    disabled={!foldersConfigured || catalogRefreshing || catalogReloading}
                  >
                    {catalogRefreshing ? "Refreshing..." : "Refresh Catalog"}
                  </button>
                </div>
              ) : null}
            </div>

            {!catalogSetupCollapsed ? (
              <div className="catalog-setup-grid">
                <div className="settings-panel catalog-locations-panel">
                  <div className="catalog-panel-head">
                    <h3>Data Locations</h3>
                    <div className="catalog-panel-head-actions">
                      <button
                        type="button"
                        className="secondary catalog-reload-btn"
                        onClick={onReloadMetadata}
                        disabled={catalogReloading || catalogRefreshing}
                        title={catalogReloading ? "Reloading metadata..." : "Reload metadata"}
                        aria-label={catalogReloading ? "Reloading metadata" : "Reload metadata"}
                      >
                        {catalogReloading ? "..." : "\u21BB"}
                      </button>
                    </div>
                  </div>
                  <div className="catalog-config-manager">
                    <label>Folder configurations</label>
                    <div className="catalog-config-row">
                      <select
                        value={selectedFolderConfigId}
                        onChange={async (e) => {
                          const nextId = e.target.value;
                          setSelectedFolderConfigId(nextId);
                          if (nextId === NEW_FOLDER_CONFIG_OPTION_ID) {
                            setFolderConfigNameInput("");
                            return;
                          }
                          const selected = folderConfigs.find((cfg) => cfg.id === nextId);
                          setFolderConfigNameInput(selected ? selected.name : "");
                          if (nextId && nextId !== activeFolderConfigId) {
                            await onSelectFolderConfig(nextId);
                          }
                        }}
                        disabled={folderConfigBusy}
                      >
                        <option value={NEW_FOLDER_CONFIG_OPTION_ID}>New configuration</option>
                        {folderConfigs.map((cfg) => (
                          <option key={cfg.id} value={cfg.id}>
                            {cfg.name}
                            {cfg.id === activeFolderConfigId ? " (active)" : ""}
                          </option>
                        ))}
                      </select>
                      <button
                        type="button"
                        className="secondary catalog-icon-btn"
                        onClick={onRenameFolderConfig}
                        disabled={folderConfigBusy || !selectedFolderConfigId || selectedFolderConfigId === NEW_FOLDER_CONFIG_OPTION_ID}
                        title="Rename selected configuration"
                        aria-label="Rename selected configuration"
                      >
                        ✎
                      </button>
                      <button
                        type="button"
                        className="danger catalog-icon-btn"
                        onClick={onDeleteFolderConfig}
                        disabled={folderConfigBusy || !selectedFolderConfigId || selectedFolderConfigId === NEW_FOLDER_CONFIG_OPTION_ID}
                        title="Delete selected configuration"
                        aria-label="Delete selected configuration"
                      >
                        🗑
                      </button>
                      <button
                        type="button"
                        className="secondary catalog-icon-btn"
                        onClick={onSaveFolderConfig}
                        disabled={folderConfigBusy}
                        title={
                          selectedFolderConfigId === NEW_FOLDER_CONFIG_OPTION_ID
                            ? "Save current folders as a new configuration"
                            : "Save current folders to selected configuration"
                        }
                        aria-label={
                          selectedFolderConfigId === NEW_FOLDER_CONFIG_OPTION_ID
                            ? "Save current folders as a new configuration"
                            : "Save current folders to selected configuration"
                        }
                      >
                        💾
                      </button>
                    </div>
                    <div className="catalog-config-meta">
                      Active: {activeFolderConfig ? activeFolderConfig.name : "None"} | Saved: {folderConfigs.length}
                    </div>
                  </div>
                  <div className="catalog-fields-grid">
                    <div>
                      <label>Source folder</label>
                      <div className="field-with-action">
                        <input
                          className="catalog-path-input"
                          title={sourceFolder}
                          value={sourceFolder}
                          onChange={(e) => setSourceFolder(e.target.value)}
                          placeholder="C:\data\source"
                        />
                        <button
                          type="button"
                          className="secondary browse-btn browse-icon-btn"
                          onClick={() => onBrowseFolder("source")}
                          title="Browse source folder"
                          aria-label="Browse source folder"
                        >
                          ...
                        </button>
                      </div>
                    </div>
                    <div>
                      <label>Target folder</label>
                      <div className="field-with-action">
                        <input
                          className="catalog-path-input"
                          title={targetFolder}
                          value={targetFolder}
                          onChange={(e) => setTargetFolder(e.target.value)}
                          placeholder="C:\data\target"
                        />
                        <button
                          type="button"
                          className="secondary browse-btn browse-icon-btn"
                          onClick={() => onBrowseFolder("target")}
                          title="Browse target folder"
                          aria-label="Browse target folder"
                        >
                          ...
                        </button>
                      </div>
                    </div>
                    <div>
                      <label>Configurations folder</label>
                      <div className="field-with-action">
                        <input
                          className="catalog-path-input"
                          title={configurationsFolder}
                          value={configurationsFolder}
                          onChange={(e) => setConfigurationsFolder(e.target.value)}
                          placeholder="C:\\data\\configurations"
                        />
                        <button
                          type="button"
                          className="secondary browse-btn browse-icon-btn"
                          onClick={() => onBrowseFolder("configurations")}
                          title="Browse configurations folder"
                          aria-label="Browse configurations folder"
                        >
                          ...
                        </button>
                      </div>
                    </div>
                    <div>
                      <label>Translations folder</label>
                      <div className="field-with-action">
                        <input
                          className="catalog-path-input"
                          title={translationsFolder}
                          value={translationsFolder}
                          onChange={(e) => setTranslationsFolder(e.target.value)}
                          placeholder="C:\\data\\translations"
                        />
                        <button
                          type="button"
                          className="secondary browse-btn browse-icon-btn"
                          onClick={() => onBrowseFolder("translations")}
                          title="Browse translations folder"
                          aria-label="Browse translations folder"
                        >
                          ...
                        </button>
                      </div>
                    </div>
                    <div>
                      <label>Rules folder</label>
                      <div className="field-with-action">
                        <input
                          className="catalog-path-input"
                          title={rulesFolder}
                          value={rulesFolder}
                          onChange={(e) => setRulesFolder(e.target.value)}
                          placeholder="C:\\data\\rules"
                        />
                        <button
                          type="button"
                          className="secondary browse-btn browse-icon-btn"
                          onClick={() => onBrowseFolder("rules")}
                          title="Browse rules folder"
                          aria-label="Browse rules folder"
                        >
                          ...
                        </button>
                      </div>
                    </div>
                    <div>
                      <label>Report folder</label>
                      <div className="field-with-action">
                        <input
                          className="catalog-path-input"
                          title={reportFolder}
                          value={reportFolder}
                          onChange={(e) => setReportFolder(e.target.value)}
                          placeholder="C:\data\reports"
                        />
                        <button
                          type="button"
                          className="secondary browse-btn browse-icon-btn"
                          onClick={() => onBrowseFolder("report")}
                          title="Browse report folder"
                          aria-label="Browse report folder"
                        >
                          ...
                        </button>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="settings-panel catalog-run-panel">
                  <h3>Catalog Run</h3>
                  <div className="catalog-run-compact">
                    <label className="catalog-check-row compact">
                      <input
                        type="checkbox"
                        className="check-input"
                        checked={includeRowCounts}
                        onChange={(e) => setIncludeRowCounts(e.target.checked)}
                      />
                      <span>Include row counts (slow)</span>
                    </label>
                    <button
                      className="refresh-catalog-btn"
                      onClick={onRefreshCatalog}
                      disabled={!foldersConfigured || catalogRefreshing || catalogReloading}
                    >
                      {catalogRefreshing ? "Refreshing..." : "Refresh Catalog"}
                    </button>
                  </div>
                  <div className="catalog-expose-grid">
                    <h4 style={{ margin: 0 }}>Expose to MCP tools</h4>
                    <label className="catalog-check-row compact">
                      <input
                        type="checkbox"
                        className="check-input"
                        checked={exposeSourceToTools}
                        onChange={(e) => setExposeSourceToTools(e.target.checked)}
                      />
                      <span>Source</span>
                    </label>
                    <label className="catalog-check-row compact">
                      <input
                        type="checkbox"
                        className="check-input"
                        checked={exposeTargetToTools}
                        onChange={(e) => setExposeTargetToTools(e.target.checked)}
                      />
                      <span>Target</span>
                    </label>
                    <label className="catalog-check-row compact">
                      <input
                        type="checkbox"
                        className="check-input"
                        checked={exposeConfigurationsToTools}
                        onChange={(e) => setExposeConfigurationsToTools(e.target.checked)}
                      />
                      <span>Configurations</span>
                    </label>
                    <label className="catalog-check-row compact">
                      <input
                        type="checkbox"
                        className="check-input"
                        checked={exposeTranslationsToTools}
                        onChange={(e) => setExposeTranslationsToTools(e.target.checked)}
                      />
                      <span>Translations</span>
                    </label>
                    <label className="catalog-check-row compact">
                      <input
                        type="checkbox"
                        className="check-input"
                        checked={exposeRulesToTools}
                        onChange={(e) => setExposeRulesToTools(e.target.checked)}
                      />
                      <span>Rules</span>
                    </label>
                  </div>
                  <div className="catalog-run-meta">
                    <span className="anthropic-meta-pill">Last refresh: {lastCatalogRefreshLabel}</span>
                    <span className={`anthropic-meta-pill ${foldersConfigured ? "is-active" : "is-inactive"}`}>
                      Paths: {foldersConfigured ? "ready" : "missing"}
                    </span>
                    <span className={`anthropic-meta-pill ${includeRowCounts ? "is-active" : "is-inactive"}`}>
                      Row counts: {includeRowCounts ? "on" : "off"}
                    </span>
                    <span className={`anthropic-meta-pill ${exposeConfigurationsToTools ? "is-active" : "is-inactive"}`}>
                      Config tools: {exposeConfigurationsToTools ? "on" : "off"}
                    </span>
                    <span className={`anthropic-meta-pill ${exposeTranslationsToTools ? "is-active" : "is-inactive"}`}>
                      Translation tools: {exposeTranslationsToTools ? "on" : "off"}
                    </span>
                    <span className={`anthropic-meta-pill ${exposeRulesToTools ? "is-active" : "is-inactive"}`}>
                      Rules tools: {exposeRulesToTools ? "on" : "off"}
                    </span>
                  </div>
                </div>
              </div>
            ) : null}
          </div>

          <div className="card">
            <h3>Datasets ({datasets.length})</h3>
            <div className="scroll">
              <table>
                <thead>
                  <tr>
                    <th>Status</th>
                    <th>ID</th>
                    <th>Side</th>
                    <th>File</th>
                    <th>Sheet</th>
                    <th>Columns</th>
                    <th>Rows</th>
                  </tr>
                </thead>
                <tbody>
                  {datasets.map((d) => {
                    const statusInfo = getDatasetStatus(d);
                    const clickable = statusInfo.level === "warning" || statusInfo.level === "error";
                    return (
                      <tr key={d.id}>
                        <td className="dataset-status-cell">
                          {clickable ? (
                            <button
                              type="button"
                              className={`dataset-status-icon ${statusInfo.level} is-clickable`}
                              title={`${statusInfo.label}: ${statusInfo.message}`}
                              aria-label={`${statusInfo.label}: ${statusInfo.message}`}
                              onClick={() =>
                                setCatalogDatasetIssue((prev) =>
                                  prev?.datasetId === d.id
                                    ? null
                                    : {
                                        datasetId: d.id,
                                        fileName: d.file_name || "",
                                        sheetName: d.sheet_name || "",
                                        level: statusInfo.level,
                                        label: statusInfo.label,
                                        message: statusInfo.message,
                                      }
                                )
                              }
                            >
                              {statusInfo.icon}
                            </button>
                          ) : (
                            <span
                              className={`dataset-status-icon ${statusInfo.level}`}
                              title={`${statusInfo.label}: ${statusInfo.message}`}
                              aria-label={`${statusInfo.label}: ${statusInfo.message}`}
                            >
                              {statusInfo.icon}
                            </span>
                          )}
                        </td>
                        <td>{d.id}</td>
                        <td>{d.side}</td>
                        <td>{d.file_name}</td>
                        <td>{d.sheet_name || "-"}</td>
                        <td>{(d.columns || []).length}</td>
                        <td>{d.row_count == null ? "-" : d.row_count}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {catalogDatasetIssue ? (
              <div className={`catalog-dataset-issue-box ${catalogDatasetIssue.level}`}>
                <div className="catalog-dataset-issue-head">
                  <strong>
                    {catalogDatasetIssue.label}: {catalogDatasetIssue.datasetId}
                  </strong>
                  <button
                    type="button"
                    className="secondary catalog-dataset-issue-close"
                    onClick={() => setCatalogDatasetIssue(null)}
                  >
                    Close
                  </button>
                </div>
                <div className="catalog-dataset-issue-meta">
                  File: {catalogDatasetIssue.fileName || "-"}
                  {catalogDatasetIssue.sheetName ? ` | Sheet: ${catalogDatasetIssue.sheetName}` : ""}
                </div>
                <div>{catalogDatasetIssue.message}</div>
              </div>
            ) : null}
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
                    <th>Key Mappings</th>
                    <th>Auto</th>
                    <th>Enabled</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {pairs.map((p) => {
                    const keyCount = Array.isArray(p.key_mappings) ? p.key_mappings.length : 0;
                    return (
                      <tr key={p.id}>
                        <td>{p.id}</td>
                        <td>{p.source_dataset}</td>
                        <td>{p.target_dataset}</td>
                        <td>{keyCount}</td>
                        <td>{String(p.auto_matched)}</td>
                        <td>{String(p.enabled)}</td>
                        <td>
                          <div style={{ display: "flex", gap: 6, flexWrap: "nowrap", whiteSpace: "nowrap" }}>
                            <button
                              type="button"
                              className="secondary"
                              onClick={() => onDeletePairKeyMappings(p)}
                              title={
                                keyCount > 0
                                  ? `Clear ${keyCount} saved key mapping(s) for this pair.`
                                  : "This pair has no saved key mappings."
                              }
                            >
                              Clear Mappings
                            </button>
                            <button
                              type="button"
                              className="danger"
                              onClick={() => onDeletePair(p)}
                              title="Delete this source-target pair."
                            >
                              Delete Pair
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
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

      {tab === "sql" ? (
        <div className="sql-shell">
          <div className="card sql-workspace-card">
            <h3 className="sql-card-title">SQL</h3>
            <div className="sql-workspace-layout">
              <div className="sql-query-column">
                <label>Query</label>
                <textarea
                  className="sql-query-input"
                  rows={10}
                  value={sqlText}
                  onChange={(e) => setSqlText(e.target.value)}
                  placeholder="SELECT * FROM source_your_table LIMIT 100"
                />
              </div>
              <div className="sql-controls-column">
                <div>
                  <label>Output</label>
                  <select value={sqlOutputMode} onChange={(e) => setSqlOutputMode(e.target.value)}>
                    <option value="grid">Print to grid (default)</option>
                    <option value="export">Export to Excel</option>
                  </select>
                </div>
                <div>
                  <label>Preview limit</label>
                  <input
                    type="number"
                    min="1"
                    max="100"
                    value={sqlLimit}
                    onChange={(e) => setSqlLimit(e.target.value)}
                    disabled={sqlOutputMode !== "grid"}
                  />
                </div>
                <div>
                  <label>Count total rows</label>
                  <select
                    value={sqlIncludeTotal ? "1" : "0"}
                    onChange={(e) => setSqlIncludeTotal(e.target.value === "1")}
                    disabled={sqlOutputMode !== "grid"}
                  >
                    <option value="0">No</option>
                    <option value="1">Yes</option>
                  </select>
                </div>
                <div>
                  <label>Export filename (optional)</label>
                  <input
                    value={sqlExportFilename}
                    onChange={(e) => setSqlExportFilename(e.target.value)}
                    placeholder="sales_check.xlsx"
                    disabled={sqlOutputMode !== "export"}
                  />
                </div>
                <button onClick={onRunSqlAction} disabled={!sqlCanExecute}>
                  {sqlOutputMode === "export" ? "Start Export" : "Run Query"}
                </button>
              </div>
            </div>
          </div>

          <div className="card sql-result-card">
            <h3 className="sql-card-title">Result</h3>
            {sqlOutputMode === "grid" ? (
              <>
                <div className="sub sql-result-meta">
                  Rows returned: {displayValue(sqlResult?.row_count || 0)} | Total rows: {displayValue(sqlResult?.total_rows || 0)} |{" "}
                  Total computed: {sqlResult?.total_computed ? "Yes" : "No"}
                </div>
                <div className="sql-result-grid-wrap">
                  <DataGrid
                    className="sql-result-grid"
                    headers={sqlGridHeaders}
                    rows={sqlGridRows}
                    emptyMessage="Run a SQL preview to see results."
                  />
                </div>
              </>
            ) : (
              <>
                {sqlExportJob?.job_id ? (
                  <>
                    <div className="sub sql-result-meta">
                      Job ID: {displayValue(sqlExportJob.job_id)} | State: {displayValue(sqlExportState)}
                    </div>
                    <div className="actions">
                      <button className="secondary" onClick={refreshSqlExportJob} disabled={sqlBusy}>
                        Refresh Export Status
                      </button>
                      <button
                        className="secondary"
                        onClick={onOpenSqlExportReport}
                        disabled={!sqlExportJob?.report?.id}
                      >
                        Open Export File
                      </button>
                    </div>
                    {sqlExportJob?.report ? (
                      <div className="sub" style={{ marginTop: 8 }}>
                        Report: {displayValue(sqlExportJob.report.file_name)} ({displayValue(sqlExportJob.report.id)})
                      </div>
                    ) : null}
                  </>
                ) : (
                  <div style={{ color: "#5b6470" }}>Start an export job to generate an XLSX report.</div>
                )}
              </>
            )}
          </div>
        </div>
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
                        <th>Origin mode</th>
                        <th>Confidence</th>
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
                              <select
                                value={normalizeOriginMode(m.origin_mode)}
                                onChange={(e) => updateMappingRow(idx, { origin_mode: e.target.value })}
                              >
                                <option value="manual">manual</option>
                                <option value="name">name</option>
                                <option value="content">content</option>
                              </select>
                            </td>
                            <td>{normalizeOriginMode(m.origin_mode) === "content" && m.confidence !== null ? Number(m.confidence).toFixed(3) : "-"}</td>
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
                          <td colSpan={7}>
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
                  <button type="button" className="secondary" onClick={applyNameMappings}>
                    Suggest By Name
                  </button>
                  <button type="button" className="secondary" onClick={applyContentAwareMappings} disabled={compareSuggestBusy}>
                    {compareSuggestBusy ? (
                      <span className="button-progress">
                        <span className="button-progress-spinner" />
                        Suggesting...
                      </span>
                    ) : (
                      "Suggest By Content"
                    )}
                  </button>
                  <button type="button" className="secondary" onClick={addMappingRow}>
                    Add Mapping Row
                  </button>
                  <button type="button" className="secondary" onClick={savePairMappings}>
                    Save Pair Mappings
                  </button>
                </div>
                {compareSuggestMessage ? <div className="sub suggestion-feedback">{compareSuggestMessage}</div> : null}
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
              <table className="jobs-table">
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
                      <td>{displayValue(j.created_at)}</td>
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
                    <div className="metric-label">Source {'->'} Target</div>
                    <div className="metric-value">
                      {displayValue(jobSummary.source)}{" -> "}{displayValue(jobSummary.target)}
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
                      <td>{displayValue(r.created_at)}</td>
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
            <div className="card-header-row">
              <h3 style={{ margin: 0 }}>{relationshipId ? `Edit Relationship #${relationshipId}` : "Create Relationship"}</h3>
              <button className="secondary header-action-btn" onClick={refreshBootstrap}>
                Refresh
              </button>
            </div>
            <div className="row">
              <div className="col-6">
                <label>Left folder prefilter</label>
                <select
                  value={relationshipLeftFolderFilter}
                  onChange={(e) => {
                    setRelationshipLeftFolderFilter(e.target.value);
                    setError("");
                  }}
                >
                  <option value="any">any</option>
                  <option value="source">source</option>
                  <option value="target">target</option>
                  <option value="configurations">configurations</option>
                  <option value="translations">translations</option>
                  <option value="rules">rules</option>
                </select>
              </div>
              <div className="col-6">
                <label>Right folder prefilter</label>
                <select
                  value={relationshipRightFolderFilter}
                  onChange={(e) => {
                    setRelationshipRightFolderFilter(e.target.value);
                    setError("");
                  }}
                >
                  <option value="any">any</option>
                  <option value="source">source</option>
                  <option value="target">target</option>
                  <option value="configurations">configurations</option>
                  <option value="translations">translations</option>
                  <option value="rules">rules</option>
                </select>
              </div>
              <div className="col-6">
                <label>Left dataset</label>
                <select
                  value={leftDatasetId}
                  onChange={(e) => {
                    setLeftDatasetId(e.target.value);
                    setRelationshipMappings([{ left_field: "", right_field: "" }]);
                    setRelationshipSuggestMessage("");
                    setError("");
                  }}
                >
                  <option value="">Select...</option>
                  {relationshipLeftDatasets.map((d) => (
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
                    setRelationshipSuggestMessage("");
                    setError("");
                  }}
                >
                  <option value="">Select...</option>
                  {relationshipRightDatasets.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.id}
                    </option>
                  ))}
                </select>
              </div>
              <div className="col-12">
                <label>Field mappings</label>
                <div className="actions actions-right" style={{ marginBottom: 8 }}>
                  <button type="button" className="secondary" onClick={addRelationshipMappingRow}>
                    Add Mapping Row
                  </button>
                  <button
                    type="button"
                    className="secondary"
                    onClick={() => suggestRelationshipMappings("name")}
                    disabled={relationshipSuggestBusy}
                  >
                    Suggest By Name
                  </button>
                  <button
                    type="button"
                    className="secondary"
                    onClick={() => suggestRelationshipMappings("content")}
                    disabled={relationshipSuggestBusy}
                  >
                    {relationshipSuggestBusy ? (
                      <span className="button-progress">
                        <span className="button-progress-spinner" />
                        Suggesting...
                      </span>
                    ) : (
                      "Suggest By Content"
                    )}
                  </button>
                  {relationshipSuggestBusy ? (
                    <button type="button" className="danger" onClick={stopRelationshipSuggestion}>
                      {relationshipSuggestStopRequested ? "Stopping..." : "Stop"}
                    </button>
                  ) : null}
                </div>
                {relationshipPairKeyRows.length ? (
                  <div className="sub" style={{ marginBottom: 8 }}>
                    Compare keys were detected for this source/target pair and will auto-fill when mappings are empty.
                  </div>
                ) : null}
                {relationshipSuggestMessage ? <div className="sub suggestion-feedback">{relationshipSuggestMessage}</div> : null}
                <div className="scroll">
                  <table className="relationship-mappings-table">
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
                {relationshipMethodNormalized === "manual" ? (
                  <input value="-" readOnly disabled />
                ) : (
                  <input
                    type="number"
                    min="0"
                    max="1"
                    step="0.001"
                    value={relationshipConfidence}
                    onChange={(e) => setRelationshipConfidence(e.target.value)}
                  />
                )}
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
              <table className="relationships-table">
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
                      <td>{String(r.method || "").trim().toLowerCase() === "manual" ? "-" : displayValue(r.confidence)}</td>
                      <td>{r.method}</td>
                      <td>{r.active ? "Yes" : "No"}</td>
                      <td>{displayValue(r.updated_at)}</td>
                      <td>
                        <div className="actions relationship-actions">
                          <button className="secondary" onClick={() => editRelationship(r)}>
                            Edit
                          </button>
                          <button className="danger" onClick={() => removeRelationship(r)}>
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
      {folderConfigModal.open ? (
        <div className="modal-backdrop" onClick={closeFolderConfigModal}>
          <div className="modal-card folder-config-modal" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            {folderConfigModal.mode === "delete" ? (
              <>
                <h4 style={{ margin: "0 0 6px" }}>Delete Folder Configuration</h4>
                <div className="sub" style={{ margin: "0 0 8px" }}>
                  Delete this saved configuration?
                </div>
                <div className="folder-config-delete-name">{folderConfigModal.configName || "-"}</div>
                <div className="actions actions-right modal-actions">
                  <button type="button" className="secondary" onClick={closeFolderConfigModal} disabled={folderConfigBusy}>
                    Cancel
                  </button>
                  <button type="button" className="danger" onClick={onConfirmDeleteFolderConfig} disabled={folderConfigBusy}>
                    {folderConfigBusy ? "Deleting..." : "Delete"}
                  </button>
                </div>
              </>
            ) : (
              <>
                <h4 style={{ margin: "0 0 6px" }}>
                  {folderConfigModal.mode === "rename" ? "Rename Folder Configuration" : "Save Folder Configuration"}
                </h4>
                <div className="sub" style={{ margin: "0 0 8px" }}>
                  {folderConfigModal.mode === "rename"
                    ? "Choose a new name for this configuration."
                    : "Name this folder setup so you can switch to it later."}
                </div>
                <label>Configuration name</label>
                <input
                  value={folderConfigModal.name}
                  onChange={(e) => setFolderConfigModal((prev) => ({ ...prev, name: e.target.value }))}
                  maxLength={80}
                  placeholder="e.g. Mock UAT"
                  autoFocus
                />
                <div className="actions actions-right modal-actions">
                  <button type="button" className="secondary" onClick={closeFolderConfigModal} disabled={folderConfigBusy}>
                    Cancel
                  </button>
                  <button
                    type="button"
                    onClick={folderConfigModal.mode === "rename" ? onConfirmRenameFolderConfig : onConfirmSaveFolderConfig}
                    disabled={folderConfigBusy || !String(folderConfigModal.name || "").trim()}
                  >
                    {folderConfigBusy ? (folderConfigModal.mode === "rename" ? "Renaming..." : "Saving...") : folderConfigModal.mode === "rename" ? "Rename" : "Save"}
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      ) : null}
      {reportDeleteModal.open ? (
        <div className="modal-backdrop" onClick={closeReportDeleteModal}>
          <div className="modal-card" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <h4 style={{ margin: "0 0 6px" }}>Delete Report</h4>
            <div className="sub" style={{ margin: "0 0 8px" }}>
              Delete this report permanently?
            </div>
            <div className="folder-config-delete-name">
              {reportDeleteModal.reportId}
              {reportDeleteModal.reportFile ? ` | ${reportDeleteModal.reportFile}` : ""}
            </div>
            <div className="actions actions-right modal-actions">
              <button type="button" className="secondary" onClick={closeReportDeleteModal} disabled={reportDeleteModal.busy}>
                Cancel
              </button>
              <button type="button" className="danger" onClick={confirmDeleteReport} disabled={reportDeleteModal.busy}>
                {reportDeleteModal.busy ? "Deleting..." : "Delete"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {relationshipDeleteModal.open ? (
        <div className="modal-backdrop" onClick={closeRelationshipDeleteModal}>
          <div className="modal-card" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <h4 style={{ margin: "0 0 6px" }}>Delete Relationship</h4>
            <div className="sub" style={{ margin: "0 0 8px" }}>
              Remove this relationship mapping?
            </div>
            <div className="folder-config-delete-name">
              ID: {relationshipDeleteModal.relationshipId}
            </div>
            <div className="sub" style={{ margin: "8px 0 0" }}>
              Left: {relationshipDeleteModal.leftLabel || "-"}
            </div>
            <div className="sub" style={{ margin: "6px 0 0" }}>
              Right: {relationshipDeleteModal.rightLabel || "-"}
            </div>
            <div className="actions actions-right modal-actions">
              <button
                type="button"
                className="secondary"
                onClick={closeRelationshipDeleteModal}
                disabled={relationshipDeleteModal.busy}
              >
                Cancel
              </button>
              <button
                type="button"
                className="danger"
                onClick={confirmDeleteRelationship}
                disabled={relationshipDeleteModal.busy}
              >
                {relationshipDeleteModal.busy ? "Deleting..." : "Delete"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {forceStopModal.open ? (
        <div className="modal-backdrop" onClick={closeForceStopModal}>
          <div className="modal-card" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <h4 style={{ margin: "0 0 6px" }}>Force Stop Service</h4>
            <div className="sub" style={{ margin: "0 0 8px" }}>
              Force stop {forceStopModal.serviceLabel}? This may kill external processes listening on the service ports.
            </div>
            <div className="actions actions-right modal-actions">
              <button type="button" className="secondary" onClick={closeForceStopModal} disabled={forceStopModal.busy}>
                Cancel
              </button>
              <button type="button" className="danger" onClick={confirmForceStopService} disabled={forceStopModal.busy}>
                {forceStopModal.busy ? "Force stopping..." : "Force Stop"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {quickMapChoiceOpen ? (
        <div className="modal-backdrop" onClick={() => applyQuickMappingsChoice("cancel")}>
          <div className="modal-card" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <h4 style={{ margin: "0 0 6px" }}>Existing mappings detected</h4>
            <div className="sub" style={{ margin: 0 }}>
              Mapping suggestion found {quickMapPendingMappings.length} {quickMapPendingLabel} field pairing(s). Choose how to apply these mappings.
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
      {pairKeyDeleteModal.open ? (
        <div className="modal-backdrop" onClick={closePairKeyDeleteModal}>
          <div className="modal-card" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <h4 style={{ margin: "0 0 6px" }}>Clear Key Mappings</h4>
            <div className="sub" style={{ margin: "0 0 8px" }}>
              Remove saved key mappings for this pair.
            </div>
            <div className="folder-config-delete-name">{pairKeyDeleteModal.pairId}</div>
            <div className="sub" style={{ margin: "8px 0 0" }}>
              Source: {pairKeyDeleteModal.sourceDataset || "-"} | Target: {pairKeyDeleteModal.targetDataset || "-"}
            </div>
            <div className="sub" style={{ margin: "6px 0 0" }}>
              Key mappings to remove: {pairKeyDeleteModal.keyCount}. Compare mappings will stay unchanged.
            </div>
            <div className="actions actions-right modal-actions">
              <button
                type="button"
                className="secondary"
                onClick={closePairKeyDeleteModal}
                disabled={pairKeyDeleteModal.busy}
              >
                Cancel
              </button>
              <button type="button" className="danger" onClick={confirmDeletePairKeyMappings} disabled={pairKeyDeleteModal.busy}>
                {pairKeyDeleteModal.busy ? "Clearing..." : "Clear Mappings"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {pairDeleteModal.open ? (
        <div className="modal-backdrop" onClick={closePairDeleteModal}>
          <div className="modal-card" role="dialog" aria-modal="true" onClick={(e) => e.stopPropagation()}>
            <h4 style={{ margin: "0 0 6px" }}>Delete Pair</h4>
            <div className="sub" style={{ margin: "0 0 8px" }}>
              This will remove the pair from the catalog.
            </div>
            <div className="folder-config-delete-name">{pairDeleteModal.pairId}</div>
            <div className="sub" style={{ margin: "8px 0 0" }}>
              Source: {pairDeleteModal.sourceDataset || "-"} | Target: {pairDeleteModal.targetDataset || "-"}
            </div>
            <div className="sub" style={{ margin: "6px 0 0" }}>
              Saved key/compare mappings and key presets for this pair will be removed.
            </div>
            <div className="actions actions-right modal-actions">
              <button type="button" className="secondary" onClick={closePairDeleteModal} disabled={pairDeleteModal.busy}>
                Cancel
              </button>
              <button type="button" className="danger" onClick={confirmDeletePair} disabled={pairDeleteModal.busy}>
                {pairDeleteModal.busy ? "Deleting..." : "Delete Pair"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
