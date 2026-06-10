const platformEl = document.getElementById('platform');
const connectionInfoEl = document.getElementById('connectionInfo');
const executeBtnEl = document.getElementById('executeBtn');
const metaEl = document.getElementById('meta');
const errorEl = document.getElementById('error');
const tableWrapEl = document.getElementById('tableWrap');
const schemaTreeEl = document.getElementById('schemaTree');
const historyListEl = document.getElementById('historyList');
const tabSchemaEl = document.getElementById('tabSchema');
const tabHistoryEl = document.getElementById('tabHistory');

let appSettings = { readOnlyMode: null };
let editor = null;
const schemaCache = {};

const defaultConnections = {
  sqlserver: {
    host: 'msjoi.database.windows.net',
    database: 'pampero',
    user: 'unpazuser'
  },
  mysql: {
    host: 'myjoi.mysql.database.azure.com',
    database: 'pampero',
    user: 'unpazuser'
  },
  postgresql: {
    host: 'pgjoi.postgres.database.azure.com',
    database: 'pampero',
    user: 'unpazuser'
  }
};

const HISTORY_KEY = 'websql_query_history';
const HISTORY_MAX = 25;

// Dialect-specific keyword/function lists
const KEYWORDS = {
  common: [
    { label: 'SELECT', detail: 'SQL' }, { label: 'FROM', detail: 'SQL' },
    { label: 'WHERE', detail: 'SQL' }, { label: 'AND', detail: 'SQL' },
    { label: 'OR', detail: 'SQL' }, { label: 'NOT', detail: 'SQL' },
    { label: 'NULL', detail: 'SQL' }, { label: 'IS NULL', detail: 'SQL' },
    { label: 'IS NOT NULL', detail: 'SQL' }, { label: 'AS', detail: 'SQL' },
    { label: 'DISTINCT', detail: 'SQL' }, { label: 'IN', detail: 'SQL' },
    { label: 'NOT IN', detail: 'SQL' }, { label: 'LIKE', detail: 'SQL' },
    { label: 'BETWEEN', detail: 'SQL' }, { label: 'EXISTS', detail: 'SQL' },
    { label: 'JOIN', detail: 'SQL' }, { label: 'INNER JOIN', detail: 'SQL' },
    { label: 'LEFT JOIN', detail: 'SQL' }, { label: 'RIGHT JOIN', detail: 'SQL' },
    { label: 'FULL OUTER JOIN', detail: 'SQL' }, { label: 'CROSS JOIN', detail: 'SQL' },
    { label: 'ON', detail: 'SQL' }, { label: 'GROUP BY', detail: 'SQL' },
    { label: 'ORDER BY', detail: 'SQL' }, { label: 'HAVING', detail: 'SQL' },
    { label: 'UNION', detail: 'SQL' }, { label: 'UNION ALL', detail: 'SQL' },
    { label: 'INTERSECT', detail: 'SQL' }, { label: 'EXCEPT', detail: 'SQL' },
    { label: 'ASC', detail: 'SQL' }, { label: 'DESC', detail: 'SQL' },
    { label: 'CASE', detail: 'SQL', snippet: 'CASE\n\tWHEN ${1:condicion} THEN ${2:valor}\n\tELSE ${3:valor}\nEND' },
    { label: 'WHEN', detail: 'SQL' }, { label: 'THEN', detail: 'SQL' },
    { label: 'ELSE', detail: 'SQL' }, { label: 'END', detail: 'SQL' },
    { label: 'INSERT INTO', detail: 'SQL' }, { label: 'UPDATE', detail: 'SQL' },
    { label: 'DELETE FROM', detail: 'SQL' }, { label: 'CREATE TABLE', detail: 'SQL' },
    { label: 'ALTER TABLE', detail: 'SQL' }, { label: 'DROP TABLE', detail: 'SQL' },
    { label: 'TRUNCATE TABLE', detail: 'SQL' }, { label: 'BEGIN', detail: 'SQL' },
    { label: 'COMMIT', detail: 'SQL' }, { label: 'ROLLBACK', detail: 'SQL' },
    // Aggregate functions
    { label: 'COUNT', detail: 'función', snippet: 'COUNT(${1:*})' },
    { label: 'SUM', detail: 'función', snippet: 'SUM(${1:columna})' },
    { label: 'AVG', detail: 'función', snippet: 'AVG(${1:columna})' },
    { label: 'MIN', detail: 'función', snippet: 'MIN(${1:columna})' },
    { label: 'MAX', detail: 'función', snippet: 'MAX(${1:columna})' },
    { label: 'COALESCE', detail: 'función', snippet: 'COALESCE(${1:expr}, ${2:alternativa})' },
    { label: 'NULLIF', detail: 'función', snippet: 'NULLIF(${1:expr1}, ${2:expr2})' },
    { label: 'CAST', detail: 'función', snippet: 'CAST(${1:expr} AS ${2:tipo})' },
    // Window functions
    { label: 'ROW_NUMBER', detail: 'ventana', snippet: 'ROW_NUMBER() OVER (${1:ORDER BY columna})' },
    { label: 'RANK', detail: 'ventana', snippet: 'RANK() OVER (${1:ORDER BY columna})' },
    { label: 'DENSE_RANK', detail: 'ventana', snippet: 'DENSE_RANK() OVER (${1:ORDER BY columna})' },
    { label: 'OVER', detail: 'SQL' }, { label: 'PARTITION BY', detail: 'SQL' },
    { label: 'LAG', detail: 'ventana', snippet: 'LAG(${1:columna}, ${2:1}) OVER (${3:ORDER BY columna})' },
    { label: 'LEAD', detail: 'ventana', snippet: 'LEAD(${1:columna}, ${2:1}) OVER (${3:ORDER BY columna})' },
  ],
  sqlserver: [
    { label: 'TOP', detail: 'SQL Server', snippet: 'TOP (${1:n})' },
    { label: 'NOLOCK', detail: 'SQL Server hint' },
    { label: 'WITH (NOLOCK)', detail: 'SQL Server hint' },
    { label: 'OUTPUT', detail: 'SQL Server' },
    { label: 'OPTION (RECOMPILE)', detail: 'SQL Server hint' },
    { label: 'MERGE', detail: 'SQL Server' },
    // Date functions
    { label: 'GETDATE', detail: 'SQL Server · fecha actual', snippet: 'GETDATE()' },
    { label: 'GETUTCDATE', detail: 'SQL Server · fecha UTC', snippet: 'GETUTCDATE()' },
    { label: 'SYSDATETIME', detail: 'SQL Server · fecha de alta precisión', snippet: 'SYSDATETIME()' },
    { label: 'DATEADD', detail: 'SQL Server', snippet: 'DATEADD(${1:day}, ${2:n}, ${3:fecha})' },
    { label: 'DATEDIFF', detail: 'SQL Server', snippet: 'DATEDIFF(${1:day}, ${2:inicio}, ${3:fin})' },
    { label: 'DATENAME', detail: 'SQL Server', snippet: 'DATENAME(${1:part}, ${2:fecha})' },
    { label: 'DATEPART', detail: 'SQL Server', snippet: 'DATEPART(${1:part}, ${2:fecha})' },
    { label: 'FORMAT', detail: 'SQL Server', snippet: "FORMAT(${1:valor}, '${2:formato}')" },
    // String/conversion
    { label: 'ISNULL', detail: 'SQL Server', snippet: 'ISNULL(${1:expr}, ${2:alternativa})' },
    { label: 'CONVERT', detail: 'SQL Server', snippet: 'CONVERT(${1:tipo}, ${2:expr})' },
    { label: 'TRY_CAST', detail: 'SQL Server', snippet: 'TRY_CAST(${1:expr} AS ${2:tipo})' },
    { label: 'TRY_CONVERT', detail: 'SQL Server', snippet: 'TRY_CONVERT(${1:tipo}, ${2:expr})' },
    { label: 'IIF', detail: 'SQL Server', snippet: 'IIF(${1:condicion}, ${2:verdadero}, ${3:falso})' },
    { label: 'CHOOSE', detail: 'SQL Server', snippet: 'CHOOSE(${1:indice}, ${2:val1}, ${3:val2})' },
    { label: 'STRING_AGG', detail: 'SQL Server 2017+', snippet: "STRING_AGG(${1:columna}, '${2:,}')" },
    { label: 'TRIM', detail: 'función', snippet: 'TRIM(${1:expr})' },
    { label: 'LEN', detail: 'SQL Server', snippet: 'LEN(${1:expr})' },
    { label: 'SUBSTRING', detail: 'SQL Server', snippet: 'SUBSTRING(${1:expr}, ${2:inicio}, ${3:largo})' },
    { label: 'REPLACE', detail: 'función', snippet: 'REPLACE(${1:expr}, ${2:buscar}, ${3:reemplazar})' },
    { label: 'UPPER', detail: 'función', snippet: 'UPPER(${1:expr})' },
    { label: 'LOWER', detail: 'función', snippet: 'LOWER(${1:expr})' },
    // System variables
    { label: '@@VERSION', detail: 'SQL Server · versión del motor' },
    { label: '@@ROWCOUNT', detail: 'SQL Server · filas afectadas' },
    { label: '@@ERROR', detail: 'SQL Server · último error' },
    { label: '@@IDENTITY', detail: 'SQL Server · último identity' },
    { label: '@@SPID', detail: 'SQL Server · session ID' },
    // System objects
    { label: 'sys.tables', detail: 'SQL Server · tablas' },
    { label: 'sys.columns', detail: 'SQL Server · columnas' },
    { label: 'sys.objects', detail: 'SQL Server · objetos' },
    { label: 'sys.indexes', detail: 'SQL Server · índices' },
    { label: 'sys.schemas', detail: 'SQL Server · esquemas' },
    { label: 'INFORMATION_SCHEMA.TABLES', detail: 'SQL Server / estándar' },
    { label: 'INFORMATION_SCHEMA.COLUMNS', detail: 'SQL Server / estándar' },
    // Types
    { label: 'NVARCHAR', detail: 'tipo SQL Server', snippet: 'NVARCHAR(${1:255})' },
    { label: 'VARCHAR', detail: 'tipo', snippet: 'VARCHAR(${1:255})' },
    { label: 'DATETIME2', detail: 'tipo SQL Server' },
    { label: 'DATETIMEOFFSET', detail: 'tipo SQL Server' },
    { label: 'UNIQUEIDENTIFIER', detail: 'tipo SQL Server (GUID)' },
    { label: 'MONEY', detail: 'tipo SQL Server' },
    { label: 'BIT', detail: 'tipo SQL Server' },
    { label: 'INT', detail: 'tipo' }, { label: 'BIGINT', detail: 'tipo' },
    { label: 'SMALLINT', detail: 'tipo' }, { label: 'TINYINT', detail: 'tipo' },
    { label: 'DECIMAL', detail: 'tipo', snippet: 'DECIMAL(${1:18},${2:2})' },
    { label: 'FLOAT', detail: 'tipo' }, { label: 'REAL', detail: 'tipo' },
  ],
  mysql: [
    { label: 'LIMIT', detail: 'MySQL', snippet: 'LIMIT ${1:10}' },
    { label: 'OFFSET', detail: 'MySQL', snippet: 'OFFSET ${1:0}' },
    { label: 'ON DUPLICATE KEY UPDATE', detail: 'MySQL' },
    { label: 'REPLACE INTO', detail: 'MySQL' },
    { label: 'SHOW TABLES', detail: 'MySQL' },
    { label: 'SHOW DATABASES', detail: 'MySQL' },
    { label: 'SHOW COLUMNS FROM', detail: 'MySQL', snippet: 'SHOW COLUMNS FROM ${1:tabla}' },
    { label: 'DESCRIBE', detail: 'MySQL', snippet: 'DESCRIBE ${1:tabla}' },
    // Date functions
    { label: 'NOW', detail: 'MySQL · fecha y hora actual', snippet: 'NOW()' },
    { label: 'CURDATE', detail: 'MySQL · fecha actual', snippet: 'CURDATE()' },
    { label: 'CURTIME', detail: 'MySQL · hora actual', snippet: 'CURTIME()' },
    { label: 'DATE_FORMAT', detail: 'MySQL', snippet: "DATE_FORMAT(${1:fecha}, '${2:%Y-%m-%d}')" },
    { label: 'DATE_ADD', detail: 'MySQL', snippet: 'DATE_ADD(${1:fecha}, INTERVAL ${2:1} ${3:DAY})' },
    { label: 'DATE_SUB', detail: 'MySQL', snippet: 'DATE_SUB(${1:fecha}, INTERVAL ${2:1} ${3:DAY})' },
    { label: 'DATEDIFF', detail: 'MySQL', snippet: 'DATEDIFF(${1:fecha1}, ${2:fecha2})' },
    { label: 'UNIX_TIMESTAMP', detail: 'MySQL', snippet: 'UNIX_TIMESTAMP(${1:fecha})' },
    { label: 'FROM_UNIXTIME', detail: 'MySQL', snippet: 'FROM_UNIXTIME(${1:timestamp})' },
    { label: 'STR_TO_DATE', detail: 'MySQL', snippet: "STR_TO_DATE('${1:texto}', '${2:%Y-%m-%d}')" },
    // String/conditional
    { label: 'IFNULL', detail: 'MySQL', snippet: 'IFNULL(${1:expr}, ${2:alternativa})' },
    { label: 'IF', detail: 'MySQL', snippet: 'IF(${1:condicion}, ${2:verdadero}, ${3:falso})' },
    { label: 'CONCAT', detail: 'MySQL', snippet: 'CONCAT(${1:expr1}, ${2:expr2})' },
    { label: 'GROUP_CONCAT', detail: 'MySQL', snippet: "GROUP_CONCAT(${1:columna} SEPARATOR '${2:,}')" },
    { label: 'LENGTH', detail: 'MySQL', snippet: 'LENGTH(${1:expr})' },
    { label: 'CHAR_LENGTH', detail: 'MySQL', snippet: 'CHAR_LENGTH(${1:expr})' },
    { label: 'SUBSTRING', detail: 'MySQL', snippet: 'SUBSTRING(${1:expr}, ${2:inicio}, ${3:largo})' },
    { label: 'REPLACE', detail: 'función', snippet: 'REPLACE(${1:expr}, ${2:buscar}, ${3:reemplazar})' },
    { label: 'UPPER', detail: 'función', snippet: 'UPPER(${1:expr})' },
    { label: 'LOWER', detail: 'función', snippet: 'LOWER(${1:expr})' },
    { label: 'TRIM', detail: 'función', snippet: 'TRIM(${1:expr})' },
    { label: 'VERSION', detail: 'MySQL · versión del motor', snippet: 'VERSION()' },
    // System
    { label: 'information_schema.tables', detail: 'MySQL · catálogo de tablas' },
    { label: 'information_schema.columns', detail: 'MySQL · catálogo de columnas' },
    // Types
    { label: 'INT', detail: 'tipo' }, { label: 'BIGINT', detail: 'tipo' },
    { label: 'TINYINT', detail: 'tipo' }, { label: 'SMALLINT', detail: 'tipo' },
    { label: 'MEDIUMINT', detail: 'tipo' },
    { label: 'VARCHAR', detail: 'tipo', snippet: 'VARCHAR(${1:255})' },
    { label: 'TEXT', detail: 'tipo' }, { label: 'MEDIUMTEXT', detail: 'tipo' },
    { label: 'LONGTEXT', detail: 'tipo' }, { label: 'TINYTEXT', detail: 'tipo' },
    { label: 'DATETIME', detail: 'tipo' }, { label: 'TIMESTAMP', detail: 'tipo' },
    { label: 'DATE', detail: 'tipo' }, { label: 'TIME', detail: 'tipo' },
    { label: 'FLOAT', detail: 'tipo' }, { label: 'DOUBLE', detail: 'tipo' },
    { label: 'DECIMAL', detail: 'tipo', snippet: 'DECIMAL(${1:10},${2:2})' },
    { label: 'ENUM', detail: 'tipo MySQL', snippet: "ENUM('${1:val1}', '${2:val2}')" },
    { label: 'AUTO_INCREMENT', detail: 'MySQL · autoincremento' },
  ],
  postgresql: [
    { label: 'LIMIT', detail: 'PostgreSQL', snippet: 'LIMIT ${1:10}' },
    { label: 'OFFSET', detail: 'PostgreSQL', snippet: 'OFFSET ${1:0}' },
    { label: 'RETURNING', detail: 'PostgreSQL' },
    { label: 'ON CONFLICT', detail: 'PostgreSQL (upsert)', snippet: 'ON CONFLICT (${1:columna}) DO UPDATE SET ${2:col} = EXCLUDED.${2:col}' },
    { label: 'ON CONFLICT DO NOTHING', detail: 'PostgreSQL' },
    { label: 'DISTINCT ON', detail: 'PostgreSQL', snippet: 'DISTINCT ON (${1:columna})' },
    { label: 'ILIKE', detail: 'PostgreSQL · LIKE sin distinción may/min' },
    { label: 'SIMILAR TO', detail: 'PostgreSQL · regex SQL' },
    { label: 'ANY', detail: 'PostgreSQL', snippet: 'ANY(${1:array})' },
    { label: 'ALL', detail: 'PostgreSQL', snippet: 'ALL(${1:array})' },
    { label: 'FILTER', detail: 'PostgreSQL · filtro de agregado', snippet: 'FILTER (WHERE ${1:condicion})' },
    // Date functions
    { label: 'NOW', detail: 'PostgreSQL · fecha y hora actual', snippet: 'NOW()' },
    { label: 'CURRENT_DATE', detail: 'PostgreSQL · fecha actual' },
    { label: 'CURRENT_TIME', detail: 'PostgreSQL · hora actual' },
    { label: 'CURRENT_TIMESTAMP', detail: 'PostgreSQL · fecha y hora actual' },
    { label: 'DATE_TRUNC', detail: 'PostgreSQL', snippet: "DATE_TRUNC('${1:day}', ${2:fecha})" },
    { label: 'DATE_PART', detail: 'PostgreSQL', snippet: "DATE_PART('${1:year}', ${2:fecha})" },
    { label: 'EXTRACT', detail: 'PostgreSQL', snippet: 'EXTRACT(${1:year} FROM ${2:fecha})' },
    { label: 'AGE', detail: 'PostgreSQL', snippet: 'AGE(${1:timestamp}, ${2:timestamp})' },
    { label: 'TO_DATE', detail: 'PostgreSQL', snippet: "TO_DATE('${1:texto}', '${2:YYYY-MM-DD}')" },
    { label: 'TO_TIMESTAMP', detail: 'PostgreSQL', snippet: "TO_TIMESTAMP('${1:texto}', '${2:formato}')" },
    // Aggregate / array
    { label: 'ARRAY_AGG', detail: 'PostgreSQL', snippet: 'ARRAY_AGG(${1:columna})' },
    { label: 'STRING_AGG', detail: 'PostgreSQL', snippet: "STRING_AGG(${1:columna}, '${2:,}')" },
    { label: 'JSON_AGG', detail: 'PostgreSQL', snippet: 'JSON_AGG(${1:expr})' },
    { label: 'JSONB_AGG', detail: 'PostgreSQL', snippet: 'JSONB_AGG(${1:expr})' },
    { label: 'JSON_BUILD_OBJECT', detail: 'PostgreSQL', snippet: "JSON_BUILD_OBJECT('${1:clave}', ${2:valor})" },
    { label: 'GENERATE_SERIES', detail: 'PostgreSQL', snippet: 'GENERATE_SERIES(${1:inicio}, ${2:fin}, ${3:paso})' },
    { label: 'UNNEST', detail: 'PostgreSQL', snippet: 'UNNEST(${1:array})' },
    // String
    { label: 'CONCAT', detail: 'función', snippet: 'CONCAT(${1:expr1}, ${2:expr2})' },
    { label: 'LENGTH', detail: 'función', snippet: 'LENGTH(${1:expr})' },
    { label: 'SUBSTRING', detail: 'función', snippet: 'SUBSTRING(${1:expr} FROM ${2:inicio} FOR ${3:largo})' },
    { label: 'REPLACE', detail: 'función', snippet: 'REPLACE(${1:expr}, ${2:buscar}, ${3:reemplazar})' },
    { label: 'UPPER', detail: 'función', snippet: 'UPPER(${1:expr})' },
    { label: 'LOWER', detail: 'función', snippet: 'LOWER(${1:expr})' },
    { label: 'TRIM', detail: 'función', snippet: 'TRIM(${1:expr})' },
    { label: 'REGEXP_REPLACE', detail: 'PostgreSQL', snippet: "REGEXP_REPLACE(${1:expr}, '${2:patron}', '${3:reemplazo}')" },
    // System
    { label: 'pg_catalog.version', detail: 'PostgreSQL · versión', snippet: 'pg_catalog.version()' },
    { label: 'information_schema.tables', detail: 'PostgreSQL · catálogo de tablas' },
    { label: 'information_schema.columns', detail: 'PostgreSQL · catálogo de columnas' },
    { label: 'pg_catalog.pg_tables', detail: 'PostgreSQL · tablas del catálogo' },
    { label: 'pg_stat_user_tables', detail: 'PostgreSQL · estadísticas de tablas' },
    // Types
    { label: 'INTEGER', detail: 'tipo' }, { label: 'BIGINT', detail: 'tipo' },
    { label: 'SMALLINT', detail: 'tipo' },
    { label: 'SERIAL', detail: 'tipo PostgreSQL · autoincremento' },
    { label: 'BIGSERIAL', detail: 'tipo PostgreSQL · autoincremento grande' },
    { label: 'TEXT', detail: 'tipo PostgreSQL' },
    { label: 'VARCHAR', detail: 'tipo', snippet: 'VARCHAR(${1:255})' },
    { label: 'BOOLEAN', detail: 'tipo PostgreSQL' },
    { label: 'BYTEA', detail: 'tipo PostgreSQL · binario' },
    { label: 'JSONB', detail: 'tipo PostgreSQL · JSON binario' },
    { label: 'JSON', detail: 'tipo PostgreSQL' },
    { label: 'UUID', detail: 'tipo PostgreSQL' },
    { label: 'INET', detail: 'tipo PostgreSQL · dirección IP' },
    { label: 'INTERVAL', detail: 'tipo PostgreSQL' },
    { label: 'NUMERIC', detail: 'tipo', snippet: 'NUMERIC(${1:10},${2:2})' },
    { label: 'REAL', detail: 'tipo' }, { label: 'FLOAT', detail: 'tipo' },
    { label: 'DOUBLE PRECISION', detail: 'tipo PostgreSQL' },
    { label: 'TIMESTAMP', detail: 'tipo' },
    { label: 'TIMESTAMP WITH TIME ZONE', detail: 'tipo PostgreSQL' },
    { label: 'DATE', detail: 'tipo' }, { label: 'TIME', detail: 'tipo' },
  ]
};

function getKeywordsForPlatform(platform) {
  return [...KEYWORDS.common, ...(KEYWORDS[platform] || [])];
}

function splitSqlStatements(sql) {
  return String(sql)
    .split(';')
    .map((s) => s.trim())
    .filter(Boolean);
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderConnectionInfo() {
  const platform = platformEl.value;
  const info = defaultConnections[platform];
  connectionInfoEl.textContent = `Servidor: ${info.host} | Base: ${info.database} | Usuario: ${info.user}`;
  renderSchemaExplorer();
  prefetchSchema(platform).then(() => {
    if (platformEl.value === platform) renderSchemaExplorer();
  });
}

// ---------- Explorador de esquema ----------

function insertIntoEditor(text) {
  if (!editor) return;
  editor.trigger('keyboard', 'type', { text });
  editor.focus();
}

function renderSchemaExplorer() {
  const platform = platformEl.value;
  const schemaData = schemaCache[platform];

  if (!schemaData) {
    schemaTreeEl.innerHTML = '<p class="side-empty">Cargando esquema...</p>';
    return;
  }

  // Las vistas quedan fuera del árbol pero siguen en el autocompletado
  const baseTables = schemaData.tables.filter((t) => t.type !== 'view');

  if (!baseTables.length) {
    schemaTreeEl.innerHTML = '<p class="side-empty">La base no tiene tablas.</p>';
    return;
  }

  schemaTreeEl.innerHTML = '';
  for (const table of baseTables) {
    const tableEl = document.createElement('div');
    tableEl.className = 'tree-table';

    const headEl = document.createElement('button');
    headEl.type = 'button';
    headEl.className = 'tree-table-head';
    headEl.innerHTML = `<span class="tree-caret">▸</span> ${escapeHtml(table.name)}`;
    headEl.title = 'Clic: ver columnas · Doble clic: insertar en el editor';

    const colsEl = document.createElement('div');
    colsEl.className = 'tree-cols hidden';
    for (const col of table.columns) {
      const colEl = document.createElement('button');
      colEl.type = 'button';
      colEl.className = 'tree-col';
      colEl.innerHTML = `${escapeHtml(col.name)} <span class="tree-type">${escapeHtml(col.type)}</span>`;
      colEl.title = 'Clic: insertar en el editor';
      colEl.addEventListener('click', () => insertIntoEditor(quoteIdentifier(col.name, platformEl.value)));
      colsEl.appendChild(colEl);
    }

    headEl.addEventListener('click', () => {
      colsEl.classList.toggle('hidden');
      headEl.querySelector('.tree-caret').textContent = colsEl.classList.contains('hidden') ? '▸' : '▾';
    });
    headEl.addEventListener('dblclick', () => insertIntoEditor(quoteIdentifier(table.name, platformEl.value)));

    tableEl.appendChild(headEl);
    tableEl.appendChild(colsEl);
    schemaTreeEl.appendChild(tableEl);
  }
}

// ---------- Historial de consultas ----------

function loadHistory() {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch (_e) {
    return [];
  }
}

function saveToHistory(sql, platform, success) {
  const trimmed = String(sql || '').trim();
  if (!trimmed) return;

  let history = loadHistory();
  // Evitar duplicados consecutivos de la misma consulta y plataforma
  history = history.filter((item) => !(item.sql === trimmed && item.platform === platform));
  history.unshift({ sql: trimmed, platform, success, ts: Date.now() });
  if (history.length > HISTORY_MAX) history = history.slice(0, HISTORY_MAX);

  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
  } catch (_e) {
    // localStorage lleno o deshabilitado: el historial es opcional
  }
  renderHistory();
}

function formatHistoryTime(ts) {
  const date = new Date(ts);
  const today = new Date();
  const sameDay = date.toDateString() === today.toDateString();
  const time = date.toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' });
  return sameDay ? time : `${date.toLocaleDateString('es-AR', { day: '2-digit', month: '2-digit' })} ${time}`;
}

function renderHistory() {
  const history = loadHistory();
  if (!history.length) {
    historyListEl.innerHTML = '<p class="side-empty">Todavía no ejecutaste consultas.</p>';
    return;
  }

  historyListEl.innerHTML = '';
  for (const item of history) {
    const itemEl = document.createElement('button');
    itemEl.type = 'button';
    itemEl.className = 'history-item' + (item.success ? '' : ' history-failed');
    itemEl.innerHTML = `
      <span class="history-meta">${escapeHtml(item.platform)} · ${formatHistoryTime(item.ts)}${item.success ? '' : ' · falló'}</span>
      <span class="history-sql">${escapeHtml(item.sql.length > 120 ? item.sql.slice(0, 120) + '…' : item.sql)}</span>
    `;
    itemEl.title = item.sql;
    itemEl.addEventListener('click', () => {
      platformEl.value = item.platform;
      renderConnectionInfo();
      if (editor) {
        editor.setValue(item.sql);
        editor.focus();
      }
    });
    historyListEl.appendChild(itemEl);
  }
}

function switchSideTab(tab) {
  const isSchema = tab === 'schema';
  tabSchemaEl.classList.toggle('active', isSchema);
  tabHistoryEl.classList.toggle('active', !isSchema);
  schemaTreeEl.classList.toggle('hidden', !isSchema);
  historyListEl.classList.toggle('hidden', isSchema);
}

const schemaFetchesInFlight = {};

async function prefetchSchema(platform) {
  if (schemaCache[platform]) return schemaCache[platform];
  if (schemaFetchesInFlight[platform]) return schemaFetchesInFlight[platform];

  schemaFetchesInFlight[platform] = (async () => {
    try {
      const response = await fetch(`/api/schema?platform=${platform}`);
      if (response.ok) {
        schemaCache[platform] = await response.json();
        return schemaCache[platform];
      }
    } catch (_e) {
      // schema completions won't be available; keyword completions still work
    } finally {
      // On failure the cache stays empty, so the next call retries
      delete schemaFetchesInFlight[platform];
    }
    return null;
  })();

  return schemaFetchesInFlight[platform];
}

async function loadSettings() {
  try {
    const response = await fetch('/api/settings');
    if (!response.ok) throw new Error('No se pudo leer la configuracion del servidor');
    const data = await response.json();
    appSettings = { readOnlyMode: Boolean(data.readOnlyMode) };
  } catch (_error) {
    appSettings = { readOnlyMode: null };
  }
  renderConnectionInfo();
}

function renderRows(columns, rows) {
  if (!rows || rows.length === 0) {
    tableWrapEl.innerHTML = '<p class="table-empty">La consulta no devolvio filas.</p>';
    return;
  }
  const safeColumns = columns.map(escapeHtml);
  const header = safeColumns.map((c) => `<th>${c}</th>`).join('');
  const body = rows
    .map((row) => {
      const tds = columns
        .map((c) => (row[c] == null
          ? '<td class="null-value">NULL</td>'
          : `<td>${escapeHtml(row[c])}</td>`))
        .join('');
      return `<tr>${tds}</tr>`;
    })
    .join('');
  tableWrapEl.innerHTML = `<table><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table>`;
}

function clearState() {
  errorEl.classList.add('hidden');
  errorEl.textContent = '';
  tableWrapEl.innerHTML = '';
  metaEl.textContent = '';
}

async function executeQuery() {
  clearState();
  executeBtnEl.disabled = true;
  executeBtnEl.textContent = 'Ejecutando...';

  const queryText = editor ? editor.getValue() : '';
  const platform = platformEl.value;

  try {
    const statements = splitSqlStatements(queryText);
    if (statements.length !== 1) {
      throw new Error('Solo se permite ejecutar una consulta por vez.');
    }

    const payload = { platform, query: queryText };
    const response = await fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'No se pudo ejecutar la consulta');

    const timingParts = [`Tiempo total: ${data.durationMs} ms`];
    if (typeof data.connectMs === 'number') timingParts.push(`Conexión: ${data.connectMs} ms`);
    if (typeof data.queryMs === 'number') timingParts.push(`Consulta: ${data.queryMs} ms`);
    if (typeof data.serverExecMs === 'number') timingParts.push(`Ejecución en servidor: ${data.serverExecMs} ms`);
    if (typeof data.transportOverheadMs === 'number') timingParts.push(`Transporte/driver: ${data.transportOverheadMs} ms`);

    const rowsLabel = data.truncated
      ? `Filas: ${data.rowCount} (mostrando las primeras ${data.rows.length})`
      : `Filas: ${data.rowCount}`;
    metaEl.textContent = `Plataforma: ${data.platform} | ${rowsLabel} | ${timingParts.join(' | ')}`;
    renderRows(data.columns || [], data.rows || []);
    saveToHistory(queryText, platform, true);
  } catch (error) {
    errorEl.classList.remove('hidden');
    errorEl.textContent = error.message;
    saveToHistory(queryText, platform, false);
  } finally {
    executeBtnEl.disabled = false;
    executeBtnEl.textContent = 'Ejecutar consulta';
  }
}


// Reserved words that need quoting when used as identifiers (common subset).
const SQL_RESERVED = new Set([
  'select','from','where','join','on','group','order','by','having','distinct',
  'as','and','or','not','null','in','like','between','exists','case','when',
  'then','else','end','union','all','insert','update','delete','create','alter',
  'drop','table','index','view','into','set','values','with','over','partition',
  'rows','range','unbounded','preceding','following','current','row','top',
  'limit','offset','returning','merge','output','except','intersect','key',
  'primary','foreign','references','default','check','unique','constraint',
  'database','schema','column','columns','trigger','procedure','function',
  'begin','commit','rollback','transaction','declare','cursor','open','fetch',
  'close','deallocate','exec','execute','if','else','while','return','cast',
  'convert','coalesce','nullif','isnull','count','sum','avg','min','max',
  'user','name','date','time','year','month','day','hour','minute','second',
  'status','type','value','values','level','position','length','replace',
  'read','write','global','local','identity','password','role','grant','revoke',
]);

function needsQuoting(name) {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) return true;
  return SQL_RESERVED.has(name.toLowerCase());
}

function quoteIdentifier(name, platform) {
  if (!needsQuoting(name)) return name;
  if (platform === 'mysql') return '`' + name + '`';
  if (platform === 'postgresql') return '"' + name + '"';
  return '[' + name + ']'; // sqlserver (and fallback)
}

// Extracts tables and aliases referenced in FROM/JOIN clauses.
// Returns a Map: lowercase(alias or name) -> canonical table name in the schema.
function extractTablesInScope(sql, schemaTables) {
  const tablesByLower = new Map();
  if (schemaTables) {
    for (const t of schemaTables) tablesByLower.set(t.name.toLowerCase(), t);
  }

  const result = new Map(); // key: alias/name lowercased, value: schema table object
  const pattern = /(?:FROM|JOIN)\s+([\w.\[\]"`]+)(?:\s+(?:AS\s+)?([\w]+))?/gi;
  let match;
  while ((match = pattern.exec(sql)) !== null) {
    const rawName = match[1].replace(/[`"[\]]/g, '').split('.').pop();
    const alias = match[2] ? match[2].replace(/[`"[\]]/g, '') : null;
    const tableObj = tablesByLower.get(rawName.toLowerCase());
    if (tableObj) {
      result.set(rawName.toLowerCase(), tableObj);
      if (alias) result.set(alias.toLowerCase(), tableObj);
    }
  }
  return result;
}

function registerCompletionProvider() {
  monaco.languages.registerCompletionItemProvider('sql', {
    triggerCharacters: [' ', '.', '('],
    provideCompletionItems(model, position) {
      const platform = platformEl.value;
      const wordInfo = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: wordInfo.startColumn,
        endColumn: wordInfo.endColumn
      };

      const lineText = model.getValueInRange({
        startLineNumber: position.lineNumber, startColumn: 1,
        endLineNumber: position.lineNumber, endColumn: position.column
      });

      const fullSql = model.getValue();
      const suggestions = [];
      const KW = monaco.languages.CompletionItemKind;
      const SNIPPET_RULE = monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet;
      const schemaData = schemaCache[platform];

      // If the initial prefetch failed (e.g. cold start right after a deploy),
      // retry in the background so upcoming completions get the schema.
      if (!schemaData) {
        prefetchSchema(platform);
      }

      // Column completions after "alias." or "table."
      const dotMatch = lineText.match(/(\w+)\.\w*$/);
      if (dotMatch) {
        const prefix = dotMatch[1].toLowerCase();
        const tablesInScope = extractTablesInScope(fullSql, schemaData ? schemaData.tables : []);
        const tableObj = tablesInScope.get(prefix)
          || (schemaData && schemaData.tables.find((t) => t.name.toLowerCase() === prefix));
        if (tableObj) {
          for (const col of tableObj.columns) {
            const quoted = quoteIdentifier(col.name, platform);
            suggestions.push({
              label: col.name,
              kind: KW.Field,
              insertText: quoted,
              detail: col.type + (quoted !== col.name ? '  [ ]' : ''),
              documentation: `${tableObj.name}.${col.name}`,
              sortText: `0_${col.name}`,
              range
            });
          }
        }
        return { suggestions };
      }

      // Columns from tables currently in scope (FROM/JOIN already written)
      const tablesInScope = extractTablesInScope(fullSql, schemaData ? schemaData.tables : []);
      const seenColumns = new Set();
      for (const tableObj of tablesInScope.values()) {
        for (const col of tableObj.columns) {
          if (seenColumns.has(col.name)) continue;
          seenColumns.add(col.name);
          const quoted = quoteIdentifier(col.name, platform);
          suggestions.push({
            label: col.name,
            kind: KW.Field,
            insertText: quoted,
            detail: `${tableObj.name} · ${col.type}` + (quoted !== col.name ? '  [ ]' : ''),
            documentation: `${tableObj.name}.${col.name}`,
            sortText: `1_${col.name}`,
            range
          });
        }
      }

      // All table names
      if (schemaData) {
        for (const table of schemaData.tables) {
          const quoted = quoteIdentifier(table.name, platform);
          suggestions.push({
            label: table.name,
            kind: KW.Class,
            insertText: quoted,
            detail: (table.schema || 'tabla') + (quoted !== table.name ? '  [ ]' : ''),
            documentation: `${table.schema ? table.schema + '.' : ''}${table.name}`,
            sortText: `2_${table.name}`,
            range
          });
        }
      }

      // Keywords and functions
      for (const kw of getKeywordsForPlatform(platform)) {
        suggestions.push({
          label: kw.label,
          kind: kw.snippet ? KW.Function : KW.Keyword,
          insertText: kw.snippet || kw.label,
          insertTextRules: kw.snippet ? SNIPPET_RULE : undefined,
          detail: kw.detail || '',
          sortText: `3_${kw.label}`,
          range
        });
      }

      return { suggestions };
    }
  });
}

function initMonaco() {
  require.config({
    paths: { vs: 'https://cdn.jsdelivr.net/npm/monaco-editor@0.52.0/min/vs' }
  });

  require(['vs/editor/editor.main'], function () {
    editor = monaco.editor.create(document.getElementById('query-editor'), {
      value: 'SELECT 1 AS ok;',
      language: 'sql',
      theme: 'vs',
      minimap: { enabled: false },
      fontSize: 14,
      fontFamily: "'Cascadia Code', Consolas, 'Courier New', monospace",
      lineNumbers: 'on',
      automaticLayout: true,
      scrollBeyondLastLine: false,
      wordWrap: 'off',
      tabSize: 2,
      suggestOnTriggerCharacters: true,
      quickSuggestions: { other: true, comments: false, strings: false },
      suggestSelection: 'first',
      acceptSuggestionOnEnter: 'smart',
      padding: { top: 10, bottom: 10 },
      renderLineHighlight: 'line',
      scrollbar: { verticalScrollbarSize: 6, horizontalScrollbarSize: 6 },
      fixedOverflowWidgets: true
    });

    editor.addCommand(
      monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
      function () { if (!executeBtnEl.disabled) executeQuery(); }
    );

    registerCompletionProvider();
  });
}

platformEl.addEventListener('change', renderConnectionInfo);
executeBtnEl.addEventListener('click', executeQuery);
tabSchemaEl.addEventListener('click', () => switchSideTab('schema'));
tabHistoryEl.addEventListener('click', () => switchSideTab('history'));

renderConnectionInfo();
loadSettings();
renderHistory();
initMonaco();
