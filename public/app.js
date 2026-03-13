const platformEl = document.getElementById('platform');
const connectionInfoEl = document.getElementById('connectionInfo');
const queryEl = document.getElementById('query');
const executeBtnEl = document.getElementById('executeBtn');
const templateBtnEls = document.querySelectorAll('.template-btn');
const metaEl = document.getElementById('meta');
const errorEl = document.getElementById('error');
const tableWrapEl = document.getElementById('tableWrap');

let appSettings = {
  readOnlyMode: null
};

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

const sqlTemplatesByPlatform = {
  sqlserver: {
    current_time: 'SELECT GETDATE() AS fecha_hora;',
    tables: 'SELECT TOP 20 TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME;',
    columns_count: 'SELECT COUNT(*) AS total FROM INFORMATION_SCHEMA.COLUMNS;',
    server_version: 'SELECT @@VERSION AS version_servidor;'
  },
  mysql: {
    current_time: 'SELECT NOW() AS fecha_hora;',
    tables: 'SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT 20;',
    columns_count: 'SELECT COUNT(*) AS total FROM information_schema.columns;',
    server_version: 'SELECT VERSION() AS version_servidor;'
  },
  postgresql: {
    current_time: 'SELECT NOW() AS fecha_hora;',
    tables: "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_schema, table_name LIMIT 20;",
    columns_count: 'SELECT COUNT(*) AS total FROM information_schema.columns;',
    server_version: 'SELECT pg_catalog.version() AS version_servidor;'
  }
};

function splitSqlStatements(sql) {
  return String(sql)
    .split(';')
    .map((statement) => statement.trim())
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
  renderTemplateButtons();
}

function resolveTemplateSql(platform, templateName) {
  const platformTemplates = sqlTemplatesByPlatform[platform];
  if (!platformTemplates) {
    return '';
  }

  return platformTemplates[templateName] || '';
}

function renderTemplateButtons() {
  const platform = platformEl.value;
  templateBtnEls.forEach((button) => {
    const templateName = button.getAttribute('data-template');
    const sql = resolveTemplateSql(platform, templateName);

    if (!sql) {
      button.disabled = true;
      button.removeAttribute('title');
      return;
    }

    button.disabled = false;
    button.setAttribute('title', sql);
  });
}

async function loadSettings() {
  try {
    const response = await fetch('/api/settings');
    if (!response.ok) {
      throw new Error('No se pudo leer la configuracion del servidor');
    }

    const data = await response.json();
    appSettings = {
      readOnlyMode: Boolean(data.readOnlyMode)
    };
  } catch (_error) {
    appSettings = {
      readOnlyMode: null
    };
  }

  renderConnectionInfo();
}

function renderRows(columns, rows) {
  if (!rows || rows.length === 0) {
    tableWrapEl.innerHTML = '<p class="table-empty">La consulta no devolvio filas.</p>';
    return;
  }

  const safeColumns = columns.map((column) => escapeHtml(column));
  const header = safeColumns.map((column) => `<th>${column}</th>`).join('');
  const body = rows
    .map((row) => {
      const tds = columns
        .map((column) => `<td>${escapeHtml(row[column] == null ? '' : row[column])}</td>`)
        .join('');
      return `<tr>${tds}</tr>`;
    })
    .join('');

  tableWrapEl.innerHTML = `
    <table>
      <thead><tr>${header}</tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
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

  try {
    const statements = splitSqlStatements(queryEl.value);
    if (statements.length !== 1) {
      throw new Error('Solo se permite ejecutar una consulta por vez.');
    }

    const payload = {
      platform: platformEl.value,
      query: queryEl.value
    };

    const response = await fetch('/api/query', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error || 'No se pudo ejecutar la consulta');
    }

    const timingParts = [`Tiempo total: ${data.durationMs} ms`];
    if (typeof data.connectMs === 'number') {
      timingParts.push(`Conexión: ${data.connectMs} ms`);
    }
    if (typeof data.queryMs === 'number') {
      timingParts.push(`Consulta: ${data.queryMs} ms`);
    }
    if (typeof data.serverExecMs === 'number') {
      timingParts.push(`Ejecución en servidor: ${data.serverExecMs} ms`);
    }
    if (typeof data.transportOverheadMs === 'number') {
      timingParts.push(`Transporte/driver: ${data.transportOverheadMs} ms`);
    }

    metaEl.textContent = `Plataforma: ${data.platform} | Filas: ${data.rowCount} | ${timingParts.join(' | ')}`;
    renderRows(data.columns || [], data.rows || []);
  } catch (error) {
    errorEl.classList.remove('hidden');
    errorEl.textContent = error.message;
  } finally {
    executeBtnEl.disabled = false;
    executeBtnEl.textContent = 'Ejecutar consulta';
  }
}

function applyTemplateQuery(event) {
  const templateName = event.currentTarget.getAttribute('data-template');
  const sql = resolveTemplateSql(platformEl.value, templateName);
  if (!sql) {
    return;
  }

  queryEl.value = sql;
  queryEl.focus();
}

function handleQueryShortcuts(event) {
  const isEnter = event.key === 'Enter';
  const hasModifier = event.ctrlKey || event.metaKey;

  if (!isEnter || !hasModifier) {
    return;
  }

  event.preventDefault();
  if (!executeBtnEl.disabled) {
    executeQuery();
  }
}

platformEl.addEventListener('change', renderConnectionInfo);
executeBtnEl.addEventListener('click', executeQuery);
queryEl.addEventListener('keydown', handleQueryShortcuts);
templateBtnEls.forEach((button) => button.addEventListener('click', applyTemplateQuery));

renderConnectionInfo();
loadSettings();
queryEl.value = 'SELECT 1 AS ok;';
