const platformEl = document.getElementById('platform');
const connectionInfoEl = document.getElementById('connectionInfo');
const queryEl = document.getElementById('query');
const executeBtnEl = document.getElementById('executeBtn');
const metaEl = document.getElementById('meta');
const errorEl = document.getElementById('error');
const tableWrapEl = document.getElementById('tableWrap');

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

function splitSqlStatements(sql) {
  return String(sql)
    .split(';')
    .map((statement) => statement.trim())
    .filter(Boolean);
}

function renderConnectionInfo() {
  const platform = platformEl.value;
  const info = defaultConnections[platform];
  connectionInfoEl.textContent = `Servidor: ${info.host} | Base: ${info.database} | Usuario: ${info.user}`;
}

function renderRows(columns, rows) {
  if (!rows || rows.length === 0) {
    tableWrapEl.innerHTML = '<p>La consulta no devolvió filas.</p>';
    return;
  }

  const header = columns.map((column) => `<th>${column}</th>`).join('');
  const body = rows
    .map((row) => {
      const tds = columns.map((column) => `<td>${row[column] ?? ''}</td>`).join('');
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

platformEl.addEventListener('change', renderConnectionInfo);
executeBtnEl.addEventListener('click', executeQuery);

renderConnectionInfo();
queryEl.value = 'SELECT 1 AS ok;';
