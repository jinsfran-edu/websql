const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const fs = require('fs/promises');
const path = require('path');
const sqlServer = require('mssql');
const mysql = require('mysql2/promise');
const { Pool } = require('pg');
const {
  databasePlatforms,
  normalizePlatform,
  normalizeDatabaseKey,
  isDatabaseAvailable,
  toInt,
  splitSqlStatements,
  isReadOnlyStatement
} = require('./lib/sql-utils');
const { detectAntipatterns, usesSelectStar } = require('./lib/antipatterns');
const { compareExerciseResults } = require('./lib/exercise-checker');
const { buildStats } = require('./lib/stats');

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const readOnlyMode = String(process.env.READ_ONLY_MODE || 'true').toLowerCase() !== 'false';
const sqlServerDiagnosticsEnabled = String(process.env.SQLSERVER_DIAGNOSTICS || 'false').toLowerCase() === 'true';
const queryTimeoutMs = Math.max(1, toInt(process.env.QUERY_TIMEOUT_MS, 15000));
const maxResultRows = Math.max(1, toInt(process.env.MAX_RESULT_ROWS, 500));
const queryStatsLogPath = process.env.QUERY_STATS_LOG_PATH
  ? path.resolve(process.env.QUERY_STATS_LOG_PATH)
  : path.join(__dirname, 'logs', 'query-stats.jsonl');
// Si se define, el dashboard (/api/stats y admin.html) exige ?key=ADMIN_KEY.
const adminKey = String(process.env.ADMIN_KEY || '');

const sqlServerPoolPromises = new Map();
let mysqlPool = null;
let postgresPool = null;
let sqlServerWarmupPromise = null;
let queryStatsDirReadyPromise = null;

const allowedOrigins = String(process.env.CORS_ALLOWED_ORIGINS || '')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || allowedOrigins.length === 0 || allowedOrigins.includes(origin)) {
        return callback(null, true);
      }
      return callback(new Error('Origin not allowed by CORS'));
    }
  })
);
app.use(express.json({ limit: '1mb' }));
app.use(express.static('public'));
// Monaco auto-hospedado: sirve node_modules/monaco-editor/min en /vendor/monaco
// (evita depender de un CDN externo en una app que ejecuta SQL con credenciales).
app.use('/vendor/monaco', express.static(path.join(__dirname, 'node_modules', 'monaco-editor', 'min')));

app.set('trust proxy', true);



// Bases disponibles y en qué plataformas existe cada una








function nowNs() {
  return process.hrtime.bigint();
}

function elapsedMs(startNs) {
  return Number(process.hrtime.bigint() - startNs) / 1e6;
}

function toFixedMs(value) {
  return Number(value.toFixed(3));
}

function normalizeIp(rawIp) {
  const value = String(rawIp || '').trim();
  if (!value) {
    return 'unknown';
  }

  if (value.startsWith('::ffff:')) {
    return value.slice(7);
  }

  if (value === '::1') {
    return '127.0.0.1';
  }

  return value;
}

function getClientIp(req) {
  const forwardedFor = req.headers['x-forwarded-for'];
  if (typeof forwardedFor === 'string' && forwardedFor.trim()) {
    const first = forwardedFor.split(',')[0].trim();
    if (first) {
      return normalizeIp(first);
    }
  }

  return normalizeIp(req.ip || req.socket?.remoteAddress || '');
}

function ensureQueryStatsDirReady() {
  if (!queryStatsDirReadyPromise) {
    queryStatsDirReadyPromise = fs
      .mkdir(path.dirname(queryStatsLogPath), { recursive: true })
      .catch((error) => {
        queryStatsDirReadyPromise = null;
        throw error;
      });
  }

  return queryStatsDirReadyPromise;
}

async function appendQueryStat(entry) {
  await ensureQueryStatsDirReady();
  await fs.appendFile(queryStatsLogPath, `${JSON.stringify(entry)}\n`, 'utf8');
}

function requireField(name, value) {
  if (!value && value !== 0) {
    throw new Error(`Missing required field: ${name}`);
  }
}

function isQueryTimeoutError(error) {
  const code = String(error?.code || '').toUpperCase();
  const message = String(error?.message || '').toLowerCase();
  return (
    code === 'ETIMEOUT'
    || code === 'PROTOCOL_SEQUENCE_TIMEOUT'
    || code === '57014'
    || message.includes('timeout')
    || message.includes('statement timeout')
  );
}







function buildSqlServerDiagnosticsBatch(queryText) {
  return [
    'SET NOCOUNT ON;',
    'DECLARE @__websql_diag_start datetime2(7) = SYSUTCDATETIME();',
    queryText,
    ';SELECT CAST(DATEDIFF_BIG(MICROSECOND, @__websql_diag_start, SYSUTCDATETIME()) AS bigint) AS __websql_diag_server_elapsed_us__;'
  ].join('\n');
}

function getSqlServerServerElapsedMs(recordsets) {
  if (!Array.isArray(recordsets) || recordsets.length < 2) {
    return null;
  }

  const lastRecordset = recordsets[recordsets.length - 1];
  if (!Array.isArray(lastRecordset) || lastRecordset.length !== 1) {
    return null;
  }

  const elapsedUs = Number(lastRecordset[0]?.__websql_diag_server_elapsed_us__);
  if (!Number.isFinite(elapsedUs) || elapsedUs < 0) {
    return null;
  }

  return toFixedMs(elapsedUs / 1000);
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getConnectionFromEnv(platform, databaseKey = 'pampero') {
  if (platform === 'sqlserver') {
    if (databaseKey === 'library') {
      return {
        host: requireEnv('SQLSERVER_HOST'),
        port: toInt(process.env.SQLSERVER_PORT, 1433),
        database: process.env.SQLSERVER_LIBRARY_DATABASE || 'library',
        user: requireEnv('SQLSERVER_LIBRARY_USER'),
        password: requireEnv('SQLSERVER_LIBRARY_PASSWORD')
      };
    }

    return {
      host: requireEnv('SQLSERVER_HOST'),
      port: toInt(process.env.SQLSERVER_PORT, 1433),
      database: requireEnv('SQLSERVER_DATABASE'),
      user: requireEnv('SQLSERVER_USER'),
      password: requireEnv('SQLSERVER_PASSWORD')
    };
  }

  if (platform === 'mysql') {
    return {
      host: requireEnv('MYSQL_HOST'),
      port: toInt(process.env.MYSQL_PORT, 3306),
      database: requireEnv('MYSQL_DATABASE'),
      user: requireEnv('MYSQL_USER'),
      password: requireEnv('MYSQL_PASSWORD'),
      ssl: String(process.env.MYSQL_SSL || 'true').toLowerCase() !== 'false'
    };
  }

  return {
    host: requireEnv('POSTGRES_HOST'),
    port: toInt(process.env.POSTGRES_PORT, 5432),
    database: requireEnv('POSTGRES_DATABASE'),
    user: requireEnv('POSTGRES_USER'),
    password: requireEnv('POSTGRES_PASSWORD'),
    ssl: String(process.env.POSTGRES_SSL || 'true').toLowerCase() !== 'false'
  };
}

async function runSqlServerQuery(connection, queryText) {
  const connectStartedAt = nowNs();
  const pool = await getSqlServerPool(connection);
  const connectMs = toFixedMs(elapsedMs(connectStartedAt));

  const diagnosticsEnabledForQuery = sqlServerDiagnosticsEnabled && isReadOnlyStatement(queryText);
  const queryToRun = diagnosticsEnabledForQuery ? buildSqlServerDiagnosticsBatch(queryText) : queryText;

  const queryStartedAt = nowNs();
  const request = pool.request();
  request.timeout = queryTimeoutMs;
  const result = await request.query(queryToRun);
  const queryMs = toFixedMs(elapsedMs(queryStartedAt));

  const rows = Array.isArray(result.recordsets) && result.recordsets.length
    ? (result.recordsets[0] || [])
    : (result.recordset || []);

  const serverExecMs = diagnosticsEnabledForQuery
    ? getSqlServerServerElapsedMs(result.recordsets)
    : null;
  const transportOverheadMs = Number.isFinite(serverExecMs)
    ? toFixedMs(Math.max(queryMs - serverExecMs, 0))
    : null;

  return {
    connectMs,
    queryMs,
    serverExecMs,
    transportOverheadMs,
    columns: rows.length ? Object.keys(rows[0]) : [],
    rows,
    rowCount: rows.length,
    info: result.rowsAffected
  };
}

async function runMySqlQuery(connection, queryText) {
  const pool = getMySqlPool(connection);
  const connectStartedAt = nowNs();
  const conn = await pool.getConnection();
  const connectMs = toFixedMs(elapsedMs(connectStartedAt));

  try {
    const queryStartedAt = nowNs();
    const [result] = await conn.query({
      sql: queryText,
      timeout: queryTimeoutMs
    });
    const queryMs = toFixedMs(elapsedMs(queryStartedAt));

    if (Array.isArray(result)) {
      return {
        connectMs,
        queryMs,
        columns: result.length ? Object.keys(result[0]) : [],
        rows: result,
        rowCount: result.length,
        info: null
      };
    }

    return {
      connectMs,
      queryMs,
      columns: [],
      rows: [],
      rowCount: result.affectedRows || 0,
      info: {
        affectedRows: result.affectedRows,
        insertId: result.insertId,
        warningStatus: result.warningStatus
      }
    };
  } finally {
    conn.release();
  }
}

async function runPostgreSqlQuery(connection, queryText) {
  const pool = getPostgreSqlPool(connection);
  const connectStartedAt = nowNs();
  const client = await pool.connect();
  const connectMs = toFixedMs(elapsedMs(connectStartedAt));

  try {
    const queryStartedAt = nowNs();
    const result = await client.query({
      text: queryText,
      query_timeout: queryTimeoutMs
    });
    const queryMs = toFixedMs(elapsedMs(queryStartedAt));

    return {
      connectMs,
      queryMs,
      columns: result.fields ? result.fields.map((field) => field.name) : [],
      rows: result.rows || [],
      rowCount: result.rowCount || 0,
      info: null
    };
  } finally {
    client.release();
  }
}

async function getSqlServerPool(connection) {
  const poolKey = `${connection.database}:${connection.user}`;

  if (!sqlServerPoolPromises.has(poolKey)) {
    const poolMin = toInt(process.env.SQLSERVER_POOL_MIN, 1);
    const poolMax = toInt(process.env.SQLSERVER_POOL_MAX, 3);

    const config = {
      server: connection.host,
      port: toInt(connection.port, 1433),
      user: connection.user,
      password: connection.password,
      database: connection.database,
      connectionTimeout: queryTimeoutMs,
      options: {
        encrypt: connection.encrypt !== false,
        trustServerCertificate: connection.trustServerCertificate === true,
        requestTimeout: queryTimeoutMs
      },
      pool: {
        max: poolMax,
        min: poolMin,
        idleTimeoutMillis: 30000
      }
    };

    const pool = new sqlServer.ConnectionPool(config);
    const poolPromise = pool.connect().catch((error) => {
      sqlServerPoolPromises.delete(poolKey);
      throw error;
    });
    sqlServerPoolPromises.set(poolKey, poolPromise);
  }

  return sqlServerPoolPromises.get(poolKey);
}

async function warmSqlServerPoolIfEnabled() {
  const enabled = String(process.env.SQLSERVER_WARMUP_ON_START || 'true').toLowerCase() !== 'false';
  if (!enabled) {
    return;
  }

  if (!sqlServerWarmupPromise) {
    sqlServerWarmupPromise = (async () => {
      try {
        const connection = getConnectionFromEnv('sqlserver');
        const pool = await getSqlServerPool(connection);
        const warmupQuery = String(process.env.SQLSERVER_WARMUP_QUERY || 'SELECT 1 AS ok;').trim();

        if (warmupQuery) {
          await pool.request().query(warmupQuery);
        }

        console.log('SQL Server pool warm-up completed.');
      } catch (error) {
        console.warn(`SQL Server warm-up skipped: ${error.message}`);
      }
    })();
  }

  await sqlServerWarmupPromise;
}

function getMySqlPool(connection) {
  if (!mysqlPool) {
    mysqlPool = mysql.createPool({
      host: connection.host,
      port: toInt(connection.port, 3306),
      user: connection.user,
      password: connection.password,
      database: connection.database,
      ssl: connection.ssl ? {} : undefined,
      connectTimeout: queryTimeoutMs,
      waitForConnections: true,
      connectionLimit: 3,
      maxIdle: 3,
      idleTimeout: 30000,
      enableKeepAlive: true
    });
  }

  return mysqlPool;
}

function getPostgreSqlPool(connection) {
  if (!postgresPool) {
    const useSsl = connection.ssl !== false;

    postgresPool = new Pool({
      host: connection.host,
      port: toInt(connection.port, 5432),
      user: connection.user,
      password: connection.password,
      database: connection.database,
      ssl: useSsl ? { rejectUnauthorized: false } : false,
      max: 3,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: queryTimeoutMs,
      statement_timeout: queryTimeoutMs,
      query_timeout: queryTimeoutMs
    });
  }

  return postgresPool;
}

async function closePools() {
  const closeTasks = [];

  for (const poolPromise of sqlServerPoolPromises.values()) {
    closeTasks.push(
      poolPromise
        .then((pool) => pool.close())
        .catch(() => null)
    );
  }
  sqlServerPoolPromises.clear();

  if (mysqlPool) {
    closeTasks.push(mysqlPool.end().catch(() => null));
    mysqlPool = null;
  }

  if (postgresPool) {
    closeTasks.push(postgresPool.end().catch(() => null));
    postgresPool = null;
  }

  await Promise.all(closeTasks);
}

app.post('/api/query', async (req, res) => {
  const startedAt = Date.now();
  let normalizedPlatform = null;
  let databaseKey = null;
  let queryText = '';

  try {
    const { platform, query, database } = req.body || {};
    normalizedPlatform = normalizePlatform(platform);
    databaseKey = normalizeDatabaseKey(database);
    queryText = String(query || '');

    if (!normalizedPlatform) {
      return res.status(400).json({ error: 'Invalid platform. Use SQL Server, MySQL, or PostgreSQL.' });
    }

    if (!databaseKey) {
      return res.status(400).json({ error: 'Base de datos desconocida.' });
    }

    if (!isDatabaseAvailable(databaseKey, normalizedPlatform)) {
      return res.status(400).json({ error: `La base "${databaseKey}" no está disponible en esa plataforma.` });
    }

    requireField('query', query);

    const statements = splitSqlStatements(query);
    if (statements.length !== 1) {
      return res.status(400).json({ error: 'Solo se permite ejecutar una consulta por vez.' });
    }

    if (readOnlyMode && !isReadOnlyStatement(statements[0])) {
      return res.status(400).json({ error: 'Modo solo lectura activo: solo se permiten consultas de lectura.' });
    }

    const connection = getConnectionFromEnv(normalizedPlatform, databaseKey);

    let result;

    if (normalizedPlatform === 'sqlserver') {
      result = await runSqlServerQuery(connection, query);
    } else if (normalizedPlatform === 'mysql') {
      result = await runMySqlQuery(connection, query);
    } else {
      result = await runPostgreSqlQuery(connection, query);
    }

    // Cap the rows sent to the browser; rowCount keeps the real total fetched
    let truncated = false;
    if (Array.isArray(result.rows) && result.rows.length > maxResultRows) {
      result.rows = result.rows.slice(0, maxResultRows);
      truncated = true;
    }

    const durationMs = Date.now() - startedAt;
    await appendQueryStat({
      timestamp: new Date().toISOString(),
      ip: getClientIp(req),
      platform: normalizedPlatform,
      database: databaseKey,
      query: queryText,
      success: true,
      statusCode: 200,
      durationMs
    });

    return res.json({
      platform: normalizedPlatform,
      database: databaseKey,
      durationMs,
      connectMs: result.connectMs,
      queryMs: result.queryMs,
      truncated,
      maxResultRows,
      advertencias: detectAntipatterns(queryText),
      ...result
    });
  } catch (error) {
    const statusCode = isQueryTimeoutError(error) ? 504 : 500;
    await appendQueryStat({
      timestamp: new Date().toISOString(),
      ip: getClientIp(req),
      platform: normalizedPlatform || 'unknown',
      query: queryText,
      success: false,
      statusCode,
      durationMs: Date.now() - startedAt,
      error: error.message || 'Unexpected error running query'
    }).catch((logError) => {
      console.error(`Failed to write query stats log: ${logError.message}`);
    });

    if (isQueryTimeoutError(error)) {
      return res.status(504).json({
        error: `La consulta supero el timeout configurado (${queryTimeoutMs} ms).`
      });
    }

    return res.status(500).json({
      error: error.message || 'Unexpected error running query'
    });
  }
});

const exercisesById = new Map();
try {
  const exercisesData = require('./exercises.json');
  for (const exercise of exercisesData.exercises || []) {
    exercisesById.set(String(exercise.id), exercise);
  }
  console.log(`Loaded ${exercisesById.size} exercises.`);
} catch (error) {
  console.warn(`No exercises loaded: ${error.message}`);
}

const solutionResultCache = new Map();

async function runQueryForPlatform(platform, queryText, databaseKey = 'pampero') {
  const connection = getConnectionFromEnv(platform, databaseKey);
  if (platform === 'sqlserver') return runSqlServerQuery(connection, queryText);
  if (platform === 'mysql') return runMySqlQuery(connection, queryText);
  return runPostgreSqlQuery(connection, queryText);
}

async function getSolutionResult(exercise, platform) {
  const key = `${exercise.id}:${platform}`;
  if (!solutionResultCache.has(key)) {
    const result = await runQueryForPlatform(platform, exercise.solucion[platform]);
    solutionResultCache.set(key, { columns: result.columns, rows: result.rows });
  }
  return solutionResultCache.get(key);
}







// ---------- Detección de antipatrones ----------



























app.get('/api/exercises', (_req, res) => {
  const list = Array.from(exercisesById.values()).map((exercise) => ({
    id: exercise.id,
    dificultad: exercise.dificultad,
    enunciado: exercise.enunciado,
    ordenado: Boolean(exercise.ordenado),
    plataformas: Object.keys(exercise.solucion || {})
  }));
  res.json({ exercises: list });
});

app.post('/api/exercises/:id/check', async (req, res) => {
  const startedAt = Date.now();
  let normalizedPlatform = null;
  let queryText = '';
  let exerciseId = null;

  try {
    const exercise = exercisesById.get(String(req.params.id));
    if (!exercise) {
      return res.status(404).json({ error: 'Ejercicio no encontrado.' });
    }
    exerciseId = exercise.id;

    const { platform, query } = req.body || {};
    normalizedPlatform = normalizePlatform(platform);
    queryText = String(query || '');

    if (!normalizedPlatform) {
      return res.status(400).json({ error: 'Invalid platform. Use SQL Server, MySQL, or PostgreSQL.' });
    }

    if (!exercise.solucion || !exercise.solucion[normalizedPlatform]) {
      return res.status(400).json({ error: 'Este ejercicio no tiene solución cargada para la plataforma elegida.' });
    }

    requireField('query', query);

    const statements = splitSqlStatements(queryText);
    if (statements.length !== 1) {
      return res.status(400).json({ error: 'Solo se permite verificar una consulta por vez.' });
    }

    if (!isReadOnlyStatement(statements[0])) {
      return res.status(400).json({ error: 'Solo se permiten consultas de lectura en la verificación.' });
    }

    const studentResult = await runQueryForPlatform(normalizedPlatform, queryText);

    let solutionResult;
    try {
      solutionResult = await getSolutionResult(exercise, normalizedPlatform);
    } catch (solutionError) {
      console.error(`Solution query failed for exercise ${exercise.id} (${normalizedPlatform}): ${solutionError.message}`);
      return res.status(500).json({ error: 'No se pudo ejecutar la consulta de referencia del ejercicio. Avisale al docente.' });
    }

    let { correcto, feedback } = compareExerciseResults(studentResult, solutionResult, exercise);

    if (correcto && exercise.ordenado && !/\border\s+by\b/i.test(queryText)) {
      correcto = false;
      feedback = ['Los datos coinciden, pero el enunciado pide un orden específico y tu consulta no incluye ORDER BY.'];
    }

    const advertencias = [];
    if (usesSelectStar(queryText)) {
      advertencias.push('Usaste SELECT *: aunque el resultado sea correcto, no es una buena práctica. Listá explícitamente las columnas que necesitás.');
    }
    advertencias.push(...detectAntipatterns(queryText));

    let truncated = false;
    let responseRows = studentResult.rows || [];
    if (responseRows.length > maxResultRows) {
      responseRows = responseRows.slice(0, maxResultRows);
      truncated = true;
    }

    const durationMs = Date.now() - startedAt;
    await appendQueryStat({
      timestamp: new Date().toISOString(),
      ip: getClientIp(req),
      platform: normalizedPlatform,
      query: queryText,
      success: true,
      statusCode: 200,
      durationMs,
      exerciseId: exercise.id,
      correcto
    });

    return res.json({
      platform: normalizedPlatform,
      exerciseId: exercise.id,
      correcto,
      feedback,
      advertencias,
      durationMs,
      columns: studentResult.columns || [],
      rows: responseRows,
      rowCount: (studentResult.rows || []).length,
      truncated,
      maxResultRows
    });
  } catch (error) {
    const statusCode = isQueryTimeoutError(error) ? 504 : 500;
    await appendQueryStat({
      timestamp: new Date().toISOString(),
      ip: getClientIp(req),
      platform: normalizedPlatform || 'unknown',
      query: queryText,
      success: false,
      statusCode,
      durationMs: Date.now() - startedAt,
      exerciseId,
      error: error.message || 'Unexpected error checking exercise'
    }).catch((logError) => {
      console.error(`Failed to write query stats log: ${logError.message}`);
    });

    if (isQueryTimeoutError(error)) {
      return res.status(504).json({
        error: `La consulta supero el timeout configurado (${queryTimeoutMs} ms).`
      });
    }

    return res.status(500).json({
      error: error.message || 'Unexpected error checking exercise'
    });
  }
});

app.get('/api/schema', async (req, res) => {
  const platform = normalizePlatform(req.query.platform);
  if (!platform) {
    return res.status(400).json({ error: 'Invalid platform' });
  }

  const databaseKey = normalizeDatabaseKey(req.query.database);
  if (!databaseKey || !isDatabaseAvailable(databaseKey, platform)) {
    return res.status(400).json({ error: 'Base de datos no disponible para esa plataforma.' });
  }

  const schemaQueries = {
    sqlserver: `SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, t.TABLE_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS c
                JOIN INFORMATION_SCHEMA.TABLES t
                  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
                ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`,
    mysql: `SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, t.TABLE_TYPE
            FROM information_schema.COLUMNS c
            JOIN information_schema.TABLES t
              ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
            WHERE c.TABLE_SCHEMA = DATABASE()
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
            LIMIT 2000`,
    postgresql: `SELECT c.table_schema, c.table_name, c.column_name, c.data_type, t.table_type
                 FROM information_schema.columns c
                 JOIN information_schema.tables t
                   ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                 WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
                 ORDER BY c.table_schema, c.table_name, c.ordinal_position
                 LIMIT 2000`
  };

  try {
    const connection = getConnectionFromEnv(platform, databaseKey);
    let result;

    if (platform === 'sqlserver') {
      result = await runSqlServerQuery(connection, schemaQueries.sqlserver);
    } else if (platform === 'mysql') {
      result = await runMySqlQuery(connection, schemaQueries.mysql);
    } else {
      result = await runPostgreSqlQuery(connection, schemaQueries.postgresql);
    }

    const tablesMap = new Map();
    for (const row of result.rows) {
      const schema = row.TABLE_SCHEMA || row.table_schema || '';
      const table = row.TABLE_NAME || row.table_name || '';
      const column = row.COLUMN_NAME || row.column_name || '';
      const dataType = row.DATA_TYPE || row.data_type || '';
      const tableType = String(row.TABLE_TYPE || row.table_type || '').toUpperCase() === 'VIEW' ? 'view' : 'table';
      const key = `${schema}.${table}`;

      if (!tablesMap.has(key)) {
        tablesMap.set(key, { schema, name: table, type: tableType, columns: [] });
      }
      tablesMap.get(key).columns.push({ name: column, type: dataType });
    }

    return res.json({ tables: Array.from(tablesMap.values()) });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/settings', (_req, res) => {
  res.json({
    readOnlyMode,
    sqlServerDiagnosticsEnabled,
    queryTimeoutMs,
    maxResultRows,
    databases: databasePlatforms
  });
});



app.get('/api/stats', async (req, res) => {
  if (adminKey && req.query.key !== adminKey) {
    return res.status(401).json({ error: 'Clave de administración inválida.' });
  }

  try {
    let content = '';
    try {
      content = await fs.readFile(queryStatsLogPath, 'utf8');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }

    const lines = content.split('\n').filter(Boolean);
    return res.json(buildStats(lines));
  } catch (error) {
    return res.status(500).json({ error: error.message || 'No se pudieron leer las estadísticas.' });
  }
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

// Solo arranca el servidor cuando se ejecuta directamente (no al importarlo
// desde los tests, que reutilizan las funciones puras de abajo).
if (require.main === module) {
  app.listen(port, () => {
    console.log(`WebSQL runner listening on port ${port}`);
    warmSqlServerPoolIfEnabled().catch(() => null);
  });

  ['SIGTERM', 'SIGINT'].forEach((signal) => {
    process.on(signal, async () => {
      await closePools();
      process.exit(0);
    });
  });
}

// Funciones puras expuestas para pruebas unitarias.

