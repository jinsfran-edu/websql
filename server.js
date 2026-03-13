const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const sqlServer = require('mssql');
const mysql = require('mysql2/promise');
const { Pool } = require('pg');

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const readOnlyMode = String(process.env.READ_ONLY_MODE || 'true').toLowerCase() !== 'false';

let sqlServerPoolPromise = null;
let mysqlPool = null;
let postgresPool = null;
let sqlServerWarmupPromise = null;

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

function normalizePlatform(platform) {
  const value = String(platform || '').trim().toLowerCase();
  if (value === 'sqlserver' || value === 'mssql') return 'sqlserver';
  if (value === 'mysql') return 'mysql';
  if (value === 'postgresql' || value === 'postgres' || value === 'pg') return 'postgresql';
  return null;
}

function toInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function nowNs() {
  return process.hrtime.bigint();
}

function elapsedMs(startNs) {
  return Number(process.hrtime.bigint() - startNs) / 1e6;
}

function toFixedMs(value) {
  return Number(value.toFixed(3));
}

function requireField(name, value) {
  if (!value && value !== 0) {
    throw new Error(`Missing required field: ${name}`);
  }
}

function splitSqlStatements(sql) {
  const statements = [];
  let current = '';
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktickQuote = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let i = 0; i < sql.length; i += 1) {
    const char = sql[i];
    const nextChar = sql[i + 1];

    if (inLineComment) {
      current += char;
      if (char === '\n') {
        inLineComment = false;
      }
      continue;
    }

    if (inBlockComment) {
      current += char;
      if (char === '*' && nextChar === '/') {
        current += nextChar;
        i += 1;
        inBlockComment = false;
      }
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote && !inBacktickQuote) {
      if (char === '-' && nextChar === '-') {
        current += char + nextChar;
        i += 1;
        inLineComment = true;
        continue;
      }

      if (char === '/' && nextChar === '*') {
        current += char + nextChar;
        i += 1;
        inBlockComment = true;
        continue;
      }
    }

    if (!inDoubleQuote && !inBacktickQuote && char === '\'' && sql[i - 1] !== '\\') {
      inSingleQuote = !inSingleQuote;
      current += char;
      continue;
    }

    if (!inSingleQuote && !inBacktickQuote && char === '"' && sql[i - 1] !== '\\') {
      inDoubleQuote = !inDoubleQuote;
      current += char;
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote && char === '`' && sql[i - 1] !== '\\') {
      inBacktickQuote = !inBacktickQuote;
      current += char;
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote && !inBacktickQuote && char === ';') {
      const trimmed = current.trim();
      if (trimmed) {
        statements.push(trimmed);
      }
      current = '';
      continue;
    }

    current += char;
  }

  const trailing = current.trim();
  if (trailing) {
    statements.push(trailing);
  }

  return statements;
}

function getLeadingSqlKeyword(statement) {
  const match = statement.trim().match(/^([a-zA-Z]+)/);
  return match ? match[1].toUpperCase() : '';
}

function isReadOnlyStatement(statement) {
  const leading = getLeadingSqlKeyword(statement);
  return ['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN'].includes(leading);
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getConnectionFromEnv(platform) {
  if (platform === 'sqlserver') {
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

  const queryStartedAt = nowNs();
  const result = await pool.request().query(queryText);
  const queryMs = toFixedMs(elapsedMs(queryStartedAt));
  const rows = result.recordset || [];

  return {
    connectMs,
    queryMs,
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
    const [result] = await conn.query(queryText);
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
    const result = await client.query(queryText);
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
  if (!sqlServerPoolPromise) {
    const poolMin = toInt(process.env.SQLSERVER_POOL_MIN, 1);
    const poolMax = toInt(process.env.SQLSERVER_POOL_MAX, 3);

    const config = {
      server: connection.host,
      port: toInt(connection.port, 1433),
      user: connection.user,
      password: connection.password,
      database: connection.database,
      options: {
        encrypt: connection.encrypt !== false,
        trustServerCertificate: connection.trustServerCertificate === true
      },
      pool: {
        max: poolMax,
        min: poolMin,
        idleTimeoutMillis: 30000
      }
    };

    const pool = new sqlServer.ConnectionPool(config);
    sqlServerPoolPromise = pool.connect().catch((error) => {
      sqlServerPoolPromise = null;
      throw error;
    });
  }

  return sqlServerPoolPromise;
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
      idleTimeoutMillis: 30000
    });
  }

  return postgresPool;
}

async function closePools() {
  const closeTasks = [];

  if (sqlServerPoolPromise) {
    closeTasks.push(
      sqlServerPoolPromise
        .then((pool) => pool.close())
        .catch(() => null)
    );
    sqlServerPoolPromise = null;
  }

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

  try {
    const { platform, query } = req.body || {};
    const normalizedPlatform = normalizePlatform(platform);

    if (!normalizedPlatform) {
      return res.status(400).json({ error: 'Invalid platform. Use SQL Server, MySQL, or PostgreSQL.' });
    }

    requireField('query', query);

    const statements = splitSqlStatements(query);
    if (statements.length !== 1) {
      return res.status(400).json({ error: 'Solo se permite ejecutar una consulta por vez.' });
    }

    if (readOnlyMode && !isReadOnlyStatement(statements[0])) {
      return res.status(400).json({ error: 'Modo solo lectura activo: solo se permiten consultas de lectura.' });
    }

    const connection = getConnectionFromEnv(normalizedPlatform);

    let result;

    if (normalizedPlatform === 'sqlserver') {
      result = await runSqlServerQuery(connection, query);
    } else if (normalizedPlatform === 'mysql') {
      result = await runMySqlQuery(connection, query);
    } else {
      result = await runPostgreSqlQuery(connection, query);
    }

    return res.json({
      platform: normalizedPlatform,
      durationMs: Date.now() - startedAt,
      connectMs: result.connectMs,
      queryMs: result.queryMs,
      ...result
    });
  } catch (error) {
    return res.status(500).json({
      error: error.message || 'Unexpected error running query'
    });
  }
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

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
