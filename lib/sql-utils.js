'use strict';

// Bases disponibles y en qué plataformas existe cada una
const databasePlatforms = {
  pampero: ['sqlserver', 'mysql', 'postgresql'],
  library: ['sqlserver']
};

function normalizePlatform(platform) {
  const value = String(platform || '').trim().toLowerCase();
  if (value === 'sqlserver' || value === 'mssql') return 'sqlserver';
  if (value === 'mysql') return 'mysql';
  if (value === 'postgresql' || value === 'postgres' || value === 'pg') return 'postgresql';
  return null;
}

function normalizeDatabaseKey(database) {
  const value = String(database || 'pampero').trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(databasePlatforms, value) ? value : null;
}

function isDatabaseAvailable(databaseKey, platform) {
  return (databasePlatforms[databaseKey] || []).includes(platform);
}

function toInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

// Separa un texto SQL en sentencias por ';', respetando strings y comentarios.
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

module.exports = {
  databasePlatforms,
  normalizePlatform,
  normalizeDatabaseKey,
  isDatabaseAvailable,
  toInt,
  splitSqlStatements,
  getLeadingSqlKeyword,
  isReadOnlyStatement
};
