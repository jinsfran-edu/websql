'use strict';

// Bases disponibles y en qué plataformas existe cada una
const databasePlatforms = {
  pampero: ['sqlserver', 'mysql', 'postgresql'],
  library: ['sqlserver', 'mysql', 'postgresql']
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

// Palabras que, aunque la sentencia empiece con SELECT o WITH, indican escritura:
// CTE con DELETE/UPDATE/INSERT, SELECT ... INTO, INTO OUTFILE, EXEC, etc.
const writeKeywords = [
  'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'INTO', 'DROP', 'CREATE', 'ALTER',
  'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL'
];
const writeKeywordPattern = new RegExp(`\\b(${writeKeywords.join('|')})\\b`, 'i');

// Quita comentarios, literales de texto e identificadores entre comillas/corchetes
// para que "WHERE titulo = 'delete'" o [update] no disparen falsos positivos.
// Una sola pasada: gana lo que aparece primero, así un '--' dentro de un texto
// no se toma como comentario. No se interpreta la barra invertida como escape
// (en T-SQL no lo es): ante la duda queda más texto visible, no menos.
const literalOrCommentPattern = /--[^\n]*|\/\*[\s\S]*?\*\/|'(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]/g;

function stripSqlLiteralsAndComments(statement) {
  return statement.replace(literalOrCommentPattern, ' ');
}

function isReadOnlyStatement(statement) {
  const leading = getLeadingSqlKeyword(statement);
  if (!['SELECT', 'WITH', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN'].includes(leading)) {
    return false;
  }
  return !writeKeywordPattern.test(stripSqlLiteralsAndComments(statement));
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
