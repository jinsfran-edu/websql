'use strict';

// Traduce errores crípticos de los motores a pistas en español para alumnos,
// con sugerencias de nombres parecidos usando el esquema de la base.

function levenshtein(a, b) {
  const s = a.toLowerCase();
  const t = b.toLowerCase();
  if (s === t) return 0;
  const m = s.length;
  const n = t.length;
  if (!m) return n;
  if (!n) return m;

  let prev = Array.from({ length: n + 1 }, (_, j) => j);
  for (let i = 1; i <= m; i += 1) {
    const curr = [i];
    for (let j = 1; j <= n; j += 1) {
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;
      curr[j] = Math.min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost);
    }
    prev = curr;
  }
  return prev[n];
}

// Mejor candidato dentro de una distancia razonable (más tolerante en nombres largos)
function closestMatch(name, candidates) {
  const maxDistance = Math.max(2, Math.floor(name.length / 4));
  let best = null;
  let bestDistance = Infinity;
  for (const candidate of candidates) {
    const distance = levenshtein(name, candidate);
    if (distance < bestDistance) {
      bestDistance = distance;
      best = candidate;
    }
  }
  return best !== null && bestDistance <= maxDistance ? best : null;
}

// Quita calificadores ("C.Nombre" -> "Nombre") y comillas/corchetes
function bareIdentifier(name) {
  return String(name || '').replace(/[`"[\]]/g, '').split('.').pop();
}

const COLUMN_PATTERNS = [
  /invalid column name '([^']+)'/i, // SQL Server
  /unknown column '([^']+)' in/i, // MySQL
  /column "([^"]+)" does not exist/i // PostgreSQL
];

const TABLE_PATTERNS = [
  /invalid object name '([^']+)'/i, // SQL Server
  /table '[^']*?\.([^'.]+)' doesn't exist/i, // MySQL
  /relation "([^"]+)" does not exist/i // PostgreSQL
];

const AMBIGUOUS_PATTERNS = [
  /ambiguous column name '([^']+)'/i, // SQL Server
  /column '([^']+)' in .* is ambiguous/i, // MySQL
  /column reference "([^"]+)" is ambiguous/i // PostgreSQL
];

const GROUPBY_PATTERNS = [
  /is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY/i, // SQL Server
  /incompatible with sql_mode=only_full_group_by/i, // MySQL
  /must appear in the GROUP BY clause or be used in an aggregate function/i // PostgreSQL
];

const SYNTAX_PATTERNS = [
  /incorrect syntax near '([^']+)'/i, // SQL Server
  /syntax error at or near "([^"]+)"/i, // PostgreSQL
  /you have an error in your sql syntax.*?near '([\s\S]*?)' at line/i // MySQL
];

function firstMatch(patterns, message) {
  for (const re of patterns) {
    const m = re.exec(message);
    if (m) return m;
  }
  return null;
}

// schemaTables: [{ name, columns: [{ name }] }] (opcional)
function explainSqlError(message, schemaTables) {
  const msg = String(message || '');
  const tables = Array.isArray(schemaTables) ? schemaTables : [];

  let m = firstMatch(COLUMN_PATTERNS, msg);
  if (m) {
    const name = bareIdentifier(m[1]);
    let hint = `La columna '${name}' no existe.`;
    // buscar la columna más parecida en todas las tablas
    let best = null;
    for (const table of tables) {
      const candidate = closestMatch(name, (table.columns || []).map((c) => c.name));
      if (candidate && (!best || levenshtein(name, candidate) < levenshtein(name, best.column))) {
        best = { column: candidate, table: table.name };
      }
    }
    if (best) {
      hint += ` ¿Quisiste decir '${best.column}' (tabla ${best.table})?`;
    } else {
      hint += ' Revisá el nombre exacto en el árbol de esquema de la izquierda.';
    }
    return hint;
  }

  m = firstMatch(TABLE_PATTERNS, msg);
  if (m) {
    const name = bareIdentifier(m[1]);
    let hint = `La tabla '${name}' no existe en esta base.`;
    const tableNames = tables.map((t) => t.name);
    const exactOtherCase = tableNames.find((t) => t.toLowerCase() === name.toLowerCase() && t !== name);
    const candidate = closestMatch(name, tableNames);
    if (exactOtherCase) {
      hint += ` Existe '${exactOtherCase}': revisá mayúsculas/minúsculas o si necesita comillas.`;
    } else if (candidate) {
      hint += ` ¿Quisiste decir '${candidate}'?`;
    } else {
      hint += ' Fijate los nombres disponibles en el árbol de esquema, y que la base elegida sea la correcta.';
    }
    return hint;
  }

  m = firstMatch(AMBIGUOUS_PATTERNS, msg);
  if (m) {
    const name = bareIdentifier(m[1]);
    // Solo tablas base (sin vistas) y acotado, para que la pista no sea ruidosa
    const owners = tables
      .filter((t) => t.type !== 'view' && (t.columns || []).some((c) => c.name.toLowerCase() === name.toLowerCase()))
      .map((t) => t.name);
    const listado = owners.length >= 2 && owners.length <= 3 ? ` (está en ${owners.join(' y ')})` : '';
    return `La columna '${name}' existe en más de una tabla del FROM${listado}: anteponé el alias de la tabla, por ejemplo C.${name}.`;
  }

  if (firstMatch(GROUPBY_PATTERNS, msg)) {
    return 'Cuando usás GROUP BY, cada columna del SELECT tiene que estar en el GROUP BY o dentro de una función de agregado (COUNT, SUM, AVG, MIN, MAX).';
  }

  m = firstMatch(SYNTAX_PATTERNS, msg);
  if (m) {
    const token = String(m[1] || '').trim().slice(0, 30);
    const cerca = token ? ` cerca de '${token}'` : '';
    return `Error de sintaxis${cerca}. Revisá lo que está justo antes: comas de más o de menos, palabras clave mal escritas, o paréntesis/comillas sin cerrar.`;
  }

  return null;
}

module.exports = {
  levenshtein,
  closestMatch,
  explainSqlError
};
