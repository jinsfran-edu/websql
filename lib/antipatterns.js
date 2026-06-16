'use strict';

// Detección heurística de antipatrones SQL para advertencias educativas.
// Se trabaja sobre el texto saneado (sin comentarios ni literales de string)
// para que su contenido no dispare falsos positivos.

function stripSqlComments(sql) {
  return String(sql || '')
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ');
}

function stripSqlStrings(sql) {
  // Reemplaza literales de string para que su contenido no dispare detecciones
  return sql.replace(/'(?:[^']|'')*'/g, "''");
}

function parenDepthAt(sql, index) {
  let depth = 0;
  for (let i = 0; i < index; i += 1) {
    if (sql[i] === '(') depth += 1;
    else if (sql[i] === ')') depth -= 1;
  }
  return depth;
}

// FROM con tablas separadas por coma (join implícito)
function hasImplicitJoin(clean) {
  const fromRegex = /\bfrom\b/gi;
  let match;
  while ((match = fromRegex.exec(clean)) !== null) {
    let depth = 0;
    let i = match.index + match[0].length;
    while (i < clean.length) {
      const ch = clean[i];
      if (ch === '(') { depth += 1; i += 1; continue; }
      if (ch === ')') { depth -= 1; if (depth < 0) break; i += 1; continue; }
      if (ch === ';') break;
      if (ch === ',' && depth === 0) return true;
      if (depth === 0 && /[a-zA-Z_]/.test(ch)) {
        let j = i;
        while (j < clean.length && /[\w$]/.test(clean[j])) j += 1;
        const word = clean.slice(i, j).toLowerCase();
        if (['where', 'group', 'having', 'order', 'union', 'intersect', 'except', 'limit', 'offset'].includes(word)) break;
        i = j;
        continue;
      }
      i += 1;
    }
  }
  return false;
}

// HAVING cuyas condiciones no usan agregados (deberían ir en el WHERE)
function hasHavingWithoutAggregate(clean) {
  const havingRegex = /\bhaving\b/gi;
  let match;
  while ((match = havingRegex.exec(clean)) !== null) {
    let depth = 0;
    let clause = '';
    let i = match.index + match[0].length;
    while (i < clean.length) {
      const ch = clean[i];
      if (ch === '(') depth += 1;
      else if (ch === ')') { depth -= 1; if (depth < 0) break; }
      if (ch === ';') break;
      if (depth === 0 && /[a-zA-Z_]/.test(ch)) {
        let j = i;
        while (j < clean.length && /[\w$]/.test(clean[j])) j += 1;
        const word = clean.slice(i, j).toLowerCase();
        if (['order', 'union', 'intersect', 'except', 'limit', 'offset'].includes(word)) break;
        clause += `${clean.slice(i, j)} `;
        i = j;
        continue;
      }
      clause += ch;
      i += 1;
    }
    if (clause.trim() && !/\b(count|sum|avg|min|max|string_agg|group_concat|array_agg|stdev|stddev|var|variance)\s*\(/i.test(clause)) {
      return true;
    }
  }
  return false;
}

// SELECT DISTINCT y GROUP BY en el mismo nivel de la consulta
function hasRedundantDistinct(clean) {
  const distinctRegex = /\bselect\s+distinct\b/gi;
  let match;
  while ((match = distinctRegex.exec(clean)) !== null) {
    const distinctDepth = parenDepthAt(clean, match.index);
    const groupRegex = /\bgroup\s+by\b/gi;
    groupRegex.lastIndex = match.index;
    let group;
    while ((group = groupRegex.exec(clean)) !== null) {
      if (parenDepthAt(clean, group.index) === distinctDepth) return true;
    }
  }
  return false;
}

// ORDER BY por número de columna (ORDER BY 1, 2)
function usesOrdinalOrderBy(clean) {
  const orderRegex = /\border\s+by\b/gi;
  let match;
  while ((match = orderRegex.exec(clean)) !== null) {
    let depth = 0;
    let item = '';
    const items = [];
    let i = match.index + match[0].length;
    while (i <= clean.length) {
      const ch = i < clean.length ? clean[i] : null;
      if (ch === null || ch === ';') { items.push(item); break; }
      if (ch === '(') { depth += 1; item += ch; i += 1; continue; }
      if (ch === ')') { depth -= 1; if (depth < 0) { items.push(item); break; } item += ch; i += 1; continue; }
      if (ch === ',' && depth === 0) { items.push(item); item = ''; i += 1; continue; }
      if (depth === 0 && /[a-zA-Z_]/.test(ch)) {
        let j = i;
        while (j < clean.length && /[\w$]/.test(clean[j])) j += 1;
        const word = clean.slice(i, j).toLowerCase();
        if (['limit', 'offset', 'union', 'intersect', 'except', 'for'].includes(word)) { items.push(item); break; }
        item += clean.slice(i, j);
        i = j;
        continue;
      }
      item += ch;
      i += 1;
    }
    if (items.some((entry) => /^\s*\d+\s*(asc|desc)?\s*$/i.test(entry))) return true;
  }
  return false;
}

function detectAntipatterns(queryText) {
  const advertencias = [];
  const noComments = stripSqlComments(queryText);
  const clean = stripSqlStrings(noComments);

  if (hasImplicitJoin(clean)) {
    advertencias.push('Estás combinando tablas con coma en el FROM (join implícito). Usá JOIN ... ON: es más claro y evita productos cartesianos accidentales.');
  }
  if (hasHavingWithoutAggregate(clean)) {
    advertencias.push('Tu HAVING no usa funciones de agregado: esas condiciones corresponden al WHERE, que filtra antes de agrupar.');
  }
  if (hasRedundantDistinct(clean)) {
    advertencias.push('DISTINCT junto con GROUP BY suele ser redundante: el GROUP BY ya elimina duplicados de las columnas agrupadas.');
  }
  if (usesOrdinalOrderBy(clean)) {
    advertencias.push('Estás ordenando por número de columna (ORDER BY 1): es frágil si cambia la lista de columnas. Usá el nombre de la columna.');
  }
  if (/\b(not\s+)?like\s+n?'%/i.test(noComments)) {
    advertencias.push("LIKE con comodín al inicio ('%...') impide usar índices: el motor recorre toda la tabla. Si podés, evitá el % inicial.");
  }

  return advertencias;
}

function usesSelectStar(queryText) {
  // "SELECT *", "SELECT DISTINCT *", "SELECT TOP n *", "SELECT alias.*"...
  // No debe marcar COUNT(*) ni otros agregados: ahí el * va precedido por "(".
  const selectStar = /\bselect\s+(distinct\s+)?(top\s*\(?\s*\d+\s*\)?\s*(with\s+ties\s+)?)?(\w+\.)?\*/i;
  // "..., alias.*" en medio de la lista de columnas
  const listStar = /,\s*\w+\.\*/;
  return selectStar.test(queryText) || listStar.test(queryText);
}

module.exports = {
  detectAntipatterns,
  usesSelectStar
};
