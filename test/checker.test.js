'use strict';

const { test, describe } = require('node:test');
const assert = require('node:assert/strict');

const {
  normalizePlatform,
  normalizeDatabaseKey,
  isDatabaseAvailable,
  splitSqlStatements,
  getLeadingSqlKeyword,
  isReadOnlyStatement,
  normalizeNumericString,
  normalizeCellValue,
  buildRowKey,
  diffRowMultisets,
  compareExerciseResults,
  usesSelectStar,
  detectAntipatterns
} = require('../server.js');

// Helpers para construir resultados con el shape { columns, rows }
function rows(...records) {
  return records;
}
function result(columns, recs) {
  return { columns, rows: recs };
}

describe('normalizePlatform', () => {
  test('acepta alias y normaliza', () => {
    assert.equal(normalizePlatform('sqlserver'), 'sqlserver');
    assert.equal(normalizePlatform('mssql'), 'sqlserver');
    assert.equal(normalizePlatform('MySQL'), 'mysql');
    assert.equal(normalizePlatform('postgres'), 'postgresql');
    assert.equal(normalizePlatform('pg'), 'postgresql');
    assert.equal(normalizePlatform('  PostgreSQL  '), 'postgresql');
  });
  test('rechaza desconocidos', () => {
    assert.equal(normalizePlatform('oracle'), null);
    assert.equal(normalizePlatform(''), null);
    assert.equal(normalizePlatform(undefined), null);
  });
});

describe('normalizeDatabaseKey / isDatabaseAvailable', () => {
  test('default pampero y validación', () => {
    assert.equal(normalizeDatabaseKey(undefined), 'pampero');
    assert.equal(normalizeDatabaseKey('LIBRARY'), 'library');
    assert.equal(normalizeDatabaseKey('otra'), null);
  });
  test('library solo en SQL Server', () => {
    assert.equal(isDatabaseAvailable('library', 'sqlserver'), true);
    assert.equal(isDatabaseAvailable('library', 'mysql'), false);
    assert.equal(isDatabaseAvailable('library', 'postgresql'), false);
    assert.equal(isDatabaseAvailable('pampero', 'mysql'), true);
  });
});

describe('splitSqlStatements (servidor)', () => {
  test('una sola sentencia', () => {
    assert.deepEqual(splitSqlStatements('SELECT 1'), ['SELECT 1']);
    assert.deepEqual(splitSqlStatements('SELECT 1;'), ['SELECT 1']);
  });
  test('separa por punto y coma', () => {
    assert.deepEqual(splitSqlStatements('SELECT 1; SELECT 2'), ['SELECT 1', 'SELECT 2']);
  });
  test('ignora ; dentro de strings', () => {
    assert.deepEqual(splitSqlStatements("SELECT ';' AS x"), ["SELECT ';' AS x"]);
  });
  test('ignora ; dentro de comentarios', () => {
    assert.deepEqual(splitSqlStatements('SELECT 1 -- ; no separa\n'), ['SELECT 1 -- ; no separa']);
    assert.deepEqual(splitSqlStatements('SELECT 1 /* ; nope */'), ['SELECT 1 /* ; nope */']);
  });
});

describe('isReadOnlyStatement', () => {
  test('lecturas permitidas', () => {
    for (const q of ['SELECT 1', 'WITH t AS (SELECT 1) SELECT * FROM t', 'SHOW TABLES', 'DESCRIBE x', 'EXPLAIN SELECT 1']) {
      assert.equal(isReadOnlyStatement(q), true, q);
    }
  });
  test('escrituras rechazadas', () => {
    for (const q of ['INSERT INTO t VALUES (1)', 'UPDATE t SET a=1', 'DELETE FROM t', 'DROP TABLE t', 'CREATE TABLE t (a int)']) {
      assert.equal(isReadOnlyStatement(q), false, q);
    }
  });
  test('keyword inicial insensible a mayúsculas/espacios', () => {
    assert.equal(getLeadingSqlKeyword('   select 1'), 'SELECT');
  });
});

describe('normalizeNumericString', () => {
  test('1.0 y 1 son iguales', () => {
    assert.equal(normalizeNumericString('1.0'), normalizeNumericString('1'));
    assert.equal(normalizeNumericString('1.0'), '1');
  });
  test('redondea a 4 decimales', () => {
    assert.equal(normalizeNumericString('3.14159'), '3.1416');
    assert.equal(normalizeNumericString('2.5000'), '2.5');
  });
  test('no numérico pasa tal cual', () => {
    assert.equal(normalizeNumericString('abc'), 'abc');
  });
});

describe('normalizeCellValue', () => {
  test('NULL es distinto de string vacío', () => {
    assert.notEqual(normalizeCellValue(null), normalizeCellValue(''));
    assert.equal(normalizeCellValue(undefined), normalizeCellValue(null));
  });
  test('números equivalentes coinciden', () => {
    assert.equal(normalizeCellValue(1), normalizeCellValue('1.0'));
    assert.equal(normalizeCellValue(1.0), '1');
  });
  test('recorta padding de CHAR (espacios finales)', () => {
    assert.equal(normalizeCellValue('ABC   '), 'ABC');
  });
  test('fechas se comparan en ISO', () => {
    const a = normalizeCellValue(new Date('2020-01-31T00:00:00Z'));
    const b = normalizeCellValue(new Date('2020-01-31T00:00:00Z'));
    assert.equal(a, b);
  });
});

describe('buildRowKey', () => {
  test('mismas filas → misma clave', () => {
    assert.equal(buildRowKey({ a: 1, b: 'x' }), buildRowKey({ a: '1.0', b: 'x' }));
  });
  test('NULL vs vacío → claves distintas', () => {
    assert.notEqual(buildRowKey({ a: null }), buildRowKey({ a: '' }));
  });
});

describe('diffRowMultisets', () => {
  test('detecta filas de más y de menos con ejemplos (máx 3)', () => {
    const student = rows({ p: 'A' }, { p: 'B' }, { p: 'X' });
    const solution = rows({ p: 'A' }, { p: 'B' }, { p: 'C' });
    const d = diffRowMultisets(student, solution);
    assert.equal(d.extraCount, 1);
    assert.equal(d.missingCount, 1);
    assert.equal(d.extraSamples.length, 1);
    assert.equal(d.missingSamples.length, 1);
  });
  test('limita las muestras a 3 aunque haya más', () => {
    const student = rows(...Array.from({ length: 10 }, (_, i) => ({ n: `extra${i}` })));
    const solution = rows();
    const d = diffRowMultisets(student, solution);
    assert.equal(d.extraCount, 10);
    assert.equal(d.extraSamples.length, 3);
  });
});

describe('compareExerciseResults', () => {
  test('multiconjunto correcto sin orden requerido', () => {
    const student = result(['p'], rows({ p: 'A' }, { p: 'B' }));
    const solution = result(['p'], rows({ p: 'B' }, { p: 'A' }));
    const r = compareExerciseResults(student, solution, {});
    assert.equal(r.correcto, true);
  });
  test('distinta cantidad de filas → incorrecto', () => {
    const student = result(['p'], rows({ p: 'A' }));
    const solution = result(['p'], rows({ p: 'A' }, { p: 'B' }));
    const r = compareExerciseResults(student, solution, {});
    assert.equal(r.correcto, false);
    assert.match(r.feedback[0], /1 fila\(s\); se esperaban 2/);
  });
  test('distinta cantidad de columnas → nombra las esperadas', () => {
    const student = result(['a'], rows({ a: 1 }));
    const solution = result(['a', 'b'], rows({ a: 1, b: 2 }));
    const r = compareExerciseResults(student, solution, {});
    assert.equal(r.correcto, false);
    assert.match(r.feedback[0], /se esperaban 2 \(a, b\)/);
  });
  test('valores calculados distintos → incorrecto con muestras', () => {
    const student = result(['pais', 'n'], rows({ pais: 'AR', n: 5 }));
    const solution = result(['pais', 'n'], rows({ pais: 'AR', n: 4 }));
    const r = compareExerciseResults(student, solution, {});
    assert.equal(r.correcto, false);
    assert.ok(r.feedback.length >= 1);
  });
  test('1 vs 1.0 se consideran iguales', () => {
    const student = result(['n'], rows({ n: 1 }));
    const solution = result(['n'], rows({ n: 1.0 }));
    assert.equal(compareExerciseResults(student, solution, {}).correcto, true);
  });

  describe('verificación de orden (ordenPor)', () => {
    const sol = result(['a', 'b'], rows({ a: 1, b: 'x' }, { a: 2, b: 'y' }, { a: 3, b: 'z' }));
    test('orden correcto en la columna clave → correcto', () => {
      const student = result(['a', 'b'], rows({ a: 1, b: 'x' }, { a: 2, b: 'y' }, { a: 3, b: 'z' }));
      assert.equal(compareExerciseResults(student, sol, { ordenado: true, ordenPor: [0] }).correcto, true);
    });
    test('orden invertido en la clave → incorrecto, indica la fila', () => {
      const student = result(['a', 'b'], rows({ a: 3, b: 'z' }, { a: 2, b: 'y' }, { a: 1, b: 'x' }));
      const r = compareExerciseResults(student, sol, { ordenado: true, ordenPor: [0] });
      assert.equal(r.correcto, false);
      assert.match(r.feedback[0], /orden no es el pedido.*fila 1/);
    });
    test('empates: permutación dentro del empate es válida', () => {
      // clave de orden = columna 0; filas con a=1 empatan, b puede ir en cualquier orden
      const solTies = result(['a', 'b'], rows({ a: 1, b: 'x' }, { a: 1, b: 'y' }, { a: 2, b: 'z' }));
      const student = result(['a', 'b'], rows({ a: 1, b: 'y' }, { a: 1, b: 'x' }, { a: 2, b: 'z' }));
      assert.equal(compareExerciseResults(student, solTies, { ordenado: true, ordenPor: [0] }).correcto, true);
    });
    test('orden multi-clave: segunda clave equivocada → incorrecto', () => {
      const solMulti = result(['cat', 'prod'], rows(
        { cat: 'A', prod: 'aa' }, { cat: 'A', prod: 'bb' }, { cat: 'B', prod: 'cc' }
      ));
      const student = result(['cat', 'prod'], rows(
        { cat: 'A', prod: 'bb' }, { cat: 'A', prod: 'aa' }, { cat: 'B', prod: 'cc' }
      ));
      const r = compareExerciseResults(student, solMulti, { ordenado: true, ordenPor: [0, 1] });
      assert.equal(r.correcto, false);
    });
  });
});

describe('usesSelectStar', () => {
  test('detecta variantes de SELECT *', () => {
    assert.equal(usesSelectStar('SELECT * FROM t'), true);
    assert.equal(usesSelectStar('SELECT DISTINCT * FROM t'), true);
    assert.equal(usesSelectStar('SELECT TOP 5 * FROM t'), true);
    assert.equal(usesSelectStar('SELECT c.* FROM Clientes c'), true);
    assert.equal(usesSelectStar('SELECT a, c.* FROM t a JOIN c'), true);
  });
  test('no marca COUNT(*) ni columnas explícitas', () => {
    assert.equal(usesSelectStar('SELECT COUNT(*) FROM t'), false);
    assert.equal(usesSelectStar('SELECT a, b FROM t'), false);
  });
});

describe('detectAntipatterns', () => {
  test('join implícito (coma en FROM)', () => {
    assert.equal(detectAntipatterns('SELECT a FROM x, y WHERE x.id = y.id').length, 1);
    assert.equal(detectAntipatterns('SELECT a FROM x JOIN y ON x.id = y.id').length, 0);
  });
  test('HAVING sin agregado', () => {
    assert.equal(detectAntipatterns('SELECT p FROM t GROUP BY p HAVING p = 1').length, 1);
    assert.equal(detectAntipatterns('SELECT p FROM t GROUP BY p HAVING COUNT(*) > 2').length, 0);
  });
  test('DISTINCT redundante con GROUP BY (mismo nivel)', () => {
    assert.equal(detectAntipatterns('SELECT DISTINCT p FROM t GROUP BY p').length, 1);
    assert.equal(detectAntipatterns('SELECT DISTINCT x FROM (SELECT p AS x FROM t GROUP BY p) s').length, 0);
  });
  test('ORDER BY por número de columna', () => {
    assert.equal(detectAntipatterns('SELECT a, b FROM t ORDER BY 1, 2').length, 1);
    assert.equal(detectAntipatterns('SELECT a FROM t ORDER BY LEN(a)').length, 0);
  });
  test('LIKE con comodín inicial', () => {
    assert.equal(detectAntipatterns("SELECT a FROM t WHERE n LIKE '%x'").length, 1);
    assert.equal(detectAntipatterns("SELECT a FROM t WHERE n LIKE 'x%'").length, 0);
  });
  test('saneo: comas/keywords dentro de strings o comentarios no disparan', () => {
    assert.equal(detectAntipatterns("SELECT p FROM t WHERE n = 'x, y FROM a, b'").length, 0);
    assert.equal(detectAntipatterns('SELECT p FROM t -- FROM a, b\nWHERE p = 1').length, 0);
  });
  test('varios antipatrones a la vez', () => {
    const found = detectAntipatterns('SELECT a, b FROM x, y ORDER BY 1');
    assert.equal(found.length, 2);
  });
});
