'use strict';

const { test, describe } = require('node:test');
const assert = require('node:assert/strict');

const { levenshtein, closestMatch, explainSqlError } = require('../lib/error-hints');

const schema = [
  { name: 'Clientes', columns: [{ name: 'IDCliente' }, { name: 'NombreEmpresa' }, { name: 'Pais' }, { name: 'Ciudad' }] },
  { name: 'Pedidos', columns: [{ name: 'IDPedido' }, { name: 'IDCliente' }, { name: 'FechaPedido' }, { name: 'Pais' }] },
  { name: 'Productos', columns: [{ name: 'IDProducto' }, { name: 'NombreProducto' }, { name: 'PrecioUnitario' }] }
];

describe('levenshtein / closestMatch', () => {
  test('distancias básicas', () => {
    assert.equal(levenshtein('abc', 'abc'), 0);
    assert.equal(levenshtein('abc', 'abd'), 1);
    assert.equal(levenshtein('Pais', 'pais'), 0); // case-insensitive
  });
  test('closestMatch respeta el umbral', () => {
    assert.equal(closestMatch('Paiss', ['Pais', 'Ciudad']), 'Pais');
    assert.equal(closestMatch('xyz', ['Pais', 'Ciudad']), null);
  });
});

describe('explainSqlError: columna inexistente', () => {
  test('SQL Server con sugerencia', () => {
    const hint = explainSqlError("Invalid column name 'NombreEmpres'.", schema);
    assert.match(hint, /columna 'NombreEmpres' no existe/);
    assert.match(hint, /'NombreEmpresa' \(tabla Clientes\)/);
  });
  test('MySQL con calificador alias.columna', () => {
    const hint = explainSqlError("Unknown column 'C.NombreEmpres' in 'field list'", schema);
    assert.match(hint, /'NombreEmpresa'/);
  });
  test('PostgreSQL sin candidato cercano → pista genérica', () => {
    const hint = explainSqlError('column "zzzz" does not exist', schema);
    assert.match(hint, /no existe/);
    assert.match(hint, /árbol de esquema/);
  });
});

describe('explainSqlError: tabla inexistente', () => {
  test('SQL Server con sugerencia', () => {
    const hint = explainSqlError("Invalid object name 'Cliente'.", schema);
    assert.match(hint, /tabla 'Cliente' no existe/);
    assert.match(hint, /'Clientes'/);
  });
  test('MySQL extrae el nombre sin la base', () => {
    const hint = explainSqlError("Table 'pampero.Pedido' doesn't exist", schema);
    assert.match(hint, /'Pedidos'/);
  });
  test('PostgreSQL: existe con otra capitalización', () => {
    const hint = explainSqlError('relation "clientes" does not exist', schema);
    assert.match(hint, /mayúsculas|comillas/);
  });
});

describe('explainSqlError: ambigüedad y GROUP BY', () => {
  test('columna ambigua lista las tablas', () => {
    const hint = explainSqlError("Ambiguous column name 'Pais'.", schema);
    assert.match(hint, /más de una tabla/);
    assert.match(hint, /Clientes y Pedidos/);
    assert.match(hint, /alias/);
  });
  test('error de GROUP BY (las 3 variantes)', () => {
    const variants = [
      "Column 'Clientes.Pais' is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause.",
      "Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'x' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by",
      'column "clientes.pais" must appear in the GROUP BY clause or be used in an aggregate function'
    ];
    for (const message of variants) {
      const hint = explainSqlError(message, schema);
      assert.match(hint, /GROUP BY/, message.slice(0, 40));
      assert.match(hint, /agregado/);
    }
  });
});

describe('explainSqlError: sintaxis y desconocidos', () => {
  test('error de sintaxis incluye el token', () => {
    const hint = explainSqlError("Incorrect syntax near 'FORM'.", schema);
    assert.match(hint, /sintaxis/);
    assert.match(hint, /'FORM'/);
  });
  test('mensaje no reconocido → null', () => {
    assert.equal(explainSqlError('Login failed for user', schema), null);
  });
  test('sin esquema no rompe', () => {
    const hint = explainSqlError("Invalid column name 'X'.", null);
    assert.match(hint, /no existe/);
  });
});
