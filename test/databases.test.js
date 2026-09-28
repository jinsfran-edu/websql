'use strict';

const { test, describe } = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');

const {
  loadDatabaseConfig,
  validateConfig,
  resolveEnvTemplate,
  resolveConnection,
  platformsByDatabase
} = require('../lib/databases');

const config = loadDatabaseConfig(path.join(__dirname, '..', 'databases.json'));

const baseEnv = {
  SQLSERVER_HOST: 'ms', SQLSERVER_DATABASE: 'pampero', SQLSERVER_USER: 'u1', SQLSERVER_PASSWORD: 'p1',
  SQLSERVER_LIBRARY_USER: 'u2', SQLSERVER_LIBRARY_PASSWORD: 'p2',
  MYSQL_HOST: 'my', MYSQL_DATABASE: 'pampero', MYSQL_USER: 'mu', MYSQL_PASSWORD: 'mp',
  POSTGRES_HOST: 'pg', POSTGRES_PORT: '6543', POSTGRES_DATABASE: 'pampero', POSTGRES_USER: 'pu', POSTGRES_PASSWORD: 'pp', POSTGRES_SSL: 'false'
};

describe('resolveEnvTemplate', () => {
  test('obligatoria, opcional y literal', () => {
    assert.equal(resolveEnvTemplate('${A}', { A: 'x' }), 'x');
    assert.equal(resolveEnvTemplate('${A:-def}', {}), 'def');
    assert.equal(resolveEnvTemplate('${A:-}', {}), '');
    assert.equal(resolveEnvTemplate('db_${A}', { A: '1' }), 'db_1');
    assert.equal(resolveEnvTemplate('library', {}), 'library');
    assert.throws(() => resolveEnvTemplate('${FALTA}', {}), /Missing required environment variable: FALTA/);
  });
});

describe('databases.json (mismo comportamiento que antes)', () => {
  test('pampero y library en los tres motores, pampero por defecto', () => {
    assert.equal(config.defaultKey, 'pampero');
    // Solo pampero y library: agregar bases nuevas a databases.json no debe romper este test.
    const platforms = platformsByDatabase(config);
    assert.deepEqual(platforms.pampero, ['sqlserver', 'mysql', 'postgresql']);
    assert.deepEqual(platforms.library, ['sqlserver', 'mysql', 'postgresql']);
  });

  test('pampero usa las variables principales de cada motor', () => {
    assert.deepEqual(resolveConnection(config, 'sqlserver', 'pampero', baseEnv),
      { host: 'ms', port: 1433, database: 'pampero', user: 'u1', password: 'p1' });
    assert.deepEqual(resolveConnection(config, 'mysql', 'pampero', baseEnv),
      { host: 'my', port: 3306, database: 'pampero', user: 'mu', password: 'mp', ssl: true });
    assert.deepEqual(resolveConnection(config, 'postgresql', 'pampero', baseEnv),
      { host: 'pg', port: 6543, database: 'pampero', user: 'pu', password: 'pp', ssl: false });
  });

  test('library en SQL Server exige usuario propio', () => {
    assert.deepEqual(resolveConnection(config, 'sqlserver', 'library', baseEnv),
      { host: 'ms', port: 1433, database: 'library', user: 'u2', password: 'p2' });
    const env = { ...baseEnv, SQLSERVER_LIBRARY_USER: '' };
    assert.throws(() => resolveConnection(config, 'sqlserver', 'library', env), /SQLSERVER_LIBRARY_USER/);
  });

  test('library en MySQL/PostgreSQL: usuario propio opcional', () => {
    assert.deepEqual(resolveConnection(config, 'mysql', 'library', baseEnv),
      { host: 'my', port: 3306, database: 'library', user: 'mu', password: 'mp', ssl: true });
    const env = { ...baseEnv, POSTGRES_LIBRARY_USER: 'lu', POSTGRES_LIBRARY_PASSWORD: 'lp', POSTGRES_LIBRARY_DATABASE: 'lib2' };
    assert.deepEqual(resolveConnection(config, 'postgresql', 'library', env),
      { host: 'pg', port: 6543, database: 'lib2', user: 'lu', password: 'lp', ssl: false });
    const sinPassword = { ...baseEnv, MYSQL_LIBRARY_USER: 'lu' };
    assert.throws(() => resolveConnection(config, 'mysql', 'library', sinPassword), /MYSQL_LIBRARY_PASSWORD/);
  });
});

describe('datos para mostrar', () => {
  test('sin contraseña no hace falta la variable de la contraseña', () => {
    const env = { ...baseEnv, SQLSERVER_PASSWORD: '' };
    assert.deepEqual(resolveConnection(config, 'sqlserver', 'pampero', env, { includePassword: false }),
      { host: 'ms', port: 1433, database: 'pampero', user: 'u1' });
    assert.throws(() => resolveConnection(config, 'sqlserver', 'pampero', env), /SQLSERVER_PASSWORD/);
  });
  test('usuario propio sin password: se puede mostrar pero no conectar', () => {
    const custom = validateConfig({ databases: { a: { mysql: { user: 'x' } } } }, 'test');
    assert.deepEqual(resolveConnection(custom, 'mysql', 'a', baseEnv, { includePassword: false }),
      { host: 'my', port: 3306, database: 'a', user: 'x', ssl: true });
    assert.throws(() => resolveConnection(custom, 'mysql', 'a', baseEnv), /password/);
  });
});

describe('bases nuevas', () => {
  test('una entrada vacía usa el nombre de la base y el usuario del motor', () => {
    const custom = validateConfig({ databases: { tienda: { mysql: {} } } }, 'test');
    assert.equal(custom.defaultKey, 'tienda');
    assert.deepEqual(resolveConnection(custom, 'mysql', 'tienda', baseEnv),
      { host: 'my', port: 3306, database: 'tienda', user: 'mu', password: 'mp', ssl: true });
    assert.throws(() => resolveConnection(custom, 'sqlserver', 'tienda', baseEnv), /no está configurada/);
  });

  test('puede usar otro servidor', () => {
    const custom = validateConfig({ databases: { demo: { postgresql: { host: '${DEMO_HOST}', port: '5433', ssl: false } } } }, 'test');
    assert.deepEqual(resolveConnection(custom, 'postgresql', 'demo', { ...baseEnv, DEMO_HOST: 'otro' }),
      { host: 'otro', port: 5433, database: 'demo', user: 'pu', password: 'pp', ssl: false });
  });

  test('rechaza configuraciones inválidas', () => {
    assert.throws(() => validateConfig({}, 'x'), /databases/);
    assert.throws(() => validateConfig({ databases: { Mala: { mysql: {} } } }, 'x'), /nombre de base inválido/);
    assert.throws(() => validateConfig({ databases: { a: { oracle: {} } } }, 'x'), /motor desconocido/);
    assert.throws(() => validateConfig({ databases: { a: {} } }, 'x'), /ningún motor/);
    assert.throws(() => validateConfig({ default: 'b', databases: { a: { mysql: {} } } }, 'x'), /por defecto/);
    assert.throws(() => validateConfig({ databases: { a: { mysql: { user: 'x', password: 'secreto' } } } }, 'x'), /variable de entorno/);
    assert.throws(() => validateConfig({ databases: { a: { mysql: { user: 'x', password: '${P:-secreto}' } } } }, 'x'), /variable de entorno/);
    const sinPassword = validateConfig({ databases: { a: { mysql: { user: 'x' } } } }, 'x');
    assert.throws(() => resolveConnection(sinPassword, 'mysql', 'a', baseEnv), /password/);
  });
});
