'use strict';

// Catálogo de bases de ejemplo, leído de databases.json (o de la ruta en
// DATABASES_CONFIG). Agregar una base nueva es sumar una entrada ahí, sin tocar
// código. Los valores admiten ${VAR} (variable de entorno obligatoria) y
// ${VAR:-valor} (opcional, con valor por defecto; ${VAR:-} = vacío).

const fs = require('fs');
const path = require('path');

const PLATFORMS = ['sqlserver', 'mysql', 'postgresql'];

// Conexión base de cada motor: la que usa una base cuando no define la suya.
const ENGINE_DEFAULTS = {
  sqlserver: { host: '${SQLSERVER_HOST}', port: '${SQLSERVER_PORT:-1433}', user: '${SQLSERVER_USER}', password: '${SQLSERVER_PASSWORD}' },
  mysql: { host: '${MYSQL_HOST}', port: '${MYSQL_PORT:-3306}', user: '${MYSQL_USER}', password: '${MYSQL_PASSWORD}', ssl: '${MYSQL_SSL:-true}' },
  postgresql: { host: '${POSTGRES_HOST}', port: '${POSTGRES_PORT:-5432}', user: '${POSTGRES_USER}', password: '${POSTGRES_PASSWORD}', ssl: '${POSTGRES_SSL:-true}' }
};

const defaultConfigPath = path.join(__dirname, '..', 'databases.json');

function validateConfig(config, source) {
  const fail = (message) => { throw new Error(`${source}: ${message}`); };
  if (!config || typeof config !== 'object' || !config.databases || typeof config.databases !== 'object') {
    fail('falta el objeto "databases".');
  }
  const keys = Object.keys(config.databases);
  if (keys.length === 0) fail('"databases" está vacío.');
  for (const key of keys) {
    if (!/^[a-z0-9_-]+$/.test(key)) fail(`nombre de base inválido "${key}" (usar minúsculas, números, _ o -).`);
    const platforms = config.databases[key];
    if (!platforms || typeof platforms !== 'object' || Object.keys(platforms).length === 0) {
      fail(`la base "${key}" no define ningún motor.`);
    }
    for (const platform of Object.keys(platforms)) {
      if (!PLATFORMS.includes(platform)) fail(`motor desconocido "${platform}" en "${key}" (válidos: ${PLATFORMS.join(', ')}).`);
      const entry = platforms[platform];
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) fail(`"${key}.${platform}" debe ser un objeto.`);
      if (entry.password !== undefined && !/^\$\{[A-Za-z_][A-Za-z0-9_]*\}$/.test(String(entry.password))) {
        fail(`"${key}.${platform}.password" debe ser una variable de entorno, p. ej. "\${MI_PASSWORD}".`);
      }
    }
  }
  const defaultKey = config.default || keys[0];
  if (!keys.includes(defaultKey)) fail(`la base por defecto "${defaultKey}" no está en "databases".`);
  return { defaultKey, databases: config.databases };
}

function loadDatabaseConfig(configPath = process.env.DATABASES_CONFIG || defaultConfigPath) {
  const resolved = path.resolve(configPath);
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(resolved, 'utf8'));
  } catch (error) {
    throw new Error(`No se pudo leer la configuración de bases ${resolved}: ${error.message}`);
  }
  return validateConfig(parsed, resolved);
}

// Reemplaza ${VAR} y ${VAR:-default} con valores de env.
function resolveEnvTemplate(value, env = process.env) {
  if (value === undefined || value === null) return value;
  return String(value).replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}/g, (_match, name, hasDefault, fallback) => {
    const current = env[name];
    if (current) return current;
    if (hasDefault !== undefined) return fallback;
    throw new Error(`Missing required environment variable: ${name}`);
  });
}

// Devuelve { platform: [keys] } para la interfaz y la validación.
function platformsByDatabase(config) {
  const result = {};
  for (const [key, platforms] of Object.entries(config.databases)) {
    result[key] = PLATFORMS.filter((platform) => platforms[platform]);
  }
  return result;
}

function toInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

// Arma la conexión de una base en un motor. Lo que la entrada no define se
// toma del motor. Si "user" queda vacío (p. ej. ${VAR:-} sin definir), se usan
// el usuario y la contraseña principales del motor.
function resolveConnection(config, platform, databaseKey, env = process.env) {
  const entry = (config.databases[databaseKey] || {})[platform];
  if (!entry) {
    throw new Error(`La base "${databaseKey}" no está configurada para ${platform}.`);
  }
  const engine = ENGINE_DEFAULTS[platform];
  const resolve = (value) => resolveEnvTemplate(value, env);

  const ownUser = entry.user !== undefined ? resolve(entry.user) : '';
  if (ownUser && entry.password === undefined) {
    throw new Error(`La base "${databaseKey}" en ${platform} define "user" pero no "password".`);
  }
  const connection = {
    host: resolve(entry.host !== undefined ? entry.host : engine.host),
    port: toInt(resolve(entry.port !== undefined ? entry.port : engine.port), toInt(resolve(engine.port), 0)),
    database: resolve(entry.database !== undefined ? entry.database : databaseKey),
    user: ownUser || resolve(engine.user),
    password: resolve(ownUser ? entry.password : engine.password)
  };
  if (!connection.database) {
    throw new Error(`La base "${databaseKey}" en ${platform} no tiene nombre de base de datos.`);
  }
  if (engine.ssl !== undefined) {
    const ssl = entry.ssl !== undefined ? entry.ssl : engine.ssl;
    connection.ssl = String(resolve(ssl)).toLowerCase() !== 'false';
  }
  return connection;
}

module.exports = {
  PLATFORMS,
  loadDatabaseConfig,
  validateConfig,
  resolveEnvTemplate,
  resolveConnection,
  platformsByDatabase
};
