# WebSQL Runner

Aplicación web para ejecutar consultas SQL en **SQL Server**, **MySQL** o **PostgreSQL** desde una sola interfaz.

La app usa únicamente conexiones predeterminadas por plataforma. Las bases disponibles se definen en [`databases.json`](databases.json) y las credenciales en variables de entorno (ver [Agregar una base de ejemplo](#agregar-una-base-de-ejemplo)). Hoy hay dos bases disponibles en los tres motores, que se eligen desde el selector de la interfaz:

| Motor | Servidor | Base | Usuario |
| --- | --- | --- | --- |
| SQL Server | `msjoi.database.windows.net` | `pampero` | `unpazuser` |
| SQL Server | `msjoi.database.windows.net` | `library` | `unpazuser2` |
| MySQL | `myjoi.mysql.database.azure.com` | `pampero` / `library` | `unpazuser` |
| PostgreSQL | `pgjoi.postgres.database.azure.com` | `pampero` / `library` | `unpazuser` |

- `pampero` es la base principal (la que definen `*_DATABASE`).
- `library` es una segunda base en el mismo servidor. En MySQL y PostgreSQL usa el mismo usuario que `pampero`; en SQL Server usa un usuario propio (`SQLSERVER_LIBRARY_USER`).

## Requisitos

- Node.js 20+
- Acceso de red a la base de datos objetivo

## Ejecutar localmente

1. Instala dependencias:
   ```bash
   npm install
   ```
2. Copia variables de entorno:
   ```bash
   copy .env.example .env
   ```
3. Inicia la app:
   ```bash
   npm run dev
   ```
4. Abre `http://localhost:3000`

## Variables de entorno

- `PORT`: puerto HTTP de la app.
- `READ_ONLY_MODE`: `true` (default) para permitir solo consultas de lectura, `false` para habilitar escritura.
- `QUERY_TIMEOUT_MS`: timeout global de consultas en milisegundos (default: `15000`).
- `QUERY_STATS_LOG_PATH`: ruta del archivo de auditoria estadistica en formato JSONL (default: `logs/query-stats.jsonl`).
- `CORS_ALLOWED_ORIGINS` (opcional): orígenes permitidos separados por coma.
- `ADMIN_KEY` (opcional, recomendado en producción): si se define, el panel docente (`admin.html`) y `GET /api/stats` exigen `?key=<ADMIN_KEY>` en la URL. Sin ella, el panel es público y muestra IPs y consultas de los alumnos.
- `MAX_RESULT_ROWS`: máximo de filas devueltas por consulta (default: `500`).
- `SQLSERVER_POOL_MAX`, `SQLSERVER_POOL_MIN`, `MYSQL_POOL_MAX`, `POSTGRES_POOL_MAX` (opcionales): tamaño del pool de conexiones por motor. El pool vive por proceso, así que conviene correr una sola instancia o dividir estos valores por la cantidad de instancias, respetando el tope de conexiones de cada base.
- `SQLSERVER_HOST`, `SQLSERVER_PORT`, `SQLSERVER_DATABASE`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD`
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_SSL`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SSL`
- Base `library`: `SQLSERVER_LIBRARY_DATABASE`, `SQLSERVER_LIBRARY_USER`, `SQLSERVER_LIBRARY_PASSWORD` (obligatorios en SQL Server). En MySQL y PostgreSQL son opcionales `MYSQL_LIBRARY_DATABASE`/`_USER`/`_PASSWORD` y `POSTGRES_LIBRARY_DATABASE`/`_USER`/`_PASSWORD`; sin usuario propio se reutiliza el principal, que debería tener solo `SELECT` sobre `library`.

- `DATABASES_CONFIG` (opcional): ruta a otro archivo de bases en lugar de `databases.json`.

### Base `library`

- SQL Server: `SQLSERVER_LIBRARY_DATABASE` (default `library`), `SQLSERVER_LIBRARY_USER`, `SQLSERVER_LIBRARY_PASSWORD`. Reutiliza `SQLSERVER_HOST` y `SQLSERVER_PORT`.
- MySQL: `MYSQL_LIBRARY_DATABASE` (default `library`). Reutiliza host, puerto, usuario, contraseña y SSL de MySQL.
- PostgreSQL: `POSTGRES_LIBRARY_DATABASE` (default `library`). Reutiliza host, puerto, usuario, contraseña y SSL de PostgreSQL.

## Agregar una base de ejemplo

Las bases se listan en `databases.json`; no hace falta tocar código. Cada base indica en qué motores existe, y la interfaz (selector, datos de conexión, explorador de esquema) se arma sola a partir de ese archivo.

```json
{
  "default": "pampero",
  "databases": {
    "pampero": { "...": "..." },
    "tienda": {
      "mysql": {},
      "postgresql": {},
      "sqlserver": {
        "user": "${SQLSERVER_TIENDA_USER}",
        "password": "${SQLSERVER_TIENDA_PASSWORD}"
      }
    }
  }
}
```

- El nombre de la base (`tienda`) va en minúsculas y es lo que se ve en el selector y lo que usan las guías de ejercicios en `"database"`.
- Una entrada vacía (`{}`) usa el servidor, puerto, usuario y contraseña principales del motor (`MYSQL_HOST`, `MYSQL_USER`, etc.) y una base con el mismo nombre.
- Campos opcionales por motor: `database`, `user`, `password`, `host`, `port` y `ssl` (este último solo MySQL/PostgreSQL). Si se define `user`, también hay que definir `password`.
- Los valores pueden leer variables de entorno: `${VAR}` es obligatoria (si falta, esa base da error al usarla) y `${VAR:-valor}` es opcional con valor por defecto. Si `user` queda vacío (por ejemplo `"${MYSQL_TIENDA_USER:-}"` sin definir), se usa el usuario principal del motor.
- Las contraseñas van siempre en variables de entorno (en Azure, en la configuración de la Web App), nunca escritas en `databases.json`.
- `default` es la base que se usa cuando no se indica ninguna.

Después de editar el archivo hay que reiniciar la app (en Azure se reinicia sola al desplegar el push a `main`). Para que los alumnos solo puedan leer, el usuario de cada base nueva debería tener únicamente permisos de `SELECT`.

## Registro estadistico de consultas

Cada llamada a `POST /api/query` se registra como una linea JSON en `QUERY_STATS_LOG_PATH`.

Campos registrados por evento:

- `timestamp`: fecha/hora UTC ISO 8601.
- `ip`: IP de origen del request.
- `platform`: motor objetivo (`sqlserver`, `mysql`, `postgresql`).
- `query`: texto SQL enviado.
- `success`: `true`/`false`.
- `statusCode`: codigo HTTP devuelto.
- `durationMs`: duracion total backend en milisegundos.
- `error`: mensaje de error (solo en fallas).

## Despliegue en Azure App Service

### Opción 1: Azure App Service (Linux, recomendado)

1. Crea un App Service para Node.js 20.
2. Configura las App Settings necesarias (por ejemplo `PORT` lo gestiona Azure automáticamente).
3. Publica desde GitHub Actions, Azure DevOps o Zip Deploy.
4. Azure ejecutará `npm install` y `npm start`.

### Opción 2: Azure App Service (Windows)

- Incluye `web.config` para integración con IISNode.
- Mantén `server.js` en la raíz del proyecto.

## API

### `POST /api/query`

Body JSON:

```json
{
  "platform": "sqlserver | mysql | postgresql",
  "database": "pampero | library | (cualquier base de databases.json)",
  "query": "SELECT 1 AS ok;"
}
```

Respuesta:

```json
{
  "platform": "postgresql",
   "durationMs": 10,
   "connectMs": 2,
   "queryMs": 1,
  "columns": ["ok"],
  "rows": [{ "ok": 1 }],
  "rowCount": 1,
  "info": null
}
```

- `database`: opcional, default `pampero`.
- `durationMs`: tiempo total de la operación HTTP en backend.
- `connectMs`: tiempo para adquirir/conectar desde el pool del motor.
- `queryMs`: tiempo de ejecución de la consulta en el motor.

## Restricciones de ejecución

- Solo se permite **una sentencia SQL por ejecución**.
- Con `READ_ONLY_MODE=true`, solo se permiten sentencias de lectura (`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `DESC`, `EXPLAIN`). Se rechazan además las que contengan palabras de escritura fuera de textos y comentarios (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `INTO`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `GRANT`, `REVOKE`, `EXEC`, `EXECUTE`, `CALL`), para cubrir casos como `WITH ... DELETE` o `SELECT ... INTO`. Este filtro es una ayuda: la protección real son los permisos del usuario de base de datos.

## Seguridad

- Esta app ejecuta SQL arbitrario con credenciales definidas por entorno.
- Úsala en entornos controlados, con usuarios de BD de mínimo privilegio.
- Restringe CORS y protege acceso con autenticación si se publicará en internet.
