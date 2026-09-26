# WebSQL Runner

Aplicación web para ejecutar consultas SQL en **SQL Server**, **MySQL** o **PostgreSQL** desde una sola interfaz.

La app usa únicamente conexiones predeterminadas por plataforma, configuradas por variables de entorno. Hay dos bases disponibles en los tres motores, que se eligen desde el selector de la interfaz:

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

### Base `library`

- SQL Server: `SQLSERVER_LIBRARY_DATABASE` (default `library`), `SQLSERVER_LIBRARY_USER`, `SQLSERVER_LIBRARY_PASSWORD`. Reutiliza `SQLSERVER_HOST` y `SQLSERVER_PORT`.
- MySQL: `MYSQL_LIBRARY_DATABASE` (default `library`). Reutiliza host, puerto, usuario, contraseña y SSL de MySQL.
- PostgreSQL: `POSTGRES_LIBRARY_DATABASE` (default `library`). Reutiliza host, puerto, usuario, contraseña y SSL de PostgreSQL.

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
  "database": "pampero | library",
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
- Con `READ_ONLY_MODE=true`, solo se permiten sentencias de lectura (`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `DESC`, `EXPLAIN`).

## Seguridad

- Esta app ejecuta SQL arbitrario con credenciales definidas por entorno.
- Úsala en entornos controlados, con usuarios de BD de mínimo privilegio.
- Restringe CORS y protege acceso con autenticación si se publicará en internet.
