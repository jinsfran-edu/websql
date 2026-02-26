# WebSQL Runner

Aplicación web para ejecutar consultas SQL en **SQL Server**, **MySQL** o **PostgreSQL** desde una sola interfaz.

La app usa únicamente conexiones predeterminadas por plataforma, configuradas por variables de entorno:

- SQL Server: 
- MySQL: 
- PostgreSQL: 
- Base de datos: 
- Usuario: 

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
- `CORS_ALLOWED_ORIGINS` (opcional): orígenes permitidos separados por coma.
- `SQLSERVER_HOST`, `SQLSERVER_PORT`, `SQLSERVER_DATABASE`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD`
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_SSL`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SSL`

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
