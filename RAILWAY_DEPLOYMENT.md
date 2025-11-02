# Calculadora de Precios - Railway Deployment

Este proyecto es una calculadora de precios para ferretería con gestión de listas de productos, búsqueda por código de barras y cálculos avanzados.

## Despliegue en Railway

### Requisitos previos
1. Cuenta en [Railway](https://railway.app/)
2. Cuenta en [GitHub](https://github.com/)
3. Este repositorio conectado a GitHub

### Pasos para desplegar

#### 1. Preparar el repositorio local
```bash
# Asegurarse de que todos los cambios estén commiteados
git add .
git commit -m "Preparar para deployment en Railway"
git push origin main
```

#### 2. Crear proyecto en Railway
1. Ir a [Railway Dashboard](https://railway.app/dashboard)
2. Click en "New Project"
3. Seleccionar "Deploy from GitHub repo"
4. Autorizar acceso a GitHub si es la primera vez
5. Seleccionar el repositorio `consulta_de_precios_calculador`

#### 3. Agregar PostgreSQL
1. En el proyecto de Railway, click en "+ New"
2. Seleccionar "Database" → "Add PostgreSQL"
3. Railway creará automáticamente la variable `DATABASE_URL`

#### 4. Configurar Volume para persistir archivos Excel (IMPORTANTE)

Los contenedores de Railway son efímeros y se recrean en cada deploy. Para que los archivos Excel no desaparezcan, necesitas crear un Volume:

1. En tu servicio principal, ve a **"Settings"**
2. Scroll hasta la sección **"Volumes"**
3. Click en **"+ New Volume"**
4. Configura:
   - **Mount Path**: `/app/listas_excel`
   - **Name**: Deja el nombre automático o usa `listas-excel`
5. Click en **"Add"**

✅ Ahora todos los archivos Excel que subas se guardarán en el Volume y persistirán entre deploys.

#### 5. Configurar variables de entorno
En la pestaña "Variables" del servicio principal, agregar:

```
LISTAS_EN_DB=1
SECRET_KEY=<generar-una-clave-segura-aleatoria>
USE_SQLITE=0
DEBUG_LOG=0
APP_TZ=America/Argentina/Buenos_Aires
VENTAS_AVANZADAS_PER_PAGE=20
LISTAS_PATH=/app/listas_excel
```

Railway ya provee automáticamente:
- `DATABASE_URL` (desde el servicio PostgreSQL)
- `PORT` (puerto asignado por Railway)

**Nota sobre LISTAS_PATH**: Aunque la app usa `/app/listas_excel` por defecto, es buena práctica definirlo explícitamente para que coincida con el mount path del Volume.

#### 6. Inicializar la base de datos (IMPORTANTE)

**Si obtienes error "relation 'import_batches' does not exist":**

Railway debe ejecutar `ensure_pg_tables()` automáticamente al iniciar, pero si las tablas no se crean, ejecuta manualmente desde el terminal de Railway:

1. En Railway, ve a tu servicio → pestaña "Settings" → scroll hasta "Deploy Logs"
2. Busca los mensajes `[INFO] Inicializando tablas PostgreSQL...` y `[SUCCESS] Tablas PostgreSQL verificadas`
3. Si no aparecen o hay errores, abre el terminal de Railway (ícono de terminal en la esquina superior derecha)
4. Ejecuta:
```bash
python init_db.py
```

Este script creará todas las tablas necesarias manualmente. Deberías ver:
```
[SUCCESS] ✓ Todas las tablas creadas correctamente.
[INFO] Tablas encontradas en la base de datos:
  - historial
  - import_batches
  - productos_listas
  - proveedores
  - usuarios
```

5. Reinicia el servicio después de la inicialización (Settings → "Restart")

#### 6. Deploy
1. Railway detectará automáticamente que es una app Python/Flask
2. Usará `requirements.txt` para instalar dependencias
3. Ejecutará el comando definido en `Procfile` o detectará `app_v5.py`
4. El despliegue iniciará automáticamente

### Configuración de PostgreSQL

La app creará las tablas automáticamente en el primer inicio:
- `proveedores`
- `historial`
- `usuarios`
- `productos_listas`
- `import_batches`

### Sincronización de listas

**IMPORTANTE**: Las listas Excel no se suben al repositorio por seguridad y tamaño.

Opciones para gestionar listas en Railway:

1. **Subir manualmente** (recomendado para inicio):
   - Usar la interfaz web de la app (pestaña Gestión)
   - Subir archivos Excel uno por uno
   - Click en "Sincronizar ahora" para importar a PostgreSQL

2. **Usar volúmenes de Railway** (para persistencia):
   - Configurar un volumen en Railway montado en `/app/listas_excel`
   - Subir archivos vía la interfaz web
   - Los archivos persisten entre deploys

3. **Automatización** (avanzado):
   - Configurar un bucket S3/Google Cloud Storage
   - Sincronizar listas desde el bucket al iniciar

### Acceso a la aplicación

Después del deploy:
1. Railway te dará una URL pública (ej: `https://tu-app.up.railway.app`)
2. Acceder con las credenciales por defecto:
   - Usuario: `CPauluk`
   - Contraseña: `20052016`
3. **IMPORTANTE**: Cambiar las credenciales inmediatamente desde la interfaz

### Mantenimiento

#### Ver logs
```bash
# En Railway Dashboard → Service → Logs
```

#### Ejecutar sincronización manual
Acceder a: `https://tu-app.up.railway.app/admin/sync_listas`

#### Backup de PostgreSQL
Railway ofrece backups automáticos en planes pagos. Para backup manual:
1. Ir a PostgreSQL service → Data
2. Descargar dump de la base

### Troubleshooting

#### Error de conexión a PostgreSQL
- Verificar que `DATABASE_URL` esté configurada
- Revisar logs para mensajes de conexión

#### Archivos Excel no persisten
- Usar volúmenes de Railway para persistencia
- O re-subir después de cada deploy

#### Puerto incorrecto
- Railway asigna `PORT` automáticamente
- El código ya lo detecta con `os.getenv('PORT')`

### Estructura del proyecto

```
.
├── app_v5.py                 # Aplicación principal Flask
├── requirements.txt          # Dependencias Python
├── runtime.txt              # Versión de Python
├── Procfile                 # Comando de inicio
├── .env.example            # Variables de entorno ejemplo
├── templates/              # Plantillas HTML
├── listas_excel/          # Listas de precios (no en repo)
└── GUIA_INTEGRACION_POSTGRESQL.md  # Guía técnica
```

### Seguridad

- Cambiar `SECRET_KEY` a un valor aleatorio seguro
- Cambiar credenciales de usuario por defecto
- No subir archivos `.env` al repositorio
- Usar variables de entorno de Railway para secretos

### Soporte

Para más información técnica sobre PostgreSQL, ver:
- `GUIA_INTEGRACION_POSTGRESQL.md` en el repositorio
