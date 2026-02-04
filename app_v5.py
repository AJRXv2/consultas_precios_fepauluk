# --- IMPORTACIONES ---
from flask import Flask, render_template, request, send_from_directory, abort, redirect, url_for, session, jsonify
import traceback
import os
import json
import tempfile
import sys
import webbrowser
from threading import Timer
from waitress import serve
import uuid 
from datetime import datetime
import math
import sqlite3
import socket
import pandas as pd
import re
import unicodedata
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
try:
    from zoneinfo import ZoneInfo
    APP_TZ_NAME = os.getenv('APP_TZ', 'America/Argentina/Buenos_Aires')
    _APP_TZ = ZoneInfo(APP_TZ_NAME)
except Exception:
    _APP_TZ = None


def now_local():
    """Devuelve datetime ahora en la zona configurada (Argentina por defecto)."""
    return datetime.now(_APP_TZ) if _APP_TZ else datetime.now()


def ts_to_local(ts: float):
    """Convierte un timestamp (epoch seconds) a datetime local."""
    try:
        return datetime.fromtimestamp(ts, _APP_TZ) if _APP_TZ else datetime.fromtimestamp(ts)
    except Exception:
        return datetime.fromtimestamp(ts)


try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # Permite correr sin PostgreSQL hasta instalar deps
    psycopg = None
    dict_row = None

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):  # type: ignore
        return False

try:
    load_dotenv()
except Exception:
    pass

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-change-me')
app.config['MAX_CONTENT_LENGTH'] = 25 * 1024 * 1024  # 25MB por archivo
app.config['UPLOAD_EXTENSIONS'] = ['.xlsx', '.xls']

# Paginación por defecto para Ventas Avanzadas (configurable por variable de entorno)
VENTAS_AVANZADAS_PER_PAGE = int(os.getenv('VENTAS_AVANZADAS_PER_PAGE', '20'))

if getattr(sys, 'frozen', False):
    BASE_PATH = os.path.dirname(sys.executable)
else:
    BASE_PATH = os.path.dirname(__file__)

DATA_FILE = os.path.join(BASE_PATH, "datos_v2.json")
HISTORIAL_FILE = os.path.join(BASE_PATH, "historial.json")
LISTAS_PATH = os.getenv('LISTAS_PATH', os.path.join(BASE_PATH, "listas_excel"))
AUTH_FILE = os.path.join(BASE_PATH, "auth.json")
SQLITE_DB_PATH = os.getenv('SQLITE_DB_PATH', os.path.join(BASE_PATH, "app_v5.sqlite3"))

os.makedirs(LISTAS_PATH, exist_ok=True)
app.config['UPLOAD_FOLDER'] = LISTAS_PATH

sqlite_dir = os.path.dirname(SQLITE_DB_PATH) or "."
os.makedirs(sqlite_dir, exist_ok=True)

DATABASE_URL = os.getenv('DATABASE_URL') if psycopg else None
USE_SQLITE = os.getenv('USE_SQLITE', '1' if not DATABASE_URL else '0').strip().lower() in ('1', 'true', 'yes', 'y')
DEBUG_LOG = os.getenv('DEBUG_LOG', '0').strip().lower() in ('1', 'true', 'yes', 'y')
LISTAS_EN_DB = os.getenv('LISTAS_EN_DB', '0').strip().lower() in ('1', 'true', 'yes', 'y')

# Archivo de configuración persistente
# En Railway, usar el volume montado en /app/listas_excel para persistencia
# En local, usar el directorio actual
if os.path.exists(LISTAS_PATH):
    CONFIG_FILE_PATH = os.path.join(LISTAS_PATH, 'app_config.json')
else:
    CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'app_config.json')

def load_app_config():
    """Carga la configuración persistente desde archivo JSON"""
    default_config = {
        'usar_fallback_excel': True,
        'modo_barcode_inteligente': True,  # Modo mejorado por defecto
        'busqueda_barcode_optimizada': True  # Búsqueda rápida por defecto
    }
    try:
        if os.path.exists(CONFIG_FILE_PATH):
            print(f'[CONFIG] Cargando configuración desde {CONFIG_FILE_PATH}', flush=True)
            with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
                saved_config = json.load(f)
                print(f'[CONFIG] Configuración cargada: {saved_config}', flush=True)
                return saved_config
        else:
            print(f'[CONFIG] No existe {CONFIG_FILE_PATH}, usando configuración por defecto', flush=True)
        return default_config
    except Exception as e:
        print(f'[CONFIG] Error cargando app_config.json: {e}', flush=True)
        return default_config

def save_app_config(config):
    """Guarda la configuración persistente en archivo JSON"""
    try:
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        print(f'[CONFIG] Configuración guardada en {CONFIG_FILE_PATH}', flush=True)
        return True
    except Exception as e:
        print(f'[CONFIG] Error guardando app_config.json: {e}', flush=True)
        return False

# Cargar configuración persistente (prioridad: archivo > variable de entorno)
app_config = load_app_config()
USAR_FALLBACK_EXCEL = app_config.get('usar_fallback_excel', 
                                     os.getenv('USAR_FALLBACK_EXCEL', '1').strip().lower() in ('1', 'true', 'yes', 'y'))
MODO_BARCODE_INTELIGENTE = app_config.get('modo_barcode_inteligente', True)
BUSQUEDA_BARCODE_OPTIMIZADA = app_config.get('busqueda_barcode_optimizada', True)
print(f'[CONFIG] app_config obtenido: {app_config}', flush=True)
print(f'[CONFIG] USAR_FALLBACK_EXCEL final: {USAR_FALLBACK_EXCEL}', flush=True)
print(f'[CONFIG] MODO_BARCODE_INTELIGENTE final: {MODO_BARCODE_INTELIGENTE}', flush=True)
print(f'[CONFIG] BUSQUEDA_BARCODE_OPTIMIZADA final: {BUSQUEDA_BARCODE_OPTIMIZADA}', flush=True)

# Print de configuración al iniciar
print('=' * 60, flush=True)
print('[CONFIG] Configuración de la aplicación:', flush=True)
print(f'[CONFIG] DATABASE_URL: {"Configurado" if DATABASE_URL else "NO configurado"}', flush=True)
print(f'[CONFIG] psycopg disponible: {psycopg is not None}', flush=True)
print(f'[CONFIG] USE_SQLITE: {USE_SQLITE}', flush=True)
print(f'[CONFIG] LISTAS_EN_DB: {LISTAS_EN_DB}', flush=True)
print(f'[CONFIG] USAR_FALLBACK_EXCEL: {USAR_FALLBACK_EXCEL}', flush=True)
print(f'[CONFIG] DEBUG_LOG: {DEBUG_LOG}', flush=True)
print(f'[CONFIG] Condición para búsqueda en DB: LISTAS_EN_DB={LISTAS_EN_DB} AND DATABASE_URL={bool(DATABASE_URL)} AND psycopg={psycopg is not None} = {LISTAS_EN_DB and DATABASE_URL and psycopg}', flush=True)
print('=' * 60, flush=True)


def log_debug(*parts):
    if DEBUG_LOG:
        try:
            print('[DEBUG]', *parts, flush=True)
        except Exception:
            pass


def get_pg_conn():
    if not DATABASE_URL or not psycopg:
        log_debug('get_pg_conn: sin DATABASE_URL o psycopg no disponible.')
        return None
    try:
        kwargs = {'row_factory': dict_row} if dict_row else {}
        conn = psycopg.connect(DATABASE_URL, **kwargs)
        log_debug('Conexión PostgreSQL establecida.')
        return conn
    except Exception as e:
        log_debug('Error conectando a PostgreSQL:', e)
        return None


def ensure_pg_tables():
    if not DATABASE_URL or not psycopg:
        log_debug('ensure_pg_tables: omite (sin DB).')
        return
    conn = get_pg_conn()
    if not conn:
        log_debug('ensure_pg_tables: no se pudo obtener conexión.')
        print('[ERROR] ensure_pg_tables: No se pudo conectar a PostgreSQL. Verifica DATABASE_URL.', flush=True)
        return
    
    # Configurar autocommit para que las tablas estén disponibles inmediatamente
    conn.autocommit = True
    
    try:
        with conn.cursor() as cur:
            print('[INFO] Inicializando tablas PostgreSQL...', flush=True)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS proveedores (
                    id TEXT PRIMARY KEY,
                    data JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS historial (
                    id_historial TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    tipo_calculo TEXT,
                    proveedor_nombre TEXT,
                    producto TEXT,
                    precio_base DOUBLE PRECISION,
                    porcentajes JSONB,
                    precio_final DOUBLE PRECISION,
                    observaciones TEXT
                );
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                -- Nuevas tablas para listas importadas desde Excel
                CREATE TABLE IF NOT EXISTS import_batches (
                    id BIGSERIAL PRIMARY KEY,
                    proveedor_key TEXT,
                    archivo TEXT,
                    mtime DOUBLE PRECISION,
                    started_at TIMESTAMP DEFAULT NOW(),
                    completed_at TIMESTAMP,
                    status TEXT,
                    total_rows INT,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS productos_listas (
                    id BIGSERIAL PRIMARY KEY,
                    proveedor_key TEXT NOT NULL,
                    proveedor_nombre TEXT,
                    archivo TEXT NOT NULL,
                    hoja TEXT NOT NULL,
                    mtime DOUBLE PRECISION NOT NULL,
                    codigo TEXT,
                    codigo_digitos TEXT,
                    codigo_normalizado TEXT,
                    nombre TEXT,
                    nombre_normalizado TEXT,
                    precio NUMERIC(14,4),
                    precio_fuente TEXT,
                    iva TEXT,
                    precios JSONB,
                    extra_datos JSONB,
                    batch_id BIGINT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_prod_listas_prov_codigo ON productos_listas (proveedor_key, codigo);
                CREATE INDEX IF NOT EXISTS idx_prod_listas_codigo_dig ON productos_listas (codigo_digitos);
                CREATE INDEX IF NOT EXISTS idx_prod_listas_arch_hoja ON productos_listas (archivo, hoja);
                """
            )
            
            # Agregar columna iva si no existe (para migración de tablas existentes)
            try:
                cur.execute("ALTER TABLE productos_listas ADD COLUMN IF NOT EXISTS iva TEXT;")
                log_debug('ensure_pg_tables: columna iva verificada.')
            except Exception as col_err:
                log_debug('ensure_pg_tables: error verificando columna iva:', col_err)
            
            # Intentar crear índice GIN para búsquedas de texto con pg_trgm (requiere extensión habilitada)
            try:
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_prod_listas_nombre_trgm 
                    ON productos_listas USING GIN (nombre_normalizado gin_trgm_ops);
                    """
                )
                log_debug('ensure_pg_tables: índice GIN trgm creado o ya existe.')
                print('[INFO] Índice GIN trgm creado.', flush=True)
            except Exception as trgm_err:
                log_debug('ensure_pg_tables: no se pudo crear índice GIN trgm (¿pg_trgm no habilitado?):', trgm_err)
                print(f'[WARN] Índice GIN trgm no creado (extensión pg_trgm no habilitada): {trgm_err}', flush=True)
        
        print('[SUCCESS] Tablas PostgreSQL verificadas correctamente.', flush=True)
        log_debug('ensure_pg_tables: tablas verificadas.')
    except Exception as e:
        log_debug('ensure_pg_tables: error creando tablas:', e)
        print(f'[ERROR] Error crítico inicializando tablas: {e}', flush=True)
        print(f'[ERROR] Traceback: {traceback.format_exc()}', flush=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_sqlite_conn():
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_sqlite_tables():
    if not USE_SQLITE:
        return
    try:
        with get_sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS proveedores (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS historial (
                    id_historial TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    tipo_calculo TEXT,
                    proveedor_nombre TEXT,
                    producto TEXT,
                    precio_base REAL,
                    porcentajes TEXT,
                    precio_final REAL,
                    observaciones TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS usuarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()
        log_debug('ensure_sqlite_tables: tablas verificadas.')
    except Exception as e:
        log_debug('ensure_sqlite_tables: error creando tablas:', e)


if USE_SQLITE:
    ensure_sqlite_tables()
elif DATABASE_URL:
    ensure_pg_tables()


def maybe_migrate_historial_json_to_pg():
    if not DATABASE_URL or not psycopg:
        return
    try:
        if not os.path.exists(HISTORIAL_FILE):
            return
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM historial")
            row = cur.fetchone()
            count = (row or {}).get('c', 0)
            if count != 0:
                return
            try:
                with open(HISTORIAL_FILE, 'r', encoding='utf-8') as f:
                    datos_json = json.load(f)
            except Exception:
                return
            if not isinstance(datos_json, list) or not datos_json:
                return
            inserted = 0
            for item in datos_json:
                try:
                    porcentajes_json = json.dumps(item.get('porcentajes', {}), ensure_ascii=False)
                    cur.execute(
                        """
                        INSERT INTO historial (id_historial, timestamp, tipo_calculo, proveedor_nombre, producto,
                                               precio_base, porcentajes, precio_final, observaciones)
                        VALUES (%(id_historial)s, %(timestamp)s, %(tipo_calculo)s, %(proveedor_nombre)s, %(producto)s,
                                %(precio_base)s, %(porcentajes)s::jsonb, %(precio_final)s, %(observaciones)s)
                        ON CONFLICT (id_historial) DO NOTHING
                        """,
                        {
                            'id_historial': item.get('id_historial', str(uuid.uuid4())),
                            'timestamp': item.get('timestamp', ''),
                            'tipo_calculo': item.get('tipo_calculo'),
                            'proveedor_nombre': item.get('proveedor_nombre'),
                            'producto': item.get('producto'),
                            'precio_base': item.get('precio_base'),
                            'porcentajes': porcentajes_json,
                            'precio_final': item.get('precio_final'),
                            'observaciones': item.get('observaciones')
                        }
                    )
                    inserted += 1
                except Exception as e:
                    log_debug('maybe_migrate_historial_json_to_pg: falla item', e)
            conn.commit()
            if inserted:
                log_debug(f'maybe_migrate_historial_json_to_pg: migradas {inserted} filas a PG.')
    except Exception as e:
        log_debug('maybe_migrate_historial_json_to_pg: error general', e)


def maybe_migrate_historial_json_to_sqlite():
    if not USE_SQLITE:
        return
    try:
        if not os.path.exists(HISTORIAL_FILE):
            return
        with get_sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS c FROM historial")
            row = cur.fetchone()
            count = (row or {}).get('c', 0) if isinstance(row, dict) else (row['c'] if row else 0)
            if count != 0:
                return
            try:
                with open(HISTORIAL_FILE, 'r', encoding='utf-8') as f:
                    datos_json = json.load(f)
            except Exception:
                return
            if not isinstance(datos_json, list) or not datos_json:
                return
            inserted = 0
            for item in datos_json:
                try:
                    porcentajes_json = json.dumps(item.get('porcentajes', {}), ensure_ascii=False)
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO historial (id_historial, timestamp, tipo_calculo, proveedor_nombre, producto,
                                                         precio_base, porcentajes, precio_final, observaciones)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item.get('id_historial', str(uuid.uuid4())),
                            item.get('timestamp', ''),
                            item.get('tipo_calculo'),
                            item.get('proveedor_nombre'),
                            item.get('producto'),
                            item.get('precio_base'),
                            porcentajes_json,
                            item.get('precio_final'),
                            item.get('observaciones')
                        )
                    )
                    inserted += 1
                except Exception as e:
                    log_debug('maybe_migrate_historial_json_to_sqlite: falla item', e)
            conn.commit()
            if inserted:
                log_debug(f'maybe_migrate_historial_json_to_sqlite: migradas {inserted} filas a SQLite.')
    except Exception as e:
        log_debug('maybe_migrate_historial_json_to_sqlite: error general', e)


if DATABASE_URL and psycopg:
    maybe_migrate_historial_json_to_pg()
else:
    maybe_migrate_historial_json_to_sqlite()

# --- AUTENTICACIÓN BÁSICA ---
def load_credentials():
    """Carga las credenciales desde PostgreSQL si está disponible; si no, desde archivo.
    Si no existen, crea el usuario por defecto.
    """
    # SQLite preferente si USE_SQLITE
    if USE_SQLITE:
        try:
            with get_sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, username, password_hash FROM usuarios ORDER BY id ASC LIMIT 1")
                row = cur.fetchone()
                if row:
                    return {'username': row['username'], 'password_hash': row['password_hash']}
                # No hay usuario -> crear por defecto
                default_hash = generate_password_hash('20052016')
                cur.execute("INSERT INTO usuarios (username, password_hash) VALUES (?, ?)", ('CPauluk', default_hash))
                conn.commit()
                return {'username': 'CPauluk', 'password_hash': default_hash}
        except Exception as e:
            log_debug('load_credentials: fallo SQLite, se usa archivo', e)
    # PostgreSQL preferente
    if DATABASE_URL and psycopg:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT username, password_hash FROM usuarios ORDER BY id ASC LIMIT 1")
                row = cur.fetchone()
                if row:
                    return {'username': row['username'], 'password_hash': row['password_hash']}
                # No hay usuario -> crear por defecto
                default_hash = generate_password_hash('20052016')
                cur.execute("INSERT INTO usuarios (username, password_hash) VALUES (%s, %s)", ('CPauluk', default_hash))
                conn.commit()
                return {'username': 'CPauluk', 'password_hash': default_hash}
        except Exception as e:
            log_debug('load_credentials: fallo PG, se usa archivo', e)
    # Fallback archivo
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'username' in data and 'password_hash' in data:
                    return data
        except Exception as e:
            log_debug('load_credentials: error leyendo auth.json', e)
    # Inicial por defecto en archivo
    creds = {'username': 'CPauluk', 'password_hash': generate_password_hash('20052016')}
    save_credentials(creds)
    return creds

def save_credentials(data):
    """Guarda credenciales en PostgreSQL o archivo según disponibilidad."""
    if USE_SQLITE:
        try:
            with get_sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM usuarios ORDER BY id ASC LIMIT 1")
                row = cur.fetchone()
                if row:
                    cur.execute("UPDATE usuarios SET username=?, password_hash=? WHERE id=?", (data['username'], data['password_hash'], row['id']))
                else:
                    cur.execute("INSERT INTO usuarios (username, password_hash) VALUES (?, ?)", (data['username'], data['password_hash']))
                conn.commit()
                return
        except Exception as e:
            log_debug('save_credentials: fallo SQLite, se guarda archivo', e)
    if DATABASE_URL and psycopg:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                # Intentar update primero
                cur.execute("UPDATE usuarios SET username=%s, password_hash=%s WHERE id = (SELECT id FROM usuarios ORDER BY id ASC LIMIT 1)", (data['username'], data['password_hash']))
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO usuarios (username, password_hash) VALUES (%s, %s)", (data['username'], data['password_hash']))
                conn.commit()
                return
        except Exception as e:
            log_debug('save_credentials: fallo PG, se guarda archivo', e)
    try:
        with open(AUTH_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_debug('save_credentials: error escribiendo auth.json', e)

credentials_cache = load_credentials()

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login', next=request.path))
        return fn(*args, **kwargs)
    return wrapper

@app.before_request
def inject_user():
    # Para acceso en templates
    request.current_user = session.get('username') if session.get('logged_in') else None

@app.route('/login', methods=['GET','POST'])
def login():
    global credentials_cache
    mensaje = None
    if request.method == 'POST':
        user = request.form.get('username','').strip()
        pwd = request.form.get('password','')
        # Recargar cache por si cambió en otro proceso
        credentials_cache = load_credentials()
        if user.lower() == credentials_cache['username'].lower() and check_password_hash(credentials_cache['password_hash'], pwd):
            session['logged_in'] = True
            session['username'] = credentials_cache['username']
            return redirect(request.args.get('next') or url_for('index'))
        else:
            mensaje = 'Credenciales inválidas.'
    return render_template('login.html', mensaje=mensaje)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/cambiar_credenciales', methods=['GET','POST'])
@login_required
def cambiar_credenciales():
    global credentials_cache
    mensaje = None
    exito = False
    if request.method == 'POST':
        actual = request.form.get('actual_password','')
        nuevo_user = request.form.get('nuevo_usuario','').strip()
        nuevo_pwd = request.form.get('nuevo_password','')
        nuevo_pwd2 = request.form.get('nuevo_password2','')
        # Refrescar credenciales actuales desde storage preferente
        credentials_cache = load_credentials()
        if not check_password_hash(credentials_cache['password_hash'], actual):
            mensaje = 'La contraseña actual no es correcta.'
        elif not nuevo_user or not nuevo_pwd:
            mensaje = 'Usuario y contraseña nuevos no pueden estar vacíos.'
        elif nuevo_pwd != nuevo_pwd2:
            mensaje = 'Las contraseñas nuevas no coinciden.'
        else:
            proposed = {
                'username': nuevo_user,
                'password_hash': generate_password_hash(nuevo_pwd)
            }
            try:
                save_credentials(proposed)
                credentials_cache = proposed
                session['username'] = nuevo_user
                mensaje = 'Credenciales actualizadas correctamente.'
                exito = True
            except Exception as e:
                mensaje = f'Error guardando nuevas credenciales: {e}'
    return render_template('cambiar_credenciales.html', mensaje=mensaje, exito=exito, usuario_actual=credentials_cache['username'])

@app.route('/configuracion', methods=['GET', 'POST'])
@login_required
def configuracion():
    global USAR_FALLBACK_EXCEL, MODO_BARCODE_INTELIGENTE, BUSQUEDA_BARCODE_OPTIMIZADA
    mensaje = None
    exito = False
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        if accion == 'toggle_fallback':
            USAR_FALLBACK_EXCEL = not USAR_FALLBACK_EXCEL
            # Guardar configuración de forma persistente
            app_config['usar_fallback_excel'] = USAR_FALLBACK_EXCEL
            if save_app_config(app_config):
                estado = "activado" if USAR_FALLBACK_EXCEL else "desactivado"
                mensaje = f'✅ Fallback a Excel {estado} correctamente. Este cambio es permanente.'
                exito = True
            else:
                mensaje = '❌ Error al guardar la configuración. El cambio es temporal.'
                exito = False
            print(f'[CONFIG] Fallback a Excel cambiado a: {USAR_FALLBACK_EXCEL}', flush=True)
        elif accion == 'toggle_barcode':
            MODO_BARCODE_INTELIGENTE = not MODO_BARCODE_INTELIGENTE
            # Guardar configuración de forma persistente
            app_config['modo_barcode_inteligente'] = MODO_BARCODE_INTELIGENTE
            if save_app_config(app_config):
                modo = "inteligente" if MODO_BARCODE_INTELIGENTE else "clásico"
                mensaje = f'✅ Modo de búsqueda por código de barras cambiado a {modo}. Este cambio es permanente.'
                exito = True
            else:
                mensaje = '❌ Error al guardar la configuración. El cambio es temporal.'
                exito = False
            print(f'[CONFIG] Modo barcode inteligente cambiado a: {MODO_BARCODE_INTELIGENTE}', flush=True)
        elif accion == 'toggle_optimizacion':
            BUSQUEDA_BARCODE_OPTIMIZADA = not BUSQUEDA_BARCODE_OPTIMIZADA
            # Guardar configuración de forma persistente
            app_config['busqueda_barcode_optimizada'] = BUSQUEDA_BARCODE_OPTIMIZADA
            if save_app_config(app_config):
                modo = "rápida (optimizada)" if BUSQUEDA_BARCODE_OPTIMIZADA else "lenta (secuencial)"
                mensaje = f'✅ Velocidad de búsqueda cambiada a {modo}. Este cambio es permanente.'
                exito = True
            else:
                mensaje = '❌ Error al guardar la configuración. El cambio es temporal.'
                exito = False
            print(f'[CONFIG] Búsqueda barcode optimizada cambiado a: {BUSQUEDA_BARCODE_OPTIMIZADA}', flush=True)
        elif accion == 'reiniciar_app':
            # Detectar si estamos en Railway
            es_railway = os.getenv('RAILWAY_ENVIRONMENT') is not None
            
            if es_railway:
                mensaje = '⚠️ Para reiniciar en Railway, ve al dashboard → tu proyecto → Settings → General → Restart Deployment'
                exito = False
            else:
                # Reinicio local
                try:
                    import sys
                    print('[CONFIG] Reiniciando aplicación...', flush=True)
                    os.execv(sys.executable, ['python'] + sys.argv)
                except Exception as e:
                    mensaje = f'❌ Error al intentar reiniciar: {e}'
                    exito = False
    
    # Información de la configuración actual
    config_info = {
        'db_tipo': None,
        'db_conectada': False,
        'listas_en_db': LISTAS_EN_DB,
        'usar_fallback_excel': USAR_FALLBACK_EXCEL,
        'modo_barcode_inteligente': MODO_BARCODE_INTELIGENTE,
        'busqueda_barcode_optimizada': BUSQUEDA_BARCODE_OPTIMIZADA,
        'psycopg_disponible': psycopg is not None,
        'es_railway': os.getenv('RAILWAY_ENVIRONMENT') is not None
    }
    
    # Detectar tipo de DB
    if DATABASE_URL:
        if 'postgres' in DATABASE_URL.lower():
            config_info['db_tipo'] = 'PostgreSQL'
        else:
            config_info['db_tipo'] = 'Otra'
        config_info['db_conectada'] = True
    elif USE_SQLITE:
        config_info['db_tipo'] = 'SQLite'
        config_info['db_conectada'] = os.path.exists(SQLITE_DB_PATH)
    
    return render_template('configuracion.html', config_info=config_info, mensaje=mensaje, exito=exito)

# --- ESTRUCTURA DE DATOS POR DEFECTO ---
default_proveedores = {
    "p001": {"nombre_base": "Ñañu", "descuento": 0.00, "iva": 0.21, "ganancia": 0.60, "es_dinamico": True},
    "p002": {"nombre_base": "Bermon", "descuento": 0.14, "iva": 0.21, "ganancia": 0.60, "es_dinamico": True},
    "p003": {"nombre_base": "Berger", "descuento": 0.10, "iva": 0.21, "ganancia": 0.60, "es_dinamico": True},
    "p004": {"nombre_base": "Cachan", "descuento": 0.26, "iva": 0.21, "ganancia": 0.50, "es_dinamico": True},
    "p005": {"nombre_base": "BremenTools", "descuento": 0.00, "iva": 0.21, "ganancia": 0.00, "es_dinamico": True},
    "p006": {"nombre_base": "BremenTools", "descuento": 0.00, "iva": 0.105, "ganancia": 0.00, "es_dinamico": True},
    "p007": {"nombre_base": "Crossmaster", "descuento": 0.07, "iva": 0.21, "ganancia": 0.60, "es_dinamico": True},
    "p008": {"nombre_base": "Chiesa", "descuento": 0.00, "iva": 0.21, "ganancia": 0.60, "es_dinamico": True},
    "p009": {"nombre_base": "Chiesa", "descuento": 0.00, "iva": 0.105, "ganancia": 0.60, "es_dinamico": True}
}

EXCEL_PROVIDER_CONFIG = {
    'brementools': {
        'fila_encabezado': 5,
        'codigo': ['codigo'],
        'producto': ['producto'],
        'precios_a_mostrar': ['precio', 'precio de venta', 'precio de lista', 'precio neto', 'precio neto unitario'],
        'iva': ['iva'],
        'extra_datos': ['unidades x caja', 'categoria', 'cantidad']
    },
    'crossmaster': {
        'fila_encabezado': 11,
        'codigo': ['codigo'],
        'producto': ['descripcion'],
        'precios_a_mostrar': ['precio lista'],
        'iva': ['iva'],
        'extra_datos': []
    },
    'berger': {
        'fila_encabezado': 0,
        'codigo': ['cod'],
        'producto': ['detalle'],
        'precios_a_mostrar': ['pventa'],
        'iva': ['iva'],
        'extra_datos': ['marca']
    },
    'chiesa': {
        'fila_encabezado': 1,
        'codigo': ['codigo'],
        'producto': ['descripcion'],
        'precios_a_mostrar': ['pr unit', 'prunit'],
        'iva': ['iva'],
        'extra_datos': ['dcto', 'oferta']
    },
    'cachan': {
        'fila_encabezado': 0,
        'codigo': ['codigo'],
        'producto': ['nombre'],
        'precios_a_mostrar': ['precio'],
        'iva': [],
        'extra_datos': ['marca']
    }
}

# --- FUNCIONES AUXILIARES ---
def normalize_text(text):
    text = str(text)
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]+', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def provider_name_to_key(name: str) -> str:
    if not name:
        return ''
    letters_only = ''.join(c for c in str(name) if c.isalpha())
    return normalize_text(letters_only)


def provider_key_from_filename(filename: str) -> str:
    base = os.path.splitext(filename)[0]
    return provider_name_to_key(base)


def get_proveedor_display_name(provider_key: str) -> str:
    try:
        for pdata in proveedores.values():
            nombre_base = pdata.get('nombre_base')
            if provider_name_to_key(nombre_base) == provider_key:
                return nombre_base
    except Exception:
        pass
    return provider_key.title()

def format_pct(valor):
    num_pct = abs(valor * 100) 
    if num_pct == int(num_pct):
        return f"{int(num_pct):02d}"
    else:
        return f"{num_pct:.1f}"

def generar_nombre_visible(prov_data):
    if not prov_data.get("es_dinamico", False):
        return prov_data.get("nombre_base", "Sin Nombre")
    base = prov_data.get("nombre_base", "")
    desc = prov_data.get("descuento", 0)
    iva = prov_data.get("iva", 0)
    ganc = prov_data.get("ganancia", 0)
    partes_nombre = [base]
    if desc != 0: partes_nombre.append(f"DESC{format_pct(desc)}")
    if iva != 0: partes_nombre.append(f"IVA{format_pct(iva)}")
    if ganc != 0: partes_nombre.append(f"GAN{format_pct(ganc)}")
    return " ".join(partes_nombre)

def parse_percentage(raw):
    if raw is None: return None
    s = str(raw).strip().replace("%", "").replace(",", ".")
    if s == "": return None
    try:
        v = float(s)
    except ValueError: return None
    if v > 1: v = v / 100.0
    return v

def formatear_precio(valor):
    if valor is None or not isinstance(valor, (int, float)):
        return "N/A"
    try:
        valor_float = float(str(valor).replace(",", "."))
        return f"{valor_float:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "N/A"

def formatear_pulgadas(nombre_producto):
    if not isinstance(nombre_producto, str):
        return nombre_producto

    # Función interna para reemplazar cada número encontrado
    def reemplazar(match):
        numero = match.group(0) # El número completo, ej: "516"
        
        # Si tiene 3 dígitos, es probable que sea X/16 o X/32. Asumimos /16.
        if len(numero) == 3:
            # Evita convertir números redondos como "100", "200", etc.
            if numero.endswith("00"):
                return numero
            return f"{numero[0]}/{numero[1:]}" # 516 -> 5/16
            
        # Si tiene 4 dígitos, es probable que sea XX/16 o XX/32. Asumimos /16.
        if len(numero) == 4:
            # Evita convertir años o números redondos
            if numero.endswith("00"):
                return numero
            return f"{numero[:2]}/{numero[2:]}" # 1116 -> 11/16

        # Si tiene 2 dígitos, podría ser 1/2, 1/4, 3/4, etc.
        if len(numero) == 2:
            return f"{numero[0]}/{numero[1]}" # 14 -> 1/4

        return numero # Devuelve el número original si no coincide

    # El regex ahora busca cualquier número de 2 a 4 dígitos que esté solo
    # (rodeado de espacios o al final de la cadena) para evitar modificar
    # códigos de producto como "AB1234".
    # \b es un "word boundary" o límite de palabra.
    return re.sub(r'\b(\d{2,4})\b', reemplazar, nombre_producto)

def parse_price_value(value):
    """
    Parsea valores de precio con formato argentino:
    - Punto (.) como separador de miles
    - Coma (,) como separador decimal
    Ejemplos: "1.234,56" -> 1234.56, "1234,56" -> 1234.56, "5000" -> 5000, "5.000" -> 5000
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    # Remover todo excepto números, comas y puntos
    text = re.sub(r"[^0-9,.-]", "", text)
    # Si hay coma, es decimal argentino. El punto siempre es miles.
    if ',' in text:
        # Quitar todos los puntos (miles), dejar solo la coma como decimal
        text = text.replace('.', '')
        text = text.replace(',', '.')
    else:
        # No hay coma, solo puede haber puntos como miles
        text = text.replace('.', '')
    try:
        return float(text)
    except ValueError:
        return None

def load_manual_products():
    cache = getattr(load_manual_products, "_cache", {})
    
    # Buscar archivo que empiece con 'productos_manual' (puede tener fecha agregada)
    file_path = None
    try:
        archivos = os.listdir(LISTAS_PATH)
        candidatos = []
        for fname in archivos:
            if fname.lower().startswith('productos_manual') and fname.lower().endswith(('.xlsx', '.xls')):
                # Ignorar versiones OLD
                if 'old' not in fname.lower():
                    candidatos.append(fname)
        
        if candidatos:
            # Si hay múltiples, tomar el más reciente por mtime
            candidatos_con_mtime = []
            for fname in candidatos:
                fpath = os.path.join(LISTAS_PATH, fname)
                try:
                    mtime = os.path.getmtime(fpath)
                    candidatos_con_mtime.append((fpath, mtime, fname))
                except Exception:
                    pass
            
            if candidatos_con_mtime:
                # Ordenar por mtime descendente y tomar el más reciente
                candidatos_con_mtime.sort(key=lambda x: x[1], reverse=True)
                file_path = candidatos_con_mtime[0][0]
    except Exception:
        pass
    
    if not file_path:
        file_path = os.path.join(LISTAS_PATH, "productos_manual.xlsx")
    
    try:
        mtime = os.path.getmtime(file_path)
    except FileNotFoundError:
        mtime = None
    except Exception:
        mtime = None
    if cache.get("mtime") == mtime and cache.get("file_path") == file_path:
        return cache.get("productos", []), cache.get("error")

    productos = []
    error = None
    if not os.path.isfile(file_path):
        error = "⚠️ No se encontró el archivo 'productos_manual.xlsx' (o variantes con fecha) dentro de listas_excel."
    else:
        try:
            # Leer columnas A (Código), B (Proveedor), C (Nombre), D (Precio)
            df = pd.read_excel(file_path, usecols="A,B,C,D", header=0)
            columnas = list(df.columns)
            while len(columnas) < 4:
                columnas.append(f"col{len(columnas)}")
            df = df.rename(columns={columnas[0]: "codigo", columnas[1]: "proveedor", columnas[2]: "nombre", columnas[3]: "precio"})
            df = df.dropna(subset=["codigo", "nombre"])
            for _, fila in df.iterrows():
                codigo = str(fila["codigo"]).strip()
                proveedor = str(fila.get("proveedor", "")).strip() if pd.notna(fila.get("proveedor")) else ""
                nombre = str(fila["nombre"]).strip()
                precio_raw = fila.get("precio")
                precio_parseado = parse_price_value(precio_raw)
                precio_valido = precio_parseado is not None and precio_parseado > 0
                if not codigo or not nombre:
                    continue
                productos.append({
                    "codigo": codigo,
                    "proveedor": proveedor,
                    "nombre": nombre,
                    "precio": float(precio_parseado) if precio_parseado is not None else 0.0,
                    "precio_valido": precio_valido,
                    "precio_fuente": precio_raw
                })
            productos.sort(key=lambda x: normalize_text(x["nombre"]))
            if not productos:
                error = "⚠️ No se encontraron filas válidas en archivo productos_manual."
        except Exception as exc:
            error = f"❌ Error leyendo archivo productos_manual: {exc}"
            productos = []

    load_manual_products._cache = {"mtime": mtime, "file_path": file_path, "productos": productos, "error": error}
    return productos, error

def buscar_productos_manual(productos, query):
    if not productos:
        return []
    if not query:
        return list(productos)

    query_normalizada = normalize_text(formatear_pulgadas(query))
    tokens = [token for token in query_normalizada.split() if token]

    log_debug(
        "buscar_productos_manual: inicio",
        {
            "query": query,
            "normalizada": query_normalizada,
            "tokens": tokens,
            "total_productos": len(productos),
        },
    )

    resultados = []
    for producto in productos:
        codigo = str(producto.get("codigo", ""))
        nombre = producto.get("nombre", "")
        nombre_proc = normalize_text(formatear_pulgadas(nombre))
        codigo_proc = normalize_text(codigo)
        combinado = f"{codigo_proc} {nombre_proc}".strip()

        coincide = False
        if tokens:
            coincide = all(token in combinado for token in tokens)
        if not coincide and query_normalizada:
            coincide = query_normalizada in nombre_proc or query_normalizada in codigo_proc
        if not tokens and not query_normalizada:
            coincide = True

        if coincide:
            resultados.append(producto)

    if resultados:
        ejemplo = [f"{p.get('codigo')} - {p.get('nombre')}" for p in resultados[:5]]
        log_debug(
            "buscar_productos_manual: resultados",
            {
                "total": len(resultados),
                "ejemplo": ejemplo,
                "query": query,
            },
        )
    else:
        log_debug(
            "buscar_productos_manual: sin_coincidencias",
            {
                "query": query,
                "normalizada": query_normalizada,
                "tokens": tokens,
            },
        )

    return resultados


def build_producto_entry(fila, actual_cols, provider_key, proveedor_display_name, sheet_name, df_columns, codigo_override=None):
    def _sanitize_value(value):
        if isinstance(value, str):
            value = value.strip()
        if pd.isna(value):
            return None
        return value

    codigo_str = ''
    if codigo_override is not None:
        codigo_str = str(codigo_override).strip()
    else:
        codigo_val = fila.get(actual_cols['codigo']) if actual_cols['codigo'] else None
        if pd.notna(codigo_val):
            codigo_str = str(codigo_val).strip()
            if codigo_str.endswith('.0'):
                codigo_str = codigo_str[:-2] or codigo_str

    producto_val = fila.get(actual_cols['producto']) if actual_cols['producto'] else None
    producto_texto = formatear_pulgadas(producto_val) if pd.notna(producto_val) else ''

    precios = {}
    for col in actual_cols['precios_a_mostrar']:
        precios[col.replace('_', ' ').title()] = _sanitize_value(fila.get(col))

    extra_datos = {}
    for col in actual_cols['extra_datos']:
        extra_datos[col.replace('_', ' ').title()] = _sanitize_value(fila.get(col))

    precios_calculados = {}

    if provider_key == 'brementools':
        precio_neto_col = next((alias for alias in ['precio neto unitario'] if alias in df_columns), None)
        if precio_neto_col and pd.notna(fila.get(precio_neto_col)):
            try:
                precio_neto = float(str(fila.get(precio_neto_col)).replace(',', '.'))
                precios_calculados['Precio Final Calculado'] = precio_neto * 1.21 * 1.60
            except (ValueError, TypeError):
                pass

    if provider_key == 'chiesa':
        precio_base_col = next((alias for alias in ['pr unit', 'prunit'] if alias in df_columns), None)
        if precio_base_col and pd.notna(fila.get(precio_base_col)):
            try:
                # Tomar el costo base SIN aplicar descuentos/oferta
                precio_base_raw = fila.get(precio_base_col)
                costo_base = parse_price_value(precio_base_raw)
                if costo_base is None:
                    raise ValueError('precio_base inválido')
                costo_base = float(costo_base)
                # Orden: base, -4%, +4%
                #precios_calculados['Costo (sin 4% extra)'] = costo_base
                precios_calculados['Costo (-4% extra)'] = costo_base * 0.96
                precios_calculados['Costo (+4% extra)'] = costo_base * 1.04
            except (ValueError, TypeError):
                pass

    producto_iva = 'N/A'
    if actual_cols['iva']:
        iva_val = fila.get(actual_cols['iva'])
        if pd.notna(iva_val):
            try:
                iva_val_str = str(iva_val).replace('%', '').replace(',', '.')
                iva_float = float(iva_val_str)
                if iva_float < 1.0 and iva_float != 0:
                    iva_float *= 100
                producto_iva = f"{iva_float:.1f}%".replace('.0%', '%')
            except Exception:
                producto_iva = str(iva_val)

    return {
        'codigo': codigo_str,
        'producto': producto_texto,
        'proveedor': f"{proveedor_display_name} (Hoja: {sheet_name})",
        'proveedor_key': provider_key,
        'sheet_name': sheet_name,
        'iva': producto_iva,
        'precios': precios,
        'extra_datos': extra_datos,
        'precios_calculados': precios_calculados,
        'fuente': 'Excel'
    }


def buscar_productos_por_codigos_multiples(codigos_lista: list, proveedor_filtrado: str = ''):
    """
    Búsqueda optimizada de productos por múltiples códigos a la vez.
    Hace UNA SOLA query en lugar de múltiples queries secuenciales.
    Mucho más rápido para códigos de barras con muchas variantes.
    """
    if not codigos_lista:
        return []
    
    # Limpiar códigos
    codigos_limpios = [c.strip() for c in codigos_lista if c and c.strip()]
    if not codigos_limpios:
        return []
    
    prov_key_filter = provider_name_to_key(proveedor_filtrado) if proveedor_filtrado else ''
    resultados = []
    
    # Si está habilitado el modo listas en DB y hay PostgreSQL disponible
    if LISTAS_EN_DB and DATABASE_URL and psycopg:
        print(f'[DEBUG buscar_productos_por_codigos_multiples] Buscando {len(codigos_limpios)} códigos en DB, prov_filter={prov_key_filter}', flush=True)
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                # Construir query con IN para buscar todos los códigos de una vez
                # Separar códigos numéricos de no-numéricos para optimizar
                codigos_numericos = [c for c in codigos_limpios if c.replace('.', '').isdigit()]
                codigos_no_numericos = [c for c in codigos_limpios if not c.replace('.', '').isdigit()]
                
                if codigos_numericos:
                    # Para códigos numéricos, usar la lógica expandida
                    if prov_key_filter:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE (
                                codigo = ANY(%s)
                                OR codigo_digitos = ANY(%s)
                            ) AND proveedor_key = %s
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigos_numericos, codigos_numericos, prov_key_filter)
                        )
                    else:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE (
                                codigo = ANY(%s)
                                OR codigo_digitos = ANY(%s)
                            )
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigos_numericos, codigos_numericos)
                        )
                    
                    rows = cur.fetchall()
                    print(f'[DEBUG buscar_productos_por_codigos_multiples] Encontrados {len(rows)} productos con códigos numéricos', flush=True)
                    
                    # Procesar resultados (mismo código que buscar_productos_por_codigo_exacto)
                    for r in rows:
                        if isinstance(r, dict):
                            prov_key = r.get('proveedor_key')
                            prov_name = r.get('proveedor_nombre') or get_proveedor_display_name(prov_key)
                            hoja = r.get('hoja')
                            codigo_raw = r.get('codigo') or ''
                            nombre_raw = r.get('nombre') or ''
                            precio_db = r.get('precio')
                            iva_db = r.get('iva')
                            precios_json = r.get('precios') or {}
                            extra_json = r.get('extra_datos') or {}
                            archivo = r.get('archivo')
                        else:
                            prov_key = r[0]
                            prov_name = r[1] or get_proveedor_display_name(prov_key)
                            archivo = r[2]
                            hoja = r[3]
                            codigo_raw = r[4] or ''
                            nombre_raw = r[5] or ''
                            precio_db = r[6]
                            iva_db = r[7]
                            precios_json = r[8] or {}
                            extra_json = r[9] or {}
                        
                        # Parsear JSONB si viene como string
                        if isinstance(precios_json, str):
                            try:
                                precios_json = json.loads(precios_json)
                            except:
                                precios_json = {}
                        if isinstance(extra_json, str):
                            try:
                                extra_json = json.loads(extra_json)
                            except:
                                extra_json = {}
                        
                        # Construir dict de precios
                        precios_a_mostrar = {}
                        if precio_db is not None and precio_db > 0:
                            precio_float = float(precio_db) if hasattr(precio_db, '__float__') else precio_db
                            if prov_key == 'brementools':
                                precios_a_mostrar['Precio de Venta'] = precio_float
                            elif prov_key == 'crossmaster':
                                precios_a_mostrar['Precio Lista'] = precio_float
                            elif prov_key == 'berger':
                                precios_a_mostrar['Precio'] = precio_float
                            elif prov_key == 'chiesa':
                                precios_a_mostrar['Pr.Unit'] = precio_float
                            elif prov_key == 'cachan':
                                precios_a_mostrar['Precio'] = precio_float
                            else:
                                precios_a_mostrar['Precio'] = precio_float
                        
                        if precios_json:
                            for k, v in precios_json.items():
                                if v is not None and v != '':
                                    try:
                                        precios_a_mostrar[k] = float(v) if hasattr(v, '__float__') else v
                                    except:
                                        precios_a_mostrar[k] = v
                        
                        resultados.append({
                            'codigo': codigo_raw,
                            'producto': nombre_raw,
                            'proveedor': f"{prov_name} (Hoja: {hoja})" if hoja else prov_name,
                            'proveedor_key': prov_key,
                            'sheet_name': hoja or '',
                            'iva': iva_db if iva_db is not None else 'N/A',
                            'precios': precios_a_mostrar,
                            'extra_datos': extra_json,
                            'precios_calculados': {},
                            'fuente': 'DB'
                        })
                
                # Si hay códigos no numéricos, buscarlos por separado
                if codigos_no_numericos:
                    if prov_key_filter:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE codigo = ANY(%s) AND proveedor_key = %s
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigos_no_numericos, prov_key_filter)
                        )
                    else:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE codigo = ANY(%s)
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigos_no_numericos,)
                        )
                    
                    # Procesar estos resultados también (mismo código)
                    rows = cur.fetchall()
                    for r in rows:
                        if isinstance(r, dict):
                            prov_key = r.get('proveedor_key')
                            prov_name = r.get('proveedor_nombre') or get_proveedor_display_name(prov_key)
                            hoja = r.get('hoja')
                            codigo_raw = r.get('codigo') or ''
                            nombre_raw = r.get('nombre') or ''
                            precio_db = r.get('precio')
                            iva_db = r.get('iva')
                            precios_json = r.get('precios') or {}
                            extra_json = r.get('extra_datos') or {}
                        else:
                            prov_key = r[0]
                            prov_name = r[1] or get_proveedor_display_name(prov_key)
                            hoja = r[3]
                            codigo_raw = r[4] or ''
                            nombre_raw = r[5] or ''
                            precio_db = r[6]
                            iva_db = r[7]
                            precios_json = r[8] or {}
                            extra_json = r[9] or {}

                        if isinstance(precios_json, str):
                            try:
                                precios_json = json.loads(precios_json)
                            except Exception:
                                precios_json = {}
                        if isinstance(extra_json, str):
                            try:
                                extra_json = json.loads(extra_json)
                            except Exception:
                                extra_json = {}

                        precios_a_mostrar = {}
                        if precio_db is not None and precio_db > 0:
                            precio_float = float(precio_db) if hasattr(precio_db, '__float__') else precio_db
                            if prov_key == 'brementools':
                                precios_a_mostrar['Precio de Venta'] = precio_float
                            elif prov_key == 'crossmaster':
                                precios_a_mostrar['Precio Lista'] = precio_float
                            elif prov_key == 'berger':
                                precios_a_mostrar['Precio'] = precio_float
                            elif prov_key == 'chiesa':
                                precios_a_mostrar['Pr.Unit'] = precio_float
                            elif prov_key == 'cachan':
                                precios_a_mostrar['Precio'] = precio_float
                            else:
                                precios_a_mostrar['Precio'] = precio_float

                        if precios_json:
                            for k, v in precios_json.items():
                                if v is not None and v != '':
                                    try:
                                        precios_a_mostrar[k] = float(v) if hasattr(v, '__float__') else v
                                    except Exception:
                                        precios_a_mostrar[k] = v

                        resultados.append({
                            'codigo': codigo_raw,
                            'producto': nombre_raw,
                            'proveedor': f"{prov_name} (Hoja: {hoja})" if hoja else prov_name,
                            'proveedor_key': prov_key,
                            'sheet_name': hoja or '',
                            'iva': iva_db if iva_db is not None else 'N/A',
                            'precios': precios_a_mostrar,
                            'extra_datos': extra_json,
                            'precios_calculados': {},
                            'fuente': 'DB'
                        })
                        
        except Exception as exc:
            print(f'[ERROR buscar_productos_por_codigos_multiples] Error en búsqueda DB: {exc}', flush=True)
    
    # Fallback a Excel si USAR_FALLBACK_EXCEL está activo y no hay resultados de DB
    if not resultados and USAR_FALLBACK_EXCEL:
        print(f'[DEBUG buscar_productos_por_codigos_multiples] Fallback a Excel para {len(codigos_limpios)} códigos', flush=True)
        # Buscar cada código en Excel (esto ya es más lento, pero es fallback)
        for codigo in codigos_limpios:
            resultados_temp = buscar_productos_por_codigo_exacto(codigo, proveedor_filtrado)
            if resultados_temp:
                resultados.extend(resultados_temp)
    
    return resultados

def buscar_productos_por_codigo_exacto(codigo_exacto: str, proveedor_filtrado: str = ''):
    """
    Busca productos con el código EXACTO (sin coincidencias parciales).
    Usado principalmente para búsqueda por código de barras.
    """
    if not codigo_exacto:
        return []

    codigo_limpio = codigo_exacto.strip()
    prov_key_filter = provider_name_to_key(proveedor_filtrado) if proveedor_filtrado else ''
    resultados = []

    # Si está habilitado el modo listas en DB y hay PostgreSQL disponible
    if LISTAS_EN_DB and DATABASE_URL and psycopg:
        print(f'[DEBUG buscar_productos_por_codigo_exacto] Buscando en DB: codigo={codigo_limpio}, prov_filter={prov_key_filter}', flush=True)
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                # Ampliar criterios de igualdad cuando el código ingresado es numérico:
                # - Igualdad exacta por columna codigo
                # - Igualdad exacta por codigo_digitos
                # - Igualdad ignorando ceros a la izquierda en codigo_digitos
                es_numerico = codigo_limpio.isdigit()
                if es_numerico:
                    if prov_key_filter:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE (
                                codigo = %s
                                OR codigo_digitos = %s
                                OR regexp_replace(codigo_digitos, '^0+', '') = regexp_replace(%s, '^0+', '')
                            ) AND proveedor_key = %s
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigo_limpio, codigo_limpio, codigo_limpio, prov_key_filter)
                        )
                    else:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE (
                                codigo = %s
                                OR codigo_digitos = %s
                                OR regexp_replace(codigo_digitos, '^0+', '') = regexp_replace(%s, '^0+', '')
                            )
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigo_limpio, codigo_limpio, codigo_limpio)
                        )
                else:
                    if prov_key_filter:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE codigo = %s AND proveedor_key = %s
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigo_limpio, prov_key_filter)
                        )
                    else:
                        cur.execute(
                            """
                            SELECT DISTINCT ON (proveedor_key, codigo)
                                   proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, iva, precios, extra_datos, mtime
                            FROM productos_listas
                            WHERE codigo = %s
                            ORDER BY proveedor_key, codigo, mtime DESC
                            """,
                            (codigo_limpio,)
                        )
                rows = cur.fetchall()
                print(f'[DEBUG buscar_productos_por_codigo_exacto] Resultados de DB: {len(rows)} filas', flush=True)
                if DEBUG_LOG:
                    log_debug(f'buscar_productos_por_codigo_exacto: encontrados {len(rows)} resultados')
                
                for r in rows:
                    if DEBUG_LOG:
                        log_debug(f'buscar_productos_por_codigo_exacto: fila raw = {r}')
                    
                    if isinstance(r, dict):
                        prov_key = r.get('proveedor_key')
                        prov_name = r.get('proveedor_nombre') or get_proveedor_display_name(prov_key)
                        hoja = r.get('hoja')
                        codigo_raw = r.get('codigo') or ''
                        nombre_raw = r.get('nombre') or ''
                        precio_db = r.get('precio')
                        iva_db = r.get('iva')
                        precios_json = r.get('precios') or {}
                        extra_json = r.get('extra_datos') or {}
                        archivo = r.get('archivo')
                        mtime_row = r.get('mtime')
                    else:
                        prov_key = r[0]
                        prov_name = r[1] or get_proveedor_display_name(prov_key)
                        archivo = r[2]
                        hoja = r[3]
                        codigo_raw = r[4] or ''
                        nombre_raw = r[5] or ''
                        precio_db = r[6]
                        iva_db = r[7]
                        precios_json = r[8] or {}
                        extra_json = r[9] or {}
                        mtime_row = r[10] if len(r) > 10 else None
                    
                    if DEBUG_LOG:
                        log_debug(f'buscar_productos_por_codigo_exacto: extraído iva_db = "{iva_db}" (tipo: {type(iva_db)})')
                    
                    # Parsear JSONB si viene como string
                    if isinstance(precios_json, str):
                        try:
                            precios_json = json.loads(precios_json)
                        except:
                            precios_json = {}
                    if isinstance(extra_json, str):
                        try:
                            extra_json = json.loads(extra_json)
                        except:
                            extra_json = {}
                    
                    # Debug: verificar qué datos vienen de la DB
                    if DEBUG_LOG:
                        log_debug(f'buscar_productos_por_codigo_exacto: codigo={codigo_raw}, precio_db={precio_db}, iva_db={iva_db}, precios_json={precios_json}, prov_key={prov_key}')
                    
                    # Construir dict de precios con nombre apropiado según proveedor
                    precios_a_mostrar = {}
                    if precio_db is not None and precio_db > 0:
                        # Convertir Decimal a float para compatibilidad con template
                        precio_float = float(precio_db) if hasattr(precio_db, '__float__') else precio_db
                        # Determinar el nombre del precio canónico según el proveedor
                        if prov_key == 'brementools':
                            precios_a_mostrar['Precio de Venta'] = precio_float
                        elif prov_key == 'crossmaster':
                            precios_a_mostrar['Precio Lista'] = precio_float
                        elif prov_key == 'berger':
                            precios_a_mostrar['Precio'] = precio_float
                        elif prov_key == 'chiesa':
                            precios_a_mostrar['Pr.Unit'] = precio_float
                        elif prov_key == 'cachan':
                            precios_a_mostrar['Precio'] = precio_float
                        else:
                            precios_a_mostrar['Precio'] = precio_float
                    
                    # Agregar precios adicionales del JSONB
                    if precios_json:
                        for k, v in precios_json.items():
                            if v is not None and v != '':
                                # Convertir a float si es necesario
                                v_float = float(v) if hasattr(v, '__float__') else v
                                # Normalizar el nombre de la clave para comparación
                                k_lower = str(k).lower().strip().replace('  ', ' ')
                                # Usar nombres exactos para precios específicos
                                if k_lower in ['precio neto', 'precioneto']:
                                    k_display = 'Precio Neto'
                                elif k_lower in ['precio neto unitario', 'precionetunitario']:
                                    k_display = 'Precio Neto Unitario'
                                elif k_lower in ['precio de lista', 'precio lista', 'preciolista', 'preciodelista']:
                                    k_display = 'Precio de Lista'
                                else:
                                    # Capitalizar nombres de precios adicionales
                                    k_display = k.title() if isinstance(k, str) else str(k)
                                
                                if k_display not in precios_a_mostrar:
                                    precios_a_mostrar[k_display] = v_float
                    
                    if DEBUG_LOG:
                        log_debug(f'buscar_productos_por_codigo_exacto: precios_a_mostrar={precios_a_mostrar}')
                    
                    iva_display = iva_db if iva_db else 'N/A'
                    if DEBUG_LOG:
                        log_debug(f'buscar_productos_por_codigo_exacto: iva_display final = "{iva_display}"')
                    
                    producto = {
                        'codigo': str(codigo_raw),
                        'producto': formatear_pulgadas(nombre_raw),
                        'proveedor': f"{prov_name} (Hoja: {hoja})",
                        'proveedor_key': prov_key,
                        'sheet_name': hoja,
                        'iva': iva_display,
                        'precios': precios_a_mostrar,
                        'extra_datos': extra_json,
                        'precios_calculados': {},
                        'fuente': 'DB'
                    }
                    if archivo:
                        producto['archivo'] = archivo
                    if mtime_row is not None:
                        try:
                            producto['fecha_archivo'] = ts_to_local(float(mtime_row)).strftime('%d/%m/%Y %H:%M')
                        except Exception:
                            producto['fecha_archivo'] = None
                    
                    if DEBUG_LOG:
                        log_debug(f'buscar_productos_por_codigo_exacto: producto construido con iva = "{producto["iva"]}"')
                    
                    resultados.append(producto)
                
                # Sólo retornamos si hay resultados desde DB; si no, seguimos con fallback a Excel
                if resultados:
                    print(f'[DEBUG buscar_productos_por_codigo_exacto] Retornando {len(resultados)} resultados de DB', flush=True)
                    return resultados
                else:
                    if not USAR_FALLBACK_EXCEL:
                        print(f'[DEBUG buscar_productos_por_codigo_exacto] No hay resultados en DB y fallback a Excel está desactivado', flush=True)
                        return []
                    print(f'[DEBUG buscar_productos_por_codigo_exacto] No hay resultados en DB, usando fallback a Excel', flush=True)
        except Exception as exc:
            print(f'[ERROR buscar_productos_por_codigo_exacto] Error en DB: {exc}', flush=True)
            log_debug('buscar_productos_por_codigo_exacto(DB): error, se usa fallback Excel', exc)

    # Fallback a Excel (solo si está habilitado)
    if not USAR_FALLBACK_EXCEL:
        print(f'[DEBUG buscar_productos_por_codigo_exacto] Fallback a Excel desactivado', flush=True)
        return []
    
    try:
        excel_files = sorted(os.listdir(LISTAS_PATH))
    except Exception as exc:
        log_debug('buscar_productos_por_codigo_exacto: no se pudo listar listas', exc)
        return []

    for filename in excel_files:
        if not filename.lower().endswith(('.xlsx', '.xls')):
            continue
        if 'old' in filename.lower():
            continue

        provider_key = provider_key_from_filename(filename)
        if prov_key_filter and provider_key != prov_key_filter:
            continue

        config = EXCEL_PROVIDER_CONFIG.get(provider_key)
        if not config:
            continue

        header_row_index = config.get('fila_encabezado')
        if header_row_index is None:
            continue

        file_path = os.path.join(LISTAS_PATH, filename)
        try:
            all_sheets = pd.read_excel(file_path, sheet_name=None, header=header_row_index)
        except Exception as exc:
            log_debug('buscar_productos_por_codigo_exacto: error leyendo', filename, exc)
            continue

        proveedor_display = get_proveedor_display_name(provider_key)

        for sheet_name, df in all_sheets.items():
            if df.empty:
                continue

            df.columns = [normalize_text(c) for c in df.columns]

            actual_cols = {
                'codigo': next((alias for alias in config['codigo'] if alias in df.columns), None),
                'producto': next((alias for alias in config['producto'] if alias in df.columns), None),
                'iva': next((alias for alias in config.get('iva', []) if alias in df.columns), None),
                'precios_a_mostrar': [alias for alias in config.get('precios_a_mostrar', []) if alias in df.columns],
                'extra_datos': [alias for alias in config.get('extra_datos', []) if alias in df.columns]
            }

            if not actual_cols['codigo'] or not actual_cols['producto']:
                continue

            # Buscar coincidencia EXACTA (robusta) por:
            # - Igualdad exacta del texto en columna código (quitando .0 típico de Excel)
            # - Igualdad exacta de sólo-dígitos
            # - Igualdad ignorando ceros a la izquierda en sólo-dígitos
            serie_codigo_texto = df[actual_cols['codigo']].apply(lambda x: str(x).strip() if pd.notna(x) else '')
            serie_codigo_texto = serie_codigo_texto.apply(lambda s: s[:-2] if s.endswith('.0') else s)

            # Construir serie con sólo dígitos
            def solo_digitos(v):
                s = str(v) if v is not None else ''
                return ''.join(ch for ch in s if ch.isdigit())
            serie_codigo_dig = df[actual_cols['codigo']].apply(lambda x: solo_digitos(x) if pd.notna(x) else '')

            codigo_dig_in = ''.join(ch for ch in codigo_limpio if ch.isdigit())
            codigo_dig_in_nl = codigo_dig_in.lstrip('0') if codigo_dig_in else ''

            coincidencias_idx = []
            # 1) Igualdad texto exacta
            idx_texto = df.index[serie_codigo_texto == codigo_limpio].tolist()
            coincidencias_idx.extend(idx_texto)
            # 2) Si el input es numérico, comparar sólo-dígitos
            if codigo_dig_in:
                idx_dig = df.index[serie_codigo_dig == codigo_dig_in].tolist()
                coincidencias_idx.extend(idx_dig)
                # 3) Igualdad ignorando ceros líderes
                if codigo_dig_in_nl:
                    idx_nl = df.index[serie_codigo_dig.apply(lambda s: s.lstrip('0')) == codigo_dig_in_nl].tolist()
                    coincidencias_idx.extend(idx_nl)

            # Quitar duplicados preservando orden
            seen_idx = set()
            coincidencias_idx = [i for i in coincidencias_idx if not (i in seen_idx or seen_idx.add(i))]

            for idx in coincidencias_idx:
                fila = df.loc[idx]
                producto = build_producto_entry(
                    fila,
                    actual_cols,
                    provider_key,
                    proveedor_display,
                    sheet_name,
                    df.columns,
                    codigo_override=codigo_limpio
                )
                resultados.append(producto)

    # Fallback adicional: buscar en productos_manual.xlsx si no hay filtro de proveedor o es 'manual'
    if not prov_key_filter or prov_key_filter == 'manual':
        try:
            productos_manual_list, err_manual = load_manual_products()
            if productos_manual_list and not err_manual:
                for p in productos_manual_list:
                    codigo_str = str(p.get('codigo', '')).strip()
                    # Comparar código exacto (texto o solo dígitos)
                    codigo_dig = ''.join(ch for ch in codigo_str if ch.isdigit())
                    codigo_limpio_dig = ''.join(ch for ch in codigo_limpio if ch.isdigit())
                    
                    if (codigo_str == codigo_limpio or 
                        codigo_dig == codigo_limpio_dig or 
                        (codigo_dig and codigo_limpio_dig and codigo_dig.lstrip('0') == codigo_limpio_dig.lstrip('0'))):
                        
                        # Verificar que no esté duplicado
                        ya_existe = any(r.get('codigo') == codigo_str and r.get('proveedor_key') == 'manual' for r in resultados)
                        if not ya_existe:
                            resultados.append({
                                'codigo': codigo_str,
                                'producto': formatear_pulgadas(p.get('nombre', '')),
                                'proveedor': f"{p.get('proveedor', 'Manual')} (Hoja: Manual)",
                                'proveedor_key': 'manual',
                                'sheet_name': 'Manual',
                                'iva': 'N/A',
                                'precios': {'Precio': p.get('precio', 0.0)},
                                'extra_datos': {},
                                'precios_calculados': {},
                                'fuente': 'Excel'
                            })
        except Exception as exc:
            log_debug('buscar_productos_por_codigo_exacto: error en productos_manual', exc)

    resultados.sort(key=lambda p: (p.get('proveedor', ''), p.get('codigo', '')))
    return resultados


def buscar_productos_por_codigo_patron(patron: str, proveedor_filtrado: str = ''):
    if not patron:
        return []

    patron = ''.join(filter(str.isdigit, str(patron)))
    if not patron:
        return []

    prov_key_filter = provider_name_to_key(proveedor_filtrado) if proveedor_filtrado else ''
    resultados = []

    # Si está habilitado el modo listas en DB y hay PostgreSQL disponible, consultar primero en la base
    if LISTAS_EN_DB and DATABASE_URL and psycopg:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                like_param = f"%{patron}%"
                if prov_key_filter:
                    cur.execute(
                        """
                        SELECT proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, NULL::text AS iva_text
                        FROM productos_listas
                        WHERE codigo_digitos LIKE %s AND proveedor_key = %s
                        ORDER BY proveedor_key, codigo
                        LIMIT 500
                        """,
                        (like_param, prov_key_filter)
                    )
                else:
                    cur.execute(
                        """
                        SELECT proveedor_key, proveedor_nombre, archivo, hoja, codigo, nombre, precio, NULL::text AS iva_text
                        FROM productos_listas
                        WHERE codigo_digitos LIKE %s
                        ORDER BY proveedor_key, codigo
                        LIMIT 500
                        """,
                        (like_param,)
                    )
                rows = cur.fetchall()
                for r in rows:
                    if isinstance(r, dict):
                        prov_key = r.get('proveedor_key')
                        prov_name = r.get('proveedor_nombre') or get_proveedor_display_name(prov_key)
                        hoja = r.get('hoja')
                        codigo_raw = r.get('codigo') or ''
                        nombre_raw = r.get('nombre') or ''
                        precio_db = r.get('precio')
                        iva_text = r.get('iva_text') or 'N/A'
                    else:
                        prov_key = r[0]
                        prov_name = (r[1] or get_proveedor_display_name(prov_key))
                        hoja = r[3]
                        codigo_raw = r[4] or ''
                        nombre_raw = r[5] or ''
                        precio_db = r[6]
                        iva_text = r[7] or 'N/A'
                    producto = {
                        'codigo': str(codigo_raw),
                        'producto': formatear_pulgadas(nombre_raw),
                        'proveedor': f"{prov_name} (Hoja: {hoja})",
                        'proveedor_key': prov_key,
                        'sheet_name': hoja,
                        'iva': iva_text,
                        'precios': {'Precio': precio_db},
                        'extra_datos': {},
                        'precios_calculados': {}
                    }
                    producto['codigo_coincidencia'] = ''.join(filter(str.isdigit, str(codigo_raw)))
                    resultados.append(producto)
                if resultados:
                    resultados.sort(key=lambda p: (p.get('proveedor', ''), p.get('codigo', '')))
                    return resultados
        except Exception as exc:
            log_debug('buscar_productos_por_codigo_patron(DB): error, se usa fallback Excel', exc)

    try:
        excel_files = sorted(os.listdir(LISTAS_PATH))
    except Exception as exc:
        log_debug('buscar_productos_por_codigo_patron: no se pudo listar listas', exc)
        return []

    for filename in excel_files:
        if not filename.lower().endswith(('.xlsx', '.xls')):
            continue
        if 'old' in filename.lower():
            continue

        provider_key = provider_key_from_filename(filename)
        if prov_key_filter and provider_key != prov_key_filter:
            continue

        config = EXCEL_PROVIDER_CONFIG.get(provider_key)
        if not config:
            continue

        header_row_index = config.get('fila_encabezado')
        if header_row_index is None:
            continue

        file_path = os.path.join(LISTAS_PATH, filename)
        try:
            all_sheets = pd.read_excel(file_path, sheet_name=None, header=header_row_index)
        except Exception as exc:
            log_debug('buscar_productos_por_codigo_patron: error leyendo', filename, exc)
            continue

        proveedor_display = get_proveedor_display_name(provider_key)

        for sheet_name, df in all_sheets.items():
            if df.empty:
                continue

            df.columns = [normalize_text(c) for c in df.columns]

            actual_cols = {
                'codigo': next((alias for alias in config['codigo'] if alias in df.columns), None),
                'producto': next((alias for alias in config['producto'] if alias in df.columns), None),
                'iva': next((alias for alias in config.get('iva', []) if alias in df.columns), None),
                'precios_a_mostrar': [alias for alias in config.get('precios_a_mostrar', []) if alias in df.columns],
                'extra_datos': [alias for alias in config.get('extra_datos', []) if alias in df.columns]
            }

            if not actual_cols['codigo'] or not actual_cols['producto']:
                continue

            codigo_series = df[actual_cols['codigo']].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else '')

            coincidencias_idx = []
            for idx, codigo_raw in codigo_series.items():
                codigo_digitos = ''.join(filter(str.isdigit, codigo_raw))
                if not codigo_digitos:
                    continue
                if patron in codigo_digitos:
                    coincidencias_idx.append((idx, codigo_raw))

            if not coincidencias_idx:
                continue

            for idx, codigo_raw in coincidencias_idx:
                fila = df.loc[idx]
                producto = build_producto_entry(
                    fila,
                    actual_cols,
                    provider_key,
                    proveedor_display,
                    sheet_name,
                    df.columns,
                    codigo_override=codigo_raw
                )
                producto['codigo_coincidencia'] = ''.join(filter(str.isdigit, str(codigo_raw)))
                resultados.append(producto)

    resultados.sort(key=lambda p: (p.get('proveedor', ''), p.get('codigo', '')))
    return resultados

# --- IMPORTADOR DE LISTAS A POSTGRESQL ---
def _listas_provider_configs():
    """Config de proveedores para importación: fila encabezado (0-based) y alias de columnas.
    Solo se usa para localizar código, nombre, precio canónico y algunos extras.
    """
    return {
        'crossmaster': {
            'header': 11,
            'codigo': ['codigo', 'código', 'codigo ean', 'código ean', 'ean', 'cod'],
            'nombre': ['descripcion', 'descripción', 'producto', 'nombre'],
            'precio_canon': ['precio lista', 'precio de lista'],
            'iva': ['iva', 'i.v.a']
        },
        'berger': {
            'header': 0,
            'codigo': ['codigo', 'código', 'cod'],
            'nombre': ['detalle', 'descripcion', 'descripción', 'producto', 'nombre'],
            'precio_canon': ['precio', 'pventa'],
            'iva': ['iva']
        },
        'brementools': {
            'header': 5,
            'codigo': ['codigo', 'código', 'codigo ean', 'código ean', 'ean'],
            'nombre': ['producto', 'descripcion', 'descripción'],
            'precio_canon': ['precio de venta', 'precio venta', 'precio venta con iva'],
            'precios_extra': ['precio de lista', 'precio lista', 'precio neto', 'precioneto', 'precio neto unitario', 'precionetunitario'],
            'iva': ['iva'],
            'extras': ['cantidad']
        },
        'cachan': {
            'header': 0,
            'codigo': ['codigo', 'código'],
            'nombre': ['nombre', 'producto', 'descripcion', 'descripción'],
            'precio_canon': ['precio'],
            'iva': []
        },
        'chiesa': {
            'header': 1,
            'codigo': ['codigo', 'código'],
            'nombre': ['descripcion', 'descripción', 'producto', 'nombre'],
            'precio_canon': ['pr unit', 'prunit'],
            'iva': ['iva', 'i.v.a']
        },
        'manual': {
            'header': 0
        }
    }

def _find_first_col(df_cols, aliases):
    if not aliases:
        return None
    for a in aliases:
        an = normalize_text(a)
        for c in df_cols:
            if normalize_text(str(c)) == an:
                return c
    return None

def sync_listas_to_db():
    """Lee archivos Excel de LISTAS_PATH y carga productos a PostgreSQL.
    Reemplaza por archivo (DELETE + INSERT en transacción) y registra lote en import_batches.
    Devuelve un dict resumen.
    """
    print("[DEBUG sync_listas_to_db] === INICIO DE SINCRONIZACIÓN ===")
    resumen = {'procesados': 0, 'insertados': 0, 'archivos': []}
    if not (DATABASE_URL and psycopg):
        print("[DEBUG sync_listas_to_db] ERROR: PostgreSQL no disponible")
        return {'error': 'PostgreSQL no disponible.'}
    print(f"[DEBUG sync_listas_to_db] PostgreSQL disponible, LISTAS_PATH={LISTAS_PATH}")

    print(f"[DEBUG sync_listas_to_db] PostgreSQL disponible, LISTAS_PATH={LISTAS_PATH}")

    prov_cfg = _listas_provider_configs()
    try:
        excel_files = sorted(f for f in os.listdir(LISTAS_PATH) if f.lower().endswith(('.xlsx', '.xls')) and 'old' not in f.lower())
        print(f"[DEBUG sync_listas_to_db] Archivos Excel encontrados: {len(excel_files)} -> {excel_files}")
    except Exception as exc:
        print(f"[DEBUG sync_listas_to_db] ERROR al listar {LISTAS_PATH}: {exc}")
        return {'error': f'No se pudo listar {LISTAS_PATH}: {exc}'}

    # Limpiar archivos obsoletos en DB (que ya no existen en disco o quedaron OLD)
    try:
        estado_db = _db_files_state()
        excel_set = set(excel_files)
        archivos_obsoletos = []
        for archivo_db in estado_db.keys():
            low = archivo_db.lower() if isinstance(archivo_db, str) else ''
            if (archivo_db not in excel_set) or ('old' in low):
                archivos_obsoletos.append(archivo_db)
        if archivos_obsoletos:
            with get_pg_conn() as conn, conn.cursor() as cur:
                cur.executemany("DELETE FROM productos_listas WHERE archivo=%s", [(a,) for a in archivos_obsoletos])
                cur.executemany("DELETE FROM import_batches WHERE archivo=%s", [(a,) for a in archivos_obsoletos])
                conn.commit()
            print(f"[DEBUG sync_listas_to_db] Limpieza: {len(archivos_obsoletos)} archivo(s) obsoletos eliminados de la DB: {archivos_obsoletos}")
    except Exception as exc:
        print(f"[WARN sync_listas_to_db] No se pudieron limpiar archivos obsoletos de la DB: {exc}")

    print("[DEBUG sync_listas_to_db] Obteniendo conexión PostgreSQL...")
    with get_pg_conn() as conn, conn.cursor() as cur:
        print("[DEBUG sync_listas_to_db] Conexión obtenida, iniciando procesamiento de archivos...")
        for filename in excel_files:
            print(f"[DEBUG sync_listas_to_db] --- Procesando archivo: {filename} ---")
            print(f"[DEBUG sync_listas_to_db] --- Procesando archivo: {filename} ---")
            provider_key = provider_key_from_filename(filename)
            print(f"[DEBUG sync_listas_to_db] provider_key inicial: {provider_key}")
            cfg = prov_cfg.get(provider_key)
            if not cfg:
                # Proveedor desconocido para el importador → intentar inferir usando nombres_base de proveedores
                print(f"[DEBUG sync_listas_to_db] No hay cfg para {provider_key}, intentando inferir...")
                try:
                    inferred_base = inferir_nombre_base_archivo(filename, proveedores)
                    inferred_key = provider_name_to_key(inferred_base)
                    cfg = prov_cfg.get(inferred_key)
                    if cfg:
                        provider_key = inferred_key
                        print(f"[DEBUG sync_listas_to_db] Proveedor inferido: {provider_key}")
                    else:
                        # No se pudo inferir, saltar archivo
                        print(f"[DEBUG sync_listas_to_db] No se pudo inferir proveedor para {filename}, se omite")
                        log_debug('sync_listas_to_db: proveedor no reconocido, se omite', filename, '-> key:', provider_key)
                        continue
                except Exception as _inf_err:
                    print(f"[DEBUG sync_listas_to_db] Error infiriendo proveedor para {filename}: {_inf_err}")
                    log_debug('sync_listas_to_db: error infiriendo proveedor para', filename, _inf_err)
                    continue
            else:
                print(f"[DEBUG sync_listas_to_db] cfg encontrado para provider_key: {provider_key}")
            
            file_path = os.path.join(LISTAS_PATH, filename)
            try:
                mtime = os.path.getmtime(file_path)
                print(f"[DEBUG sync_listas_to_db] mtime del archivo: {mtime}")
            except Exception:
                mtime = 0.0
                print(f"[DEBUG sync_listas_to_db] No se pudo obtener mtime, usando 0.0")

            # Leer todas las hojas respetando header
            header = cfg.get('header', 0)
            print(f"[DEBUG sync_listas_to_db] Leyendo Excel con header={header}...")
            try:
                all_sheets = pd.read_excel(file_path, sheet_name=None, header=header)
                print(f"[DEBUG sync_listas_to_db] Excel leído exitosamente, hojas: {list(all_sheets.keys())}")
            except Exception as exc:
                print(f"[DEBUG sync_listas_to_db] ERROR leyendo {filename}: {exc}")
                log_debug('sync_listas_to_db: error leyendo', filename, exc)
                continue

            # Crear batch y limpiar productos previos de este archivo (transacción)
            print(f"[DEBUG sync_listas_to_db] Creando batch para {filename}...")
            cur.execute(
                "INSERT INTO import_batches (proveedor_key, archivo, mtime, status) VALUES (%s,%s,%s,%s) RETURNING id",
                (provider_key, filename, mtime, 'running')
            )
            _row = cur.fetchone()
            batch_id = (_row['id'] if isinstance(_row, dict) else _row[0]) if _row is not None else None
            print(f"[DEBUG sync_listas_to_db] Batch creado con ID: {batch_id}")
            total_insertados = 0
            # Reemplazo por archivo
            print(f"[DEBUG sync_listas_to_db] Eliminando productos previos de {filename}...")
            cur.execute("DELETE FROM productos_listas WHERE archivo=%s", (filename,))
            deleted_rows = cur.rowcount
            print(f"[DEBUG sync_listas_to_db] Eliminados {deleted_rows} registros previos")

            proveedor_display = get_proveedor_display_name(provider_key)
            print(f"[DEBUG sync_listas_to_db] proveedor_display: {proveedor_display}")

            for sheet_name, df in all_sheets.items():
                print(f"[DEBUG sync_listas_to_db] Procesando hoja: {sheet_name}, filas: {len(df) if df is not None else 0}")
                if df is None or df.empty:
                    print(f"[DEBUG sync_listas_to_db] Hoja {sheet_name} está vacía, saltando...")
                    continue
                # Mantener nombres originales y también versión normalizada para búsqueda
                df_columns = list(df.columns)
                # Intentar mapear columnas según aliases
                codigo_col = _find_first_col(df_columns, cfg.get('codigo', []))
                nombre_col = _find_first_col(df_columns, cfg.get('nombre', []))
                print(f"[DEBUG sync_listas_to_db] Columnas detectadas - codigo_col: {codigo_col}, nombre_col: {nombre_col}")
                iva_col = _find_first_col(df_columns, cfg.get('iva', []))
                precio_canon_col = _find_first_col(df_columns, cfg.get('precio_canon', []))
                precios_extra_alias = cfg.get('precios_extra', []) or []
                extra_cols = [c for c in df_columns if normalize_text(str(c)) in [normalize_text(x) for x in precios_extra_alias]]
                cantidad_col = _find_first_col(df_columns, cfg.get('extras', [])) if cfg.get('extras') else None

                if not codigo_col or not nombre_col:
                    print(f"[DEBUG sync_listas_to_db] Hoja {sheet_name}: no se encontraron columnas código/nombre, saltando...")
                    continue

                # OPTIMIZACIÓN: Recopilar todos los datos en un batch antes de insertar
                batch_data = []
                filas_insertadas_hoja = 0
                
                for _, fila in df.iterrows():
                    # Código
                    raw_code = fila.get(codigo_col)
                    if pd.isna(raw_code):
                        continue
                    code = str(raw_code).strip()
                    if code.endswith('.0'):
                        code = code[:-2] or code
                    # Nombre
                    raw_name = fila.get(nombre_col)
                    if pd.isna(raw_name):
                        continue
                    name = str(raw_name).strip()

                    # Precio canónico
                    price_val = None
                    if precio_canon_col and pd.notna(fila.get(precio_canon_col)):
                        price_val = parse_price_value(fila.get(precio_canon_col))

                    # Nota: Importamos todos los productos, incluso sin precio
                    # (antes había un 'if price_val is None: continue' que los saltaba)

                    # IVA textual si existe (guardar tal cual, sin procesar)
                    iva_text = None
                    if iva_col and pd.notna(fila.get(iva_col)):
                        raw_iva = fila.get(iva_col)
                        
                        # Si es numérico, convertir a porcentaje
                        if isinstance(raw_iva, (int, float)):
                            # Si es menor a 1, probablemente es decimal (0.21 -> 21%)
                            if raw_iva < 1:
                                iva_value = raw_iva * 100
                            else:
                                iva_value = raw_iva
                            
                            # Si es un número entero, mostrar sin decimales
                            if iva_value == int(iva_value):
                                iva_text = str(int(iva_value)) + '%'
                            else:
                                # Si tiene decimales, mantenerlos
                                iva_text = str(iva_value) + '%'
                        else:
                            # Si es string, intentar convertir a número y aplicar misma lógica
                            try:
                                iva_num = float(str(raw_iva).strip().replace('%', '').replace(',', '.'))
                                if iva_num < 1:
                                    iva_value = iva_num * 100
                                else:
                                    iva_value = iva_num
                                
                                if iva_value == int(iva_value):
                                    iva_text = str(int(iva_value)) + '%'
                                else:
                                    iva_text = str(iva_value) + '%'
                            except:
                                # Si no se puede convertir, guardar tal cual
                                iva_text = str(raw_iva).strip()

                    # Otros precios visibles
                    precios_dict = {}
                    if provider_key == 'brementools':
                        # Mantener visibles: de venta (canon), de lista, neto, neto unitario
                        for alias in ['precio de lista', 'precio lista', 'precio neto', 'precioneto', 'precio neto unitario', 'precionetunitario']:
                            col = _find_first_col(df_columns, [alias])
                            if col and pd.notna(fila.get(col)):
                                v = parse_price_value(fila.get(col))
                                if v is not None:
                                    precios_dict[alias] = v
                    else:
                        for col in extra_cols:
                            if pd.notna(fila.get(col)):
                                v = parse_price_value(fila.get(col))
                                if v is not None:
                                    precios_dict[str(col)] = v

                    # Extras mínimos
                    extra = {}
                    # Capturar 'categoria' si existe en la hoja
                    try:
                        categoria_col = _find_first_col(df_columns, ['categoria'])
                        if categoria_col and pd.notna(fila.get(categoria_col)):
                            extra['Categoria'] = str(fila.get(categoria_col)).strip()
                    except Exception:
                        pass

                    # Capturar cantidad (BremenTools)
                    if provider_key == 'brementools' and cantidad_col:
                        try:
                            raw_qty = fila.get(cantidad_col)
                            if pd.notna(raw_qty):
                                qty_val = None
                                if isinstance(raw_qty, (int, float)):
                                    qty_val = raw_qty
                                else:
                                    try:
                                        qty_val_num = float(str(raw_qty).strip().replace(',', '.'))
                                        qty_val = int(qty_val_num) if qty_val_num.is_integer() else qty_val_num
                                    except Exception:
                                        qty_val = str(raw_qty).strip()
                                if qty_val is not None:
                                    extra['Cantidad'] = qty_val
                        except Exception:
                            pass

                    codigo_digitos = ''.join(filter(str.isdigit, code))
                    nombre_norm = normalize_text(formatear_pulgadas(name))
                    codigo_norm = normalize_text(code)

                    # Agregar a batch en lugar de insertar inmediatamente
                    batch_data.append((
                        provider_key, proveedor_display, filename, sheet_name, mtime,
                        code, codigo_digitos, codigo_norm,
                        name, nombre_norm,
                        float(price_val) if price_val is not None else None,
                        cfg.get('precio_canon', ['precio'])[0],
                        iva_text,
                        json.dumps(precios_dict, ensure_ascii=False),
                        json.dumps(extra, ensure_ascii=False),
                        batch_id
                    ))
                
                # Insertar todos los datos de la hoja en un solo batch usando executemany
                if batch_data:
                    print(f"[DEBUG sync_listas_to_db] Insertando {len(batch_data)} filas en batch...")
                    cur.executemany(
                        """
                        INSERT INTO productos_listas
                        (proveedor_key, proveedor_nombre, archivo, hoja, mtime,
                         codigo, codigo_digitos, codigo_normalizado,
                         nombre, nombre_normalizado,
                         precio, precio_fuente, iva, precios, extra_datos, batch_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s)
                        """,
                        batch_data
                    )
                    filas_insertadas_hoja = len(batch_data)
                    total_insertados += filas_insertadas_hoja
                    print(f"[DEBUG sync_listas_to_db] Batch insertado exitosamente: {filas_insertadas_hoja} filas")
                
                print(f"[DEBUG sync_listas_to_db] Hoja {sheet_name}: insertadas {filas_insertadas_hoja} filas")

            # Cerrar batch
            print(f"[DEBUG sync_listas_to_db] Cerrando batch {batch_id}, total insertados: {total_insertados}")
            cur.execute(
                "UPDATE import_batches SET status=%s, completed_at=NOW(), total_rows=%s WHERE id=%s",
                ('completed', total_insertados, batch_id)
            )

            resumen['procesados'] += 1
            resumen['insertados'] += total_insertados
            resumen['archivos'].append({'archivo': filename, 'filas': total_insertados, 'proveedor': provider_key})
            print(f"[DEBUG sync_listas_to_db] Archivo {filename} completado: {total_insertados} productos")

    # Importar productos manuales a la misma tabla (proveedor_key='manual')
    print("[DEBUG sync_listas_to_db] === Procesando productos_manual.xlsx ===")
    try:
        productos_manual, err = load_manual_products()
        print(f"[DEBUG sync_listas_to_db] productos_manual cargados: {len(productos_manual) if productos_manual else 0}, error: {err}")
        if productos_manual:
            with get_pg_conn() as conn, conn.cursor() as cur:
                # Buscar el archivo real (puede tener fecha agregada)
                filename = 'productos_manual.xlsx'  # valor por defecto
                file_path = os.path.join(LISTAS_PATH, filename)
                try:
                    archivos = os.listdir(LISTAS_PATH)
                    candidatos = []
                    for fname in archivos:
                        if fname.lower().startswith('productos_manual') and fname.lower().endswith(('.xlsx', '.xls')):
                            if 'old' not in fname.lower():
                                candidatos.append(fname)
                    if candidatos:
                        candidatos_con_mtime = []
                        for fname in candidatos:
                            fpath = os.path.join(LISTAS_PATH, fname)
                            try:
                                mt = os.path.getmtime(fpath)
                                candidatos_con_mtime.append((fpath, mt, fname))
                            except Exception:
                                pass
                        if candidatos_con_mtime:
                            candidatos_con_mtime.sort(key=lambda x: x[1], reverse=True)
                            file_path = candidatos_con_mtime[0][0]
                            filename = candidatos_con_mtime[0][2]
                except Exception:
                    pass
                
                try:
                    mtime = os.path.getmtime(file_path)
                except Exception:
                    mtime = 0.0
                cur.execute(
                    "INSERT INTO import_batches (proveedor_key, archivo, mtime, status) VALUES (%s,%s,%s,%s) RETURNING id",
                    ('manual', filename, mtime, 'running')
                )
                _row2 = cur.fetchone()
                batch_id = (_row2['id'] if isinstance(_row2, dict) else _row2[0]) if _row2 is not None else None
                cur.execute("DELETE FROM productos_listas WHERE archivo=%s", (filename,))
                deleted_manual = cur.rowcount
                print(f"[DEBUG sync_listas_to_db] productos_manual: eliminados {deleted_manual} registros previos")
                
                # OPTIMIZACIÓN: Recopilar todos los datos en un batch antes de insertar
                batch_data_manual = []
                for p in productos_manual:
                    code = str(p.get('codigo', '')).strip()
                    name = str(p.get('nombre', '')).strip()
                    price = p.get('precio')
                    if not code or not name:
                        continue
                    if price is None:
                        continue
                    codigo_digitos = ''.join(filter(str.isdigit, code))
                    nombre_norm = normalize_text(formatear_pulgadas(name))
                    codigo_norm = normalize_text(code)
                    precios_dict = {'precio': float(price)}
                    
                    batch_data_manual.append((
                        'manual', 'Manual', filename, '-', mtime,
                        code, codigo_digitos, codigo_norm,
                        name, nombre_norm,
                        float(price), 'precio', json.dumps(precios_dict, ensure_ascii=False), json.dumps({}, ensure_ascii=False), batch_id
                    ))
                
                # Insertar todos los productos manuales en un solo batch
                total_insertados = 0
                if batch_data_manual:
                    print(f"[DEBUG sync_listas_to_db] productos_manual: insertando {len(batch_data_manual)} filas en batch...")
                    cur.executemany(
                        """
                        INSERT INTO productos_listas
                        (proveedor_key, proveedor_nombre, archivo, hoja, mtime,
                         codigo, codigo_digitos, codigo_normalizado,
                         nombre, nombre_normalizado,
                         precio, precio_fuente, precios, extra_datos, batch_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s)
                        """,
                        batch_data_manual
                    )
                    total_insertados = len(batch_data_manual)
                    print(f"[DEBUG sync_listas_to_db] productos_manual: batch insertado exitosamente")
                
                cur.execute(
                    "UPDATE import_batches SET status=%s, completed_at=NOW(), total_rows=%s WHERE id=%s",
                    ('completed', total_insertados, batch_id)
                )
                resumen['procesados'] += 1
                resumen['insertados'] += total_insertados
                resumen['archivos'].append({'archivo': filename, 'filas': total_insertados, 'proveedor': 'manual'})
                print(f"[DEBUG sync_listas_to_db] productos_manual completado: {total_insertados} productos insertados")
        elif err:
            print(f"[DEBUG sync_listas_to_db] productos_manual error: {err}")
            log_debug('sync_listas_to_db: productos_manual error', err)
    except Exception as exc:
        print(f"[DEBUG sync_listas_to_db] ERROR importando manual: {exc}")
        log_debug('sync_listas_to_db: error importando manual', exc)

    print(f"[DEBUG sync_listas_to_db] === FIN DE SINCRONIZACIÓN === Resumen: {resumen}")
    return resumen


def _excel_files_state():
    """Devuelve un dict {archivo: mtime} de los Excel vigentes en LISTAS_PATH.
    Ignora archivos con 'old' en el nombre y no-xlsx/xls.
    """
    estado = {}
    try:
        for fname in os.listdir(LISTAS_PATH):
            low = fname.lower()
            if not (low.endswith('.xlsx') or low.endswith('.xls')):
                continue
            if 'old' in low:
                continue
            fpath = os.path.join(LISTAS_PATH, fname)
            try:
                estado[fname] = os.path.getmtime(fpath)
            except Exception:
                pass
    except Exception as exc:
        log_debug('_excel_files_state: no se pudo listar', LISTAS_PATH, exc)
    return estado


def _db_files_state():
    """Consulta la DB y devuelve un dict {archivo: max_mtime_en_db}.
    Si no hay DB disponible, devuelve {}.
    """
    if not (DATABASE_URL and psycopg):
        return {}
    estado = {}
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT archivo, MAX(mtime) AS m FROM productos_listas GROUP BY archivo")
            for row in (cur.fetchall() or []):
                if isinstance(row, dict):
                    archivo = row.get('archivo')
                    m = row.get('m')
                else:
                    archivo, m = row[0], row[1]
                if archivo:
                    try:
                        estado[archivo] = float(m) if m is not None else None
                    except Exception:
                        estado[archivo] = m
    except Exception as exc:
        log_debug('_db_files_state: error consultando DB', exc)
    return estado


def listas_db_desactualizadas(tolerancia_segundos: float = 1.0) -> bool:
    """Compara mtimes de Excel en disco vs valores en productos_listas.
    Devuelve True si falta algún archivo en DB o algún mtime difiere por más de tolerancia.
    """
    if not (LISTAS_EN_DB and DATABASE_URL and psycopg):
        return False
    fs = _excel_files_state()
    if not fs:
        # No hay excels, no hay nada que sincronizar
        return False
    db = _db_files_state()
    for archivo, mtime_fs in fs.items():
        mtime_db = db.get(archivo)
        if mtime_db is None:
            log_debug('listas_db_desactualizadas: falta en DB', archivo)
            return True
        try:
            if abs(float(mtime_db) - float(mtime_fs)) > tolerancia_segundos:
                log_debug('listas_db_desactualizadas: mtime distinto', archivo, mtime_db, '!=', mtime_fs)
                return True
        except Exception:
            # Si no se puede comparar, forzar sync por seguridad
            return True
    # Archivos que existen en DB pero ya no en disco (o marcados OLD) también disparan sincronización
    for archivo_db in db.keys():
        low = archivo_db.lower() if isinstance(archivo_db, str) else ''
        if archivo_db not in fs or 'old' in low:
            log_debug('listas_db_desactualizadas: archivo huérfano/OLD en DB', archivo_db)
            return True
    return False


def maybe_auto_sync_listas() -> dict | None:
    """Si las listas en DB están desactualizadas respecto a los Excel, ejecuta sync_listas_to_db().
    Devuelve el resumen del sync o None si no hizo nada o falló.
    """
    print("[DEBUG maybe_auto_sync_listas] Verificando si se necesita sincronización...")
    if not (LISTAS_EN_DB and DATABASE_URL and psycopg):
        print("[DEBUG maybe_auto_sync_listas] Condiciones no cumplidas (LISTAS_EN_DB o DATABASE_URL)")
        return None
    try:
        desactualizadas = listas_db_desactualizadas()
        print(f"[DEBUG maybe_auto_sync_listas] listas_db_desactualizadas() = {desactualizadas}")
        if desactualizadas:
            print('[DEBUG maybe_auto_sync_listas] Iniciando sincronización automática...')
            log_debug('maybe_auto_sync_listas: iniciando sincronización automática…')
            resumen = sync_listas_to_db()
            print(f'[DEBUG maybe_auto_sync_listas] Sincronización completada, resumen: {resumen}')
            log_debug('maybe_auto_sync_listas: resumen', resumen)
            return resumen if isinstance(resumen, dict) else None
        else:
            print("[DEBUG maybe_auto_sync_listas] DB está actualizada, no se requiere sincronización")
    except Exception as exc:
        print(f"[DEBUG maybe_auto_sync_listas] ERROR durante auto-sync: {exc}")
        log_debug('maybe_auto_sync_listas: error durante auto-sync', exc)
    return None


def buscar_productos_manual_db(query: str, page: int, per_page: int):
    """Busca productos del proveedor manual en PostgreSQL con paginación.
    Devuelve (resultados:list[dict], total:int).
    """
    if not (DATABASE_URL and psycopg):
        return [], 0
    query = (query or '').strip()
    tokens = [t for t in normalize_text(formatear_pulgadas(query)).split() if t]
    offset = max(0, (max(1, int(page)) - 1) * max(1, int(per_page)))

    where = ["proveedor_key = 'manual'"]
    params = []
    for t in tokens:
        where.append("(nombre_normalizado LIKE %s OR codigo_normalizado LIKE %s)")
        like = f"%{t}%"
        params.extend([like, like])
    where_sql = ' AND '.join(where) if where else 'TRUE'

    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS c FROM productos_listas WHERE {where_sql}", params)
            row = cur.fetchone()
            total = (row.get('c') if isinstance(row, dict) else (row[0] if row else 0)) or 0

            cur.execute(
                f"""
                SELECT codigo, nombre, precio
                FROM productos_listas
                WHERE {where_sql}
                ORDER BY nombre_normalizado ASC
                LIMIT %s OFFSET %s
                """,
                params + [per_page, offset]
            )
            resultados = []
            for r in cur.fetchall():
                if isinstance(r, dict):
                    codigo = r.get('codigo')
                    nombre = r.get('nombre')
                    precio = r.get('precio')
                else:
                    codigo, nombre, precio = r[0], r[1], r[2]
                resultados.append({
                    'codigo': str(codigo) if codigo is not None else '',
                    'nombre': nombre or '',
                    'precio': float(precio) if precio is not None else None,
                    'proveedor': 'Manual'
                })
            return resultados, total
    except Exception as exc:
        log_debug('buscar_productos_manual_db: error', exc)
        return [], 0


def buscar_productos_avanzados_db(query: str, page: int, per_page: int, proveedor_filter: str = None):
    """Busca productos de TODOS los proveedores en PostgreSQL con paginación.
    Devuelve (resultados:list[dict], total:int).
    Cada resultado incluye: codigo, nombre, precio (canonical), precios (JSONB), proveedor_key, proveedor_nombre, precio_valido.
    """
    if not (DATABASE_URL and psycopg):
        return [], 0
    query = (query or '').strip()
    tokens = [t for t in normalize_text(formatear_pulgadas(query)).split() if t]
    offset = max(0, (max(1, int(page)) - 1) * max(1, int(per_page)))

    where = []
    params = []
    
    # Filtro por proveedor si se especifica
    if proveedor_filter:
        where.append("proveedor_key = %s")
        params.append(proveedor_filter)
    
    # Búsqueda por tokens en nombre o código
    for t in tokens:
        where.append("(nombre_normalizado LIKE %s OR codigo_normalizado LIKE %s)")
        like = f"%{t}%"
        params.extend([like, like])
    
    where_sql = ' AND '.join(where) if where else 'TRUE'

    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            # Contar total
            cur.execute(f"SELECT COUNT(*) AS c FROM productos_listas WHERE {where_sql}", params)
            row = cur.fetchone()
            total = (row.get('c') if isinstance(row, dict) else (row[0] if row else 0)) or 0

            # Obtener resultados paginados
            cur.execute(
                f"""
                SELECT codigo, nombre, precio, precios, proveedor_key, proveedor_nombre, extra_datos, iva
                FROM productos_listas
                WHERE {where_sql}
                ORDER BY proveedor_nombre ASC, nombre_normalizado ASC
                LIMIT %s OFFSET %s
                """,
                params + [per_page, offset]
            )
            resultados = []
            for r in cur.fetchall():
                if isinstance(r, dict):
                    codigo = r.get('codigo')
                    nombre = r.get('nombre')
                    precio = r.get('precio')
                    precios = r.get('precios')
                    proveedor_key = r.get('proveedor_key')
                    proveedor_nombre = r.get('proveedor_nombre')
                    extra_datos = r.get('extra_datos')
                    iva = r.get('iva')
                else:
                    codigo, nombre, precio, precios, proveedor_key, proveedor_nombre, extra_datos, iva = r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]
                
                # Parsear JSONB si viene como string
                if isinstance(precios, str):
                    try:
                        precios = json.loads(precios)
                    except:
                        precios = {}
                if isinstance(extra_datos, str):
                    try:
                        extra_datos = json.loads(extra_datos)
                    except:
                        extra_datos = {}
                
                precio_valido = precio is not None and precio > 0
                
                # Construir dict de precios con nombre apropiado según proveedor
                precios_a_mostrar = {}
                if precio is not None and precio > 0:
                    # Convertir Decimal a float para compatibilidad con template
                    precio_float = float(precio) if hasattr(precio, '__float__') else precio
                    # Determinar el nombre del precio canónico según el proveedor
                    if proveedor_key == 'brementools':
                        precios_a_mostrar['Precio de Venta'] = precio_float
                    elif proveedor_key == 'crossmaster':
                        precios_a_mostrar['Precio Lista'] = precio_float
                    elif proveedor_key == 'berger':
                        precios_a_mostrar['Precio'] = precio_float
                    elif proveedor_key == 'chiesa':
                        precios_a_mostrar['Pr.Unit'] = precio_float
                    elif proveedor_key == 'cachan':
                        precios_a_mostrar['Precio'] = precio_float
                    elif proveedor_key == 'manual':
                        precios_a_mostrar['Precio'] = precio_float
                    else:
                        precios_a_mostrar['Precio'] = precio_float
                
                # Agregar precios adicionales del JSONB
                if precios:
                    for k, v in precios.items():
                        if v is not None and v != '':
                            # Convertir a float si es necesario
                            v_float = float(v) if hasattr(v, '__float__') else v
                            # Normalizar el nombre de la clave para comparación
                            k_lower = str(k).lower().strip().replace('  ', ' ')
                            # Usar nombres exactos para precios específicos
                            if k_lower in ['precio neto', 'precioneto']:
                                k_display = 'Precio Neto'
                            elif k_lower in ['precio neto unitario', 'precionetunitario']:
                                k_display = 'Precio Neto Unitario'
                            elif k_lower in ['precio de lista', 'precio lista', 'preciolista', 'preciodelista']:
                                k_display = 'Precio de Lista'
                            else:
                                # Capitalizar nombres de precios adicionales
                                k_display = k.title() if isinstance(k, str) else str(k)
                            
                            if k_display not in precios_a_mostrar:
                                precios_a_mostrar[k_display] = v_float
                
                # Precios calculados especiales por proveedor
                precios_calculados = {}
                if proveedor_key == 'chiesa' and (precio is not None):
                    try:
                        base = float(precio) if hasattr(precio, '__float__') else precio
                        precios_calculados['Costo (-4% extra)'] = round(base * 0.96, 4)
                        precios_calculados['Costo (+4% extra)'] = round(base * 1.04, 4)
                    except Exception:
                        pass

                resultados.append({
                    'codigo': str(codigo) if codigo is not None else '',
                    'nombre': nombre or '',
                    'precio': float(precio) if precio is not None else None,
                    'precio_valido': precio_valido,
                    'precios': precios_a_mostrar,
                    'proveedor': proveedor_nombre or proveedor_key or '',
                    'proveedor_key': proveedor_key or '',
                    'extra_datos': extra_datos or {},
                    'iva': iva or 'N/A',
                    'precios_calculados': precios_calculados,
                    'fuente': 'DB'
                })
            return resultados, total
    except Exception as exc:
        log_debug('buscar_productos_avanzados_db: error', exc)
        return [], 0

app.jinja_env.globals.update(generar_nombre_visible=generar_nombre_visible, formatear_precio=formatear_precio)

# --- FUNCIONES DB ---
def load_proveedores():
    # PostgreSQL preferente si está disponible
    if USE_SQLITE:
        try:
            with get_sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, data FROM proveedores")
                rows = cur.fetchall()
                if rows:
                    return {r['id']: json.loads(r['data']) for r in rows}
                # Si vacío, insertar default
                for pid, pdata in default_proveedores.items():
                    cur.execute("INSERT OR IGNORE INTO proveedores (id, data) VALUES (?, ?)", (pid, json.dumps(pdata, ensure_ascii=False)))
                conn.commit()
                return json.loads(json.dumps(default_proveedores))
        except Exception as e:
            log_debug('load_proveedores: fallo SQLite', e)
            print(f"[WARN] load_proveedores SQLite fallo: {e}. Se usa JSON local.")
    elif DATABASE_URL:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT id, data FROM proveedores")
                rows = cur.fetchall()
                if rows:
                    return {r['id']: r['data'] for r in rows}
                # Si vacío, insertar default
                for pid, pdata in default_proveedores.items():
                    cur.execute("INSERT INTO proveedores (id, data) VALUES (%s, %s::jsonb) ON CONFLICT (id) DO NOTHING", (pid, json.dumps(pdata)))
                conn.commit()
                return json.loads(json.dumps(default_proveedores))
        except Exception as e:
            log_debug('load_proveedores: fallo PG', e)
            print(f"[WARN] load_proveedores PG fallo: {e}. Se usa JSON local.")
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: no se pudo leer {DATA_FILE} -> usando valores por defecto. Error: {e}")
    return json.loads(json.dumps(default_proveedores))

def save_proveedores(data):
    # Guardar en PostgreSQL si existe
    if USE_SQLITE:
        try:
            with get_sqlite_conn() as conn:
                cur = conn.cursor()
                for pid, pdata in data.items():
                    cur.execute(
                        """
                        INSERT INTO proveedores (id, data) VALUES (?, ?)
                        ON CONFLICT(id) DO UPDATE SET data=excluded.data
                        """,
                        (pid, json.dumps(pdata, ensure_ascii=False))
                    )
                # Borrar los que no están ya
                cur.execute("SELECT id FROM proveedores")
                ids_db = {r['id'] for r in cur.fetchall()}
                ids_local = set(data.keys())
                for to_del in ids_db - ids_local:
                    cur.execute("DELETE FROM proveedores WHERE id=?", (to_del,))
                conn.commit()
                return
        except Exception as e:
            log_debug('save_proveedores: fallo SQLite', e)
            print(f"[WARN] save_proveedores SQLite fallo: {e}. Se intenta fallback JSON.")
    elif DATABASE_URL:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                for pid, pdata in data.items():
                    cur.execute("""
                        INSERT INTO proveedores (id, data) VALUES (%s, %s::jsonb)
                        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data
                    """, (pid, json.dumps(pdata)))
                # Borrar los que no están ya
                cur.execute("SELECT id FROM proveedores")
                ids_db = {r['id'] for r in cur.fetchall()}
                ids_local = set(data.keys())
                for to_del in ids_db - ids_local:
                    cur.execute("DELETE FROM proveedores WHERE id=%s", (to_del,))
                conn.commit()
                return
        except Exception as e:
            log_debug('save_proveedores: fallo PG', e)
            print(f"[WARN] save_proveedores PG fallo: {e}. Se intenta fallback JSON.")
    dirpath = os.path.dirname(DATA_FILE) or "."
    fd, tmp_path = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmpf:
            json.dump(data, tmpf, ensure_ascii=False, indent=4)
        os.replace(tmp_path, DATA_FILE)
    except Exception:
        try: os.remove(tmp_path)
        except Exception: pass
        raise

def load_historial():
    if USE_SQLITE:
        try:
            with get_sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM historial ORDER BY timestamp ASC")
                rows = cur.fetchall()
                result = []
                for r in rows:
                    d = dict(r)
                    val = d.get('porcentajes')
                    if isinstance(val, str):
                        try:
                            d['porcentajes'] = json.loads(val)
                        except Exception:
                            pass
                    result.append(d)
                return result
        except Exception as e:
            log_debug('load_historial: fallo SQLite', e)
            print(f"[WARN] load_historial SQLite fallo: {e}. Usando JSON local.")
    elif DATABASE_URL:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                cur.execute("SELECT * FROM historial ORDER BY timestamp ASC")
                rows = cur.fetchall()
                # Aseguramos que porcentajes sea dict si viene como texto
                for r in rows:
                    val = r.get('porcentajes')
                    if isinstance(val, str):
                        try:
                            r['porcentajes'] = json.loads(val)
                        except Exception:
                            pass
                return rows
        except Exception as e:
            log_debug('load_historial: fallo PG', e)
            print(f"[WARN] load_historial PG fallo: {e}. Usando JSON local.")
    if not os.path.exists(HISTORIAL_FILE):
        return []
    try:
        with open(HISTORIAL_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def atomic_save_historial_list(historial_list):
    if USE_SQLITE:
        try:
            with get_sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM historial")
                for item in historial_list:
                    cur.execute(
                        """
                        INSERT INTO historial (id_historial, timestamp, tipo_calculo, proveedor_nombre, producto,
                                               precio_base, porcentajes, precio_final, observaciones)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item.get('id_historial'),
                            item.get('timestamp'),
                            item.get('tipo_calculo'),
                            item.get('proveedor_nombre'),
                            item.get('producto'),
                            item.get('precio_base'),
                            json.dumps(item.get('porcentajes', {}), ensure_ascii=False),
                            item.get('precio_final'),
                            item.get('observaciones')
                        )
                    )
                conn.commit()
                return
        except Exception as e:
            log_debug('atomic_save_historial_list: fallo SQLite', e)
            print(f"[WARN] atomic_save_historial_list SQLite fallo: {e}. Fallback JSON.")
    elif DATABASE_URL:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                # estrategia simple: truncar y reinsertar
                cur.execute("DELETE FROM historial")
                for item in historial_list:
                    data_insert = dict(item)
                    data_insert['porcentajes'] = json.dumps(item.get('porcentajes', {}), ensure_ascii=False)
                    cur.execute("""
                        INSERT INTO historial (id_historial, timestamp, tipo_calculo, proveedor_nombre, producto,
                                               precio_base, porcentajes, precio_final, observaciones)
                        VALUES (%(id_historial)s, %(timestamp)s, %(tipo_calculo)s, %(proveedor_nombre)s, %(producto)s,
                                %(precio_base)s, %(porcentajes)s::jsonb, %(precio_final)s, %(observaciones)s)
                    """, data_insert)
                conn.commit()
                return
        except Exception as e:
            log_debug('atomic_save_historial_list: fallo PG', e)
            print(f"[WARN] atomic_save_historial_list PG fallo: {e}. Fallback JSON.")
    dirpath = os.path.dirname(HISTORIAL_FILE) or "."
    fd, tmp_path = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmpf:
            json.dump(historial_list, tmpf, ensure_ascii=False, indent=4)
        os.replace(tmp_path, HISTORIAL_FILE)
    except Exception:
        try: os.remove(tmp_path)
        except Exception: pass
        raise

def add_entry_to_historial(nueva_entrada):
    if USE_SQLITE:
        try:
            with get_sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO historial (id_historial, timestamp, tipo_calculo, proveedor_nombre, producto,
                                           precio_base, porcentajes, precio_final, observaciones)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        nueva_entrada.get('id_historial'),
                        nueva_entrada.get('timestamp'),
                        nueva_entrada.get('tipo_calculo'),
                        nueva_entrada.get('proveedor_nombre'),
                        nueva_entrada.get('producto'),
                        nueva_entrada.get('precio_base'),
                        json.dumps(nueva_entrada.get('porcentajes', {}), ensure_ascii=False),
                        nueva_entrada.get('precio_final'),
                        nueva_entrada.get('observaciones')
                    )
                )
                conn.commit()
                log_debug('add_entry_to_historial (sqlite): insert OK', nueva_entrada.get('id_historial'))
                return
        except Exception as e:
            log_debug('add_entry_to_historial: fallo SQLite', e)
            print(f"[WARN] add_entry_to_historial SQLite fallo: {e}. Se usa JSON.")
    elif DATABASE_URL:
        try:
            with get_pg_conn() as conn, conn.cursor() as cur:
                data_insert = dict(nueva_entrada)
                data_insert['porcentajes'] = json.dumps(nueva_entrada.get('porcentajes', {}), ensure_ascii=False)
                cur.execute("""
                    INSERT INTO historial (id_historial, timestamp, tipo_calculo, proveedor_nombre, producto,
                                           precio_base, porcentajes, precio_final, observaciones)
                    VALUES (%(id_historial)s, %(timestamp)s, %(tipo_calculo)s, %(proveedor_nombre)s, %(producto)s,
                            %(precio_base)s, %(porcentajes)s::jsonb, %(precio_final)s, %(observaciones)s)
                """, data_insert)
                conn.commit()
                log_debug('add_entry_to_historial: insert OK', data_insert.get('id_historial'))
                return
        except Exception as e:
            log_debug('add_entry_to_historial: fallo PG', e)
            print(f"[WARN] add_entry_to_historial PG fallo: {e}. Se usa JSON.")
    historial_actual = load_historial() or []
    historial_actual.append(nueva_entrada)
    atomic_save_historial_list(historial_actual)

# --- ACTUALIZACIÓN DE LISTAS EXCEL ---
def inferir_nombre_base_archivo(nombre_original, proveedores_dict):
    """Intenta inferir el nombre base del proveedor a partir del nombre de archivo subido.
    Compara la porción alfabética normalizada contra los nombres_base existentes.
    """
    base_sin_ext = os.path.splitext(nombre_original)[0]
    letras = ''.join(c for c in base_sin_ext if c.isalpha())
    norm_archivo = normalize_text(letras)
    for p in proveedores_dict.values():
        norm_prov = normalize_text(''.join(c for c in p.get('nombre_base','') if c.isalpha()))
        if norm_prov and (norm_prov in norm_archivo or norm_archivo in norm_prov):
            return p['nombre_base']
    # Si no se encuentra coincidencia devuelve el nombre original sin números
    return letras or base_sin_ext

def humanizar_tiempo_desde(timestamp_segundos):
    try:
        delta = now_local() - ts_to_local(timestamp_segundos)
        if delta.days > 0:
            return f"{delta.days} día(s) atrás"
        horas = delta.seconds // 3600
        if horas > 0:
            return f"{horas} hora(s) atrás"
        minutos = (delta.seconds % 3600) // 60
        if minutos > 0:
            return f"{minutos} minuto(s) atrás"
        return "Hace instantes"
    except Exception:
        return "-"

# --- LÓGICA DE CÁLCULO ---
proveedores = load_proveedores()

def core_math(precio, iva, descuentos, ganancias):
    precio_actual = precio
    for desc in descuentos:
        if desc is not None: precio_actual *= (1 - desc)
    if iva is not None: precio_actual *= (1 + iva)
    for ganc in ganancias:
        if ganc is not None: precio_actual *= (1 + ganc)
    return round(precio_actual, 4)

# --- RUTA PRINCIPAL ---
@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    global proveedores 
    mensaje = None
    resultado_auto = None
    resultado_manual = None
    productos_encontrados = None
    proveedor_id_seleccionado = None
    datos_seleccionados = {}
    active_tab = "busqueda" 
    proveedor_buscado = ""
    filtro_resultados = ""
    # --- MODIFICACIÓN ---
    datos_calculo_auto = {}
    datos_calculo_manual = {}
    productos_manual = []
    productos_manual_error = None
    ventas_inputs = {
        "codigo": "",
        "monto": "",
        "cantidad": "",
        "precio": "",
        "codigo_comp": "",
        "precio_comp": "",
        "cantidad_comp": "",
        "combo_cantidad_principal": "",
        "combo_cantidad_complemento": "",
        "combo_monto_principal": "",
        "combo_monto_complemento": "",
        "combo_monto_disponible": "",
        "nombre_principal": "",
        "nombre_complemento": ""
    }
    ventas_resultado_por_monto = None
    ventas_resultado_por_cantidad = None
    ventas_resultado_combo = None
    ventas_mensaje = None
    ventas_busqueda_query = ""
    ventas_busqueda_page = 1
    ventas_busqueda_total_paginas = 0
    ventas_busqueda_total_resultados = 0
    ventas_busqueda_resultados = []
    ventas_producto_seleccionado = None
    ventas_producto_complemento = None
    per_page = VENTAS_AVANZADAS_PER_PAGE
    ventas_formularios = {
        "ventas_avanzadas_buscar",
        "ventas_avanzadas_select",
        "ventas_avanzadas_monto",
        "ventas_avanzadas_cantidad",
        "ventas_avanzadas_select_complemento",
        "ventas_avanzadas_combo",
        "ventas_operaciones_manage"
    }
    ventas_operaciones = list(session.get("ventas_operaciones", []))

    operaciones_actualizadas = False
    for operacion in ventas_operaciones:
        if "id" not in operacion:
            operacion["id"] = str(uuid.uuid4())
            operaciones_actualizadas = True
    if operaciones_actualizadas:
        session['ventas_operaciones'] = ventas_operaciones
        session.modified = True

    productos_manual, productos_manual_error = load_manual_products()
    if productos_manual_error:
        ventas_mensaje = productos_manual_error

    def preparar_busqueda(query, page):
        nonlocal ventas_busqueda_query, ventas_busqueda_page, ventas_busqueda_total_paginas
        nonlocal ventas_busqueda_total_resultados, ventas_busqueda_resultados
        query = (query or "").strip()
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        if page < 1:
            page = 1

        ventas_busqueda_query = query
        ventas_busqueda_page = page
        coincidencias = []
        if LISTAS_EN_DB and DATABASE_URL and psycopg:
            # Buscar en PostgreSQL (TODOS los proveedores, no solo manual)
            resultados_db, total_db = buscar_productos_avanzados_db(query, page, per_page, proveedor_filter=None)
            ventas_busqueda_total_resultados = int(total_db)
            ventas_busqueda_total_paginas = max(1, math.ceil(ventas_busqueda_total_resultados / per_page)) if total_db else 0
            ventas_busqueda_resultados = resultados_db
            coincidencias = resultados_db  # para construir el mensaje
            return coincidencias
        else:
            # Fallback a Excel: solo productos manuales
            coincidencias = buscar_productos_manual(productos_manual, query)
            ventas_busqueda_total_resultados = len(coincidencias)

        log_debug(
            "preparar_busqueda: resultados",
            {
                "query": query,
                "pagina": ventas_busqueda_page,
                "total_coincidencias": ventas_busqueda_total_resultados,
                "productos_pagina": len(coincidencias[:per_page]),
            },
        )

        if ventas_busqueda_total_resultados == 0:
            ventas_busqueda_total_paginas = 0
            ventas_busqueda_page = 1
            ventas_busqueda_resultados = []
            return coincidencias

        ventas_busqueda_total_paginas = max(1, math.ceil(ventas_busqueda_total_resultados / per_page))
        if ventas_busqueda_page > ventas_busqueda_total_paginas:
            ventas_busqueda_page = ventas_busqueda_total_paginas
        inicio = (ventas_busqueda_page - 1) * per_page
        fin = inicio + per_page
        ventas_busqueda_resultados = coincidencias[inicio:fin]
        return coincidencias

    formulario = None
    if request.method == "POST":
        formulario = request.form.get("formulario")
        active_tab = request.form.get("active_tab", "busqueda")

        persist_aliases = {
            "codigo": ["ventas_codigo"],
            "nombre_principal": ["ventas_nombre", "combo_nombre_principal"],
            "precio": ["combo_precio_principal", "ventas_precio"],
            "monto": ["ventas_monto"],
            "cantidad": ["ventas_cantidad"],
            "codigo_comp": ["ventas_codigo_comp"],
            "nombre_complemento": ["ventas_nombre_comp", "combo_nombre_complemento"],
            "precio_comp": ["combo_precio_complemento", "ventas_precio_comp"],
            "combo_cantidad_principal": ["combo_cantidad_principal"],
            "combo_cantidad_complemento": ["combo_cantidad_complemento"],
            "combo_monto_principal": ["combo_monto_principal"],
            "combo_monto_complemento": ["combo_monto_complemento"],
            "combo_monto_disponible": ["combo_monto_disponible"]
        }
        for dest_key, aliases in persist_aliases.items():
            for alias in aliases:
                if alias in request.form:
                    alias_value = request.form.get(alias, "")
                    ventas_inputs[dest_key] = alias_value.strip() if isinstance(alias_value, str) else alias_value
                    break

        if formulario == "consulta_producto":
            termino_busqueda = request.form.get("termino_busqueda", "").strip()
            proveedor_buscado = request.form.get("proveedor_busqueda", "") # Capturar proveedor
            filtro_resultados = request.form.get("filtro_resultados", "").strip()
            # Paginación (acepta GET o POST)
            try:
                busqueda_page_value = int((request.values.get("page") or 1))
            except Exception:
                busqueda_page_value = 1
            try:
                busqueda_per_page_value = int((request.values.get("per_page") or 20))
            except Exception:
                busqueda_per_page_value = 20
            if busqueda_per_page_value not in (10, 20, 50, 100):
                busqueda_per_page_value = 20
            if busqueda_page_value < 1:
                busqueda_page_value = 1

            if not termino_busqueda:
                mensaje = "⚠️ POR FAVOR, INGRESA UN CÓDIGO O NOMBRE."
            else:
                productos_encontrados = []
                proveedor_key_filter = provider_name_to_key(proveedor_buscado) if proveedor_buscado else ''

                # Intentar buscar en la base de datos primero si está habilitada
                total_db = 0
                if LISTAS_EN_DB and DATABASE_URL and psycopg:
                    print(f'[DEBUG consulta_producto] Buscando en DB: termino="{termino_busqueda}", proveedor="{proveedor_key_filter}"', flush=True)
                    try:
                        # Traer TODOS los resultados (máximo 5000) para paginación del lado del cliente
                        resultados_db, total_db = buscar_productos_avanzados_db(
                            termino_busqueda,
                            page=1,
                            per_page=5000,  # Traer hasta 5000 resultados
                            proveedor_filter=proveedor_key_filter if proveedor_key_filter else None
                        )
                        
                        print(f'[DEBUG consulta_producto] Resultados de DB: {total_db} productos encontrados', flush=True)
                        
                        if resultados_db:
                            # Convertir formato de buscar_productos_avanzados_db al formato esperado por el template
                            for r in resultados_db:
                                productos_encontrados.append({
                                    'codigo': r.get('codigo', ''),
                                    'producto': r.get('nombre', ''),
                                    'proveedor': r.get('proveedor', ''),
                                    'proveedor_key': r.get('proveedor_key', ''),
                                    'sheet_name': '',  # No aplicable para DB
                                    'iva': r.get('iva', 'N/A'),
                                    'precios': r.get('precios', {}),
                                    'extra_datos': r.get('extra_datos', {}),
                                    'precios_calculados': r.get('precios_calculados', {}),
                                    'fuente': r.get('fuente', 'DB')  # Asegurar que tenga fuente
                                })
                            
                            print(f'[DEBUG consulta_producto] Usando {len(productos_encontrados)} resultados de DB', flush=True)
                    except Exception as exc:
                        print(f'[ERROR consulta_producto] Error en búsqueda DB: {exc}', flush=True)
                        log_debug('consulta_producto: error en búsqueda DB, usando fallback a Excel', exc)
                
                # Fallback a búsqueda en Excel si no hay resultados de DB o DB no está habilitada
                if not productos_encontrados:
                    if not USAR_FALLBACK_EXCEL:
                        print(f'[DEBUG consulta_producto] No hay resultados de DB y fallback a Excel está desactivado', flush=True)
                    else:
                        print(f'[DEBUG consulta_producto] No hay resultados de DB, usando fallback a Excel', flush=True)
                        
                        # 1. Buscar en productos_manual.xlsx primero (si no hay filtro de proveedor o es 'manual')
                        if not proveedor_key_filter or proveedor_key_filter == 'manual':
                            productos_manual_list, err_manual = load_manual_products()
                            if productos_manual_list and not err_manual:
                                # Buscar por código o nombre
                                if termino_busqueda.isdigit() and len(termino_busqueda) > 2:
                                    # Búsqueda por código
                                    for p in productos_manual_list:
                                        codigo_str = str(p.get('codigo', '')).strip()
                                        if codigo_str == termino_busqueda:
                                            productos_encontrados.append({
                                                'codigo': codigo_str,
                                                'producto': p.get('nombre', ''),
                                                'proveedor': f"{p.get('proveedor', 'Manual')} (Hoja: Manual)",
                                                'proveedor_key': 'manual',
                                                'sheet_name': 'Manual',
                                                'iva': 'N/A',
                                                'precios': {'Precio': p.get('precio', 0.0)},
                                            'extra_datos': {},
                                            'precios_calculados': {},
                                            'fuente': 'Excel'
                                        })
                            else:
                                # Búsqueda por nombre
                                termino_norm = normalize_text(formatear_pulgadas(termino_busqueda))
                                palabras = [token for token in termino_norm.split() if token]
                                for p in productos_manual_list:
                                    nombre_norm = normalize_text(formatear_pulgadas(p.get('nombre', '')))
                                    if palabras:
                                        if all(palabra in nombre_norm for palabra in palabras):
                                            productos_encontrados.append({
                                                'codigo': str(p.get('codigo', '')),
                                                'producto': p.get('nombre', ''),
                                                'proveedor': f"{p.get('proveedor', 'Manual')} (Hoja: Manual)",
                                                'proveedor_key': 'manual',
                                                'sheet_name': 'Manual',
                                                'iva': 'N/A',
                                                'precios': {'Precio': p.get('precio', 0.0)},
                                                'extra_datos': {},
                                                'precios_calculados': {},
                                                'fuente': 'Excel'
                                            })
                                    else:
                                        if termino_norm in nombre_norm:
                                            productos_encontrados.append({
                                                'codigo': str(p.get('codigo', '')),
                                                'producto': p.get('nombre', ''),
                                                'proveedor': f"{p.get('proveedor', 'Manual')} (Hoja: Manual)",
                                                'proveedor_key': 'manual',
                                                'sheet_name': 'Manual',
                                                'iva': 'N/A',
                                                'precios': {'Precio': p.get('precio', 0.0)},
                                                'extra_datos': {},
                                                'precios_calculados': {},
                                                'fuente': 'Excel'
                                            })

                    # 2. Buscar en archivos Excel de proveedores
                    try:
                        excel_files = sorted(os.listdir(LISTAS_PATH))
                    except Exception as exc:
                        mensaje = f"❌ ERROR LISTANDO ARCHIVOS: {exc}"
                        excel_files = []

                    for filename in excel_files:
                        if not filename.lower().endswith(('.xlsx', '.xls')):
                            continue
                        if 'old' in filename.lower():
                            continue

                        provider_key = provider_key_from_filename(filename)
                        if proveedor_key_filter and provider_key != proveedor_key_filter:
                            continue

                        config = EXCEL_PROVIDER_CONFIG.get(provider_key)
                        if not config:
                            continue

                        header_row_index = config.get('fila_encabezado')
                        if header_row_index is None:
                            continue

                        file_path = os.path.join(LISTAS_PATH, filename)
                        try:
                            all_sheets = pd.read_excel(file_path, sheet_name=None, header=header_row_index)
                        except Exception as exc:
                            mensaje = f"❌ ERROR PROCESANDO {filename}: {exc}"
                            continue

                        proveedor_display_name = get_proveedor_display_name(provider_key)

                        for sheet_name, df in all_sheets.items():
                            if df.empty:
                                continue

                            df.columns = [normalize_text(c) for c in df.columns]
                            actual_cols = {
                                'codigo': next((alias for alias in config['codigo'] if alias in df.columns), None),
                                'producto': next((alias for alias in config['producto'] if alias in df.columns), None),
                                'iva': next((alias for alias in config.get('iva', []) if alias in df.columns), None),
                                'precios_a_mostrar': [alias for alias in config.get('precios_a_mostrar', []) if alias in df.columns],
                                'extra_datos': [alias for alias in config.get('extra_datos', []) if alias in df.columns]
                            }

                            if not actual_cols['codigo'] or not actual_cols['producto']:
                                continue

                            codigo_series = df[actual_cols['codigo']].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else '')

                            if termino_busqueda.isdigit() and len(termino_busqueda) > 2:
                                condition = codigo_series == termino_busqueda
                            else:
                                termino_norm = normalize_text(formatear_pulgadas(termino_busqueda))
                                palabras = [token for token in termino_norm.split() if token]
                                producto_busqueda = df[actual_cols['producto']].apply(lambda x: normalize_text(formatear_pulgadas(x)))
                                if palabras:
                                    condition = producto_busqueda.apply(lambda nombre: all(palabra in nombre for palabra in palabras))
                                else:
                                    condition = producto_busqueda.str.contains(termino_norm)

                            if not condition.any():
                                continue

                            for idx in df.index[condition]:
                                fila = df.loc[idx]
                                codigo_val = codigo_series.loc[idx]
                                producto = build_producto_entry(
                                    fila,
                                    actual_cols,
                                    provider_key,
                                    proveedor_display_name,
                                    sheet_name,
                                    df.columns,
                                    codigo_override=codigo_val
                                )
                                productos_encontrados.append(producto)
                
                # Si estamos en fallback Excel (sin DB) y aún no se filtró, aplicar paginado en memoria
                if (not (LISTAS_EN_DB and DATABASE_URL and psycopg)) or total_db == 0:
                    if productos_encontrados:
                        start_idx = (busqueda_page_value - 1) * busqueda_per_page_value
                        end_idx = start_idx + busqueda_per_page_value
                        total_db = len(productos_encontrados)
                        productos_encontrados = productos_encontrados[start_idx:end_idx]

                # Aplicar filtro adicional si se especificó
                if filtro_resultados and productos_encontrados:
                    productos_filtrados = []
                    filtro_norm = normalize_text(filtro_resultados)
                    for producto in productos_encontrados:
                        # Busca el filtro en el nombre, código o marca del producto
                        texto_busqueda = f"{producto['producto']} {producto['codigo']} {producto.get('extra_datos', {}).get('Marca', '')}"
                        if filtro_norm in normalize_text(texto_busqueda):
                            productos_filtrados.append(producto)
                    
                    # Para mostrar conteo total filtrado manteniendo paginación
                    total_filtrado = len(productos_filtrados)
                    mensaje = f"✅ SE ENCONTRARON {total_filtrado} COINCIDENCIA(S) AL FILTRAR POR '{filtro_resultados}'."
                    # Paginar manualmente los filtrados
                    start_idx = (busqueda_page_value - 1) * busqueda_per_page_value
                    end_idx = start_idx + busqueda_per_page_value
                    productos_encontrados = productos_filtrados[start_idx:end_idx]
                    total_db = total_filtrado

                # Si no se aplicó filtro y usamos DB, total_db ya viene de la consulta
                if not filtro_resultados:
                    if LISTAS_EN_DB and DATABASE_URL and psycopg:
                        total = total_db
                    else:
                        total = len(productos_encontrados)
                else:
                    total = total_db

                if not productos_encontrados and not mensaje:
                    mensaje = f"ℹ️ NO SE ENCONTRARON RESULTADOS PARA '{termino_busqueda}'."
                elif productos_encontrados and not mensaje:
                    mensaje = f"✅ SE ENCONTRARON {total} COINCIDENCIA(S)."

                # Calcular totales de paginación para template
                try:
                    import math
                    busqueda_total_paginas = max(1, math.ceil(max(0, int(total)) / busqueda_per_page_value))
                except Exception:
                    busqueda_total_paginas = 1
                busqueda_total_resultados = int(total)
                # Guardar en variables de contexto más adelante
                locals().update({
                    'busqueda_page_value': busqueda_page_value,
                    'busqueda_per_page_value': busqueda_per_page_value,
                    'busqueda_total_paginas': busqueda_total_paginas,
                    'busqueda_total_resultados': busqueda_total_resultados,
                })
        
        elif formulario == "ventas_avanzadas_buscar":
            active_tab = "ventas_avanzadas"
            query = request.form.get("ventas_busqueda", "").strip()
            page = request.form.get("ventas_page", "1")
            coincidencias = preparar_busqueda(query, page)
            ventas_resultado_por_monto = None
            ventas_resultado_por_cantidad = None
            if coincidencias:
                ventas_mensaje = f"✅ Se encontraron {len(coincidencias)} producto(s) coincidente(s)."
            else:
                ventas_mensaje = "ℹ️ No se encontraron productos para esa búsqueda."

        elif formulario == "ventas_avanzadas_select":
            active_tab = "ventas_avanzadas"
            query = request.form.get("ventas_busqueda", "").strip()
            page = request.form.get("ventas_page", "1")
            preparar_busqueda(query, page)
            ventas_resultado_por_monto = None
            ventas_resultado_por_cantidad = None
            codigo_sel = request.form.get("ventas_select_codigo", "").strip()
            nombre_sel = request.form.get("ventas_select_nombre", "").strip()
            precio_sel = parse_price_value(request.form.get("ventas_select_precio"))
            proveedor_sel = request.form.get("ventas_select_proveedor", "").strip()

            producto = None
            
            # Primero buscar en ventas_busqueda_resultados (pueden ser de DB o Excel)
            if codigo_sel and ventas_busqueda_resultados:
                # Buscar coincidencia exacta por código, nombre y precio
                producto = next((p for p in ventas_busqueda_resultados 
                                if str(p.get("codigo", "")) == codigo_sel 
                                and normalize_text(p.get("nombre", "")) == normalize_text(nombre_sel)
                                and (precio_sel is None or (p.get("precio") is not None and abs(p.get("precio", 0) - precio_sel) < 0.01))), None)
                
                # Si no se encuentra, buscar solo por código
                if not producto:
                    producto = next((p for p in ventas_busqueda_resultados if str(p.get("codigo", "")) == codigo_sel), None)
            
            # Fallback: buscar en productos_manual de Excel si no está en DB
            if not producto and codigo_sel and productos_manual:
                producto = next((p for p in productos_manual 
                                if str(p.get("codigo", "")) == codigo_sel 
                                and normalize_text(p.get("nombre", "")) == normalize_text(nombre_sel)
                                and (precio_sel is None or abs(p.get("precio", 0) - precio_sel) < 0.01)), None)
                if not producto:
                    producto = next((p for p in productos_manual if str(p.get("codigo", "")) == codigo_sel), None)

            # Si aún no hay producto pero tenemos datos, construirlo
            if not producto and codigo_sel and precio_sel is not None and nombre_sel:
                precio_valido = precio_sel > 0
                producto = {
                    "codigo": codigo_sel, 
                    "nombre": nombre_sel, 
                    "precio": precio_sel,
                    "precio_valido": precio_valido,
                    "proveedor": proveedor_sel or "Manual"
                }

            if producto:
                ventas_producto_seleccionado = producto
                ventas_inputs["codigo"] = str(producto.get("codigo", ""))
                ventas_inputs["nombre_principal"] = producto.get("nombre", "")
                if producto.get("precio_valido", False) or (producto.get("precio") is not None and producto.get("precio", 0) > 0):
                    precio_fmt = f"{float(producto.get('precio', 0)):.2f}"
                    ventas_inputs["precio"] = precio_fmt.rstrip("0").rstrip(".") if "." in precio_fmt else precio_fmt
                else:
                    ventas_inputs["precio"] = ""
                if not producto.get("precio_valido", False) and (producto.get("precio") is None or producto.get("precio", 0) <= 0):
                    ventas_mensaje = "⚠️ El producto seleccionado no tiene precio válido."
                elif not ventas_mensaje:
                    ventas_mensaje = "✅ Producto seleccionado."
            else:
                ventas_mensaje = "⚠️ No se pudo seleccionar el producto."

        elif formulario == "ventas_avanzadas_select_complemento":
            active_tab = "ventas_avanzadas"
            query = request.form.get("ventas_busqueda", "").strip()
            page = request.form.get("ventas_page", "1")
            preparar_busqueda(query, page)
            ventas_resultado_por_monto = None
            ventas_resultado_por_cantidad = None

            codigo_sel = request.form.get("ventas_select_codigo", "").strip()
            nombre_sel = request.form.get("ventas_select_nombre", "").strip()
            precio_sel = parse_price_value(request.form.get("ventas_select_precio"))
            proveedor_sel = request.form.get("ventas_select_proveedor", "").strip()

            producto = None
            
            # Primero buscar en ventas_busqueda_resultados (pueden ser de DB o Excel)
            if codigo_sel and ventas_busqueda_resultados:
                # Buscar coincidencia exacta por código, nombre y precio
                producto = next((p for p in ventas_busqueda_resultados 
                                if str(p.get("codigo", "")) == codigo_sel 
                                and normalize_text(p.get("nombre", "")) == normalize_text(nombre_sel)
                                and (precio_sel is None or (p.get("precio") is not None and abs(p.get("precio", 0) - precio_sel) < 0.01))), None)
                
                # Si no se encuentra, buscar solo por código
                if not producto:
                    producto = next((p for p in ventas_busqueda_resultados if str(p.get("codigo", "")) == codigo_sel), None)
            
            # Fallback: buscar en productos_manual de Excel si no está en DB
            if not producto and codigo_sel and productos_manual:
                producto = next((p for p in productos_manual 
                                if str(p.get("codigo", "")) == codigo_sel 
                                and normalize_text(p.get("nombre", "")) == normalize_text(nombre_sel)
                                and (precio_sel is None or abs(p.get("precio", 0) - precio_sel) < 0.01)), None)
                if not producto:
                    producto = next((p for p in productos_manual if str(p.get("codigo", "")) == codigo_sel), None)

            # Si aún no hay producto pero tenemos datos, construirlo
            if not producto and codigo_sel and precio_sel is not None and nombre_sel:
                precio_valido = precio_sel > 0
                producto = {
                    "codigo": codigo_sel, 
                    "nombre": nombre_sel, 
                    "precio": precio_sel, 
                    "precio_valido": precio_valido,
                    "proveedor": proveedor_sel or "Manual"
                }

            if producto:
                ventas_producto_complemento = producto
                ventas_inputs["codigo_comp"] = str(producto.get("codigo", ""))
                ventas_inputs["nombre_complemento"] = producto.get("nombre", "")
                if producto.get("precio_valido", False) or (producto.get("precio") is not None and producto.get("precio", 0) > 0):
                    precio_fmt = f"{float(producto.get('precio', 0)):.2f}"
                    ventas_inputs["precio_comp"] = precio_fmt.rstrip("0").rstrip(".") if "." in precio_fmt else precio_fmt
                else:
                    ventas_inputs["precio_comp"] = ""
                tiene_precio = producto.get("precio_valido", False) or (producto.get("precio") is not None and producto.get("precio", 0) > 0)
                ventas_mensaje = "✅ Complemento seleccionado." if tiene_precio else "ℹ️ Complemento seleccionado sin precio válido, ingresá uno manualmente para los cálculos."
            else:
                ventas_mensaje = "⚠️ No se pudo seleccionar el complemento."

        elif formulario == "calcular_auto":
            datos_calculo_auto = {k: v for k, v in request.form.items()} # Capturar datos
            proveedor_id = request.form.get("proveedor_id")
            precio_raw = request.form.get("precio")
            producto_label = request.form.get("auto_producto", "") # Capturar el producto opcional

            if proveedor_id and precio_raw:
                try:
                    precio = float(precio_raw.replace(".", "").replace(",", "."))
                    datos_prov = proveedores.get(proveedor_id)
                    descuentos = [datos_prov.get("descuento", 0)]
                    ganancias = [datos_prov.get("ganancia", 0)]
                    iva = datos_prov.get("iva", 0)
                    precio_final = core_math(precio, iva, descuentos, ganancias)
                    
                    nombre_visible_prov = generar_nombre_visible(proveedores[proveedor_id])
                    resultado_auto = f"{formatear_precio(precio_final)} (Proveedor: {nombre_visible_prov})"
                    add_entry_to_historial({
                        "id_historial": str(uuid.uuid4()), "timestamp": now_local().strftime("%Y-%m-%d %H:%M:%S"),
                        "tipo_calculo": "Automático", "proveedor_nombre": nombre_visible_prov,
                        "producto": producto_label or "N/A", # Guardar el producto
                        "precio_base": precio, "porcentajes": {"descuento": descuentos[0], "iva": iva, "ganancia": ganancias[0]},
                        "precio_final": precio_final, "observaciones": ""
                    })
                except Exception as e:
                    mensaje = f"⚠️ ERROR CÁLCULO AUTO: {e}"
            else:
                mensaje = "⚠️ COMPLETA PROVEEDOR Y PRECIO."

        elif formulario == "calcular_manual":
            datos_calculo_manual = {k: v for k, v in request.form.items()} # Capturar datos
            
            precio_raw = datos_calculo_manual.get("manual_precio")
            if precio_raw:
                try:
                    precio = float(precio_raw.replace(".", "").replace(",", "."))
                    nombre_prov_label = datos_calculo_manual.get("manual_proveedor_label", "").strip() or "N/A"
                    producto_label = datos_calculo_manual.get("manual_producto", "")
                    obs_label = datos_calculo_manual.get("manual_observaciones", "")

                    desc_manual = parse_percentage(datos_calculo_manual.get("manual_descuento")) or 0.0
                    desc_extra1 = parse_percentage(datos_calculo_manual.get("desc_extra_1")) or 0.0
                    desc_extra2 = parse_percentage(datos_calculo_manual.get("desc_extra_2")) or 0.0
                    
                    iva_manual = parse_percentage(datos_calculo_manual.get("manual_iva")) or 0.0
                    
                    ganc_manual = parse_percentage(datos_calculo_manual.get("manual_ganancia")) or 0.0
                    ganc_extra = parse_percentage(datos_calculo_manual.get("ganancia_extra")) or 0.0

                    descuentos = [desc_manual, desc_extra1, desc_extra2]
                    ganancias = [ganc_manual, ganc_extra]
                    
                    precio_final = core_math(precio, iva_manual, descuentos, ganancias)
                    resultado_manual = f"{formatear_precio(precio_final)}"
                    
                    mensaje = "✅ Cálculo Manual Realizado y Guardado en Historial."
                    
                    add_entry_to_historial({
                        "id_historial": str(uuid.uuid4()), "timestamp": now_local().strftime("%Y-%m-%d %H:%M:%S"),
                        "tipo_calculo": "Manual", "proveedor_nombre": nombre_prov_label, 
                        "producto": producto_label or "N/A", "precio_base": precio,
                        "porcentajes": {
                            "descuento": desc_manual, "descuento_extra_1": desc_extra1, "descuento_extra_2": desc_extra2,
                            "iva": iva_manual, "ganancia": ganc_manual, "ganancia_extra": ganc_extra
                        },
                        "precio_final": precio_final, "observaciones": obs_label or ""
                    })
                except Exception as e:
                    mensaje = f"⚠️ ERROR CÁLCULO MANUAL: {e}"
            else:
                mensaje = "⚠️ PRECIO MANUAL NO PUEDE ESTAR VACÍO."

        elif formulario == "ventas_avanzadas_monto":
            active_tab = "ventas_avanzadas"
            ventas_inputs["codigo"] = request.form.get("ventas_codigo", "").strip()
            ventas_inputs["monto"] = request.form.get("ventas_monto", "").strip()
            ventas_inputs["precio"] = request.form.get("ventas_precio", "").strip()
            query = request.form.get("ventas_busqueda", "").strip()
            page = request.form.get("ventas_page", "1")
            preparar_busqueda(query, page)
            ventas_resultado_por_monto = None
            if not productos_manual:
                ventas_mensaje = productos_manual_error or "⚠️ No hay productos manuales cargados."
            elif not ventas_inputs["codigo"] or not ventas_inputs["monto"]:
                ventas_mensaje = "⚠️ Seleccioná un producto y escribí el monto disponible."
            else:
                producto = next((p for p in productos_manual if str(p["codigo"]) == ventas_inputs["codigo"]), None)
                if not producto:
                    ventas_mensaje = "⚠️ Producto no encontrado en la lista manual."
                else:
                    ventas_inputs["nombre_principal"] = producto.get("nombre", "")
                    precio_input = ventas_inputs["precio"].strip()
                    try:
                        monto = float(ventas_inputs["monto"].replace(".", "").replace(",", "."))
                    except ValueError:
                        ventas_mensaje = "⚠️ El monto ingresado no es válido."
                    else:
                        precio_unitario = None
                        if precio_input:
                            precio_unitario = parse_price_value(precio_input)
                        if (precio_unitario is None or precio_unitario <= 0) and producto.get("precio_valido", False):
                            precio_unitario = producto["precio"]
                        if precio_unitario is None or precio_unitario <= 0:
                            ventas_mensaje = "⚠️ Ingresá un precio unitario válido mayor a cero."
                            ventas_producto_seleccionado = producto
                        else:
                            precio_fmt = f"{float(precio_unitario):.2f}"
                            ventas_inputs["precio"] = precio_input or (precio_fmt.rstrip("0").rstrip(".") if "." in precio_fmt else precio_fmt)
                            cantidad = int(monto // precio_unitario)
                            cantidad_exacta = monto / precio_unitario if precio_unitario else 0
                            resto = round(monto - (cantidad * precio_unitario), 2)
                            ventas_resultado_por_monto = {
                                "producto": producto,
                                "monto": monto,
                                "precio_unitario": precio_unitario,
                                "cantidad": cantidad,
                                "cantidad_exacta": cantidad_exacta,
                                "resto": resto if resto > 0 else 0.0
                            }
                            ventas_producto_seleccionado = producto
                            if cantidad == 0:
                                ventas_mensaje = "ℹ️ Con el monto indicado no alcanza para una unidad completa."
                            else:
                                ventas_mensaje = "✅ Cálculo realizado."
                            if ventas_resultado_por_monto:
                                nueva_operacion = {
                                    "id": str(uuid.uuid4()),
                                    "tipo": "monto",
                                    "timestamp": now_local().strftime("%H:%M:%S"),
                                    "producto": {
                                        "codigo": producto.get("codigo"),
                                        "nombre": producto.get("nombre"),
                                        "precio": float(precio_unitario)
                                    },
                                    "monto": float(monto),
                                    "cantidad": int(cantidad),
                                    "cantidad_exacta": float(cantidad_exacta),
                                    "resto": float(ventas_resultado_por_monto.get("resto", 0.0))
                                }
                                ventas_operaciones.append(nueva_operacion)
                                ventas_operaciones = ventas_operaciones[-20:]
                                session['ventas_operaciones'] = ventas_operaciones
                                session.modified = True

        elif formulario == "ventas_avanzadas_cantidad":
            active_tab = "ventas_avanzadas"
            ventas_inputs["codigo"] = request.form.get("ventas_codigo", "").strip()
            ventas_inputs["cantidad"] = request.form.get("ventas_cantidad", "").strip()
            ventas_inputs["precio"] = request.form.get("ventas_precio", "").strip()
            query = request.form.get("ventas_busqueda", "").strip()
            page = request.form.get("ventas_page", "1")
            preparar_busqueda(query, page)
            ventas_resultado_por_cantidad = None
            if not productos_manual:
                ventas_mensaje = productos_manual_error or "⚠️ No hay productos manuales cargados."
            elif not ventas_inputs["codigo"] or not ventas_inputs["cantidad"]:
                ventas_mensaje = "⚠️ Seleccioná un producto y la cantidad deseada."
            else:
                producto = next((p for p in productos_manual if str(p["codigo"]) == ventas_inputs["codigo"]), None)
                if not producto:
                    ventas_mensaje = "⚠️ Producto no encontrado en la lista manual."
                else:
                    ventas_inputs["nombre_principal"] = producto.get("nombre", "")
                    precio_input = ventas_inputs["precio"].strip()
                    try:
                        cantidad_solicitada = float(ventas_inputs["cantidad"].replace(",", "."))
                    except ValueError:
                        ventas_mensaje = "⚠️ La cantidad ingresada no es válida."
                    else:
                        if cantidad_solicitada <= 0:
                            ventas_mensaje = "⚠️ La cantidad debe ser mayor a cero."
                        else:
                            precio_unitario = None
                            if precio_input:
                                precio_unitario = parse_price_value(precio_input)
                            if (precio_unitario is None or precio_unitario <= 0) and producto.get("precio_valido", False):
                                precio_unitario = producto["precio"]
                            if precio_unitario is None or precio_unitario <= 0:
                                ventas_mensaje = "⚠️ Ingresá un precio unitario válido mayor a cero."
                                ventas_producto_seleccionado = producto
                            else:
                                ventas_inputs["precio"] = precio_input or (f"{float(precio_unitario):.2f}".rstrip("0").rstrip(".") if "." in f"{float(precio_unitario):.2f}" else f"{float(precio_unitario):.2f}")
                                total = cantidad_solicitada * precio_unitario
                                ventas_resultado_por_cantidad = {
                                    "producto": producto,
                                    "cantidad": cantidad_solicitada,
                                    "precio_unitario": precio_unitario,
                                    "total": total
                                }
                                ventas_producto_seleccionado = producto
                                ventas_mensaje = "✅ Cálculo realizado."
                                if ventas_resultado_por_cantidad:
                                    nueva_operacion = {
                                        "id": str(uuid.uuid4()),
                                        "tipo": "cantidad",
                                        "timestamp": now_local().strftime("%H:%M:%S"),
                                        "producto": {
                                            "codigo": producto.get("codigo"),
                                            "nombre": producto.get("nombre"),
                                            "precio": float(precio_unitario)
                                        },
                                        "cantidad": float(cantidad_solicitada),
                                        "total": float(total)
                                    }
                                    ventas_operaciones.append(nueva_operacion)
                                    ventas_operaciones = ventas_operaciones[-20:]
                                    session['ventas_operaciones'] = ventas_operaciones
                                    session.modified = True

        elif formulario == "ventas_avanzadas_combo":
            active_tab = "ventas_avanzadas"
            ventas_inputs["codigo"] = request.form.get("ventas_codigo", "").strip()
            ventas_inputs["codigo_comp"] = request.form.get("ventas_codigo_comp", "").strip()
            ventas_inputs["combo_cantidad_principal"] = request.form.get("combo_cantidad_principal", "").strip()
            ventas_inputs["combo_cantidad_complemento"] = request.form.get("combo_cantidad_complemento", "").strip()
            ventas_inputs["combo_monto_principal"] = request.form.get("combo_monto_principal", "").strip()
            ventas_inputs["combo_monto_complemento"] = request.form.get("combo_monto_complemento", "").strip()
            ventas_inputs["combo_monto_disponible"] = request.form.get("combo_monto_disponible", "").strip()
            ventas_inputs["precio"] = request.form.get("combo_precio_principal", "").strip()
            ventas_inputs["precio_comp"] = request.form.get("combo_precio_complemento", "").strip()
            nombre_principal_form = request.form.get("ventas_nombre", "").strip()
            nombre_complemento_form = request.form.get("ventas_nombre_comp", "").strip()
            if nombre_principal_form:
                ventas_inputs["nombre_principal"] = nombre_principal_form
            if nombre_complemento_form:
                ventas_inputs["nombre_complemento"] = nombre_complemento_form
            query = request.form.get("ventas_busqueda", "").strip()
            page = request.form.get("ventas_page", "1")
            preparar_busqueda(query, page)
            ventas_resultado_combo = None

            producto_principal = next((p for p in productos_manual if str(p["codigo"]) == ventas_inputs["codigo"]), None)
            if not producto_principal and ventas_inputs["codigo"]:
                precio_fallback = parse_price_value(ventas_inputs["precio"])
                producto_principal = {
                    "codigo": ventas_inputs["codigo"],
                    "nombre": nombre_principal_form or ventas_inputs["codigo"],
                    "precio": precio_fallback or 0.0,
                    "precio_valido": (precio_fallback or 0.0) > 0
                }
                ventas_inputs["nombre_principal"] = producto_principal["nombre"]

            producto_comp = next((p for p in productos_manual if str(p["codigo"]) == ventas_inputs["codigo_comp"]), None)
            if not producto_comp and ventas_inputs["codigo_comp"]:
                precio_comp_fallback = parse_price_value(ventas_inputs["precio_comp"])
                producto_comp = {
                    "codigo": ventas_inputs["codigo_comp"],
                    "nombre": nombre_complemento_form or ventas_inputs["codigo_comp"],
                    "precio": precio_comp_fallback or 0.0,
                    "precio_valido": (precio_comp_fallback or 0.0) > 0
                }
                ventas_inputs["nombre_complemento"] = producto_comp["nombre"]

            if not ventas_inputs["codigo"] or not producto_principal:
                ventas_mensaje = "⚠️ Seleccioná el producto principal desde la lista para usar la venta combinada."
            elif not ventas_inputs["codigo_comp"] or not producto_comp:
                ventas_mensaje = "⚠️ Seleccioná el complemento desde la lista para usar la venta combinada."
            else:
                precio_principal = parse_price_value(ventas_inputs["precio"])
                if (precio_principal is None or precio_principal <= 0) and producto_principal.get("precio_valido", False):
                    precio_principal = producto_principal.get("precio", 0)

                precio_complemento = parse_price_value(ventas_inputs["precio_comp"])
                if (precio_complemento is None or precio_complemento <= 0) and producto_comp.get("precio_valido", False):
                    precio_complemento = producto_comp.get("precio", 0)

                if precio_principal is None or precio_principal <= 0 or precio_complemento is None or precio_complemento <= 0:
                    ventas_mensaje = "⚠️ Asegurate de ingresar precios válidos mayores a cero para ambos productos."
                else:
                    # Refrescar campos con valores normalizados
                    precio_principal_fmt = f"{float(precio_principal):.2f}"
                    ventas_inputs["precio"] = precio_principal_fmt.rstrip("0").rstrip(".") if "." in precio_principal_fmt else precio_principal_fmt
                    precio_complemento_fmt = f"{float(precio_complemento):.2f}"
                    ventas_inputs["precio_comp"] = precio_complemento_fmt.rstrip("0").rstrip(".") if "." in precio_complemento_fmt else precio_complemento_fmt

                    ventas_producto_seleccionado = producto_principal
                    ventas_producto_complemento = producto_comp
                    # NO sobrescribir los nombres si ya fueron ingresados por el usuario
                    if not ventas_inputs.get("nombre_principal"):
                        ventas_inputs["nombre_principal"] = producto_principal.get("nombre", "")
                    if not ventas_inputs.get("nombre_complemento"):
                        ventas_inputs["nombre_complemento"] = producto_comp.get("nombre", "")

                    # Obtener montos individuales
                    monto_principal_raw = ventas_inputs["combo_monto_principal"].strip()
                    monto_complemento_raw = ventas_inputs["combo_monto_complemento"].strip()
                    monto_principal = parse_price_value(monto_principal_raw) if monto_principal_raw else None
                    monto_complemento = parse_price_value(monto_complemento_raw) if monto_complemento_raw else None

                    # Obtener cantidades
                    cantidad_principal_raw = ventas_inputs["combo_cantidad_principal"].strip()
                    cantidad_complemento_raw = ventas_inputs["combo_cantidad_complemento"].strip()
                    
                    try:
                        cantidad_principal_input = float(cantidad_principal_raw.replace(",", ".")) if cantidad_principal_raw else 0.0
                        cantidad_complemento_input = float(cantidad_complemento_raw.replace(",", ".")) if cantidad_complemento_raw else 0.0
                    except ValueError:
                        ventas_mensaje = "⚠️ Las cantidades ingresadas no son válidas."
                    else:
                        # *** CASO ESPECIAL: Presupuesto total compartido ***
                        # Si ambos montos son iguales y no hay cantidades, significa que el cliente
                        # tiene ese monto TOTAL para comprar ambos productos
                        if (monto_principal is not None and monto_complemento is not None and 
                            monto_principal == monto_complemento and monto_principal > 0 and
                            cantidad_principal_input == 0 and cantidad_complemento_input == 0):
                            
                            presupuesto_total = monto_principal
                            
                            # Generar TODAS las combinaciones válidas
                            max_principal = int(presupuesto_total // precio_principal) + 1
                            max_complemento = int(presupuesto_total // precio_complemento) + 1
                            
                            combinaciones_validas = []
                            
                            for cant_p in range(max_principal + 1):
                                for cant_c in range(max_complemento + 1):
                                    if cant_p == 0 and cant_c == 0:
                                        continue
                                    total_combo = (cant_p * precio_principal) + (cant_c * precio_complemento)
                                    if total_combo <= presupuesto_total:
                                        resto = presupuesto_total - total_combo
                                        combinaciones_validas.append({
                                            "cant_principal": cant_p,
                                            "cant_complemento": cant_c,
                                            "total": total_combo,
                                            "resto": resto,
                                            # Prioridad: favorece gastar más dinero y tener productos de ambos tipos
                                            "score": total_combo + (100 if cant_p > 0 and cant_c > 0 else 0)
                                        })
                            
                            if not combinaciones_validas:
                                ventas_mensaje = f"⚠️ El presupuesto de ${presupuesto_total:.2f} no alcanza para comprar ningún producto."
                            else:
                                # Ordenar por score (mejor aprovechamiento) y tomar las mejores opciones
                                combinaciones_validas.sort(key=lambda x: x["score"], reverse=True)
                                
                                # Filtrar para mostrar opciones diversas e interesantes
                                variantes = []
                                variantes_set = set()  # Para evitar duplicados
                                
                                # 1. La mejor combinación (que gasta más)
                                mejor = combinaciones_validas[0]
                                variantes.append(mejor)
                                variantes_set.add((mejor["cant_principal"], mejor["cant_complemento"]))
                                
                                # 2. Solo producto principal (máximo posible)
                                solo_principal = max([c for c in combinaciones_validas if c["cant_complemento"] == 0], 
                                                   key=lambda x: x["cant_principal"], default=None)
                                if solo_principal and (solo_principal["cant_principal"], solo_principal["cant_complemento"]) not in variantes_set:
                                    variantes.append(solo_principal)
                                    variantes_set.add((solo_principal["cant_principal"], solo_principal["cant_complemento"]))
                                
                                # 3. Solo complemento (máximo posible)
                                solo_complemento = max([c for c in combinaciones_validas if c["cant_principal"] == 0], 
                                                      key=lambda x: x["cant_complemento"], default=None)
                                if solo_complemento and (solo_complemento["cant_principal"], solo_complemento["cant_complemento"]) not in variantes_set:
                                    variantes.append(solo_complemento)
                                    variantes_set.add((solo_complemento["cant_principal"], solo_complemento["cant_complemento"]))
                                
                                # 4. Opciones mixtas con diferentes proporciones
                                opciones_mixtas = [c for c in combinaciones_validas 
                                                  if c["cant_principal"] > 0 and c["cant_complemento"] > 0]
                                
                                if opciones_mixtas:
                                    # Calcular el total máximo de unidades para normalizar
                                    max_unidades_p = max([c["cant_principal"] for c in opciones_mixtas])
                                    max_unidades_c = max([c["cant_complemento"] for c in opciones_mixtas])
                                    
                                    # Buscar opciones con diferentes proporciones de UNIDADES
                                    proporciones_deseadas = [
                                        (0.8, 0.2),   # 80% principal, 20% complemento
                                        (0.6, 0.4),   # 60-40
                                        (0.5, 0.5),   # 50-50 balanceado
                                        (0.4, 0.6),   # 40-60
                                        (0.2, 0.8),   # 20% principal, 80% complemento
                                        (0.3, 0.7),   # 30-70
                                        (0.7, 0.3),   # 70-30
                                    ]
                                    
                                    for prop_p, prop_c in proporciones_deseadas:
                                        if len(variantes) >= 10:  # Límite de variantes
                                            break
                                        
                                        # Buscar la combinación más cercana a esta proporción
                                        mejor_candidato = None
                                        menor_diferencia = float('inf')
                                        
                                        for c in opciones_mixtas:
                                            key = (c["cant_principal"], c["cant_complemento"])
                                            if key in variantes_set:
                                                continue
                                            
                                            # Calcular proporción real de esta combinación
                                            total_unidades = c["cant_principal"] + c["cant_complemento"]
                                            if total_unidades == 0:
                                                continue
                                            
                                            prop_actual_p = c["cant_principal"] / total_unidades
                                            prop_actual_c = c["cant_complemento"] / total_unidades
                                            
                                            # Diferencia con la proporción buscada
                                            diferencia = abs(prop_actual_p - prop_p) + abs(prop_actual_c - prop_c)
                                            
                                            # Priorizar combinaciones que gasten más
                                            diferencia = diferencia - (c["total"] / presupuesto_total * 0.1)
                                            
                                            if diferencia < menor_diferencia:
                                                menor_diferencia = diferencia
                                                mejor_candidato = c
                                        
                                        if mejor_candidato:
                                            variantes.append(mejor_candidato)
                                            variantes_set.add((mejor_candidato["cant_principal"], mejor_candidato["cant_complemento"]))
                                
                                # 5. Agregar opciones que gasten casi todo el presupuesto
                                # (resto < 10% del presupuesto)
                                opciones_eficientes = [c for c in combinaciones_validas 
                                                      if c["resto"] < presupuesto_total * 0.1 and
                                                      (c["cant_principal"], c["cant_complemento"]) not in variantes_set]
                                
                                for opcion in opciones_eficientes[:3]:  # Agregar hasta 3 más
                                    if len(variantes) >= 12:
                                        break
                                    variantes.append(opcion)
                                    variantes_set.add((opcion["cant_principal"], opcion["cant_complemento"]))
                                
                                # Limitar a 10 variantes más diversas
                                variantes = variantes[:10]
                                
                                # Preparar datos para mostrar
                                variantes_display = []
                                for idx, var in enumerate(variantes, 1):
                                    cant_p = var["cant_principal"]
                                    cant_c = var["cant_complemento"]
                                    
                                    # Verificar si las cantidades son balanceadas (iguales o cercanas)
                                    # Consideramos "cercanas" si la diferencia es <= 20% del mayor
                                    max_cant = max(cant_p, cant_c)
                                    min_cant = min(cant_p, cant_c)
                                    diferencia_porcentual = ((max_cant - min_cant) / max_cant * 100) if max_cant > 0 else 0
                                    es_balanceado = diferencia_porcentual <= 20  # <= 20% de diferencia
                                    
                                    variantes_display.append({
                                        "opcion": idx,
                                        "cant_principal": cant_p,
                                        "cant_complemento": cant_c,
                                        "subtotal_principal": round(cant_p * precio_principal, 2),
                                        "subtotal_complemento": round(cant_c * precio_complemento, 2),
                                        "total": round(var["total"], 2),
                                        "resto": round(var["resto"], 2),
                                        "es_balanceado": es_balanceado
                                    })
                                
                                # Usar la mejor como predeterminada
                                cantidad_principal = mejor["cant_principal"]
                                cantidad_complemento = mejor["cant_complemento"]
                                subtotal_principal = round(cantidad_principal * precio_principal, 2)
                                subtotal_complemento = round(cantidad_complemento * precio_complemento, 2)
                                total_general = round(mejor["total"], 2)
                                resto_total = round(mejor["resto"], 2)
                                
                                ventas_resultado_combo = {
                                    "modo": "presupuesto_total",
                                    "producto_principal": producto_principal,
                                    "producto_complemento": producto_comp,
                                    "cantidad_principal": cantidad_principal,
                                    "cantidad_complemento": cantidad_complemento,
                                    "precio_principal": precio_principal,
                                    "precio_complemento": precio_complemento,
                                    "subtotal_principal": subtotal_principal,
                                    "subtotal_complemento": subtotal_complemento,
                                    "total": total_general,
                                    "presupuesto_total": presupuesto_total,
                                    "resto_total": resto_total,
                                    "monto_principal": None,
                                    "monto_complemento": None,
                                    "resto_principal": 0.0,
                                    "resto_complemento": 0.0,
                                    "variantes": variantes_display  # Lista de opciones alternativas
                                }
                                
                                ventas_mensaje = f"✅ Mejor combinación calculada con presupuesto de ${presupuesto_total:.2f}"
                                
                                nueva_operacion = {
                                    "id": str(uuid.uuid4()),
                                    "tipo": "combo",
                                    "modo": "presupuesto_total",
                                    "timestamp": now_local().strftime("%H:%M:%S"),
                                    "presupuesto_total": float(presupuesto_total),
                                    "productos": [
                                        {
                                            "codigo": producto_principal.get("codigo"),
                                            "nombre": producto_principal.get("nombre"),
                                            "precio": float(precio_principal),
                                            "cantidad": float(cantidad_principal),
                                            "subtotal": float(subtotal_principal)
                                        },
                                        {
                                            "codigo": producto_comp.get("codigo"),
                                            "nombre": producto_comp.get("nombre"),
                                            "precio": float(precio_complemento),
                                            "cantidad": float(cantidad_complemento),
                                            "subtotal": float(subtotal_complemento)
                                        }
                                    ],
                                    "total": float(total_general),
                                    "resto": float(resto_total)
                                }
                                ventas_operaciones.append(nueva_operacion)
                                ventas_operaciones = ventas_operaciones[-20:]
                                session['ventas_operaciones'] = ventas_operaciones
                                session.modified = True
                        
                        # *** CASO NORMAL: Cantidades o montos individuales ***
                        else:
                            # Determinar cantidad_principal (prioridad: cantidad > monto)
                            if cantidad_principal_input > 0:
                                cantidad_principal = cantidad_principal_input
                            elif monto_principal is not None and monto_principal > 0:
                                cantidad_principal = int(monto_principal // precio_principal)
                            else:
                                cantidad_principal = 0.0

                            # Determinar cantidad_complemento (prioridad: cantidad > monto)
                            if cantidad_complemento_input > 0:
                                cantidad_complemento = cantidad_complemento_input
                            elif monto_complemento is not None and monto_complemento > 0:
                                cantidad_complemento = int(monto_complemento // precio_complemento)
                            else:
                                cantidad_complemento = 0.0

                            if cantidad_principal <= 0 and cantidad_complemento <= 0:
                                ventas_mensaje = "⚠️ Ingresá al menos una cantidad o monto mayor a cero."
                            else:
                                subtotal_principal = round(cantidad_principal * precio_principal, 2)
                                subtotal_complemento = round(cantidad_complemento * precio_complemento, 2)
                                total_general = round(subtotal_principal + subtotal_complemento, 2)

                                # Calcular restos si se usaron montos
                                resto_principal = 0.0
                                resto_complemento = 0.0
                                if monto_principal is not None and monto_principal > 0 and cantidad_principal_input <= 0:
                                    resto_principal = round(monto_principal - subtotal_principal, 2)
                                if monto_complemento is not None and monto_complemento > 0 and cantidad_complemento_input <= 0:
                                    resto_complemento = round(monto_complemento - subtotal_complemento, 2)

                                ventas_resultado_combo = {
                                    "modo": "individual",
                                    "producto_principal": producto_principal,
                                    "producto_complemento": producto_comp,
                                    "cantidad_principal": cantidad_principal,
                                    "cantidad_complemento": cantidad_complemento,
                                    "precio_principal": precio_principal,
                                    "precio_complemento": precio_complemento,
                                    "subtotal_principal": subtotal_principal,
                                    "subtotal_complemento": subtotal_complemento,
                                    "total": total_general,
                                    "monto_principal": monto_principal,
                                    "monto_complemento": monto_complemento,
                                    "resto_principal": resto_principal if resto_principal > 0 else 0.0,
                                    "resto_complemento": resto_complemento if resto_complemento > 0 else 0.0
                                }

                                ventas_mensaje = "✅ Venta combinada calculada."

                                nueva_operacion = {
                                    "id": str(uuid.uuid4()),
                                    "tipo": "combo",
                                    "modo": "individual",
                                    "timestamp": now_local().strftime("%H:%M:%S"),
                                    "productos": [
                                        {
                                            "codigo": producto_principal.get("codigo"),
                                            "nombre": producto_principal.get("nombre"),
                                            "precio": float(precio_principal),
                                            "cantidad": float(cantidad_principal),
                                            "subtotal": float(subtotal_principal),
                                            "monto": float(monto_principal) if monto_principal else None,
                                            "resto": float(resto_principal) if resto_principal > 0 else 0.0
                                        },
                                        {
                                            "codigo": producto_comp.get("codigo"),
                                            "nombre": producto_comp.get("nombre"),
                                            "precio": float(precio_complemento),
                                            "cantidad": float(cantidad_complemento),
                                            "subtotal": float(subtotal_complemento),
                                            "monto": float(monto_complemento) if monto_complemento else None,
                                            "resto": float(resto_complemento) if resto_complemento > 0 else 0.0
                                        }
                                    ],
                                    "total": float(total_general)
                                }
                                ventas_operaciones.append(nueva_operacion)
                                ventas_operaciones = ventas_operaciones[-20:]
                                session['ventas_operaciones'] = ventas_operaciones
                                session.modified = True

        elif formulario == "ventas_operaciones_manage":
            active_tab = "ventas_avanzadas"
            
            # Restaurar estado de búsqueda e inputs
            query = request.form.get("ventas_busqueda", "").strip()
            page = request.form.get("ventas_page", "1")
            preparar_busqueda(query, page)
            
            # Restaurar todos los inputs de ventas usando los aliases
            for dest_key, aliases in persist_aliases.items():
                for alias in aliases:
                    if alias in request.form:
                        valor = request.form.get(alias, "")
                        if isinstance(valor, str):  # Solo procesar strings, no listas
                            ventas_inputs[dest_key] = valor.strip()
                            break  # Usar solo el primer alias encontrado
            
            accion = request.form.get("accion")
            log_debug(f"ventas_operaciones_manage: accion={accion}, ops_antes={len(ventas_operaciones)}")
            
            if accion == "seleccionados":
                ids_a_borrar = set(request.form.getlist("ventas_operaciones_ids"))
                log_debug(f"IDs a borrar: {ids_a_borrar}")
                if ids_a_borrar:
                    operaciones_previas = len(ventas_operaciones)
                    ventas_operaciones = [op for op in ventas_operaciones if op.get("id") not in ids_a_borrar]
                    cantidad_borrada = operaciones_previas - len(ventas_operaciones)
                    log_debug(f"Operaciones después de borrar: {len(ventas_operaciones)}")
                    ventas_mensaje = f"✅ Se borraron {cantidad_borrada} operación(es) seleccionada(s)."
                else:
                    ventas_mensaje = "ℹ️ No seleccionaste operaciones para borrar."
            elif accion == "todo":
                ventas_operaciones = []
                log_debug("Todas las operaciones borradas")
                ventas_mensaje = "✅ Se borró todo el historial de operaciones de Ventas Avanzadas."
            else:
                ventas_mensaje = "ℹ️ Acción no reconocida."
            
            session['ventas_operaciones'] = ventas_operaciones
            session.modified = True
            log_debug(f"Sesión actualizada, ops={len(ventas_operaciones)}")

        elif formulario == "editar":
            proveedor_id_seleccionado = request.form.get("editar_proveedor_id")
            if "guardar" in request.form and proveedor_id_seleccionado:
                target_data = proveedores.get(proveedor_id_seleccionado, {})
                target_data["nombre_base"] = request.form.get("edit_nombre_base", target_data["nombre_base"])
                target_data["es_dinamico"] = request.form.get("edit_es_dinamico") == "true"
                for clave in ["descuento", "iva", "ganancia"]:
                    parsed = parse_percentage(request.form.get(clave))
                    if parsed is not None:
                        target_data[clave] = parsed
                proveedores[proveedor_id_seleccionado] = target_data
                try:
                    save_proveedores(proveedores)
                    mensaje = "✅ CAMBIOS GUARDADOS."
                except Exception as e:
                    mensaje = f"❌ ERROR GUARDANDO DATOS.JSON: {e}"
            if proveedor_id_seleccionado:
                datos_seleccionados = proveedores.get(proveedor_id_seleccionado, {})

        elif formulario == "agregar":
            nombre_base = request.form.get("nuevo_nombre_base", "").strip()
            if not nombre_base:
                mensaje = "⚠️ ERROR: EL NOMBRE BASE NO PUEDE ESTAR VACÍO."
            else:
                proveedores[str(uuid.uuid4())] = {
                    "nombre_base": nombre_base, "es_dinamico": request.form.get("nuevo_es_dinamico") == "true",
                    "descuento": parse_percentage(request.form.get("nuevo_descuento")) or 0.0,
                    "iva": parse_percentage(request.form.get("nuevo_iva")) or 0.0,
                    "ganancia": parse_percentage(request.form.get("nuevo_ganancia")) or 0.0
                }
                try:
                    save_proveedores(proveedores)
                    mensaje = f"✅ PROVEEDOR '{nombre_base}' AÑADIDO."
                except Exception as e:
                    mensaje = f"❌ ERROR GUARDANDO DATOS.JSON: {e}"

        elif formulario == "borrar":
            proveedor_id_a_borrar = request.form.get("borrar_proveedor_id")
            if proveedor_id_a_borrar and proveedor_id_a_borrar in proveedores:
                nombre_borrado = generar_nombre_visible(proveedores.pop(proveedor_id_a_borrar))
                try:
                    save_proveedores(proveedores)
                    mensaje = f"✅ PROVEEDOR '{nombre_borrado}' BORRADO."
                except Exception as e:
                    mensaje = f"❌ ERROR GUARDANDO DATOS.JSON: {e}"
            else:
                mensaje = "⚠️ ERROR: PROVEEDOR NO ENCONTRADO O NO SELECCIONADO."
        
        elif formulario == "borrar_historial_seleccionado":
            ids_para_borrar = request.form.getlist("historial_ids_a_borrar")
            if ids_para_borrar:
                nuevo_historial = [item for item in load_historial() if item.get("id_historial") not in ids_para_borrar]
                try:
                    atomic_save_historial_list(nuevo_historial)
                    mensaje = f"✅ {len(ids_para_borrar)} ENTRADA(S) BORRADA(S)."
                except Exception as e:
                    mensaje = f"❌ ERROR GUARDANDO HISTORIAL: {e}"
            else:
                mensaje = "ℹ️ NO SE SELECCIONÓ NINGUNA ENTRADA."

        elif formulario == "borrar_todo_historial":
            try:
                atomic_save_historial_list([])
                mensaje = "✅ TODO EL HISTORIAL BORRADO."
            except Exception as e:
                mensaje = f"❌ ERROR BORRANDO TODO EL HISTORIAL: {e}"

        elif formulario == "borrar_todas_listas":
            active_tab = "gestion"
            try:
                archivos_borrados = 0
                for fname in os.listdir(LISTAS_PATH):
                    low = fname.lower()
                    if low.endswith((".xlsx", ".xls")):
                        try:
                            os.remove(os.path.join(LISTAS_PATH, fname))
                            archivos_borrados += 1
                        except Exception as exc:
                            log_debug("borrar_todas_listas: error eliminando archivo", fname, exc)

                productos_borrados = None
                batches_borrados = None
                db_error = None
                if DATABASE_URL and psycopg:
                    try:
                        with get_pg_conn() as conn, conn.cursor() as cur:
                            cur.execute("SELECT COUNT(*) FROM productos_listas")
                            row_prod = cur.fetchone()
                            productos_borrados = (list(row_prod.values())[0] if isinstance(row_prod, dict) else (row_prod[0] if row_prod else 0))
                            cur.execute("DELETE FROM productos_listas")

                            cur.execute("SELECT COUNT(*) FROM import_batches")
                            row_batch = cur.fetchone()
                            batches_borrados = (list(row_batch.values())[0] if isinstance(row_batch, dict) else (row_batch[0] if row_batch else 0))
                            cur.execute("DELETE FROM import_batches")
                            conn.commit()
                    except Exception as exc:
                        db_error = exc
                        log_debug("borrar_todas_listas: error limpiando DB", exc)

                if db_error:
                    mensaje = f"⚠️ Se borraron {archivos_borrados} archivo(s) Excel, pero falló limpiar la base: {db_error}"
                elif productos_borrados is not None:
                    mensaje = f"✅ Limpieza completa. Archivos Excel borrados: {archivos_borrados}. Registros DB borrados: {productos_borrados} productos, {batches_borrados} lotes."
                else:
                    mensaje = f"✅ Archivos Excel borrados: {archivos_borrados}. (DB de listas no configurada)"
            except Exception as e:
                mensaje = f"❌ ERROR BORRANDO LISTAS: {e}"

        elif formulario == "borrar_listas_old":
            # Eliminar todos los archivos con OLD en el nombre (sin tocar vigentes)
            try:
                eliminados = 0
                for fname in os.listdir(LISTAS_PATH):
                    if not fname.lower().endswith(('.xlsx','.xls')):
                        continue
                    if 'old' in fname.lower():
                        try:
                            os.remove(os.path.join(LISTAS_PATH, fname))
                            eliminados += 1
                        except Exception:
                            pass
                mensaje = f"✅ {eliminados} LISTA(S) OLD ELIMINADA(S)." if eliminados else "ℹ️ NO HABÍA LISTAS OLD PARA BORRAR."
                if eliminados and LISTAS_EN_DB and DATABASE_URL and psycopg:
                    try:
                        sync_res = sync_listas_to_db()
                        log_debug('borrar_listas_old: sync después de borrar OLD', sync_res)
                    except Exception as exc:
                        log_debug('borrar_listas_old: error al resincronizar', exc)
                active_tab = "gestion"
            except Exception as e:
                mensaje = f"❌ ERROR ELIMINANDO LISTAS OLD: {e}"

        elif formulario == "borrar_lista_old_individual":
            fname = request.form.get('filename','')
            if fname and 'old' in fname.lower() and fname.lower().endswith(('.xlsx','.xls')):
                try:
                    os.remove(os.path.join(LISTAS_PATH, fname))
                    mensaje = f"✅ LISTA OLD '{fname}' ELIMINADA."
                    if LISTAS_EN_DB and DATABASE_URL and psycopg:
                        try:
                            sync_res = sync_listas_to_db()
                            log_debug('borrar_lista_old_individual: sync después de borrar', sync_res)
                        except Exception as exc:
                            log_debug('borrar_lista_old_individual: error al resincronizar', exc)
                except Exception as e:
                    mensaje = f"❌ ERROR ELIMINANDO '{fname}': {e}"
            else:
                mensaje = "⚠️ ARCHIVO NO VÁLIDO PARA ELIMINAR."
            active_tab = "gestion"

        elif formulario == "borrar_lista_vigente":
            fname = request.form.get('filename','')
            if fname and fname.lower().endswith(('.xlsx','.xls')):
                try:
                    os.remove(os.path.join(LISTAS_PATH, fname))
                    mensaje = f"✅ LISTA '{fname}' ELIMINADA."
                    if LISTAS_EN_DB and DATABASE_URL and psycopg:
                        try:
                            sync_res = sync_listas_to_db()
                            log_debug('borrar_lista_vigente: sync después de borrar', sync_res)
                        except Exception as exc:
                            log_debug('borrar_lista_vigente: error al resincronizar', exc)
                    active_tab = "gestion"
                except Exception as e:
                    mensaje = f"❌ ERROR ELIMINANDO '{fname}': {e}"
            else:
                mensaje = "⚠️ ARCHIVO NO VÁLIDO PARA ELIMINAR."
            active_tab = "gestion"

        elif formulario == "subir_lista":
            # Manejo de carga de archivos Excel
            active_tab = "gestion"  # Permanecer en gestión tras subir
            archivos = request.files.getlist('archivos_excel')
            override_prov = request.form.get('proveedor_archivo', '').strip()
            incluir_dia = request.form.get('incluir_dia') == 'true'
            resultados_subida = []
            if not archivos or (len(archivos) == 1 and archivos[0].filename == ''):
                mensaje = "⚠️ NO SE SELECCIONÓ NINGÚN ARCHIVO."  # no early return, continuamos
            else:
                for archivo in archivos:
                    nombre_orig = archivo.filename
                    ext = os.path.splitext(nombre_orig)[1].lower()
                    if ext not in app.config['UPLOAD_EXTENSIONS']:
                        resultados_subida.append(f"❌ {nombre_orig}: extensión no permitida")
                        continue
                    try:
                        # Caso especial: productos_manual.xlsx mantiene su nombre original (sin fecha, sin OLD)
                        nombre_orig_lower = nombre_orig.lower()
                        if nombre_orig_lower.startswith('productos_manual'):
                            nombre_final = nombre_orig
                            ruta_final = os.path.join(LISTAS_PATH, nombre_final)
                            # Guardar directamente (sobrescribe si existe)
                            archivo.save(ruta_final)
                            resultados_subida.append(f"✅ {nombre_orig} (guardado sin fecha)")
                        else:
                            # Para otros proveedores: agregar fecha y manejar versiones OLD
                            nombre_base = override_prov or inferir_nombre_base_archivo(nombre_orig, proveedores)
                            # Construir fecha
                            fecha_formato = "%d%m%Y" if incluir_dia else "%m%Y"
                            fecha_str = now_local().strftime(fecha_formato)
                            nombre_final = f"{nombre_base}-{fecha_str}{ext}"
                            ruta_final = os.path.join(LISTAS_PATH, nombre_final)
                            
                            # Política: solo 1 versión OLD por proveedor.
                            # Pasos: eliminar cualquier OLD existente del proveedor, luego renombrar la vigente a OLD.
                            try:
                                norm_prov_subida = normalize_text(nombre_base)
                                archivos_existentes = os.listdir(LISTAS_PATH)
                                # 1) Borrar OLD previas del proveedor
                                for existing in archivos_existentes:
                                    if not existing.lower().endswith(('.xlsx', '.xls')):
                                        continue
                                    if 'old' in existing.lower():
                                        prov_part_old = os.path.splitext(existing)[0].split('-')[0]
                                        if normalize_text(prov_part_old) == norm_prov_subida:
                                            try:
                                                os.remove(os.path.join(LISTAS_PATH, existing))
                                            except Exception:
                                                pass
                                # 2) Renombrar la vigente (si existe) a OLD
                                for existing in archivos_existentes:
                                    if not existing.lower().endswith(('.xlsx', '.xls')):
                                        continue
                                    if 'old' in existing.lower():
                                        continue  # ya hemos limpiado las old
                                    prov_part = os.path.splitext(existing)[0].split('-')[0]
                                    if normalize_text(prov_part) == norm_prov_subida:
                                        src_path = os.path.join(LISTAS_PATH, existing)
                                        base_no_ext, ext_exist = os.path.splitext(existing)
                                        dst_path = os.path.join(LISTAS_PATH, f"{base_no_ext}-OLD{ext_exist}")
                                        # Si por alguna razón quedó un archivo destino, lo eliminamos para sobreescribir limpio
                                        if os.path.exists(dst_path):
                                            try: os.remove(dst_path)
                                            except Exception: pass
                                        try:
                                            os.rename(src_path, dst_path)
                                        except Exception as e_rn:
                                            resultados_subida.append(f"⚠️ No se pudo renombrar a OLD: {existing} -> {e_rn}")
                                        break  # solo una vigente
                            except Exception as e_mark:
                                resultados_subida.append(f"⚠️ Aviso al gestionar versiones OLD: {e_mark}")
                            
                            # Guardar (overwrite permitido)
                            archivo.save(ruta_final)
                            resultados_subida.append(f"✅ {nombre_orig} -> {nombre_final}")
                    except Exception as e:
                        resultados_subida.append(f"❌ {nombre_orig}: error {e}")
                mensaje = " | ".join(resultados_subida)

    historial = load_historial()
    historial.reverse() 
    lista_proveedores_display = sorted([(p_id, generar_nombre_visible(p_data)) for p_id, p_data in proveedores.items()], key=lambda x: x[1])
    
    # Crear lista única de nombres base de proveedores para el dropdown
    lista_nombres_proveedores = sorted(list(set(p_data['nombre_base'] for p_data in proveedores.values())))
    # Agregar "Manual" si existe productos_manual.xlsx
    productos_manual_list, err_manual = load_manual_products()
    if productos_manual_list and not err_manual:
        if 'Manual' not in lista_nombres_proveedores:
            lista_nombres_proveedores.append('Manual')
            lista_nombres_proveedores.sort()

    if ventas_inputs["codigo"] and not ventas_producto_seleccionado:
        producto = next((p for p in productos_manual if str(p.get("codigo", "")) == ventas_inputs["codigo"]), None)
        if not producto:
            precio_fallback = parse_price_value(ventas_inputs.get("precio"))
            producto = {
                "codigo": ventas_inputs["codigo"],
                "nombre": ventas_inputs.get("nombre_principal") or ventas_inputs["codigo"],
                "precio": precio_fallback or 0.0,
                "precio_valido": (precio_fallback or 0.0) > 0
            }
        ventas_producto_seleccionado = producto

    if ventas_inputs["codigo_comp"] and not ventas_producto_complemento:
        producto_comp = next((p for p in productos_manual if str(p.get("codigo", "")) == ventas_inputs["codigo_comp"]), None)
        if not producto_comp:
            precio_comp_fallback = parse_price_value(ventas_inputs.get("precio_comp"))
            producto_comp = {
                "codigo": ventas_inputs["codigo_comp"],
                "nombre": ventas_inputs.get("nombre_complemento") or ventas_inputs["codigo_comp"],
                "precio": precio_comp_fallback or 0.0,
                "precio_valido": (precio_comp_fallback or 0.0) > 0
            }
        ventas_producto_complemento = producto_comp

    # --- Calcular últimas actualizaciones de archivos Excel ---
    ultimas_actualizaciones = {}
    try:
        for fname in os.listdir(LISTAS_PATH):
            if not fname.lower().endswith(('.xlsx', '.xls')):
                continue
            ruta = os.path.join(LISTAS_PATH, fname)
            try:
                mtime = os.path.getmtime(ruta)
            except Exception:
                continue
            provider_part = os.path.splitext(fname)[0].split('-')[0]
            norm_provider_part = normalize_text(provider_part)
            nombre_match = next((p['nombre_base'] for p in proveedores.values() if normalize_text(p['nombre_base']) == norm_provider_part), provider_part)
            data_existente = ultimas_actualizaciones.get(nombre_match)
            if not data_existente or mtime > data_existente['mtime']:
                ultimas_actualizaciones[nombre_match] = {
                    'filename': fname,
                    'mtime': mtime,
                    'fecha': ts_to_local(mtime).strftime('%d/%m/%Y %H:%M'),
                    'hace': humanizar_tiempo_desde(mtime)
                }
    except Exception:
        pass
    ultimas_actualizaciones_list = sorted([
        {'proveedor': k, **v} for k, v in ultimas_actualizaciones.items()
    ], key=lambda x: x['proveedor'])

    # Listas vigentes y antiguas para descarga
    listas_vigentes = []
    listas_old = []
    try:
        for fname in os.listdir(LISTAS_PATH):
            if not fname.lower().endswith(('.xlsx','.xls')): continue
            full_path = os.path.join(LISTAS_PATH, fname)
            info = {
                'filename': fname,
                'fecha': ts_to_local(os.path.getmtime(full_path)).strftime('%d/%m/%Y %H:%M')
            }
            if 'old' in fname.lower():
                listas_old.append(info)
            else:
                listas_vigentes.append(info)
        listas_vigentes.sort(key=lambda x: x['filename'])
        listas_old.sort(key=lambda x: x['filename'])
    except Exception:
        pass


    contexto = {
        "proveedores_lista": lista_proveedores_display,
        "resultado_auto": resultado_auto,
        "resultado_manual": resultado_manual,
        "productos_encontrados": productos_encontrados,
        "mensaje": mensaje,
        # Paginación de búsqueda normal
        "busqueda_page": locals().get("busqueda_page_value", 1),
        "busqueda_per_page": locals().get("busqueda_per_page_value", 20),
        "busqueda_total_paginas": locals().get("busqueda_total_paginas", 1),
        "busqueda_total_resultados": locals().get("busqueda_total_resultados", (len(productos_encontrados) if productos_encontrados else 0)),
        "proveedor_id_seleccionado": proveedor_id_seleccionado,
        "datos_seleccionados": datos_seleccionados,
        "historial": historial,
        "active_tab": active_tab,
        "lista_nombres_proveedores": lista_nombres_proveedores,
        "proveedor_buscado": proveedor_buscado,
        "filtro_resultados": filtro_resultados,
        "datos_calculo_auto": datos_calculo_auto,
        "datos_calculo_manual": datos_calculo_manual,
        "productos_manual": productos_manual,
        "productos_manual_error": productos_manual_error,
        "ventas_inputs": ventas_inputs,
        "ventas_resultado_por_monto": ventas_resultado_por_monto,
        "ventas_resultado_por_cantidad": ventas_resultado_por_cantidad,
    "ventas_resultado_combo": ventas_resultado_combo,
        "ventas_mensaje": ventas_mensaje,
        "ventas_busqueda_query": ventas_busqueda_query,
        "ventas_busqueda_page": ventas_busqueda_page,
        "ventas_busqueda_total_paginas": ventas_busqueda_total_paginas,
        "ventas_busqueda_total_resultados": ventas_busqueda_total_resultados,
        "ventas_busqueda_resultados": ventas_busqueda_resultados,
        "ventas_producto_seleccionado": ventas_producto_seleccionado,
    "ventas_producto_complemento": ventas_producto_complemento,
        "ventas_resultados_por_pagina": per_page,
        "ventas_operaciones": ventas_operaciones,
        "ultimas_actualizaciones": ultimas_actualizaciones_list,
        "listas_path": LISTAS_PATH,
        "listas_vigentes": listas_vigentes,
        "listas_old": listas_old
    }

    if (
        request.method == "POST"
        and formulario in ventas_formularios
        and request.headers.get("X-Requested-With") == "XMLHttpRequest"
    ):
        fragmento = render_template("ventas_avanzadas_fragment.html", **contexto)
        return jsonify({
            "html": fragmento,
            "mensaje": contexto.get("ventas_mensaje")
        })

    return render_template("index_v5.html", **contexto)

@app.route('/download_lista/<path:filename>')
@login_required
def download_lista(filename):
    # Seguridad básica: evitar path traversal
    if '..' in filename or filename.startswith('/'):
        abort(400)
    ext = os.path.splitext(filename)[1].lower()
    if ext not in app.config['UPLOAD_EXTENSIONS']:
        abort(404)
    file_path = os.path.join(LISTAS_PATH, filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(LISTAS_PATH, filename, as_attachment=True)

@app.route('/admin/sync_listas', methods=['POST','GET'])
@login_required
def admin_sync_listas():
    if not (DATABASE_URL and psycopg):
        return jsonify({'ok': False, 'error': 'PostgreSQL no disponible.'}), 400
    try:
        resumen = sync_listas_to_db()
        if isinstance(resumen, dict) and resumen.get('error'):
            return jsonify({'ok': False, 'error': resumen.get('error'), 'resumen': resumen}), 400
        return jsonify({'ok': True, 'resumen': resumen})
    except Exception as exc:
        log_debug('admin_sync_listas: error', exc)
        err_text = f"{type(exc).__name__}: {exc}\n" + traceback.format_exc(limit=3)
        return jsonify({'ok': False, 'error': err_text}), 500


def extraer_codigo_de_barras(codigo_barras: str, proveedor_filtro: str = '') -> list:
    """
    Extrae múltiples variantes de código de producto desde un código de barras.
    Soporta EAN-13, UPC-A y lógica especial para Crossmaster.
    
    Modo inteligente (MODO_BARCODE_INTELIGENTE=True):
    - 12 dígitos (UPC-A): busca solo en Crossmaster
    - 13 dígitos (EAN-13): busca en todos los proveedores
    
    Args:
        codigo_barras: Código escaneado (puede tener guiones, espacios, etc.)
        proveedor_filtro: Nombre del proveedor para aplicar lógica específica
    
    Returns:
        Lista de códigos candidatos para buscar
    """
    # Fase 0: Limpiar código
    codigo_limpio = ''.join(filter(str.isdigit, codigo_barras))
    if not codigo_limpio:
        return []
    
    longitud = len(codigo_limpio)
    variantes = []
    proveedor_key = provider_name_to_key(proveedor_filtro) if proveedor_filtro else ''
    
    # **MODO INTELIGENTE**: Detección automática por longitud
    if MODO_BARCODE_INTELIGENTE and not proveedor_key:
        print(f'[BARCODE] Modo inteligente activado. Código: {codigo_limpio} ({longitud} dígitos)', flush=True)
        
        if longitud == 12:
            # UPC-A (12 dígitos) -> Solo Crossmaster
            print(f'[BARCODE] Detectado UPC-A, buscando solo en Crossmaster', flush=True)
            proveedor_key = 'crossmaster'
        elif longitud == 13:
            # EAN-13 (13 dígitos) -> Todos los proveedores
            print(f'[BARCODE] Detectado EAN-13, buscando en todos los proveedores', flush=True)
            # Usar lógica estándar para todos
            pass
    
    # Fase 1: Patrón específico por proveedor
    if proveedor_key == 'crossmaster':
        # Crossmaster usa UPC-A (12 dígitos) con lógica dual
        if longitud == 12:
            # Patrón 1: Con sufijo .1 (posiciones 7-10, índices 6-9)
            if longitud >= 10:
                segmento1 = codigo_limpio[6:10]  # Posiciones 7-10
                for prefijo in ['992', '993', '994', '995', '996', '997', '998']:
                    variantes.append(f"{prefijo}{segmento1}.1")
            
            # Patrón 2: Sin sufijo (posiciones 8-11, índices 7-10)
            if longitud >= 11:
                segmento2 = codigo_limpio[7:11]  # Posiciones 8-11
                for prefijo in ['992', '993', '994', '995', '996', '997', '998']:
                    variantes.append(f"{prefijo}{segmento2}")
        
        # Si no es 12 dígitos, aplicar lógica estándar después
        if not variantes:
            if longitud >= 6:
                ultimos_seis = codigo_limpio[-6:]
                if len(ultimos_seis) >= 5:
                    variantes.append(ultimos_seis[1:5])
    
    elif proveedor_key in ['brementools', 'bremen', 'berger', 'nortedist']:
        # Proveedores con método estándar EAN-13
        if longitud >= 6:
            ultimos_seis = codigo_limpio[-6:]
            if len(ultimos_seis) >= 5:
                variantes.append(ultimos_seis[1:5])
    
    # Fase 2: Modo automático (sin proveedor específico o como fallback)
    if not proveedor_key or not variantes:
        if longitud == 13:
            # EAN-13 estándar
            ultimos_seis = codigo_limpio[-6:]
            if len(ultimos_seis) >= 5:
                variantes.append(ultimos_seis[1:5])
        
        elif longitud == 12:
            # UPC-A: Intentar ambos métodos
            ultimos_seis = codigo_limpio[-6:]
            if len(ultimos_seis) >= 5:
                variantes.append(ultimos_seis[1:5])
            
            # También probar lógica Crossmaster automáticamente
            if longitud >= 10:
                segmento1 = codigo_limpio[6:10]
                for prefijo in ['992', '993', '994', '995', '996', '997', '998']:
                    variantes.append(f"{prefijo}{segmento1}.1")
            if longitud >= 11:
                segmento2 = codigo_limpio[7:11]
                for prefijo in ['992', '993', '994', '995', '996', '997', '998']:
                    variantes.append(f"{prefijo}{segmento2}")
        
        elif longitud == 10:
            # Códigos de 10 dígitos
            ultimos_seis = codigo_limpio[-6:]
            if len(ultimos_seis) >= 5:
                variantes.append(ultimos_seis[1:5])
        
        elif longitud >= 6:
            # Códigos de 6+ dígitos: múltiples extracciones
            ultimos_seis = codigo_limpio[-6:]
            if len(ultimos_seis) >= 5:
                variantes.append(ultimos_seis[1:5])  # Posiciones 2-5
            if len(ultimos_seis) >= 4:
                variantes.append(ultimos_seis[0:4])  # Posiciones 1-4
        
        elif longitud >= 4:
            # Códigos cortos: usar directamente
            variantes.append(codigo_limpio)
    
    # Eliminar duplicados manteniendo orden
    seen = set()
    variantes_unicas = []
    for v in variantes:
        if v and v not in seen:
            seen.add(v)
            variantes_unicas.append(v)
    
    print(f'[BARCODE] Variantes generadas: {variantes_unicas[:5]}{"..." if len(variantes_unicas) > 5 else ""}', flush=True)
    return variantes_unicas


@app.route('/busqueda_codigos', methods=['GET', 'POST'])
@login_required
def barcode_search():
    barcode_input = ''
    mensaje = None
    codigos_intentados = []
    resultados = []
    proveedor_filtro = ''
    proveedor_filtro_manual = ''  # Para mantener en el formulario
    modo_detectado = None

    if request.method == 'POST':
        barcode_input = request.form.get('barcode', '').strip()
        proveedor_filtro_manual = request.form.get('proveedor', '').strip()
        proveedor_filtro = proveedor_filtro_manual  # Inicialmente usar el del formulario
        ejecutar_busqueda = True
    else:
        barcode_input = request.args.get('codigo', '').strip()
        proveedor_filtro_manual = request.args.get('proveedor', '').strip()
        proveedor_filtro = proveedor_filtro_manual
        ejecutar_busqueda = bool(barcode_input)

    # Auto-sincronizar listas si están desactualizadas (solo si se usa DB para listas)
    if LISTAS_EN_DB and DATABASE_URL and psycopg:
        auto_sync_info = maybe_auto_sync_listas()
        if auto_sync_info and not auto_sync_info.get('error'):
            try:
                archivos = auto_sync_info.get('procesados')
                insertados = auto_sync_info.get('insertados')
                mensaje = (mensaje or '') + ("\n" if mensaje else '') + f"🔄 Listas sincronizadas automáticamente: {archivos} archivo(s), {insertados} fila(s) insertadas."
            except Exception:
                pass

    if ejecutar_busqueda:
        if len(barcode_input) < 4:
            mensaje = '⚠️ Ingresá al menos 4 caracteres del código.'
        else:
            # Detectar modo inteligente y aplicar filtro automático
            codigo_limpio = barcode_input.strip()
            codigo_numerico = ''.join(filter(str.isdigit, codigo_limpio))
            longitud = len(codigo_numerico)
            
            # **MODO INTELIGENTE**: Aplicar filtro automático SOLO si el usuario NO seleccionó un proveedor manualmente
            if MODO_BARCODE_INTELIGENTE and not proveedor_filtro_manual and codigo_numerico:
                if longitud == 12:
                    # UPC-A (12 dígitos) -> Filtrar solo Crossmaster
                    proveedor_filtro = 'Crossmaster'
                    modo_detectado = 'UPC-A (12 dígitos) → Búsqueda automática en Crossmaster'
                    print(f'[BARCODE] Modo inteligente: UPC-A detectado, filtrando por Crossmaster', flush=True)
                elif longitud == 13:
                    # EAN-13 (13 dígitos) -> Buscar en todos (asegurar que no hay filtro)
                    proveedor_filtro = ''
                    modo_detectado = 'EAN-13 (13 dígitos) → Búsqueda en todos los proveedores'
                    print(f'[BARCODE] Modo inteligente: EAN-13 detectado, buscando en todos', flush=True)
            
            # Intentar primero con el código tal como se ingresó (para códigos normales de productos)
            codigos_intentados = [codigo_limpio]
            
            # Buscar primero con el código original
            resultados_temp = buscar_productos_por_codigo_exacto(codigo_limpio, proveedor_filtro)
            if resultados_temp:
                resultados.extend(resultados_temp)
            
            # Si no encontró nada y parece ser un código de barras (solo dígitos, >= 12 caracteres),
            # intentar con las variantes de código de barras
            if not resultados and barcode_input.isdigit() and len(barcode_input) >= 12:
                variantes_barcode = extraer_codigo_de_barras(barcode_input, proveedor_filtro)
                if variantes_barcode:
                    codigos_intentados.extend(variantes_barcode)
                    
                    # **OPTIMIZACIÓN CONDICIONAL**: Elegir método según configuración
                    import time
                    inicio = time.time()
                    
                    if BUSQUEDA_BARCODE_OPTIMIZADA:
                        # **MODO RÁPIDO**: Buscar todas las variantes en UNA SOLA QUERY
                        print(f'[BARCODE] Modo rápido: Buscando {len(variantes_barcode)} variantes en una sola consulta...', flush=True)
                        resultados_temp = buscar_productos_por_codigos_multiples(variantes_barcode, proveedor_filtro)
                        if resultados_temp:
                            resultados.extend(resultados_temp)
                    else:
                        # **MODO LENTO**: Buscar cada variante secuencialmente (para debugging)
                        print(f'[BARCODE] Modo lento: Buscando {len(variantes_barcode)} variantes secuencialmente...', flush=True)
                        for codigo_exacto in variantes_barcode:
                            resultados_temp = buscar_productos_por_codigo_exacto(codigo_exacto, proveedor_filtro)
                            if resultados_temp:
                                resultados.extend(resultados_temp)
                    
                    tiempo_transcurrido = time.time() - inicio
                    modo_usado = "rápido (optimizado)" if BUSQUEDA_BARCODE_OPTIMIZADA else "lento (secuencial)"
                    print(f'[BARCODE] Búsqueda completada en {tiempo_transcurrido:.2f} segundos (modo {modo_usado})', flush=True)
            
            # Eliminar duplicados (por si un producto coincide con múltiples códigos)
            resultados_unicos = []
            codigos_vistos = set()
            for r in resultados:
                key = (r.get('codigo'), r.get('proveedor_key'))
                if key not in codigos_vistos:
                    codigos_vistos.add(key)
                    resultados_unicos.append(r)
            resultados = resultados_unicos
            
            if resultados:
                mensaje_base = f"✅ Se encontraron {len(resultados)} producto(s) con código exacto."
                if modo_detectado:
                    mensaje_base = f"🔍 {modo_detectado}\n{mensaje_base}"
                if len(codigos_intentados) > 1:
                    mensaje_base += f" Variantes probadas: {', '.join(codigos_intentados[:3])}{'...' if len(codigos_intentados) > 3 else ''}"
                mensaje = mensaje_base
            else:
                mensaje_base = f"ℹ️ No se encontraron productos con código exacto."
                if modo_detectado:
                    mensaje_base = f"🔍 {modo_detectado}\n{mensaje_base}"
                if len(codigos_intentados) > 1:
                    mensaje_base += f" Variantes probadas: {', '.join(codigos_intentados[:5])}{'...' if len(codigos_intentados) > 5 else ''}"
                mensaje = mensaje_base

    lista_proveedores = sorted({p.get('nombre_base') for p in proveedores.values() if p.get('nombre_base')})
    # Agregar "Manual" si existe productos_manual.xlsx
    productos_manual_list, err_manual = load_manual_products()
    if productos_manual_list and not err_manual:
        if 'Manual' not in lista_proveedores:
            lista_proveedores.append('Manual')
            lista_proveedores.sort()

    return render_template(
        'barcode_search.html',
        barcode_input=barcode_input,
        codigo_patron=', '.join(codigos_intentados[:3]) if codigos_intentados else '',
        resultados=resultados,
        mensaje=mensaje,
        lista_proveedores=lista_proveedores,
        proveedor_filtro=proveedor_filtro_manual,  # Usar el manual para el formulario
        modo_barcode_inteligente=MODO_BARCODE_INTELIGENTE
    )

@app.route('/health')
def health():
    modo_storage = 'sqlite' if USE_SQLITE else ('postgresql' if (DATABASE_URL and psycopg) else 'json')
    prov_count = 'n/a'
    try:
        prov_count = len(proveedores)
    except Exception:
        pass
    histo_len = None
    try:
        histo_len = len(load_historial())
    except Exception:
        histo_len = 'err'
    
    # Verificar si hay productos en la DB
    productos_en_db = None
    if DATABASE_URL and psycopg:
        try:
            with get_pg_conn() as conn:
                if conn:
                    with conn.cursor() as cur:
                        cur.execute('SELECT COUNT(*) as total FROM productos_listas')
                        row = cur.fetchone()
                        if isinstance(row, dict):
                            productos_en_db = row.get('total') or row.get('count') or list(row.values())[0]
                        else:
                            productos_en_db = row[0] if row else 0
        except Exception as e:
            productos_en_db = f'error: {e}'
    
    return {
        'status': 'ok',
        'storage': modo_storage,
        'listas_en_db': LISTAS_EN_DB,
        'database_url_configured': bool(DATABASE_URL),
        'productos_en_db': productos_en_db,
        'proveedores': prov_count,
        'historial_count': histo_len,
        'debug': DEBUG_LOG
    }, 200

def abrir_navegador(port):
    """Función disponible pero desactivada por defecto."""
    if os.getenv('OPEN_BROWSER', '0') == '1':
        try:
            webbrowser.open_new(f'http://127.0.0.1:{port}/')
        except Exception:
            pass

def get_lan_ips():
    """Devuelve una lista de IPv4 locales útiles para acceso LAN."""
    ips = []
    # Método 1: interfaz por defecto vía socket UDP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("127."):
            ips.append(ip)
    except Exception:
        pass
    # Método 2: nombre del host
    try:
        host = socket.gethostname()
        for ip in socket.gethostbyname_ex(host)[2]:
            if ip and ip not in ips and not ip.startswith("127."):
                ips.append(ip)
    except Exception:
        pass
    return ips

def is_port_free(port: int, host: str = '0.0.0.0') -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
        return True
    except OSError:
        return False

def pick_port() -> int:
    # Si hay PORT en el entorno, se usa tal cual
    try:
        env_port = os.getenv('PORT')
        if env_port:
            return int(env_port)
    except Exception:
        pass
    # Preferencia: 5000, luego una lista de alternativas comunes
    # Evitar 5000; empezar por 5001 y subir
    candidates = [5001, 5002, 5003, 5004, 5005, 8080, 8081]
    for p in candidates:
        if is_port_free(p):
            return p
    # Último recurso: 5000 (puede fallar si está ocupado)
    return 5000

if __name__ == "__main__":
    # Elegir automáticamente un puerto disponible cercano a 5000 (o PORT si está definido)
    port = pick_port()
    # No se abre navegador automáticamente (evitar 127.0.0.1); habilitar con OPEN_BROWSER=1
    print(f"Iniciando servidor en http://0.0.0.0:{port}/ (Waitress)")
    print(f"Las listas de precios en formato Excel deben guardarse en: {LISTAS_PATH}")
    # Mostrar URL LAN recomendada (primera IP disponible)
    try:
        ips = get_lan_ips()
        if ips:
            print(f"URL de acceso (LAN): http://{ips[0]}:{port}/")
        else:
            print("[INFO] No se detectó IP LAN; usa http://127.0.0.1:%d/ en este equipo" % port)
    except Exception:
        pass
    try:
        serve(app, host='0.0.0.0', port=port)
    except Exception as e:
        print(f"[ERROR] No se pudo iniciar el servidor en el puerto {port}: {e}")
        print("Sugerencia: define otra variable PORT (por ejemplo 5000) y vuelve a ejecutar.")
        sys.exit(1)