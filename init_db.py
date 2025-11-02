#!/usr/bin/env python3
"""
Script para inicializar la base de datos PostgreSQL en Railway.
Ejecutar una sola vez después del primer deployment.

Uso en Railway (desde el terminal):
    python init_db.py
"""

import os
import sys

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    print("[ERROR] psycopg no está instalado. Instala con: pip install 'psycopg[binary]'")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("[ERROR] Variable de entorno DATABASE_URL no configurada.")
    sys.exit(1)

print(f"[INFO] Conectando a PostgreSQL...")
print(f"[INFO] DATABASE_URL: {DATABASE_URL[:50]}...")

try:
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    print("[SUCCESS] Conexión exitosa a PostgreSQL.")
except Exception as e:
    print(f"[ERROR] No se pudo conectar a PostgreSQL: {e}")
    sys.exit(1)

try:
    with conn, conn.cursor() as cur:
        print("[INFO] Creando tabla proveedores...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS proveedores (
                id TEXT PRIMARY KEY,
                data JSONB NOT NULL
            );
        """)
        
        print("[INFO] Creando tabla historial...")
        cur.execute("""
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
        """)
        
        print("[INFO] Creando tabla usuarios...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        print("[INFO] Creando tabla import_batches...")
        cur.execute("""
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
        """)
        
        print("[INFO] Creando tabla productos_listas...")
        cur.execute("""
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
        """)
        
        print("[INFO] Creando índices...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_prod_listas_prov_codigo ON productos_listas (proveedor_key, codigo);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_prod_listas_codigo_dig ON productos_listas (codigo_digitos);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_prod_listas_arch_hoja ON productos_listas (archivo, hoja);")
        
        # Agregar columna iva si no existe
        try:
            cur.execute("ALTER TABLE productos_listas ADD COLUMN IF NOT EXISTS iva TEXT;")
            print("[INFO] Columna iva verificada.")
        except Exception as col_err:
            print(f"[WARN] No se pudo agregar columna iva: {col_err}")
        
        # Intentar crear índice GIN (opcional, requiere pg_trgm)
        try:
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_prod_listas_nombre_trgm 
                ON productos_listas USING GIN (nombre_normalizado gin_trgm_ops);
            """)
            print("[SUCCESS] Índice GIN trgm creado.")
        except Exception as trgm_err:
            print(f"[WARN] Índice GIN trgm no creado (extensión pg_trgm no habilitada): {trgm_err}")
        
        conn.commit()
        print("\n" + "="*60)
        print("[SUCCESS] ✓ Todas las tablas creadas correctamente.")
        print("="*60)
        
        # Verificar tablas creadas
        cur.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            ORDER BY tablename;
        """)
        tables = [row['tablename'] for row in cur.fetchall()]
        print("\n[INFO] Tablas encontradas en la base de datos:")
        for table in tables:
            print(f"  - {table}")
        
except Exception as e:
    print(f"\n[ERROR] Error durante la inicialización: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
finally:
    try:
        conn.close()
        print("\n[INFO] Conexión cerrada.")
    except Exception:
        pass
