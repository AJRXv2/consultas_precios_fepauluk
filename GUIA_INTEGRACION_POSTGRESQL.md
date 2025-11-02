# Guía de Integración PostgreSQL para Sistema de Listas de Precios

## Contexto del Sistema Actual

Este sistema (`calculadora_web_9`) utiliza PostgreSQL para almacenar y buscar productos de múltiples proveedores. Los productos se importan desde archivos Excel ubicados en la carpeta `listas_excel/`.

## Arquitectura de Base de Datos

### Estructura de Tablas

```sql
-- Tabla principal de productos
CREATE TABLE productos_listas (
    id BIGSERIAL PRIMARY KEY,
    proveedor_key TEXT NOT NULL,          -- Identificador normalizado del proveedor (ej: 'brementools', 'crossmaster')
    proveedor_nombre TEXT,                 -- Nombre visible del proveedor
    archivo TEXT NOT NULL,                 -- Nombre del archivo Excel origen
    hoja TEXT NOT NULL,                    -- Nombre de la hoja Excel
    mtime DOUBLE PRECISION NOT NULL,       -- Timestamp de última modificación del archivo
    codigo TEXT,                           -- Código del producto tal como aparece en Excel
    codigo_digitos TEXT,                   -- Solo los dígitos del código (para búsquedas)
    codigo_normalizado TEXT,               -- Código normalizado (sin espacios, minúsculas)
    nombre TEXT,                           -- Nombre del producto
    nombre_normalizado TEXT,               -- Nombre normalizado para búsquedas
    precio NUMERIC(14,4),                  -- Precio canónico principal
    precio_fuente TEXT,                    -- Nombre de la columna de donde vino el precio
    iva TEXT,                              -- Porcentaje de IVA (ej: "21%", "10.5%")
    precios JSONB,                         -- Otros precios adicionales {"precio de lista": 1000, ...}
    extra_datos JSONB,                     -- Datos extra del producto
    batch_id BIGINT,                       -- ID del lote de importación
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Índices para búsquedas rápidas
CREATE INDEX idx_prod_listas_prov_codigo ON productos_listas (proveedor_key, codigo);
CREATE INDEX idx_prod_listas_codigo_dig ON productos_listas (codigo_digitos);
CREATE INDEX idx_prod_listas_arch_hoja ON productos_listas (archivo, hoja);

-- Índice GIN para búsquedas de texto (requiere extensión pg_trgm)
CREATE INDEX idx_prod_listas_nombre_trgm ON productos_listas 
USING GIN (nombre_normalizado gin_trgm_ops);

-- Tabla de lotes de importación
CREATE TABLE import_batches (
    id BIGSERIAL PRIMARY KEY,
    proveedor_key TEXT NOT NULL,
    archivo TEXT NOT NULL,
    mtime DOUBLE PRECISION,
    status TEXT DEFAULT 'running',        -- 'running', 'completed', 'failed'
    total_rows INT DEFAULT 0,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Tabla de proveedores configurados
CREATE TABLE proveedores (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL
);
```

## Configuración de Proveedores

### Configuración para Importación (Python)

```python
def _listas_provider_configs():
    """Configuración de cada proveedor para importar desde Excel"""
    return {
        'crossmaster': {
            'header': 11,  # Fila de encabezado (0-based)
            'codigo': ['codigo', 'código', 'codigo ean', 'ean', 'cod'],
            'nombre': ['descripcion', 'descripción', 'producto', 'nombre'],
            'precio_canon': ['precio con iva', 'psv con iva'],
            'iva': ['iva', 'i.v.a']
        },
        'berger': {
            'header': 0,
            'codigo': ['codigo', 'código', 'cod'],
            'nombre': ['detalle', 'descripcion', 'producto', 'nombre'],
            'precio_canon': ['precio', 'pventa'],
            'iva': ['iva']
        },
        'brementools': {
            'header': 5,
            'codigo': ['codigo', 'código', 'codigo ean', 'ean'],
            'nombre': ['producto', 'descripcion'],
            'precio_canon': ['precio de venta', 'precio venta'],
            'precios_extra': ['precio de lista', 'precio neto unitario'],
            'iva': ['iva']
        },
        'cachan': {
            'header': 0,
            'codigo': ['codigo', 'código'],
            'nombre': ['nombre', 'producto', 'descripcion'],
            'precio_canon': ['precio'],
            'iva': []
        },
        'chiesa': {
            'header': 1,
            'codigo': ['codigo', 'código'],
            'nombre': ['descripcion', 'producto', 'nombre'],
            'precio_canon': ['pr unit', 'prunit'],
            'iva': ['iva', 'i.v.a']
        }
    }
```

**Explicación de campos:**
- `header`: Fila donde están los encabezados (0-based, 0 = primera fila)
- `codigo`: Lista de posibles nombres para la columna de código
- `nombre`: Lista de posibles nombres para la columna de nombre/descripción
- `precio_canon`: Columna que contiene el precio principal a guardar
- `precios_extra`: Columnas con precios adicionales (se guardan en JSONB)
- `iva`: Columna con el porcentaje de IVA

## Proceso de Importación

### 1. Escanear Archivos Excel

```python
import os
import pandas as pd

LISTAS_PATH = 'ruta/a/listas_excel'

excel_files = sorted(f for f in os.listdir(LISTAS_PATH) 
                     if f.lower().endswith(('.xlsx', '.xls')) 
                     and 'old' not in f.lower())
```

### 2. Normalizar Nombres de Columnas

```python
import unicodedata
import re

def normalize_text(text):
    """Normaliza texto: sin acentos, minúsculas, sin caracteres especiales"""
    text = str(text)
    text = ''.join(c for c in unicodedata.normalize('NFD', text) 
                   if unicodedata.category(c) != 'Mn')
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]+', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# Al leer Excel, normalizar columnas
df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
df.columns = [normalize_text(c) for c in df.columns]
```

### 3. Buscar Columnas por Alias

```python
def _find_first_col(df_cols, aliases):
    """Encuentra la primera columna que coincide con algún alias"""
    if not aliases:
        return None
    for alias in aliases:
        alias_norm = normalize_text(alias)
        for col in df_cols:
            if normalize_text(str(col)) == alias_norm:
                return col
    return None
```

### 4. Parsear Precios (Formato Argentino)

```python
def parse_price_value(value):
    """
    Parsea precios con formato argentino:
    - Punto (.) como separador de miles
    - Coma (,) como separador decimal
    Ejemplos: "1.234,56" -> 1234.56
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    
    text = str(value).strip()
    if not text:
        return None
    
    # Remover caracteres no numéricos excepto comas, puntos y signos
    text = re.sub(r"[^0-9,.-]", "", text)
    
    # Si hay coma, es decimal argentino
    if ',' in text:
        text = text.replace('.', '')  # Quitar separadores de miles
        text = text.replace(',', '.')  # Coma a punto decimal
    else:
        # Sin coma, puntos son separadores de miles
        text = text.replace('.', '')
    
    try:
        return float(text)
    except ValueError:
        return None
```

### 5. Procesar IVA

```python
def process_iva_value(raw_iva):
    """
    Convierte valores de IVA a formato de porcentaje con símbolo.
    - Si viene como decimal (0.21) -> "21%"
    - Si viene como entero (21) -> "21%"
    """
    if pd.isna(raw_iva):
        return None
    
    if isinstance(raw_iva, (int, float)):
        # Si es menor a 1, es decimal (0.21 -> 21%)
        if raw_iva < 1:
            iva_value = raw_iva * 100
        else:
            iva_value = raw_iva
        
        # Formato sin decimales si es entero
        if iva_value == int(iva_value):
            return str(int(iva_value)) + '%'
        else:
            return str(iva_value) + '%'
    else:
        # String: intentar convertir
        try:
            iva_text = str(raw_iva).strip().replace('%', '').replace(',', '.')
            iva_num = float(iva_text)
            if iva_num < 1:
                iva_value = iva_num * 100
            else:
                iva_value = iva_num
            
            if iva_value == int(iva_value):
                return str(int(iva_value)) + '%'
            else:
                return str(iva_value) + '%'
        except:
            return str(raw_iva).strip()
```

### 6. Importar Productos (Reemplazo por Archivo)

```python
import json

def sync_listas_to_db(conn):
    """
    Importa productos desde Excel a PostgreSQL.
    Estrategia: DELETE + INSERT por archivo (transaccional)
    """
    cfg = _listas_provider_configs()
    
    for filename in excel_files:
        provider_key = provider_key_from_filename(filename)
        config = cfg.get(provider_key)
        if not config:
            continue
        
        # Crear lote de importación
        cur.execute(
            "INSERT INTO import_batches (proveedor_key, archivo, mtime, status) "
            "VALUES (%s,%s,%s,%s) RETURNING id",
            (provider_key, filename, mtime, 'running')
        )
        batch_id = cur.fetchone()[0]
        
        # Limpiar productos anteriores de este archivo
        cur.execute("DELETE FROM productos_listas WHERE archivo=%s", (filename,))
        
        # Leer Excel
        all_sheets = pd.read_excel(file_path, sheet_name=None, header=config['header'])
        
        total_insertados = 0
        for sheet_name, df in all_sheets.items():
            df.columns = [normalize_text(c) for c in df.columns]
            
            # Buscar columnas
            codigo_col = _find_first_col(df.columns, config['codigo'])
            nombre_col = _find_first_col(df.columns, config['nombre'])
            precio_col = _find_first_col(df.columns, config['precio_canon'])
            iva_col = _find_first_col(df.columns, config.get('iva', []))
            
            if not codigo_col or not nombre_col:
                continue
            
            for _, fila in df.iterrows():
                # Extraer datos
                codigo = str(fila[codigo_col]).strip()
                nombre = str(fila[nombre_col]).strip()
                precio = parse_price_value(fila.get(precio_col))
                iva = process_iva_value(fila.get(iva_col)) if iva_col else None
                
                # Preparar campos normalizados
                codigo_digitos = ''.join(filter(str.isdigit, codigo))
                codigo_norm = normalize_text(codigo)
                nombre_norm = normalize_text(nombre)
                
                # Precios adicionales
                precios_dict = {}
                # (lógica para extraer precios_extra según proveedor)
                
                # Insertar
                cur.execute(
                    """
                    INSERT INTO productos_listas
                    (proveedor_key, proveedor_nombre, archivo, hoja, mtime,
                     codigo, codigo_digitos, codigo_normalizado,
                     nombre, nombre_normalizado,
                     precio, precio_fuente, iva, precios, extra_datos, batch_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s)
                    """,
                    (provider_key, proveedor_nombre, filename, sheet_name, mtime,
                     codigo, codigo_digitos, codigo_norm,
                     nombre, nombre_norm,
                     precio, config['precio_canon'][0], iva,
                     json.dumps(precios_dict, ensure_ascii=False),
                     json.dumps({}, ensure_ascii=False),
                     batch_id)
                )
                total_insertados += 1
        
        # Completar lote
        cur.execute(
            "UPDATE import_batches SET status=%s, completed_at=NOW(), total_rows=%s WHERE id=%s",
            ('completed', total_insertados, batch_id)
        )
        conn.commit()
```


## Conversión de Tipos (IMPORTANTE)

### Problema: Decimal vs Float

PostgreSQL devuelve campos `NUMERIC` como objetos `Decimal` de Python, que no se serializan bien en JSON ni se formatean correctamente en templates.

**Solución:**

```python
def convert_decimal_to_float(value):
    """Convierte Decimal a float para compatibilidad"""
    if hasattr(value, '__float__'):
        return float(value)
    return value

# Al construir productos
precio_float = convert_decimal_to_float(row['precio'])
```

### Nombres de Precios según Proveedor

```python
def get_precio_display_name(proveedor_key: str) -> str:
    """Devuelve el nombre a mostrar para el precio canónico"""
    nombres_precio = {
        'brementools': 'Precio de Venta',
        'crossmaster': 'Precio con IVA',
        'berger': 'Precio',
        'chiesa': 'Pr.Unit',
        'cachan': 'Precio',
        'manual': 'Precio'
    }
    return nombres_precio.get(proveedor_key, 'Precio')
```

## Variables de Entorno

```bash
# PostgreSQL
DATABASE_URL=postgresql://usuario:password@localhost:5432/nombre_db

# Flags
LISTAS_EN_DB=1          # 1=usar PostgreSQL, 0=usar Excel directo
DEBUG_LOG=1             # 1=activar logs de debug
```

## Conexión a PostgreSQL

```python
import psycopg
from psycopg.rows import dict_row

def get_pg_conn():
    """Retorna conexión PostgreSQL con dict_row factory"""
    conn = psycopg.connect(
        os.environ['DATABASE_URL'],
        row_factory=dict_row
    )
    return conn
```

## Flujo Completo de Trabajo

1. **Usuario sube archivos Excel** a `listas_excel/`
2. **Usuario hace clic en "Sincronizar"** en la interfaz web
3. **Sistema ejecuta `sync_listas_to_db()`**:
   - Lee cada archivo Excel
   - Borra productos anteriores del mismo archivo (DELETE)
   - Inserta nuevos productos (INSERT)
   - Registra lote en `import_batches`
4. **Usuario escanea código de barras**
5. **Sistema detecta entrada rápida** y ejecuta búsqueda automática
6. **Sistema extrae variantes** del código de barras
7. **Sistema busca en DB** con cada variante (búsqueda exacta)
8. **Sistema muestra resultados** con precios y datos formateados

## Puntos Clave para Integración

1. **Normalización consistente** de textos (sin acentos, minúsculas)
2. **Búsqueda exacta** (`WHERE codigo = %s`) no LIKE
3. **Conversión Decimal → Float** antes de serializar
4. **Procesamiento de IVA** (decimal → porcentaje con símbolo)
5. **Parseo de precios** con formato argentino (punto=miles, coma=decimal)
6. **Variantes de códigos de barras** según estándar y proveedor
7. **Índices apropiados** para búsquedas rápidas
8. **Transacciones** para importación (DELETE + INSERT atómico)


Esta guía proporciona toda la lógica y estructura necesaria para replicar el sistema en otra aplicación más grande.
