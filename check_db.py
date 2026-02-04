from app_v5 import get_pg_conn

conn = get_pg_conn()
cur = conn.cursor()

cur.execute('SELECT COUNT(*) as total FROM productos_listas')
row = cur.fetchone()
print(f'Row type: {type(row)}, content: {row}')
if isinstance(row, dict):
    # Try different possible key names
    total = row.get('total') or row.get('count') or list(row.values())[0]
else:
    total = row[0]
print(f'Total productos en DB: {total}')

cur.execute('SELECT proveedor_nombre, COUNT(*) as total FROM productos_listas GROUP BY proveedor_nombre ORDER BY proveedor_nombre')
print('\nPor proveedor:')
rows = cur.fetchall()
for r in rows:
    if isinstance(r, dict):
        nombre = r.get('proveedor_nombre', 'Unknown')
        count = r.get('total') or r.get('count') or list(r.values())[1]
        print(f'  {nombre}: {count}')
    else:
        print(f'  {r[0]}: {r[1]}')

conn.close()
