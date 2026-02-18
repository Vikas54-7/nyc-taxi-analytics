import duckdb

conn = duckdb.connect()
result = conn.execute("""
    SELECT * FROM read_parquet('data/raw/*.parquet')
    LIMIT 1
""").fetchone()

# Get column names
columns = conn.execute("""
    DESCRIBE SELECT * FROM read_parquet('data/raw/*.parquet')
""").fetchall()

print("Column names in parquet files:")
for col in columns:
    print(f"  - {col[0]} ({col[1]})")

conn.close()