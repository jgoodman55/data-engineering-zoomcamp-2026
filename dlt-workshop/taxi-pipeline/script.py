import duckdb

conn = duckdb.connect(database="/workspaces/data-engineering-zoomcamp-2026/dlt-workshop/taxi-pipeline/taxi_pipeline.duckdb", read_only=True)

tables = conn.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name
""").fetchall()

result_df = conn.execute(f'SELECT * FROM "taxi_pipeline_dataset"."taxi_data"').fetchdf()
print(len(result_df))
# conn.execute(f'DELETE FROM "taxi_pipeline_dataset"."taxi_data"')
# for schema, table_name in tables:
#     count = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]

#     print(f"{schema}.{table_name}: {count:,} rows")


conn.close()
