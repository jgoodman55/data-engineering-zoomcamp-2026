import marimo

__generated_with = "0.19.11"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import duckdb
    import os

    return (duckdb,)


@app.cell
def _(duckdb):
    import dlt

    pipeline = dlt.pipeline(pipeline_name='taxi_pipeline', destination='duckdb')

    db_path = "/workspaces/data-engineering-zoomcamp-2026/dlt-workshop/taxi-pipeline/taxi_pipeline.duckdb"  # File is in current directory

    conn = duckdb.connect(db_path, read_only=True)

    tables_result = conn.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """).fetchall()

    rows = []
    for schema, table_name in tables_result:
        count = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"').fetchone()[0]
        rows.append(f"- **{schema}.{table_name}** ({count:,} rows)")

    conn.close()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
