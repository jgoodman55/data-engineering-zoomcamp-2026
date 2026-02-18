import duckdb

conn = duckdb.connect(database="/workspaces/data-engineering-zoomcamp-2026/dlt-workshop/taxi-pipeline/taxi_pipeline.duckdb", read_only=True)

tables = conn.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name
""").fetchall()

result_df = conn.execute(f'SELECT * FROM "taxi_pipeline_dataset"."taxi_data"').fetchdf()

# Q1: What is the date range of the data in the `taxi_data` table?
print("Q1: ",min(result_df['trip_pickup_date_time']), max(result_df['trip_pickup_date_time']))

# Q2: What proportion of trips are paid with credit card?
print("Q2: ", result_df[result_df['payment_type'] == 'Credit'].shape[0] / result_df.shape[0])

# Q3: What is the total amount of money generated in tips? (1 point)
print("Q3: ", result_df['tip_amt'].sum())

conn.close()
