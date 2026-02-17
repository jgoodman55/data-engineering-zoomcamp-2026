import marimo

__generated_with = "0.19.11"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import ibis
    import pandas as pd
    from datetime import datetime
    import os

    return ibis, mo, os


@app.cell
def _(ibis, mo, os):
    # USE ABSOLUTE PATH if possible, or ensure this matches your dlt output
    db_path = "taxi_pipeline.duckdb" 
    
    if not os.path.exists(db_path):
        con = None
        result_msg = mo.md(f"⚠️ **Database not found at {os.path.abspath(db_path)}**")
    else:
        try:
            # read_only=True is CRITICAL to avoid the ConnectionError/IO Error
            con = ibis.duckdb.connect(db_path, read_only=True)
            
            # dlt puts data in specific schemas. Let's find them.
            schemas = con.sql("SELECT schema_name FROM information_schema.schemata").execute()['schema_name'].tolist()
            dlt_schema = next((s for s in schemas if "taxi_pipeline_dataset" in s), "main")
            
            result_msg = mo.md(f"✅ Connected! Using schema: `{dlt_schema}`")
        except Exception as e:
            con = None
            result_msg = mo.md(f"❌ Connection failed: {e}")
            
    return con, dlt_schema, result_msg


@app.cell
def _(con, dlt_schema, mo):
    try:
        # Use the detected schema
        taxi_data = con.table("taxi_data", schema=dlt_schema)
        row_count = taxi_data.count().execute()
        table_msg = mo.md(f"✅ Table `taxi_data` loaded with **{row_count:,}** rows.")
    except Exception as e:
        taxi_data = None
        table_msg = mo.md(f"⚠️ Could not find `taxi_data` in `{dlt_schema}`. Error: {e}")
    return taxi_data, table_msg


@app.cell
def _(taxi_data):
    print(taxi_data)
    return


@app.cell
def _(mo, taxi_data):
    if taxi_data is not None:
        # Find date columns
        columns = taxi_data.schema()
        date_cols = [col for col in columns.names if 'date' in col.lower() or 'time' in col.lower()]

        if date_cols:
            mo.md(f"**Date/Time columns found:** {date_cols}")
        else:
            mo.md("⚠️ No date/time columns found in the data")

        # Get table info
        record_count = taxi_data.count().execute()
        mo.md(f"**Total records:** {record_count:,}")
    return (date_cols,)


@app.cell
def _(date_cols, mo, taxi_data):
    if taxi_data is not None and date_cols:
        # Use the first date column found
        date_col = date_cols[0]

        # Query for min and max dates
        date_range = taxi_data.aggregation(
            min_date=taxi_data[date_col].min(),
            max_date=taxi_data[date_col].max(),
            count=taxi_data.count()
        ).execute()

        min_date = date_range['min_date'][0]
        max_date = date_range['max_date'][0]
        total_records = date_range['count'][0]

        stats_md = mo.md(f"""
        ## Data Range Statistics

        **Date Column:** `{date_col}`

        **Earliest Date:** {min_date}

        **Latest Date:** {max_date}

        **Total Records:** {total_records:,}
        """)

        stats_md
    else:
        mo.md("Unable to calculate date range statistics")
    return (date_col,)


@app.cell
def _(date_col, date_cols, mo, taxi_data):
    if taxi_data is not None and date_cols:
        import plotly.express as px

        # Get records by date to visualize distribution
        daily_counts = taxi_data.group_by(
            taxi_data[date_col].cast('date').name('date')
        ).aggregate(
            count=taxi_data.count()
        ).order_by('date').execute()

        # Create line chart
        fig = px.line(
            daily_counts,
            x='date',
            y='count',
            title=f'Daily Record Count - {date_col}',
            labels={'count': 'Number of Records', 'date': 'Date'},
            markers=True
        )

        fig.update_layout(
            height=400,
            template='plotly_light',
            hovermode='x unified'
        )

        mo.plotly(fig)
    else:
        mo.md("Unable to create visualization")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
