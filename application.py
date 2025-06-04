import os
import io
import csv

from flask import Flask, Response, jsonify
from sqlalchemy import create_engine, text

app = Flask(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# STEP 1: Read your RDS connection string from an environment variable
#   In Elastic Beanstalk you’ll set RDS_DATABASE_URL as:
#   "postgresql+psycopg2://adminuser:YourPassword@imperial-ml-db.czsu4kwsqz4p.eu-north-1.rds.amazonaws.com:5432/btc_db"
#───────────────────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("RDS_DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing RDS_DATABASE_URL environment variable")

# STEP 2: Create a SQLAlchemy engine (it will lazily connect on first use)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


@app.route("/btc.csv", methods=["GET"])
def serve_btc_csv():
    """
    Query the entire btc_prices table, stream as CSV.
    """
    # 1) Open a raw connection (will be returned when you iterate over rows)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # 2) Execute a simple SELECT
    cursor.execute("SELECT timestamp, asset_name, open, high, low, close, volume FROM btc_prices;")

    # 3) Use Python’s csv.writer to write to an in-memory buffer in chunks.
    def generate():
        # Write header row
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["timestamp", "asset_name", "open", "high", "low", "close", "volume"])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        # Stream each row from the cursor
        for row in cursor:
            writer.writerow(row)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

        cursor.close()
        conn.close()

    # 4) Flask Response: streaming, with text/csv MIME
    return Response(generate(), mimetype="text/csv")


@app.route("/btc.json", methods=["GET"])
def serve_btc_json():
    """
    Query the entire btc_prices table, return JSON.
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT timestamp, asset_name, open, high, low, close, volume FROM btc_prices;"))
        rows = result.fetchall()
        # Convert each row to a dict
        columns = result.keys()
        data = [dict(zip(columns, row)) for row in rows]
    # Flask will jsonify the list, converting timestamps automatically to strings
    return jsonify(data)


if __name__ == "__main__":
    # For local testing. Elastic Beanstalk will use Gunicorn instead.
    app.run(host="0.0.0.0", port=5001, debug=True)
