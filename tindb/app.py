from flask import Flask, render_template, request, send_file

import pandas as pd
import teradatasql
from io import BytesIO
from datetime import datetime
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# Teradata config
TERADATA_HOST = '192.168.137.128'
TERADATA_USER = 'dbc'
TERADATA_PASSWORD = 'dbc'
TERADATA_DB = 'tin_db'


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/download', methods=['POST'])
def download():
    # Get user input
    be_tin = request.form['fe_tin']   # HTML field 'fe_tin'
    date_str = request.form['date']   # HTML field 'date'

    try:
        # Validate and format date
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%Y-%m-%d')

        # Your SQL with 5 placeholders
        query = f"""
        WITH abc AS
        (
            SELECT * FROM {TERADATA_DB}.records
            WHERE txn_date >= ?
        ),
        a AS
        (
            SELECT * FROM {TERADATA_DB}.records
            WHERE txn_date >= ?
        )
        SELECT x.* 
        FROM
        (
            SELECT * FROM abc WHERE tin IN (?)
            UNION
            SELECT * FROM abc WHERE tin IN (?)
        ) x
        WHERE x.txn_date >= ?
        """

        # Build 5 parameters by repeating user input
        # txn_date used 3 times, tin used 2 times
        params = [formatted_date, formatted_date, be_tin, be_tin, formatted_date]

        # Log query and parameters
        logging.info("Query with placeholders:\n%s", query)
        logging.info("Params: %s", params)

        # Build final query for SQL editor preview (for logging/debugging)
        final_query = query
        for p in params:
            final_query = final_query.replace("?", f"'{p}'", 1)
        logging.info("Final SQL query for SQL editor:\n%s", final_query)

        # Execute query
        with teradatasql.connect(
            host=TERADATA_HOST,
            user=TERADATA_USER,
            password=TERADATA_PASSWORD
        ) as con:
            logging.info("Connected to Teradata successfully.")
            df = pd.read_sql(query, con, params=params)

        if df.empty:
            logging.warning(f"No records found for tin={be_tin}, Date={formatted_date}")
            return "No records found.", 404

        # Log number of rows fetched
        logging.info(f"Fetched {len(df)} rows for tin={be_tin}, Date={formatted_date}")

        # Prepare Excel file
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Report')
        output.seek(0)

        # Filename
        filename = f"{be_tin}_{formatted_date}.xlsx"
        logging.info(f"Excel report generated: {filename}")

        return send_file(
            output,
            download_name=filename,
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        logging.exception("Unexpected error occurred")
        return f"Error: {str(e)}", 500


if __name__ == '__main__':
    app.run(debug=True)


