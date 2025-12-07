from flask import Flask, render_template, request, send_file
import pandas as pd
import teradatasql
from io import BytesIO
from datetime import datetime
import logging

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

TERADATA_HOST = '192.168.137.128'
TERADATA_USER = 'dbc'
TERADATA_PASSWORD = 'dbc'
TERADATA_DB = 'tin_db'


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/download', methods=['POST'])
def download():
    fe_tin = request.form['fe_tin']
    end_date_str = request.form['end_date']
    start_date_str = request.form.get('start_date')  # optional
    npi_value = request.form.get('npi')               # optional

    try:
        # Validate end date
        end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d')
        formatted_end_date = end_date_obj.strftime('%Y-%m-%d')

        # Use today's date if start_date is not provided
        if start_date_str:
            start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
        else:
            start_date_obj = datetime.today()
        formatted_start_date = start_date_obj.strftime('%Y-%m-%d')

        # Split TINs by comma and strip spaces
        tin_list = [tin.strip() for tin in fe_tin.split(",") if tin.strip()]
        if not tin_list:
            return "Please provide at least one TIN.", 400

        # Create placeholders for TINs
        tin_placeholders = ','.join(['?'] * len(tin_list))

        # Base SQL query
        query = f"""
        SELECT * FROM {TERADATA_DB}.records
        WHERE end_date >= ?
          AND start_date <= ?
          AND tin IN ({tin_placeholders})
        """

        params = [formatted_end_date, formatted_start_date] + tin_list

        # Optional NPI filter
        if npi_value:
            query += " AND npi = ?"
            params.append(npi_value)

        logging.info("Final SQL query:\n%s", query)
        logging.info("Params: %s", params)

        # Execute query
        with teradatasql.connect(
            host=TERADATA_HOST,
            user=TERADATA_USER,
            password=TERADATA_PASSWORD
        ) as con:
            cur = con.cursor()
            cur.execute(query, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=cols)

        if df.empty:
            logging.warning(f"No records found for TINs={fe_tin}, End Date={formatted_end_date}, Start Date={formatted_start_date}")
            return "No records found.", 404

        # Prepare Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Report')
        output.seek(0)

        filename = f"{'_'.join(tin_list)}_{formatted_end_date}.xlsx"
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
