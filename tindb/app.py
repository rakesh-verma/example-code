from flask import Flask, render_template, request, send_file
import pandas as pd
import teradatasql
from io import BytesIO
from datetime import datetime
import logging

app = Flask(__name__)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()]
)

# Teradata config
TERADATA_HOST = "192.168.137.128"
TERADATA_USER = "dbc"
TERADATA_PASSWORD = "dbc"
TERADATA_DB = "tin_db"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/download", methods=["POST"])
def download():
    logging.info("----- New request started -----")

    try:
        # ---------------------------------------
        # INPUT READ
        # ---------------------------------------
        fe_tin_raw = request.form.get("fe_tin", "").strip()
        end_date_str = request.form.get("end_date", "").strip()
        start_date_str = request.form.get("start_date", "").strip()
        npi = request.form.get("npi", "").strip()

        logging.info(
            f"Inputs - fe_tin: {fe_tin_raw} | end_date: {end_date_str} | "
            f"start_date: {start_date_str or '(empty)'} | npi: {npi or '(empty)'}"
        )

        # ---------------------------------------
        # TIN VALIDATION
        # ---------------------------------------
        tins = [t.strip() for t in fe_tin_raw.split(",") if t.strip()]

        if not tins:
            return "TIN is required.", 400

        for t in tins:
            if len(t) != 9:
                logging.warning("Invalid TIN length: %s", t)
                return f"TIN '{t}' must be 9 characters (letters/numbers allowed).", 400

        # ---------------------------------------
        # END DATE VALIDATION
        # ---------------------------------------
        try:
            end_date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
        except:
            logging.warning("Invalid End Date format")
            return "End Date must be in yyyy-mm-dd format.", 400

        # ---------------------------------------
        # START DATE OPTIONAL
        # ---------------------------------------
        if start_date_str:
            try:
                start_date_obj = datetime.strptime(start_date_str, "%Y-%m-%d")
            except:
                logging.warning("Invalid Start Date format")
                return "Start Date must be in yyyy-mm-dd format.", 400
        else:
            start_date_obj = datetime.today()
            logging.info(f"Start Date empty → Using today's date: {start_date_obj}")

        start_date = start_date_obj.strftime("%Y-%m-%d")
        end_date = end_date_obj.strftime("%Y-%m-%d")

        # ---------------------------------------
        # NPI VALIDATION (alphanumeric, 10 chars)
        # ---------------------------------------
        use_npi_filter = False

        if npi:
            if len(npi) != 10 or not npi.isalnum():
                logging.warning("Invalid NPI: %s", npi)
                return "NPI must be exactly 10 alphanumeric characters.", 400
            use_npi_filter = True
            npi_list = [npi]

        # ---------------------------------------
        # BUILD PLACEHOLDERS
        # ---------------------------------------
        tin_placeholders = ",".join(["?"] * len(tins))

        if use_npi_filter:
            npi_placeholders = ",".join(["?"] * len(npi_list))

        logging.info(f"TIN placeholders: {tin_placeholders}")
        logging.info(f"NPI filter enabled: {use_npi_filter}")

        # ---------------------------------------
        # BUILD SQL DYNAMICALLY
        # ---------------------------------------
        base_cte = f"""
        WITH abc AS
        (
            SELECT *
            FROM {TERADATA_DB}.records
            WHERE end_date >= ?
              AND start_date <= ?
        )
        """

        final_where = f"""
        WHERE x.end_date >= ?
          AND x.start_date <= ?
        """

        if use_npi_filter:
            final_where += f" AND (x.npi IN ({npi_placeholders}))"

        query = base_cte + f"""
        SELECT *
        FROM
        (
            SELECT * FROM abc WHERE tin IN ({tin_placeholders})
        ) x
        {final_where}
        """

        # ---------------------------------------
        # PARAM ORDER BUILD
        # ---------------------------------------
        params = [
            end_date, start_date,   # CTE filters
            *tins,                  # TIN list
            end_date, start_date    # Final end_date/start_date filters
        ]

        if use_npi_filter:
            params += npi_list

        logging.info("Executing SQL...")
        logging.info("SQL:\n%s", query)
        logging.info("Params: %s", params)

        # ---------------------------------------
        # DATABASE EXECUTION
        # ---------------------------------------
        with teradatasql.connect(
            host=TERADATA_HOST,
            user=TERADATA_USER,
            password=TERADATA_PASSWORD
        ) as con:

            logging.info("Connected to Teradata.")
            cur = con.cursor()
            cur.execute(query, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=cols)

        logging.info(f"Rows returned: {len(df)}")

        if df.empty:
            logging.warning("No matching records found.")
            return "No records found.", 404

        # ---------------------------------------
        # EXCEL EXPORT
        # ---------------------------------------
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Report")
        output.seek(0)

        filename = f"tin_report_{end_date}.xlsx"
        logging.info(f"Excel ready: {filename}")

        return send_file(
            output,
            download_name=filename,
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        logging.exception("Unexpected Error")
        return f"Error: {str(e)}", 500


if __name__ == "__main__":
    app.run(debug=True)
