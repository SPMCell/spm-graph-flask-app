import os
import time
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for, flash, send_from_directory
from docx import Document
from werkzeug.utils import secure_filename

# Import configuration values
from config import (
    TEMPLATE_FILE,
    REPORT_FILE,
    DATA_FILE,
    STATIONS_FILE,
    CHART_FILE,
    LOCOPILOTS_FILE
)

# Import functions from automation_module
from automation_module import (
    load_station_data,
    process_train_data,
    extract_time_only,
    get_best_pilot_match,
    get_valid_date,
    get_valid_time,
    get_route_segment,
    get_stop_events,
    filter_close_stop_events,
    get_station_name_for_stop,
    adjust_absolute_km,
    load_and_prepare_data,
    lookup_stop_name,
    generate_excel_chart,
    assign_stop_labels
)

app = Flask(__name__)
app.secret_key = "your_secret_key_here"  # Change this for production
UPLOAD_FOLDER = "data"
ALLOWED_EXTENSIONS = {"xlsx"}  # Only allow .xlsx

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Check file extension
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        if "file" not in request.files:
            flash("No file uploaded.", "error")
            return redirect(url_for("index"))

        file = request.files["file"]

        if file.filename == "":
            flash("No selected file.", "error")
            return redirect(url_for("index"))

        if not (file and allowed_file(file.filename)):
            flash("Invalid file format. Please upload only .xlsx files.", "error")
            return redirect(url_for("index"))

        # Save uploaded file
        filename = secure_filename(file.filename)
        timestamp = int(time.time())
        new_filename = f"{timestamp}_{filename}"
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], new_filename)
        file.save(file_path)
        # Convert .xls to .xlsx if necessary.
        ext = os.path.splitext(filename)[1].lower()
        if ext == ".xls":
            try:
                # Read the .xls file using xlrd.
                df = pd.read_excel(file_path, engine="xlrd")
                # Create new filename with .xlsx extension including the timestamp.
                new_filename = f"{timestamp}_{os.path.splitext(filename)[0]}.xlsx"
                new_file_path = os.path.join(app.config["UPLOAD_FOLDER"], new_filename)
                # Write to .xlsx using openpyxl.
                df.to_excel(new_file_path, index=False, engine="openpyxl")
                # Optionally remove the original .xls file.
                os.remove(file_path)
                # Update file_path to point to the new .xlsx file.
                file_path = new_file_path
                flash(f"File converted to .xlsx format: {new_filename}", "info")
            except Exception as e:
                flash(f"Error converting file: {str(e)}", "error")
                return redirect(url_for("index"))
        else:
            flash(f"File {filename} uploaded successfully.", "success")

        # Retrieve form inputs
        raw_from_date = request.form.get("from_date", "").strip()
        raw_to_date = request.form.get("to_date", "").strip()
        raw_from_time = request.form.get("from_time", "").strip()
        raw_to_time = request.form.get("to_time", "").strip()

        try:
            from_date = get_valid_date(raw_from_date)
            to_date = get_valid_date(raw_to_date)
            from_time = get_valid_time(raw_from_time)
            to_time = get_valid_time(raw_to_time)
        except Exception as e:
            flash(f"Error in date/time inputs: {e}", "error")
            return redirect(url_for("index"))

        # Other inputs
        train_no = request.form.get("train_no", "").strip()
        loco_no = request.form.get("loco_no", "").strip()
        from_station = request.form.get("from_station", "").strip()
        to_station = request.form.get("to_station", "").strip()
        loco_pilot_name = request.form.get("loco_pilot_name", "").strip()
        crew_id = request.form.get("crew_id", "").strip()
        nli = request.form.get("nli", "").strip()

        start_datetime = f"{from_date} {from_time}"
        end_datetime = f"{to_date} {to_time}"

        user_data = {
            "Train No": train_no,
            "Loco No": loco_no,
            "From": from_station,
            "To": to_station,
            "Loco Pilot Name": loco_pilot_name
        }

        # Loco Pilot match
        pilot_match = get_best_pilot_match(loco_pilot_name)
        if pilot_match:
            user_data["Loco Pilot Name"] = pilot_match["name"].upper()
            user_data["Crew ID"] = pilot_match["crew_id"].upper()
            user_data["NLI"] = pilot_match["nli"].upper()
        else:
            user_data["Crew ID"] = crew_id.upper() if crew_id else ""
            user_data["NLI"] = nli.upper() if nli else ""
            flash("No close pilot match found. Using provided Crew ID and NLI.", "warning")

        section = f"{from_station.upper()}-{to_station.upper()}"
        lp_name = user_data["Loco Pilot Name"]

        station_data = load_station_data(STATIONS_FILE)
        starting_station = from_station.upper()
        ending_station = to_station.upper()

        matching_routes = []
        for route_name, route_info in station_data.get("routes", {}).items():
            route_stations = route_info.get("stations", {})
            if starting_station in route_stations and ending_station in route_stations:
                matching_routes.append(route_name)

        if not matching_routes:
            flash(f"No route found between {starting_station} and {ending_station}.", "error")
            return redirect(url_for("index"))
        if len(matching_routes) > 1:
            flash("Multiple routes found. Using the first one.", "info")

        selected_route = matching_routes[0]
        route_stations = station_data["routes"][selected_route]["stations"]

        if starting_station not in route_stations:
            flash(f"Starting station {starting_station} not found in selected route.", "error")
            return redirect(url_for("index"))

        segment_stations = get_route_segment(route_stations, starting_station, ending_station)
        if not segment_stations:
            flash("Could not determine route segment.", "error")
            return redirect(url_for("index"))

        base_km = float(segment_stations[starting_station]["km"])

        df_train = load_and_prepare_data(file_path, start_datetime=start_datetime, end_datetime=end_datetime)
        if df_train.empty:
            flash("No data in the specified datetime range.", "error")
            return redirect(url_for("index"))

        os.makedirs("output", exist_ok=True)
        trimmed_data_path = os.path.join("output", "trimmed_data.xlsx")
        df_train.to_excel(trimmed_data_path, index=False)
        flash(f"Trimmed data saved to {trimmed_data_path}", "info")

        df_train = process_train_data(df_train, base_km, segment_stations)
        df_train = adjust_absolute_km(df_train, segment_stations, tolerance=2.6, offset_threshold=0.3)

        stop_events = get_stop_events(df_train, speed_threshold=0.1)
        filtered_stop_events = filter_close_stop_events(stop_events, merge_distance=1.5)

        for idx, row in filtered_stop_events.iterrows():
            detected_km = row["absolute_km"]
            stop_name = get_station_name_for_stop(segment_stations, detected_km, tolerance=1.5)
            print(f"Detected stop: {stop_name or 'N/A'} at km {detected_km}, time: {row.get('Time hh:mn:ss', 'N/A')}")

        generate_excel_chart(df_train, CHART_FILE, train_no, section, lp_name, segment_stations)

        doc = Document(TEMPLATE_FILE)
        for p in doc.paragraphs:
            if "{TRAIN_NO}" in p.text:
                p.text = p.text.replace("{TRAIN_NO}", train_no)
        doc.save(REPORT_FILE)

        flash(f"Automation complete! Report saved as {REPORT_FILE}", "success")
        return render_template("result.html", report_link=url_for("download_report"))

    return render_template("index.html")

@app.route("/download")
@app.route("/download_report")
def download_report():
    return send_from_directory(directory="output",
                               path=os.path.basename(REPORT_FILE),
                               as_attachment=True)

@app.route("/download_chart")
def download_chart():
    return send_from_directory(directory="output",
                               path=os.path.basename(CHART_FILE),
                               as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
