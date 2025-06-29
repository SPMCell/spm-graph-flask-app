from automation_module import get_valid_date, get_valid_time, get_best_pilot_match, load_station_data, \
    process_train_data, load_and_prepare_data, generate_excel_chart
from config import TEMPLATE_FILE, REPORT_FILE
from docx import Document
from automation_module import get_stop_events, get_station_name_for_stop, adjust_absolute_km, filter_close_stop_events, get_route_segment
import os


def main():
    # Load the Word template.
    doc = Document(TEMPLATE_FILE)

    from_date = get_valid_date("Enter From Date (DD-MM-YYYY): ")
    to_date = get_valid_date("Enter To Date (DD-MM-YYYY): ")

    # Get validated time inputs.
    from_time = get_valid_time("Enter From Time (HH:MM or HH:MM:SS): ")
    to_time = get_valid_time("Enter To Time (HH:MM or HH:MM:SS): ")

    # Collect user inputs for the extra form fields.
    fields = ["Train No", "Loco No", "From", "To", "Loco Pilot Name"]
    user_data = {field: input(f"{field}: ").strip() for field in fields}

    # Add the validated date and time values to user_data
    user_data["From Date(DD-MM-YYYY)"] = from_date
    user_data["To Date(DD-MM-YYYY)"] = to_date
    user_data["From Time(HH:MM:SS)"] = from_time
    user_data["To Time(HH:MM:SS)"] = to_time

    # Combine date and time for filtering.
    # (User should enter date as DD-MM-YYYY and time as HH:MM:SS)
    start_datetime = from_date + " " + from_time
    end_datetime = to_date + " " + to_time

    # Loco Pilot matching.
    pilot_match = get_best_pilot_match(user_data["Loco Pilot Name"])
    if pilot_match:
        user_data["Loco Pilot Name"] = pilot_match["name"].upper()
        user_data["Crew ID"] = pilot_match["crew_id"].upper()
        user_data["NLI"] = pilot_match["nli"].upper()
        print("Pilot matched successfully.")
    else:
        print("No close pilot match found. Please update Crew ID and NLI manually if needed.")
        user_data["Crew ID"] = input("Crew ID: ").strip().upper()
        user_data["NLI"] = input("NLI: ").strip().upper()

    train_no = user_data["Train No"]
    section = f'{user_data["From"].upper()}-{user_data["To"].upper()}'
    lp_name = f'{user_data["Loco Pilot Name"]}'

    # Load station data.
    station_data = load_station_data("stations.json")
    starting_station = user_data["From"].upper()
    ending_station = user_data["To"].upper()
    #station_info = station_data[starting_station]
    #base_km = float(station_info["km"])

    # ---------------------------------------------------------------------
    available_routes = station_data.get("routes", {})
    matching_routes = []  # list to hold all routes that include both starting_station and ending_station

    for route_name, route_info in available_routes.items():
        route_stations = route_info.get("stations", {})
        if starting_station in route_stations and ending_station in route_stations:
            matching_routes.append(route_name)

    if not matching_routes:
        print(f"Error: No route found that contains both {starting_station} and {ending_station}.")
        return

    # If more than one route is available, ask the user to choose.
    if len(matching_routes) > 1:
        print("Multiple routes found for your journey:")
        for idx, route in enumerate(matching_routes):
            print(f"{idx + 1}. {route}")
        chosen = input("Please choose the route number: ").strip()
        try:
            chosen_idx = int(chosen) - 1
            selected_route = matching_routes[chosen_idx]
        except (ValueError, IndexError):
            print("Invalid selection. Exiting.")
            return
    else:
        selected_route = matching_routes[0]

    print(f"Selected Route: {selected_route}")
    # Extract the stations for this route.
    route_stations = available_routes[selected_route]["stations"]

    # Use the starting station's official km value as the base.
    if starting_station not in route_stations:
        print(f"Error: Starting station {starting_station} is not found in route {selected_route}.")
        return

    # now extract only the segment between starting_station and ending_station:
    segment_stations = get_route_segment(route_stations, starting_station, ending_station)
    print("Segment Stations:", segment_stations)

    if not segment_stations:
        print("Error: Could not determine the route segment from the given stations.")
        return

    base_km = float(segment_stations[starting_station]["km"])

    # ---------------------------------------------------------------------#
    # Load and Filter the data using the data_loader function.
    # Note: load_and_prepare_data is designed to filter the data based on
    # the user-defined datetime range.
    # ---------------------------------------------------------------------
    df_train = load_and_prepare_data("data/your_data.xlsx",
                                     start_datetime=start_datetime,
                                     end_datetime=end_datetime)

    # Check if any data remains after filtering.
    if df_train.empty:
        print("No data available in the specified datetime range. Please re-check your inputs.")
        return  # or exit the script

    # Save the trimmed (filtered) DataFrame for inspection.
    os.makedirs("output", exist_ok=True)
    output_trimmed_excel_path = "output/trimmed_data.xlsx"
    df_train.to_excel(output_trimmed_excel_path, index=False)
    print(f"Trimmed data saved to {output_trimmed_excel_path}")

    # ---------------------------------------------------------------------
    # Now, process the data further.
    # ---------------------------------------------------------------------
    df_train = process_train_data(df_train, base_km, segment_stations)

    df_train = adjust_absolute_km(df_train, segment_stations, tolerance=2.6, offset_threshold=0.3)

    stop_events = get_stop_events(df_train, speed_threshold=0.1)

    filtered_stop_events = filter_close_stop_events(stop_events, merge_distance=1.5)

    print("Detected stops with associated station names:")
    for idx, row in filtered_stop_events.iterrows():
        detected_km = row["absolute_km"]
        station = get_station_name_for_stop(segment_stations, detected_km, tolerance=1.5)
        if station:
            print(f"Station: {station} | Time: {row['Time hh:mn:ss']} | Position: {detected_km}")
        else:
            print(f"No station match found for stop at km {detected_km} at time {row['Time hh:mn:ss']}")

    # Generate an Excel chart.
    output_excel_path = "output/excel_chart.xlsx"
    generate_excel_chart(df_train, output_excel_path, train_no, section, lp_name, segment_stations)

    # Save the updated Word document.
    doc.save(REPORT_FILE)
    print(f"Automation complete! Updated report saved as {REPORT_FILE}")


if __name__ == "__main__":
    main()
