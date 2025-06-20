from flask import Flask, jsonify, request
import pandas as pd
import os


# Define your global DataFrame
traffic_df = None

app = Flask(__name__)

def load_traffic_data():
    global traffic_df
    print("Loading Austin Traffic Data...")
    traffic_df = pd.read_csv("atxtraffic.csv")
    print(f"Loaded {len(traffic_df)} rows into memory.")

@app.route("/")
def index():
    global traffic_df
    sample = traffic_df.head(10).to_dict(orient="records")
    return jsonify(sample)

@app.route("/head")
def top():
    global traffic_df
    num = int(request.args.get('count'))
    sample = traffic_df.head(num).to_dict(orient='records')
    return jsonify(sample)

@app.route("/shape")
def get_shape():
    global traffic_df
    if traffic_df is not None:
        rows, cols = traffic_df.shape
        return jsonify({
            "rows": rows,
            "columns": cols
        })
    else:
        return jsonify({"error": "Data not loaded"}), 500

@app.route("/columns")
def get_columns():
    global traffic_df
    if traffic_df is not None:
        return jsonify({
            "columns": traffic_df.columns.tolist()
        })
    else:
        return jsonify({"error": "Data not loaded"}), 500

@app.route("/info")
def get_info():
    global traffic_df
    if traffic_df is not None:
        info = {
            "rows": traffic_df.shape[0],
            "columns": traffic_df.shape[1],
            "column_details": []
        }
        for col in traffic_df.columns:
            info["column_details"].append({
                "name": col,
                "dtype": str(traffic_df[col].dtype),
                "nulls": int(traffic_df[col].isnull().sum())
            })
        return jsonify(info)
    else:
        return jsonify({"error": "Data not loaded"}), 500

@app.route("/describe")
def get_describe():
    global traffic_df
    if traffic_df is not None:
        description = traffic_df.describe(include='all').to_dict()
        return jsonify(description)
    else:
        return jsonify({"error": "Data not loaded"}), 500

@app.route("/UniqueValues")
def get_unique_values():
    global traffic_df
    column = request.args.get('ColumnName')

    if traffic_df is None:
        return jsonify({"error": "Data not loaded"}), 500

    if column not in traffic_df.columns:
        return jsonify({"error": f"Column '{column}' not found in dataset"}), 400

    unique_values = traffic_df[column].dropna().unique().tolist()
    unique_count = len(unique_values)

    return jsonify({
        "column": column,
        "unique_values": unique_values,
        "unique_count": unique_count
    })

@app.route("/FilterByYear")
def filter_by_year():
    global traffic_df
    column_name = request.args.get("ColumnName")
    column_value = request.args.get("ColumnValue")
    year = request.args.get("Year")

    if traffic_df is None:
        return jsonify({"error": "Data not loaded"}), 500

    if column_name not in traffic_df.columns:
        return jsonify({"error": f"Column '{column_name}' not found"}), 400

    if year is None or not year.isdigit():
        return jsonify({"error": "Invalid or missing 'Year' parameter"}), 400

    # Try to identify the datetime column
    date_column = None
    for col in traffic_df.columns:
        if pd.api.types.is_datetime64_any_dtype(traffic_df[col]):
            date_column = col
            break
    if date_column is None:
        # Try to parse a column if none are datetime yet
        for col in traffic_df.columns:
            try:
                traffic_df[col] = pd.to_datetime(traffic_df[col])
                date_column = col
                break
            except Exception:
                continue
    if date_column is None:
        return jsonify({"error": "No datetime column found"}), 400

    # Filter data
    filtered = traffic_df[
        (traffic_df[column_name] == column_value) &
        (traffic_df[date_column].dt.year == int(year))
    ]

    return jsonify({
        "column": column_name,
        "value": column_value,
        "year": int(year),
        "match_count": len(filtered),
        "matches": filtered.to_dict(orient="records")
    })


@app.route("/FilterByHourRange")
def filter_by_hour_range():
    global traffic_df

    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return jsonify({"error": "Missing 'start' or 'end' query parameter"}), 400

    try:
        start = int(start)
        end = int(end)
    except ValueError:
        return jsonify({"error": "Start and end must be integers"}), 400

    if "Published Date" not in traffic_df.columns:
        return jsonify({"error": "'Published Date' column not found"}), 500

    # Ensure 'Published Date' is datetime
    if not pd.api.types.is_datetime64_any_dtype(traffic_df["Published Date"]):
        try:
            traffic_df["Published Date"] = pd.to_datetime(traffic_df["Published Date"])
        except Exception as e:
            return jsonify({"error": f"Failed to parse 'Published Date': {str(e)}"}), 500

    filtered = traffic_df[
        traffic_df["Published Date"].dt.hour.between(start, end)
    ]

    results = filtered.head(100).copy()
    results = results.fillna("")
    results["Published Date"] = results["Published Date"].astype(str)

    return jsonify({
        "start_hour": start,
        "end_hour": end,
        "match_count": len(filtered),
        "matches": results.to_dict(orient="records")  # limit to 100 for safety
    })

@app.route("/Nearby")
def filter_by_proximity():
    global traffic_df

    lat = request.args.get("lat")
    lon = request.args.get("lon")

    if not lat or not lon:
        return jsonify({"error": "Missing 'lat' or 'lon' query parameters"}), 400

    try:
        lat = float(lat)
        lon = float(lon)
    except ValueError:
        return jsonify({"error": "Latitude and Longitude must be numeric"}), 400

    if "Latitude" not in traffic_df.columns or "Longitude" not in traffic_df.columns:
        return jsonify({"error": "Dataset must contain 'Latitude' and 'Longitude' columns"}), 500

    if "Published Date" not in traffic_df.columns:
        return jsonify({"error": "'Published Date' column not found"}), 500

    # Ensure 'Published Date' is datetime
    if not pd.api.types.is_datetime64_any_dtype(traffic_df["Published Date"]):
        try:
            traffic_df["Published Date"] = pd.to_datetime(traffic_df["Published Date"])
        except Exception as e:
            return jsonify({"error": f"Failed to parse 'Published Date': {str(e)}"}), 500

    # Haversine distance calculation
    def haversine_distance(row):
        try:
            lat2 = float(row["Latitude"])
            lon2 = float(row["Longitude"])
            if pd.isna(lat2) or pd.isna(lon2):
                return float("inf")  # Skip rows with missing data
        except Exception:
            return float("inf")

        # Haversine formula
        from math import radians, cos, sin, asin, sqrt
        lon1, lat1, lon2, lat2 = map(radians, [lon, lat, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        return 6371 * c  # km

    # Apply haversine safely
    df = traffic_df.copy()
    df["DistanceKM"] = df.apply(haversine_distance, axis=1)

    nearby = df[df["DistanceKM"] <= 1.0]

    results = nearby.head(100).copy()
    results = results.fillna("")
    results["Published Date"] = results["Published Date"].astype(str)

    return jsonify({
        "lat": lat,
        "lon": lon,
        "radius_km": 1.0,
        "match_count": len(nearby),
        "matches": results.drop(columns=["DistanceKM"]).to_dict(orient="records")
        })

def load_traffic_data():
    global traffic_df
    print("Loading Austin Traffic Data...")
    traffic_df = pd.read_csv("atxtraffic.csv")

    # Clean data
    traffic_df = traffic_df.dropna(subset=["Latitude", "Longitude", "Published Date"])
    traffic_df["Published Date"] = pd.to_datetime(traffic_df["Published Date"],
                                                  errors="coerce")
    traffic_df = traffic_df.dropna(subset=["Published Date"])
    print(f"Cleaned and loaded {len(traffic_df)} rows into memory.")

if __name__ == "__main__":
    load_traffic_data()  # <- This runs BEFORE the server starts
    app.run(debug=True, host="0.0.0.0", port=8067)
