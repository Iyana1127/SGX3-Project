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


if __name__ == "__main__":
    load_traffic_data()  # <- This runs BEFORE the server starts
    app.run(debug=True, host="0.0.0.0", port=8067)
