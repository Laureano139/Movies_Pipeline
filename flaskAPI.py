import os
import pandas as pd
import pyarrow.parquet as pq
from flask import Flask, jsonify, request

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'combinedData.parquet')

try:
    table = pq.read_table(PROCESSED_DATA_PATH)
    df_data = table.to_pandas()
    print("Data loaded successfully into the API.")
except Exception as e:
    print(f"Error loading Parquet data: {e}")
    df_data = pd.DataFrame()


@app.route('/')
def home():
    return "API is running! Visit /data to access the data!"

@app.route('/data', methods=['GET'])
def get_data():
    """
    Data endpoint.
    Supports pagination and content type filtering.
    Examples:
    - /data?limit=10&page=1
    - /data?content_type=movie
    """
    if df_data.empty:
        return jsonify({"error": "Data is not available!"}), 500

    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    offset = (page - 1) * limit

    content_type_filter = request.args.get('content_type')
    if content_type_filter:
        filtered_df = df_data[df_data['content_type'] == content_type_filter]
    else:
        filtered_df = df_data.copy()

    paginated_data = filtered_df.iloc[offset:offset + limit]

    response_data = paginated_data.to_dict(orient='records')

    serializable_data = []
    for record in response_data:
        serializable_record = {}
        for key, value in record.items():
            if isinstance(value, (int, float, bool, str, list, type(None))):
                serializable_record[key] = value
            else:
                try:
                    serializable_record[key] = value.item()
                except (AttributeError, ValueError):
                    serializable_record[key] = str(value)
        serializable_data.append(serializable_record)

    return jsonify({
        "total_results": len(filtered_df),
        "page": page,
        "total_pages": (len(filtered_df) + limit - 1) // limit,
        "results": serializable_data
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)