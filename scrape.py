from flask import Flask, jsonify, request, send_file
import requests
from datetime import datetime, timedelta
import os
import json
from dotenv import load_dotenv
import zipfile
import io

load_dotenv()

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# API URLs
WAISL_URL = "https://hyd-apoc.waisl.in/digitaltwin/ui-api/queue/distribution"
FLIGHT_DETAILS_URL = "https://hyd-apoc.waisl.in/digitaltwin/ui-api/airport/aodb/flightDetails"
CUSS_DISTRIBUTION_URL = "https://hyd-apoc.waisl.in/digitaltwin/ui-api/airport/cuss/distribution"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6",
    "content-type": "application/json",
    "origin": "https://hyd-apoc.waisl.in",
    "priority": "u=1, i",
    "referer": "https://hyd-apoc.waisl.in/digitaltwin/en/main-dashboard/departure/639f655bb7bb6a75c0a3f645",
    "sec-ch-ua": "\"Google Chrome\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "x-csrf-token": "RDv-nhxJp2jyD4tlFNQ5pxQujaX01wydCYTbqf9hSYcMlmh6cAPIrCR4k1zfOrxWIvkNkXcZoMfCtGiwOrS9y55SfbY7rlFP"
}
 
COOKIES = {
    "_ga": "GA1.1.1507819002.1741868206",
    "cf_clearance": "06JcE__FUJhD1KMQ3lM1IezhQFAR5pDEqrDqv_5b25I-1742483715-1.2.1.1-8VmTydhcf14U9lXGS5KHpsFG.ErV1Ndo_XMWvmO6e3juT2wuyJ4UvMhx1XM8JCgfwdsKcj0oY_J6iXH.7ssuyk9iQQz0vJVPjreItLuIvCJ65QhP4AL7MPlewiK2a1Yy7nEk6Nffa9DAZCK9sFizWCn85vjRk9lCtW_wXQLwABstJ1vLcAXStZlIQclQB0VqbdBRJn58ovFnI2M1zMYdppSXKOZHG5Pe1hrXh8CixlemSrAMk0XaDOY7WRGhzaumQDvjHLNkuO7_s70z3xYRaYQEKwHNngRnYlzjXvkHCI2h8NuoQnpvlr6lWbrRdq6TckMQBd3UGELPNUKgZZEquQOY74czk6D2ZOHJl30TNN8",
    "_ga_LDNDCC4ENR": "GS1.1.1742487560.4.0.1742487560.60.0.0",
    "JSESSIONID": "M2VlYTBjODItOTdjMi00ZWIwLWExMDgtMzAzNzc4Mjg5ZjJm",
    "CSRF-TOKEN": "RDv-nhxJp2jyD4tlFNQ5pxQujaX01wydCYTbqf9hSYcMlmh6cAPIrCR4k1zfOrxWIvkNkXcZoMfCtGiwOrS9y55SfbY7rlFP"
}

DOWNLOAD_FOLDER = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

@app.route('/waisl-flights', methods=['GET'])
def get_flight_details():
    """
    Fetch flight details for a given date.
    """
    try:
        date_str = request.args.get("date")
        save_to_file = request.args.get("save", "false").lower() == "true"

        if not date_str:
            return jsonify({"error": "Missing 'date' parameter. Use ?date=YYYY-MM-DD"}), 400

        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        start = int(date_obj.timestamp() * 1000)
        end = int((date_obj + timedelta(days=1)).timestamp() * 1000 - 1)

        payload = {
            "aircraftTypes": None,
            "airlineCodes": None,
            "boardingGates": None,
            "destinationCodes": None,
            "detailsType": "airline",
            "distributionType": "daily",
            "endTime": end,
            "gateType": None,
            "movementIndicator": None,
            "natureTypes": None,
            "operationalStatuses": ["OPERATIONAL"],
            "originCodes": None,
            "regionType": None,
            "runWays": None,
            "startTime": start,
            "trafficType": None
        }

        # Make API request
        response = requests.post(
            FLIGHT_DETAILS_URL,
            headers=HEADERS,
            cookies=COOKIES,
            json=payload,
            verify=False,
            timeout=15
        )
        
        # Print response details for debugging
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {response.headers}")
        print(f"Response Text: {response.text}")
        
        response.raise_for_status()
        data = response.json()

        # Save to file if requested
        if save_to_file:
            file_path = os.path.join(DOWNLOAD_FOLDER, f"flightDetails_{date_str}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            return jsonify({
                "status": "saved",
                "file_url": f"/download-flight-json/{date_str}"
            })

        return jsonify(data)

    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": f"Request failed: {str(e)}",
            "status_code": getattr(e.response, 'status_code', None),
            "response_text": getattr(e.response, 'text', None)
        }), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/waisl-distribution', methods=['GET', 'POST'])
def get_waisl_distribution():
    """
    Fetch CUSS distribution data.
    """
    try:
        date_str = request.args.get("date")

        if not date_str:
            return jsonify({"error": "Missing 'date' parameter. Use ?date=YYYY-MM-DD"}), 400

        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        start = int(date_obj.timestamp() * 1000)
        end = int((date_obj + timedelta(days=1)).timestamp() * 1000 - 1)
        
        payload = {
            "startTime": start,
            "endTime": end,
            "distributionType": "daily",
            "groupBy": None
        }

        response = requests.post(
            CUSS_DISTRIBUTION_URL,
            headers=HEADERS,
            cookies=COOKIES,
            json=payload,
            verify=False,
            timeout=15
        )

        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {response.headers}")
        print(f"Response Text: {response.text}")

        response.raise_for_status()  
        data = response.json()

        return jsonify(data)

    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    except requests.exceptions.RequestException as e:
        status_code = getattr(e.response, 'status_code', 500)
        response_text = getattr(e.response, 'text', 'No response text')
        return jsonify({"error": f"Request failed: {str(e)}, Status Code: {status_code}, Response Text: {response_text}"}), status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download-flight-json/<date_str>', methods=['GET'])
def download_flight_json(date_str):
    """
    Endpoint to download saved flight details JSON files.
    """
    file_path = os.path.join(DOWNLOAD_FOLDER, f"flightDetails_{date_str}.json")
    
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    
    return jsonify({"error": f"No file found for {date_str}"}), 404

@app.route('/waisl-batch-scrape', methods=['GET'])
def batch_scrape():
    """
    Scrape data for a date range and return a zip file containing all JSON files.
    Also saves individual JSON files to the downloads folder.
    """
    try:
        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")

        if not start_date_str or not end_date_str:
            return jsonify({"error": "Missing 'start_date' or 'end_date' parameter. Use ?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD"}), 400

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        # Track processed files for response
        processed_files = []
        
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                try:
                    # Get and save flight data
                    flight_data = get_flight_data_for_date(current_date)
                    flight_filename = f"flightDetails_{date_str}.json"
                    
                    # Save to zip
                    zf.writestr(flight_filename, json.dumps(flight_data, indent=2))
                    
                    # Save to downloads folder
                    flight_file_path = os.path.join(DOWNLOAD_FOLDER, flight_filename)
                    with open(flight_file_path, 'w', encoding='utf-8') as f:
                        json.dump(flight_data, f, indent=2)
                    processed_files.append(flight_filename)

                    # Get and save distribution data
                    distribution_data = get_distribution_data_for_date(current_date)
                    distribution_filename = f"distribution_{date_str}.json"
                    
                    # Save to zip
                    zf.writestr(distribution_filename, json.dumps(distribution_data, indent=2))
                    
                    # Save to downloads folder
                    distribution_file_path = os.path.join(DOWNLOAD_FOLDER, distribution_filename)
                    with open(distribution_file_path, 'w', encoding='utf-8') as f:
                        json.dump(distribution_data, f, indent=2)
                    processed_files.append(distribution_filename)

                except Exception as e:
                    print(f"Error processing date {date_str}: {str(e)}")
                
                current_date += timedelta(days=1)

        memory_file.seek(0)
        
        # Return both the zip file and information about saved files
        response = send_file(
            memory_file,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'waisl_data_{start_date_str}_to_{end_date_str}.zip'
        )
        
        # Add custom headers to show processing results
        response.headers['X-Processed-Files'] = ','.join(processed_files)
        return response

    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_flight_data_for_date(date_obj):
    """Helper function to get flight data for a specific date"""
    start = int(date_obj.timestamp() * 1000)
    end = int((date_obj + timedelta(days=1)).timestamp() * 1000 - 1)

    payload = {
        "aircraftTypes": None,
        "airlineCodes": None,
        "boardingGates": None,
        "destinationCodes": None,
        "detailsType": "airline",
        "distributionType": "daily",
        "endTime": end,
        "gateType": None,
        "movementIndicator": None,
        "natureTypes": None,
        "operationalStatuses": ["OPERATIONAL"],
        "originCodes": None,
        "regionType": None,
        "runWays": None,
        "startTime": start,
        "trafficType": None
    }

    response = requests.post(
        FLIGHT_DETAILS_URL,
        headers=HEADERS,
        cookies=COOKIES,
        json=payload,
        verify=False,
        timeout=15
    )
    response.raise_for_status()
    return response.json()

def get_distribution_data_for_date(date_obj):
    """Helper function to get distribution data for a specific date"""
    start = int(date_obj.timestamp() * 1000)
    end = int((date_obj + timedelta(days=1)).timestamp() * 1000 - 1)

    payload = {
        "startTime": start,
        "endTime": end,
        "distributionType": "daily",
        "groupBy": None
    }

    response = requests.post(
        CUSS_DISTRIBUTION_URL,
        headers=HEADERS,
        cookies=COOKIES,
        json=payload,
        verify=False,
        timeout=15
    )
    response.raise_for_status()
    return response.json()

@app.route('/available-dates', methods=['GET'])
def get_available_dates():
    """
    Get a list of dates for which we have stored data.
    """
    try:
        flight_files = [f for f in os.listdir(DOWNLOAD_FOLDER) if f.startswith('flightDetails_') and f.endswith('.json')]
        dates = [f.replace('flightDetails_', '').replace('.json', '') for f in flight_files]
        dates.sort()
        return jsonify({
            "dates": dates,
            "count": len(dates)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stored-flight-details/<date_str>', methods=['GET'])
def get_stored_flight_details(date_str):
    """
    Get flight details from stored JSON file for a specific date.
    """
    try:
        file_path = os.path.join(DOWNLOAD_FOLDER, f"flightDetails_{date_str}.json")
        if not os.path.exists(file_path):
            return jsonify({"error": f"No flight data found for date {date_str}"}), 404
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stored-distribution/<date_str>', methods=['GET'])
def get_stored_distribution(date_str):
    """
    Get distribution data from stored JSON file for a specific date.
    """
    try:
        file_path = os.path.join(DOWNLOAD_FOLDER, f"distribution_{date_str}.json")
        if not os.path.exists(file_path):
            return jsonify({"error": f"No distribution data found for date {date_str}"}), 404
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    debug = os.getenv("DEBUG", "True").lower() == "true"
    app.run(debug=debug, port=port)
