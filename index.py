from flask import Flask, request, jsonify, render_template_string, Response, render_template
from bs4 import BeautifulSoup
import requests
import json
import re
import time
from datetime import datetime
import logging
import imdb
from kidsinmind import KidsInMindScraper
import dove
import parentpreviews
import cringMDB
import commonsensemedia
import movieguide
from SQLiteCache import SqliteCache
from waitress import serve
import logging
from paste.translogger import TransLogger
import traceback
from collections import defaultdict
import sqlite3
import geoip2.database
import ipaddress
import atexit
import os
import threading
import calendar

# Create the Flask app instance
app = Flask(__name__)

# Now you can set the logger level
logger = logging.getLogger('waitress')
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)
app.config['DEBUG'] = True

# Initialize the SqliteCache
db = SqliteCache()

# Update the update_stats function
def update_stats(is_cached, sex_nudity_category, country):
    stats = db.get_all_stats()
    current_year = datetime.now().year
    current_month = datetime.now().strftime('%Y-%m')
    current_day = datetime.now().strftime('%Y-%m-%d')

    # Update total hits
    stats['total_hits'] = stats.get('total_hits', 0) + 1
    if is_cached:
        stats['cached_hits'] = stats.get('cached_hits', 0) + 1
    else:
        stats['fresh_hits'] = stats.get('fresh_hits', 0) + 1

    # Update This Year's Statistics
    hits_by_year = stats.get('hits_by_year', {})
    hits_by_year[str(current_year)] = hits_by_year.get(str(current_year), 0) + 1
    stats['hits_by_year'] = hits_by_year

    # Update This Month's Statistics
    hits_by_month = stats.get('hits_by_month', {})
    hits_by_month[current_month] = hits_by_month.get(current_month, 0) + 1
    stats['hits_by_month'] = hits_by_month

    # Update daily hits
    hits_by_day = stats.get('hits_by_day', {})
    hits_by_day[current_day] = hits_by_day.get(current_day, 0) + 1
    stats['hits_by_day'] = hits_by_day

    # Update Sex & Nudity Categories
    if sex_nudity_category:
        sex_nudity_categories = stats.get('sex_nudity_categories', {})
        sex_nudity_categories[sex_nudity_category] = sex_nudity_categories.get(sex_nudity_category, 0) + 1
        stats['sex_nudity_categories'] = sex_nudity_categories

    # Update Countries Using the API
    if country:
        countries = stats.get('countries', {})
        countries[country] = countries.get(country, 0) + 1
        stats['countries'] = countries

    # Save all stats
    for key, value in stats.items():
        db.set_stat(key, value)

# Initialize the GeoIP reader
geoip_reader = geoip2.database.Reader('GeoLite2-Country.mmdb')

def get_country_from_ip(ip):
    try:
        # Check if the IP is a private address
        if ipaddress.ip_address(ip).is_private:
            return "Private IP"
        
        # Look up the IP
        response = geoip_reader.country(ip)
        return response.country.name or "Unknown"
    except geoip2.errors.AddressNotFoundError:
        return "Unknown"
    except ValueError:
        return "Invalid IP"

# Don't forget to close the reader when your application exits
# You can do this in your main block or use atexit to ensure it's called
atexit.register(geoip_reader.close)

# Add this function to get movie/TV show name from OMDB API
def get_title_from_omdb(imdb_id):
    omdb_api_key = os.environ.get('OMDB_API_KEY')  # Get the API key from environment variable
    if not omdb_api_key:
        app.logger.error("OMDB API key not found in environment variables")
        return None

    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={omdb_api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        
        if data.get('Response') == 'True':
            return data.get('Title')
        else:
            app.logger.warning(f"No title found for IMDb ID: {imdb_id}")
            return None
    except requests.RequestException as e:
        app.logger.error(f"Error fetching data from OMDB: {str(e)}")
        return None

@app.route('/get_data', methods=['GET'])
def get_data():
    try:
        app.logger.info("Received request for /get_data")
        starttime = time.time()

        # Get parameters from the query string
        imdb_id = request.args.get('imdb_id')
        video_name = request.args.get('video_name', '').replace("+"," ").replace("%20"," ").replace(":","").replace("%3A", "")
        release_year = request.args.get('release_year')
        provider = request.args.get('provider', '').lower()

        app.logger.info(f"Request parameters: imdb_id={imdb_id}, video_name={video_name}, release_year={release_year}, provider={provider}")

        if not provider:
            return jsonify({"error": "Provider parameter is required"}), 400

        if imdb_id:
            key = f"{imdb_id}_{provider}"
        else:
            key = f"{video_name.replace(':','').replace('-','_').replace(' ','_').lower()}_{provider}"
        
        app.logger.info(f"Cache key: {key}")

        result = db.get(key)
        is_cached = result is not None
        
        if is_cached:
            app.logger.info(f"Cached result structure: {json.dumps(result, indent=2)}")
            app.logger.info(f"Returning cached result for {result.get('title', 'Unknown title')} from {provider}")
            
            # Check if review-items exist and are not empty
            review_items = result.get('review-items')
            if not review_items:
                app.logger.warning(f"No review items found in cached result for {result.get('title', 'Unknown title')} from {provider}")
                sex_nudity_category = None
            else:
                sex_nudity_category = next((item.get('cat') for item in review_items if item.get('name') == 'Sex & Nudity'), None)
            
            # Get country from IP
            ip_address = request.remote_addr
            country = get_country_from_ip(ip_address)

            # When calling update_stats, include the country
            update_stats(is_cached, sex_nudity_category, country)
            result['is_cached'] = True
            return jsonify(result)
        
        # Get video name from OMDB if not provided
        if not video_name:
            video_name = get_title_from_omdb(imdb_id)
            if not video_name:
                return jsonify({"error": "Could not retrieve video name from OMDB"}), 400
        
        app.logger.info(f"Fetching fresh data for {video_name or imdb_id} from {provider}")
        
        # Provider-specific logic
        if "imdb" in provider:
            result = imdb.imdb_parentsguide(imdb_id, video_name)
        elif "kidsinmind" in provider:
            result = KidsInMindScraper(imdb_id, video_name)
        elif "dove" in provider:
            result = dove.DoveFoundationScrapper(video_name)
        elif "parentpreview" in provider:
            result = parentpreviews.ParentPreviewsScraper(imdb_id, video_name)
        elif "cring" in provider:
            result = cringMDB.cringMDBScraper(imdb_id, video_name)
        elif "commonsense" in provider:
            result = commonsensemedia.CommonSenseScrapper(imdb_id, video_name)
        elif "movieguide" in provider:
            result = movieguide.MovieGuideOrgScrapper(imdb_id, video_name)
        else:
            return jsonify({"error": f"Unknown provider: {provider}"}), 400

        if result:
            if not isinstance(result, dict):
                app.logger.error(f"Invalid result format for {video_name or imdb_id} from {provider}")
                return jsonify({"error": "Invalid result format"}), 500
            
            if 'title' not in result or 'provider' not in result:
                app.logger.error(f"Missing required keys in result for {video_name or imdb_id} from {provider}")
                return jsonify({"error": "Invalid result format"}), 500
            
            # Check if review-items exist and are not empty
            review_items = result.get('review-items')
            if not review_items:
                app.logger.warning(f"No review items found for {video_name or imdb_id} from {provider}")
                sex_nudity_category = None
            else:
                sex_nudity_category = next((item.get('cat') for item in review_items if item.get('name') == 'Sex & Nudity'), None)
            
            # Get country from IP
            ip_address = request.remote_addr
            country = get_country_from_ip(ip_address)

            # When calling update_stats, include the country
            update_stats(is_cached, sex_nudity_category, country)
            
            # Only store in cache if review-items are not null
            if review_items:
                app.logger.info(f"Storing result in cache for {result['title']} from {provider}")
                app.logger.info(f"Storing result in cache: {json.dumps(result, indent=2)}")
                db.set(key, result)
            else:
                app.logger.info(f"Not storing result in cache due to null review-items for {result['title']} from {provider}")
            
            result['is_cached'] = False
            return jsonify(result)
        else:
            app.logger.info(f"No data found for {video_name or imdb_id} from {provider}")
            return jsonify({"error": "No data found"}), 404

    except Exception as e:
        app.logger.error(f"Error in get_data: {str(e)}")
        app.logger.error(f"Error traceback: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500

# Add this function to check the API status
def is_api_running():
    # You can implement a more sophisticated check here if needed
    return True

# Modify the api_documentation function
@app.route('/', methods=['GET'])
def api_documentation():
    api_status = "green" if is_api_running() else "red"
    return render_template('documentation.html', api_status=api_status)

# Update the logging configuration
@app.before_first_request
def setup_logging():
    logging.basicConfig(level=logging.INFO)
    app.logger.setLevel(logging.INFO)

    class SQLiteHandler(logging.Handler):
        def emit(self, record):
            db.add_log(record.levelname, self.format(record))

    handler = SQLiteHandler()
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)

# Add a new route for logs
@app.route('/logs', methods=['GET'])
def show_logs():
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page
    logs = db.get_logs(limit=per_page, offset=offset)
    api_status = "green" if is_api_running() else "red"
    
    logs_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>API Logs</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{
                padding: 20px;
            }}
            .nav-bar {{
                background-color: #2c3e50;
                padding: 10px;
                margin-bottom: 20px;
            }}
            .nav-bar a {{
                color: white;
                text-decoration: none;
                padding: 5px 10px;
            }}
            .nav-bar a:hover {{
                background-color: #34495e;
            }}
            .status-indicator {{
                display: inline-block;
                width: 10px;
                height: 10px;
                border-radius: 50%;
                margin-left: 5px;
            }}
            .status-green {{
                background-color: #00ff00;
            }}
            .status-red {{
                background-color: #ff0000;
            }}
            .table {{
                font-size: 0.9rem;
            }}
            .pagination {{
                margin-top: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="nav-bar">
            <a href="/">Home</a>
            <a href="/stats">Statistics</a>
            <a href="/logs">Logs</a>
            <a href="/tryout">Tryout</a>
            <span id="api-status" style="color: white;">API Status: <span class="status-indicator status-{api_status}"></span></span>
        </div>
        <div class="container">
            <h1 class="mb-4">API Logs</h1>
            <table class="table table-striped table-hover">
                <thead>
                    <tr>
                        <th>Timestamp</th>
                        <th>Level</th>
                        <th>Message</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(f"<tr><td>{log[1]}</td><td><span class='badge bg-{get_log_level_color(log[2])}'>{log[2]}</span></td><td>{log[3]}</td></tr>" for log in logs)}
                </tbody>
            </table>
            <nav aria-label="Page navigation">
                <ul class="pagination justify-content-center">
                    <li class="page-item {'' if page > 1 else 'disabled'}">
                        <a class="page-link" href="{f'/logs?page={page-1}' if page > 1 else '#'}" tabindex="-1" {'aria-disabled="true"' if page == 1 else ''}>Previous</a>
                    </li>
                    <li class="page-item"><a class="page-link" href="#">Page {page}</a></li>
                    <li class="page-item">
                        <a class="page-link" href="/logs?page={page+1}">Next</a>
                    </li>
                </ul>
            </nav>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """
    return logs_html

def get_log_level_color(level):
    colors = {
        'DEBUG': 'secondary',
        'INFO': 'info',
        'WARNING': 'warning',
        'ERROR': 'danger',
        'CRITICAL': 'dark'
    }
    return colors.get(level, 'secondary')

# Update the show_stats function
@app.route('/stats', methods=['GET'])
def show_stats():
    api_status = "green" if is_api_running() else "red"
    current_year = datetime.now().year
    current_month = datetime.now().strftime('%Y-%m')
    
    # Get cached records count
    cached_records_count = db.get_cached_records_count()

    # Get all stats
    stats = db.get_all_stats()

    # Initialize default values if stats are missing
    total_hits = stats.get('total_hits', 0)
    cached_hits = stats.get('cached_hits', 0)
    fresh_hits = stats.get('fresh_hits', 0)

    # Prepare data for charts
    overall_data = {
        'labels': ['Total Hits', 'Cached Hits', 'Fresh Hits'],
        'data': [total_hits, cached_hits, fresh_hits]
    }
    
    # This Year's Statistics (per month)
    months_this_year = [f"{current_year}-{month:02d}" for month in range(1, 13)]
    hits_by_month = stats.get('hits_by_month', {})
    this_year_data = {
        'labels': months_this_year,
        'total': [hits_by_month.get(month, 0) for month in months_this_year],
    }
    
    # This Month's Statistics (per day)
    days_in_month = calendar.monthrange(current_year, int(current_month.split('-')[1]))[1]
    days_this_month = [f"{current_month}-{day:02d}" for day in range(1, days_in_month + 1)]
    hits_by_day = stats.get('hits_by_day', {})
    this_month_data = {
        'labels': days_this_month,
        'total': [hits_by_day.get(day, 0) for day in days_this_month],
    }
    
    stats_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>API Statistics Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
            }}
            h1, h2 {{
                color: #2c3e50;
            }}
            .stat-box {{
                background-color: #f0f0f0;
                border-radius: 5px;
                padding: 15px;
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            .chart-container {{
                width: 100%;
                height: 300px;
                margin-bottom: 20px;
            }}
            .nav-bar {{
                background-color: #2c3e50;
                padding: 10px;
                margin-bottom: 20px;
            }}
            .nav-bar a {{
                color: white;
                text-decoration: none;
                padding: 5px 10px;
            }}
            .api-status {{
                color: white;
            }}
            .nav-bar a:hover {{
                background-color: #34495e;
            }}
            .status-indicator {{
                display: inline-block;
                width: 10px;
                height: 10px;
                border-radius: 50%;
                margin-left: 5px;
            }}
            .status-green {{
                background-color: #00ff00;
            }}
            .status-red {{
                background-color: #ff0000;
            }}
        </style>
    </head>
    <body>
        <div class="nav-bar">
            <a href="/">Home</a>
            <a href="/stats">Statistics</a>
            <a href="/logs">Logs</a>
            <a href="/tryout">Tryout</a>
            <span id="api-status" style="color: white;">API Status: <span class="status-indicator status-{api_status}"></span></span>
        </div>
        <h1>API Statistics Dashboard</h1>
        
        <div class="stat-box">
            <h2>Overall Statistics</h2>
            <p>Total Hits: {total_hits}</p>
            <p>Cached Hits: {cached_hits}</p>
            <p>Fresh Hits: {fresh_hits}</p>
            <div class="chart-container">
                <canvas id="overallChart"></canvas>
            </div>
        </div>
        
        <div class="stat-box">
            <h2>This Year's Statistics ({current_year})</h2>
            <p>Total Hits: {sum(this_year_data['total'])}</p>
            <div class="chart-container">
                <canvas id="thisYearChart"></canvas>
            </div>
        </div>
        
        <div class="stat-box">
            <h2>This Month's Statistics ({current_month})</h2>
            <p>Total Hits: {sum(this_month_data['total'])}</p>
            <div class="chart-container">
                <canvas id="thisMonthChart"></canvas>
            </div>
        </div>
        
        <div class="stat-box">
            <h2>Cached Records</h2>
            <p>Total Cached Records: {cached_records_count}</p>
        </div>
        
        <div class="stat-box">
            <h2>Sex & Nudity Categories</h2>
            <table>
                <tr><th>Category</th><th>Count</th></tr>
                {''.join(f"<tr><td>{cat}</td><td>{count}</td></tr>" for cat, count in stats.get('sex_nudity_categories', {}).items())}
            </table>
        </div>
        
        <div class="stat-box">
            <h2>Countries Using the API</h2>
            <table>
                <tr><th>Country</th><th>Hits</th></tr>
                {''.join(f"<tr><td>{country}</td><td>{count}</td></tr>" for country, count in stats.get('countries', {}).items())}
            </table>
        </div>

        <script>
            // Overall Statistics Chart
            new Chart(document.getElementById('overallChart'), {{
                type: 'bar',
                data: {{
                    labels: {overall_data['labels']},
                    datasets: [{{
                        label: 'Hits',
                        data: {overall_data['data']},
                        backgroundColor: ['rgba(75, 192, 192, 0.6)', 'rgba(54, 162, 235, 0.6)', 'rgba(255, 206, 86, 0.6)']
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        y: {{
                            beginAtZero: true
                        }}
                    }}
                }}
            }});

            // This Year's Statistics Chart
            new Chart(document.getElementById('thisYearChart'), {{
                type: 'line',
                data: {{
                    labels: {this_year_data['labels']},
                    datasets: [
                        {{
                            label: 'Total Hits',
                            data: {this_year_data['total']},
                            borderColor: 'rgba(75, 192, 192, 1)',
                            fill: false
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        y: {{
                            beginAtZero: true
                        }}
                    }}
                }}
            }});

            // This Month's Statistics Chart
            new Chart(document.getElementById('thisMonthChart'), {{
                type: 'line',
                data: {{
                    labels: {this_month_data['labels']},
                    datasets: [
                        {{
                            label: 'Total Hits',
                            data: {this_month_data['total']},
                            borderColor: 'rgba(75, 192, 192, 1)',
                            fill: false
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        y: {{
                            beginAtZero: true
                        }}
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """
    return stats_html

@app.route('/tryout', methods=['GET', 'POST'])
def tryout():
    api_status = "green" if is_api_running() else "red"
    providers = ['imdb', 'kidsinmind', 'dove', 'parentpreview', 'cring', 'commonsense', 'movieguide']
    result = None
    error = None
    is_cached = None
    process_time = None

    if request.method == 'POST':
        imdb_id = request.form.get('imdb_id')
        provider = request.form.get('provider')
        video_name = request.form.get('video_name')
        release_year = request.form.get('release_year')

        if not imdb_id or not provider:
            error = "IMDB ID and Provider are required fields."
        else:
            try:
                start_time = time.time()
                params = {
                    'imdb_id': imdb_id,
                    'provider': provider,
                    'video_name': video_name,
                    'release_year': release_year
                }
                # Use the current request's host for the API call
                api_url = f"{request.scheme}://{request.host}/get_data"
                response = requests.get(api_url, params=params)
                result = response.json()
                process_time = round(time.time() - start_time, 2)
                
                # Check if the result was cached
                is_cached = result.get('is_cached', False)
                
                # If 'cached' key is not in the result, check if process time is very short
                if is_cached is None:
                    is_cached = process_time < 0.1  # Assume cached if process time is less than 0.1 seconds
                
                # If the result is empty or contains only 'NA' values, set result to None
                if not result.get('review-items') or all(item.get('Description', '').lower() == 'na' for item in result.get('review-items', [])):
                    result = None
                
            except Exception as e:
                error = f"An error occurred: {str(e)}"

    return render_template('tryout.html', api_status=api_status, providers=providers, result=result, error=error, is_cached=is_cached, process_time=process_time)

# Run the Flask app
if __name__ == '__main__':
    import logging
    from logging.handlers import RotatingFileHandler
    import os
    
    # Configure the root logger
    logging.basicConfig(level=logging.INFO)
    
    # Define the log file path
    log_file = 'api.log'
    
    # Create the log file if it doesn't exist
    if not os.path.exists(log_file):
        open(log_file, 'a').close()
        print(f"Created log file: {log_file}")
    
    # Create a RotatingFileHandler
    file_handler = RotatingFileHandler(log_file, maxBytes=500*1024, backupCount=1)
    file_handler.setLevel(logging.INFO)
    
    # Create a formatter and set it for the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add the file handler to the root logger
    logging.getLogger('').addHandler(file_handler)
    
    # Set the logger for the Flask app
    app.logger.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    
    # Log some initial information
    logging.info("Application started")
    
    host = "0.0.0.0"
    port = 8080
    logging.info(f"Server starting on http://{host}:{port}")
    logging.info(f"API documentation available at http://{host}:{port}/")
    logging.info(f"API endpoint: http://{host}:{port}/get_data")
    logging.info(f"Status endpoint: http://{host}:{port}/status")
    logging.info(f"Stats dashboard: http://{host}:{port}/stats")
    serve(TransLogger(app, setup_console_handler=True), host=host, port=port)