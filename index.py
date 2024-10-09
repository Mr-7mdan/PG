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
import asyncio
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

# Load stats from file if it exists
STATS_FILE = 'stats.json'

def save_stats():
    with open(STATS_FILE, 'w') as f:
        json.dump(stats, f, default=str)

def load_stats():
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'r') as f:
            loaded_stats = json.load(f)
        # Convert defaultdict back to regular dict for JSON serialization
        loaded_stats['hits_by_year'] = defaultdict(lambda: {'total': 0, 'cached': 0, 'fresh': 0}, {int(k): v for k, v in loaded_stats['hits_by_year'].items()})
        loaded_stats['hits_by_month'] = defaultdict(lambda: {'total': 0, 'cached': 0, 'fresh': 0}, {k: v for k, v in loaded_stats['hits_by_month'].items()})
        loaded_stats['sex_nudity_categories'] = defaultdict(int, {k: v for k, v in loaded_stats['sex_nudity_categories'].items()})
        loaded_stats['countries'] = defaultdict(int, {k: v for k, v in loaded_stats['countries'].items()})
        return loaded_stats
    return {
        'total_hits': 0,
        'cached_hits': 0,
        'fresh_hits': 0,
        'hits_by_year': defaultdict(lambda: {'total': 0, 'cached': 0, 'fresh': 0}),
        'hits_by_month': defaultdict(lambda: {'total': 0, 'cached': 0, 'fresh': 0}),
        'sex_nudity_categories': defaultdict(int),
        'countries': defaultdict(int)
    }

# Initialize tracking data
stats = load_stats()

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

def update_stats(is_cached, sex_nudity_category=None):
    current_time = datetime.now()
    year = current_time.year
    month = current_time.strftime('%Y-%m')
    day = current_time.strftime('%Y-%m-%d')
    
    stats['total_hits'] += 1
    if is_cached:
        stats['cached_hits'] += 1
    else:
        stats['fresh_hits'] += 1
    
    stats['hits_by_year'].setdefault(year, {'total': 0, 'cached': 0, 'fresh': 0})
    stats['hits_by_year'][year]['total'] += 1
    if is_cached:
        stats['hits_by_year'][year]['cached'] += 1
    else:
        stats['hits_by_year'][year]['fresh'] += 1
    
    stats['hits_by_month'].setdefault(month, {'total': 0, 'cached': 0, 'fresh': 0})
    stats['hits_by_month'][month]['total'] += 1
    if is_cached:
        stats['hits_by_month'][month]['cached'] += 1
    else:
        stats['hits_by_month'][month]['fresh'] += 1
    
    stats.setdefault('hits_by_day', {})
    stats['hits_by_day'].setdefault(day, {'total': 0, 'cached': 0, 'fresh': 0})
    stats['hits_by_day'][day]['total'] += 1
    if is_cached:
        stats['hits_by_day'][day]['cached'] += 1
    else:
        stats['hits_by_day'][day]['fresh'] += 1
    
    if sex_nudity_category is not None:
        category = str(sex_nudity_category) if sex_nudity_category else 'None'
        stats['sex_nudity_categories'][category] = stats['sex_nudity_categories'].get(category, 0) + 1
    
    # Get country from IP address
    ip = request.remote_addr
    country = get_country_from_ip(ip)
    stats['countries'][country] = stats['countries'].get(country, 0) + 1

    app.logger.info(f"Updated stats: total_hits={stats['total_hits']}, cached_hits={stats['cached_hits']}, fresh_hits={stats['fresh_hits']}")
    save_stats()  # Save stats after each update

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
            
            update_stats(is_cached, sex_nudity_category)
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
            
            update_stats(is_cached, sex_nudity_category)
            
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
    # For now, we'll always return True. In a real-world scenario,
    # you might want to check database connections, external services, etc.
    return True

# Modify the api_documentation function
@app.route('/', methods=['GET'])
def api_documentation():
    api_status = "green" if is_api_running() else "red"
    return render_template('documentation.html', api_status=api_status)

# Add a new route for logs
@app.route('/logs', methods=['GET'])
def show_logs():
    logs_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>API Logs</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                line-height: 1.4;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
            }
            h1 {
                color: #2c3e50;
            }
            .nav-bar {
                background-color: #2c3e50;
                padding: 10px;
                margin-bottom: 20px;
            }
            .nav-bar a {
                color: white;
                text-decoration: none;
                padding: 5px 10px;
            }
            .nav-bar a:hover {
                background-color: #34495e;
            }
            #log-container {
                background-color: #f0f0f0;
                border-radius: 5px;
                padding: 15px;
                height: 800px;
                overflow-y: scroll;
                font-family: monospace;
                white-space: pre-wrap;
                font-size: 12px;
                line-height: 1.2;
            }
        </style>
    </head>
    <body>
        <div class="nav-bar">
            <a href="/">Home</a>
            <a href="/stats">Statistics</a>
            <a href="/logs">Logs</a>
            <a href="/tryout">Tryout</a>
        </div>
        <h1>API Logs (Live Updates)</h1>
        <div id="log-container"></div>
        <script>
            const logContainer = document.getElementById('log-container');
            const eventSource = new EventSource('/stream-logs');
            
            eventSource.onmessage = function(event) {
                const logs = JSON.parse(event.data);
                const currentContent = logContainer.innerHTML;
                const newContent = logs.join('\\n');
                
                if (newContent !== currentContent) {
                    logContainer.innerHTML = newContent;
                    logContainer.scrollTop = logContainer.scrollHeight;
                }
            };
            
            eventSource.onerror = function(error) {
                console.error('EventSource failed:', error);
                eventSource.close();
            };
        </script>
    </body>
    </html>
    """
    return logs_html

@app.route('/stream-logs')
def stream_logs():
    def generate():
        log_file = 'api.log'
        last_content = ""
        while True:
            try:
                if not os.path.exists(log_file):
                    with open(log_file, 'w') as f:
                        initial_log = "Log file created. Waiting for logs..."
                        f.write(initial_log + '\n')
                    app.logger.info(f"Created log file: {log_file}")
                    yield f"data: {json.dumps([initial_log])}\n\n"
                    last_content = initial_log
                else:
                    with open(log_file, 'r') as f:
                        content = ''.join(f.readlines()[-1000:])
                        if content != last_content:
                            yield f"data: {json.dumps(content.splitlines())}\n\n"
                            last_content = content
            except Exception as e:
                error_message = f"Error reading log file: {str(e)}"
                app.logger.error(error_message)
                yield f"data: {json.dumps([error_message])}\n\n"
            
            time.sleep(1)  # Update every second

    return Response(generate(), mimetype='text/event-stream')

# Modify the show_stats function to include the status indicator and logs link
@app.route('/stats', methods=['GET'])
def show_stats():
    api_status = "green" if is_api_running() else "red"
    current_year = datetime.now().year
    current_month = datetime.now().strftime('%Y-%m')
    
    # Get cached records count
    cached_records_count = db.get_cached_records_count()

    # Prepare data for charts
    overall_data = {
        'labels': ['Total Hits', 'Cached Hits', 'Fresh Hits'],
        'data': [stats['total_hits'], stats['cached_hits'], stats['fresh_hits']]
    }
    
    # This Year's Statistics (per month)
    months_this_year = [f"{current_year}-{month:02d}" for month in range(1, 13)]
    this_year_data = {
        'labels': months_this_year,
        'total': [stats['hits_by_month'].get(month, {'total': 0})['total'] for month in months_this_year],
        'cached': [stats['hits_by_month'].get(month, {'cached': 0})['cached'] for month in months_this_year],
        'fresh': [stats['hits_by_month'].get(month, {'fresh': 0})['fresh'] for month in months_this_year]
    }
    
    # This Month's Statistics (per day)
    days_in_month = calendar.monthrange(current_year, int(current_month.split('-')[1]))[1]
    days_this_month = [f"{current_month}-{day:02d}" for day in range(1, days_in_month + 1)]
    this_month_data = {
        'labels': days_this_month,
        'total': [stats.get('hits_by_day', {}).get(day, {'total': 0})['total'] for day in days_this_month],
        'cached': [stats.get('hits_by_day', {}).get(day, {'cached': 0})['cached'] for day in days_this_month],
        'fresh': [stats.get('hits_by_day', {}).get(day, {'fresh': 0})['fresh'] for day in days_this_month]
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
            <span>API Status: <span class="status-indicator status-{api_status}"></span></span>
        </div>
        <h1>API Statistics Dashboard</h1>
        
        <div class="stat-box">
            <h2>Overall Statistics</h2>
            <p>Total Hits: {stats['total_hits']}</p>
            <p>Cached Hits: {stats['cached_hits']}</p>
            <p>Fresh Hits: {stats['fresh_hits']}</p>
            <div class="chart-container">
                <canvas id="overallChart"></canvas>
            </div>
        </div>
        
        <div class="stat-box">
            <h2>This Year's Statistics ({current_year})</h2>
            <p>Total Hits: {stats['hits_by_year'].get(current_year, {'total': 0})['total']}</p>
            <p>Cached Hits: {stats['hits_by_year'].get(current_year, {'cached': 0})['cached']}</p>
            <p>Fresh Hits: {stats['hits_by_year'].get(current_year, {'fresh': 0})['fresh']}</p>
            <div class="chart-container">
                <canvas id="thisYearChart"></canvas>
            </div>
        </div>
        
        <div class="stat-box">
            <h2>This Month's Statistics ({current_month})</h2>
            <p>Total Hits: {stats['hits_by_month'].get(current_month, {'total': 0})['total']}</p>
            <p>Cached Hits: {stats['hits_by_month'].get(current_month, {'cached': 0})['cached']}</p>
            <p>Fresh Hits: {stats['hits_by_month'].get(current_month, {'fresh': 0})['fresh']}</p>
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
                {''.join(f"<tr><td>{cat}</td><td>{count}</td></tr>" for cat, count in stats['sex_nudity_categories'].items())}
            </table>
        </div>
        
        <div class="stat-box">
            <h2>Countries Using the API</h2>
            <table>
                <tr><th>Country</th><th>Hits</th></tr>
                {''.join(f"<tr><td>{country}</td><td>{count}</td></tr>" for country, count in stats['countries'].items())}
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

            // This Year's Statistics Chart (per month)
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
                        }},
                        {{
                            label: 'Cached Hits',
                            data: {this_year_data['cached']},
                            borderColor: 'rgba(54, 162, 235, 1)',
                            fill: false
                        }},
                        {{
                            label: 'Fresh Hits',
                            data: {this_year_data['fresh']},
                            borderColor: 'rgba(255, 206, 86, 1)',
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

            // This Month's Statistics Chart (per day)
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
                        }},
                        {{
                            label: 'Cached Hits',
                            data: {this_month_data['cached']},
                            borderColor: 'rgba(54, 162, 235, 1)',
                            fill: false
                        }},
                        {{
                            label: 'Fresh Hits',
                            data: {this_month_data['fresh']},
                            borderColor: 'rgba(255, 206, 86, 1)',
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
                response = requests.get(f'http://localhost:{port}/get_data', params=params)
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

    return render_template('tryout.html', providers=providers, result=result, error=error, is_cached=is_cached, process_time=process_time)

# Save stats periodically
def periodic_save_stats():
    save_stats()
    threading.Timer(300, periodic_save_stats).start()  # 300 seconds = 5 minutes

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
    periodic_save_stats()  # Start periodic saving
    serve(TransLogger(app, setup_console_handler=True), host=host, port=port)