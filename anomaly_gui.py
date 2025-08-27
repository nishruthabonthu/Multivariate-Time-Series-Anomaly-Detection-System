#!/usr/bin/env python3
"""
Flask Real-Time Anomaly Detection Interface
Users input data directly in the web interface
"""

from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
import subprocess
import os
import pandas as pd
import json
from datetime import datetime, timedelta
import threading
import csv
import io
import sys

app = Flask(__name__)
app.secret_key = 'realtime_anomaly_detection_2024'

# Configuration
TEMP_FOLDER = 'temp'
DETECTOR_SCRIPT = 'anomaly_detector.py'

# Ensure folders exist
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Global variable to store current job status
current_job = {'status': 'idle', 'output': '', 'progress': 0, 'results': None}

@app.route('/')
def index():
    """Main page with real-time data entry"""
    return render_template('index.html')

@app.route('/process_data', methods=['POST'])
def process_data():
    """Process real-time data input"""
    print("=== DEBUG: process_data route called ===")
    print(f"Request method: {request.method}")
    print(f"Content type: {request.content_type}")
    
    global current_job
    
    try:
        print("DEBUG: Entering try block")
        
        # Reset job status
        current_job = {'status': 'idle', 'output': '', 'progress': 0, 'results': None}
        
        # Check if detector script exists
        if not os.path.exists(DETECTOR_SCRIPT):
            print(f"DEBUG: Detector script not found: {DETECTOR_SCRIPT}")
            return jsonify({'error': f'{DETECTOR_SCRIPT} not found in current directory!'})
        
        print("DEBUG: Getting JSON data")
        # Get form data
        data = request.get_json()
        if not data:
            print("DEBUG: No JSON data received")
            return jsonify({'error': 'No JSON data received'})
        
        print(f"DEBUG: Received data: {data}")
            
        method = data.get('method', 'statistical')
        timestamp_col = data.get('timestamp_col', 'timestamp')
        rows_data = data.get('rows', [])
        feature_columns = data.get('feature_columns', [])
        
        print(f"DEBUG: Parsed - method:{method}, rows:{len(rows_data)}, features:{feature_columns}")
        
        # Validate input
        if not rows_data:
            return jsonify({'error': 'No data provided!'})
        
        if not feature_columns:
            return jsonify({'error': 'No feature columns defined!'})
        
        # Create CSV from input data
        input_filename = f"realtime_input_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_filename = f"realtime_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        input_path = os.path.join(TEMP_FOLDER, input_filename)
        output_path = os.path.join(TEMP_FOLDER, output_filename)
        
        print(f"DEBUG: Creating CSV at {input_path}")
        
        # Write CSV file
        with open(input_path, 'w', newline='', encoding='utf-8') as csvfile:
            # Create header
            header = [timestamp_col] + feature_columns
            writer = csv.writer(csvfile)
            writer.writerow(header)
            
            # Write data rows
            for row in rows_data:
                csv_row = [row.get('timestamp', '')] + [row.get(col, 0) for col in feature_columns]
                writer.writerow(csv_row)
        
        print("DEBUG: CSV created, starting background thread")
        
        # Start detection in background thread
        threading.Thread(
            target=run_detection_background,
            args=(input_path, output_path, method, timestamp_col),
            daemon=True
        ).start()
        
        result = {
            'success': True,
            'input_filename': input_filename,
            'output_filename': output_filename,
            'method': method,
            'rows_count': len(rows_data)
        }
        
        print(f"DEBUG: Returning success: {result}")
        return jsonify(result)
        
    except Exception as e:
        print(f"DEBUG: Exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Server error: {str(e)}'})

def run_detection_background(input_path, output_path, method, timestamp_col):
    """Run detection in background thread"""
    global current_job
    
    try:
        current_job['status'] = 'running'
        current_job['output'] = '🚀 Starting real-time anomaly detection...\n\n'
        current_job['progress'] = 10
        
        # Show input data info
        try:
            df_input = pd.read_csv(input_path)
            current_job['output'] += f"📊 Processing {len(df_input)} rows with {len(df_input.columns)-1} features\n"
            current_job['output'] += f"🔧 Method: {method}\n"
            current_job['output'] += f"🕐 Timestamp column: {timestamp_col}\n\n"
        except:
            pass
        
        current_job['progress'] = 20
        
        # Build command
        # Use sys.executable to ensure we're using the same python interpreter
        cmd = [
            sys.executable,
            DETECTOR_SCRIPT,
            input_path,
            output_path,
            method,
            timestamp_col
        ]
        
        current_job['output'] += f"⚙️ Running: {' '.join(cmd[1:])}\n\n"
        current_job['progress'] = 30
        
        # Run subprocess
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            universal_newlines=True
        )
        
        # Read output line by line
        if process.stdout:
            for line in process.stdout:
                current_job['output'] += line
                # Simulate progress
                if current_job['progress'] < 80:
                    current_job['progress'] += 2
        
        # Wait for completion
        return_code = process.wait()
        
        if return_code == 0:
            current_job['status'] = 'completed'
            current_job['output'] += '\n✅ Real-time detection completed successfully!\n'
            current_job['progress'] = 90
            
            # Load and analyze results
            try:
                if os.path.exists(output_path):
                    df = pd.read_csv(output_path)
                    current_job['results'] = df.to_dict('records')
                    
                    if 'anomaly_score_0_100' in df.columns:
                        scores = df['anomaly_score_0_100']
                        high = (scores > 70).sum()
                        medium = ((scores > 30) & (scores <= 70)).sum()
                        low = (scores <= 30).sum()
                        
                        summary = f"""
📈 Real-Time Detection Summary:
   🚨 High anomalies (>70): {high}
   ⚠️ Medium anomalies (30-70): {medium}
   ✅ Normal/Low (<30): {low}
   📊 Max anomaly score: {scores.max():.1f}
   📋 Data points analyzed: {len(df)}
   
🔍 Top Anomalous Points:
"""
                        # Show top 3 anomalies
                        top_anomalies = df.nlargest(3, 'anomaly_score_0_100')
                        for idx, row in top_anomalies.iterrows():
                            summary += f"   • Score {row['anomaly_score_0_100']:.1f}: {row.get('top_contributors', 'N/A')}\n"
                        
                        current_job['output'] += summary
                        current_job['progress'] = 100
            except Exception as e:
                current_job['output'] += f'⚠️ Could not load results: {str(e)}\n'
                current_job['progress'] = 100
        else:
            current_job['status'] = 'failed'
            current_job['output'] += f'\n❌ Detection failed with return code: {return_code}\n'
            current_job['progress'] = 100
            
    except Exception as e:
        current_job['status'] = 'failed'
        current_job['output'] += f'\n❌ Error in real-time detection: {str(e)}\n'
        current_job['progress'] = 100

@app.route('/status')
def get_status():
    """Get current job status (AJAX endpoint)"""
    return jsonify(current_job)

@app.route('/results')
def results_page():
    """Results display page"""
    return render_template('realtime_results.html')

@app.route('/generate_sample')
def generate_sample():
    """Generate sample data for testing"""
    # Generate time series with some anomalies
    base_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    sample_data = []
    
    for i in range(24):  # 24 hours of data
        timestamp = (base_time + timedelta(hours=i)).strftime('%Y-%m-%d %H:%M:%S')
        
        # Normal patterns with some anomalies
        if i in [11, 12, 13]:  # Anomalies at hours 11, 12, 13
            cpu = 90 + (i * 2)
            memory = 95 + i
            network = 3000 + (i * 100)
            disk = 1500 + (i * 50)
            temp = 85 + i
        else:  # Normal values
            cpu = 40 + (i % 5) * 2
            memory = 60 + (i % 3) * 5
            network = 1000 + (i % 4) * 100
            disk = 500 + (i % 6) * 50
            temp = 65 + (i % 4)
        
        sample_data.append({
            'timestamp': timestamp,
            'cpu_usage': cpu,
            'memory_usage': memory,
            'network_io': network,
            'disk_io': disk,
            'temperature': temp
        })
    
    return jsonify({
        'feature_columns': ['cpu_usage', 'memory_usage', 'network_io', 'disk_io', 'temperature'],
        'rows': sample_data
    })

@app.route('/test_detection')
def test_detection():
    """Simple test endpoint"""
    try:
        # Create test data
        test_data = {
            'method': 'statistical',
            'timestamp_col': 'timestamp', 
            'feature_columns': ['cpu_usage', 'memory_usage', 'temperature'],
            'rows': [
                {'timestamp': '2024-01-01 10:00:00', 'cpu_usage': 45, 'memory_usage': 60, 'temperature': 65},
                {'timestamp': '2024-01-01 11:00:00', 'cpu_usage': 50, 'memory_usage': 65, 'temperature': 68},
                {'timestamp': '2024-01-01 12:00:00', 'cpu_usage': 95, 'memory_usage': 98, 'temperature': 90},
                {'timestamp': '2024-01-01 13:00:00', 'cpu_usage': 48, 'memory_usage': 62, 'temperature': 66},
            ]
        }
        
        # Simulate the same processing
        global current_job
        current_job = {'status': 'idle', 'output': '', 'progress': 0, 'results': None}
        
        input_filename = f"test_input_{datetime.now().strftime('%H%M%S')}.csv"
        output_filename = f"test_results_{datetime.now().strftime('%H%M%S')}.csv"
        
        input_path = os.path.join(TEMP_FOLDER, input_filename)
        output_path = os.path.join(TEMP_FOLDER, output_filename)
        
        # Write CSV
        import csv
        with open(input_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'cpu_usage', 'memory_usage', 'temperature'])
            for row in test_data['rows']:
                writer.writerow([row['timestamp'], row['cpu_usage'], row['memory_usage'], row['temperature']])
        
        # Start detection
        threading.Thread(
            target=run_detection_background,
            args=(input_path, output_path, 'statistical', 'timestamp'),
            daemon=True
        ).start()
        
        return jsonify({'success': True, 'message': 'Test started'})
        
    except Exception as e:
        return jsonify({'error': f'Test failed: {str(e)}'})

if __name__ == '__main__':
    print("🚀 Starting Real-Time Anomaly Detection Interface...")
    print("📍 Open your browser to: http://localhost:5000")
    print("🔧 Make sure anomaly_detector.py is in the same directory!")
    print("💡 Enter data directly in the web interface!")
    app.run(debug=True, host='0.0.0.0', port=5000)