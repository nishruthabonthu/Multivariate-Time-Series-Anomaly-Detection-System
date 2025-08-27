#!/usr/bin/env python3
"""
Flask Web Interface for Anomaly Detection System
Uses existing anomaly_detector.py without modifying it
"""

from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
import subprocess
import os
import pandas as pd
import json
from pathlib import Path
from werkzeug.utils import secure_filename
import threading
import time

app = Flask(__name__)
app.secret_key = 'anomaly_detection_hackathon_2024'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Configuration
UPLOAD_FOLDER = 'uploads'
RESULTS_FOLDER = 'results'
DETECTOR_SCRIPT = 'anomaly_detector.py'

# Ensure folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

# Global variable to store current job status
current_job = {'status': 'idle', 'output': '', 'progress': 0}

@app.route('/')
def index():
    """Main page with form"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and start detection"""
    global current_job
    
    try:
        # Reset job status
        current_job = {'status': 'idle', 'output': '', 'progress': 0}
        
        # Check if detector script exists
        if not os.path.exists(DETECTOR_SCRIPT):
            flash(f'Error: {DETECTOR_SCRIPT} not found in current directory!', 'error')
            return redirect(url_for('index'))
        
        # Get form data
        method = request.form.get('method', 'statistical')
        timestamp_col = request.form.get('timestamp_col', 'timestamp')
        
        # Handle file upload
        if 'file' not in request.files:
            flash('No file selected!', 'error')
            return redirect(url_for('index'))
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected!', 'error')
            return redirect(url_for('index'))
        
        if file and file.filename.lower().endswith('.csv'):
            # Save uploaded file
            filename = secure_filename(file.filename)
            input_path = os.path.join(UPLOAD_FOLDER, filename)
            file.save(input_path)
            
            # Generate output filename
            output_filename = f"{Path(filename).stem}_results.csv"
            output_path = os.path.join(RESULTS_FOLDER, output_filename)
            
            # Start detection in background thread
            threading.Thread(
                target=run_detection_background,
                args=(input_path, output_path, method, timestamp_col),
                daemon=True
            ).start()
            
            return render_template('results.html', 
                                 filename=filename,
                                 method=method,
                                 timestamp_col=timestamp_col,
                                 output_filename=output_filename)
        else:
            flash('Please upload a CSV file!', 'error')
            return redirect(url_for('index'))
            
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('index'))

def run_detection_background(input_path, output_path, method, timestamp_col):
    """Run detection in background thread"""
    global current_job
    
    try:
        current_job['status'] = 'running'
        current_job['output'] = '🚀 Starting anomaly detection...\n\n'
        current_job['progress'] = 10
        
        # Build command
        cmd = [
            'python',
            DETECTOR_SCRIPT,
            input_path,
            output_path,
            method,
            timestamp_col
        ]
        
        current_job['output'] += f"Command: {' '.join(cmd[1:])}\n\n"
        current_job['progress'] = 20
        
        # Run subprocess
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            universal_newlines=True
        )
        
        current_job['progress'] = 30
        
        # Read output line by line
        for line in process.stdout:
            current_job['output'] += line
            # Simulate progress
            if current_job['progress'] < 80:
                current_job['progress'] += 2
        
        # Wait for completion
        return_code = process.wait()
        
        if return_code == 0:
            current_job['status'] = 'completed'
            current_job['output'] += '\n✅ Detection completed successfully!\n'
            current_job['progress'] = 100
            
            # Load and analyze results
            try:
                if os.path.exists(output_path):
                    df = pd.read_csv(output_path)
                    if 'anomaly_score_0_100' in df.columns:
                        scores = df['anomaly_score_0_100']
                        high = (scores > 70).sum()
                        medium = ((scores > 30) & (scores <= 70)).sum()
                        low = (scores <= 30).sum()
                        
                        summary = f"""
📈 Detection Summary:
   🚨 High anomalies (>70): {high}
   ⚠️ Medium anomalies (30-70): {medium}
   ✅ Normal/Low (<30): {low}
   📊 Max anomaly score: {scores.max():.1f}
   📋 Total data points: {len(df)}
"""
                        current_job['output'] += summary
            except Exception as e:
                current_job['output'] += f'⚠️ Could not load results summary: {str(e)}\n'
        else:
            current_job['status'] = 'failed'
            current_job['output'] += f'\n❌ Detection failed with return code: {return_code}\n'
            current_job['progress'] = 100
            
    except Exception as e:
        current_job['status'] = 'failed'
        current_job['output'] += f'\n❌ Error running detection: {str(e)}\n'
        current_job['progress'] = 100

@app.route('/status')
def get_status():
    """Get current job status (AJAX endpoint)"""
    return jsonify(current_job)

@app.route('/results_csv/<filename>')
def get_results_csv(filename):
    """Get results CSV data for display"""
    try:
        filepath = os.path.join(RESULTS_FOLDER, filename)
        if not os.path.exists(filepath):
            return jsonify({'error': 'Results file not found'}), 404
        
        # Read CSV
        df = pd.read_csv(filepath)
        
        # Limit rows for display (show first 100 rows)
        display_df = df.head(100) if len(df) > 100 else df
        
        # Convert to dict for JSON
        data = {
            'columns': list(display_df.columns),
            'data': display_df.values.tolist(),
            'total_rows': len(df),
            'showing_rows': len(display_df)
        }
        
        return jsonify(data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Download results file"""
    try:
        filepath = os.path.join(RESULTS_FOLDER, filename)
        if os.path.exists(filepath):
            return send_file(filepath, as_attachment=True)
        else:
            flash('File not found!', 'error')
            return redirect(url_for('index'))
    except Exception as e:
        flash(f'Error downloading file: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/sample')
def download_sample():
    """Download sample CSV file"""
    # Create sample data
    sample_data = """timestamp,cpu_usage,memory_usage,network_io,disk_io,temperature
2024-01-01 00:00:00,45.2,62.1,1024,512,68.5
2024-01-01 01:00:00,42.8,58.9,1156,489,67.2
2024-01-01 02:00:00,41.5,55.7,1089,501,66.8
2024-01-01 03:00:00,39.2,52.3,965,478,65.9
2024-01-01 04:00:00,38.1,51.1,892,445,65.2
2024-01-01 05:00:00,37.5,50.8,823,432,64.8
2024-01-01 06:00:00,46.8,65.2,1245,567,69.1
2024-01-01 07:00:00,52.1,71.3,1456,634,71.2
2024-01-01 08:00:00,58.9,78.5,1678,712,73.4
2024-01-01 09:00:00,61.2,82.1,1834,789,74.8
2024-01-01 10:00:00,63.5,85.3,1945,823,76.1
2024-01-01 11:00:00,95.8,97.2,3456,1567,89.2
2024-01-01 12:00:00,97.1,98.8,3678,1689,91.5
2024-01-01 13:00:00,94.2,96.5,3234,1456,88.7
2024-01-01 14:00:00,65.3,86.7,1978,834,76.8
2024-01-01 15:00:00,62.8,84.2,1823,798,75.3"""
    
    # Save sample file
    sample_path = 'sample_data.csv'
    with open(sample_path, 'w') as f:
        f.write(sample_data)
    
    return send_file(sample_path, as_attachment=True, download_name='sample_input.csv')

if __name__ == '__main__':
    print("🚀 Starting Anomaly Detection Web Interface...")
    print("📍 Open your browser to: http://localhost:5000")
    print("🔧 Make sure anomaly_detector.py is in the same directory!")
    app.run(debug=True, host='0.0.0.0', port=5000)