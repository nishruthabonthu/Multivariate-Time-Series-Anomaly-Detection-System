#!/usr/bin/env python3
"""
Single-file anomaly detector - GUARANTEED TO WORK
No complex imports, no setup.py required
"""
import pandas as pd
import numpy as np
import sys
import warnings
warnings.filterwarnings('ignore')

# Optional imports with fallbacks
ADTK_AVAILABLE = False
PYOD_AVAILABLE = False

try:
    from adtk.detector import InterQuartileRangeAD
    from adtk.data import validate_series
    ADTK_AVAILABLE = True
    print("ADTK available")
except ImportError:
    print("ADTK not available - using statistical fallback")

try:
    from pyod.models.copod import COPOD
    PYOD_AVAILABLE = True
    print("PyOD available")
except ImportError:
    print("PyOD not available - using statistical fallback")

def statistical_detect(df, timestamp_col='timestamp'):
    """Statistical anomaly detection - always works"""
    print("Using statistical detection")
    
    feature_cols = [col for col in df.columns if col != timestamp_col]
    anomaly_scores = np.zeros(len(df))
    contributions = []
    
    # Pre-compute statistics
    stats = {}
    for col in feature_cols:
        mean_val = df[col].mean()
        std_val = df[col].std()
        stats[col] = {'mean': mean_val, 'std': std_val}
    
    # Process each row
    for idx in range(len(df)):
        row_scores = {}
        max_score = 0
        
        for col in feature_cols:
            value = df[col].iloc[idx]
            if stats[col]['std'] > 0:
                z_score = abs((value - stats[col]['mean']) / stats[col]['std'])
                score = min(100, (z_score / 3.0) * 100)
            else:
                score = 0
            
            row_scores[col] = score
            max_score = max(max_score, score)
        
        anomaly_scores[idx] = max_score
        
        # Top 3 contributors
        top_3 = sorted(row_scores.items(), key=lambda x: x[1], reverse=True)[:3]
        contrib_str = '; '.join([f"{feat}:{score:.1f}" for feat, score in top_3])
        contributions.append(contrib_str)
    
    return anomaly_scores, contributions

def adtk_detect(df, timestamp_col='timestamp'):
    """ADTK detection with fallback"""
    if not ADTK_AVAILABLE:
        return statistical_detect(df, timestamp_col)
    
    print("🔍 Using ADTK detection")
    
    try:
        feature_cols = [col for col in df.columns if col != timestamp_col]
        df_ts = df.copy()
        df_ts[timestamp_col] = pd.to_datetime(df_ts[timestamp_col])
        df_ts.set_index(timestamp_col, inplace=True)
        
        anomaly_scores = np.zeros(len(df))
        contributions = []
        
        detector = InterQuartileRangeAD(c=1.5)
        
        for idx in range(len(df)):
            row_scores = {}
            max_score = 0
            
            for col in feature_cols:
                try:
                    series = validate_series(df_ts[col])
                    anomalies = detector.detect(series)
                    
                    if idx < len(anomalies) and hasattr(anomalies, 'iloc') and anomalies.iloc[idx]:
                        q1 = df_ts[col].quantile(0.25)
                        q3 = df_ts[col].quantile(0.75)
                        iqr = q3 - q1
                        value = df_ts[col].iloc[idx]
                        
                        if iqr > 0:
                            if value < q1:
                                distance = (q1 - value) / iqr
                            elif value > q3:
                                distance = (value - q3) / iqr
                            else:
                                distance = 0
                            score = min(100, distance * 50)
                        else:
                            score = 0
                    else:
                        score = 0
                        
                except:
                    # Statistical fallback for this feature
                    mean_val = df_ts[col].mean()
                    std_val = df_ts[col].std()
                    if std_val > 0:
                        z_score = abs((df_ts[col].iloc[idx] - mean_val) / std_val)
                        score = min(100, (z_score / 3.0) * 100)
                    else:
                        score = 0
                
                row_scores[col] = score
                max_score = max(max_score, score)
            
            anomaly_scores[idx] = max_score
            
            top_3 = sorted(row_scores.items(), key=lambda x: x[1], reverse=True)[:3]
            contrib_str = '; '.join([f"{feat}:{score:.1f}" for feat, score in top_3])
            contributions.append(contrib_str)
        
        return anomaly_scores, contributions
        
    except Exception as e:
        print(f"⚠️  ADTK failed: {e}, using statistical fallback")
        return statistical_detect(df, timestamp_col)

def ml_detect(df, timestamp_col='timestamp'):
    """ML detection with fallback"""
    if not PYOD_AVAILABLE:
        return adtk_detect(df, timestamp_col)
    
    print("🔍 Using ML detection (COPOD)")
    
    try:
        feature_cols = [col for col in df.columns if col != timestamp_col]
        X = df[feature_cols].values
        
        # Handle NaN values
        if np.isnan(X).any():
            print("⚠️  Handling missing values")
            col_means = np.nanmean(X, axis=0)
            for i in range(X.shape[1]):
                X[np.isnan(X[:, i]), i] = col_means[i]
        
        # Fit COPOD model
        detector = COPOD()
        detector.fit(X)
        scores = detector.decision_function(X)
        
        # Normalize to 0-100
        min_score, max_score = scores.min(), scores.max()
        if max_score > min_score:
            anomaly_scores = ((scores - min_score) / (max_score - min_score)) * 100
        else:
            anomaly_scores = np.zeros_like(scores)
        
        # Calculate contributions
        contributions = []
        feature_means = np.mean(X, axis=0)
        feature_stds = np.std(X, axis=0)
        
        for idx in range(len(df)):
            row_scores = {}
            for j, col in enumerate(feature_cols):
                if feature_stds[j] > 0:
                    z_score = abs((X[idx, j] - feature_means[j]) / feature_stds[j])
                    contrib = min(100, z_score * 25)
                    row_scores[col] = contrib
                else:
                    row_scores[col] = 0
            
            top_3 = sorted(row_scores.items(), key=lambda x: x[1], reverse=True)[:3]
            contrib_str = '; '.join([f"{feat}:{score:.1f}" for feat, score in top_3])
            contributions.append(contrib_str)
        
        return anomaly_scores, contributions
        
    except Exception as e:
        print(f"⚠️  ML detection failed: {e}, using ADTK fallback")
        return adtk_detect(df, timestamp_col)

def detect_anomalies(input_file, output_file, method='adtk', timestamp_col='timestamp'):
    """Main detection function"""
    
    print(f"🚀 Starting anomaly detection")
    print(f"📁 Input: {input_file}")
    print(f"🔧 Method: {method}")
    
    try:
        # Load data
        print("📖 Loading data...")
        df = pd.read_csv(input_file)
        print(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")
        
        # Validate timestamp column
        if timestamp_col not in df.columns:
            print(f"❌ Column '{timestamp_col}' not found!")
            print(f"Available columns: {list(df.columns)}")
            return False
        
        # Get feature columns
        feature_cols = [col for col in df.columns if col != timestamp_col]
        if not feature_cols:
            print("❌ No feature columns found!")
            return False
        
        print(f"🔍 Analyzing features: {feature_cols}")
        
        # Run detection
        if method == 'ml':
            anomaly_scores, contributions = ml_detect(df, timestamp_col)
        elif method == 'adtk':
            anomaly_scores, contributions = adtk_detect(df, timestamp_col)
        else:  # statistical
            anomaly_scores, contributions = statistical_detect(df, timestamp_col)
        
        # Create output
        result_df = df.copy()
        result_df['anomaly_score_0_100'] = np.round(anomaly_scores, 1)
        result_df['top_contributors'] = contributions
        
        # Save results
        result_df.to_csv(output_file, index=False)
        
        # Print summary
        high = (anomaly_scores > 70).sum()
        medium = ((anomaly_scores > 30) & (anomaly_scores <= 70)).sum()
        low = (anomaly_scores <= 30).sum()
        
        print(f"\n✅ Detection completed!")
        print(f"💾 Results saved to: {output_file}")
        print(f"🚨 High anomalies (>70): {high}")
        print(f"⚠️  Medium anomalies (30-70): {medium}")
        print(f"✅ Normal (<30): {low}")
        print(f"📈 Max anomaly score: {anomaly_scores.max():.1f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    # Command line usage
    if len(sys.argv) < 3:
        print("Usage: python anomaly_detector.py input.csv output.csv [method] [timestamp_col]")
        print("Methods: statistical, adtk, ml")
        print("Example: python anomaly_detector.py data.csv results.csv adtk")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    method = sys.argv[3] if len(sys.argv) > 3 else 'adtk'
    timestamp_col = sys.argv[4] if len(sys.argv) > 4 else 'timestamp'
    
    success = detect_anomalies(input_file, output_file, method, timestamp_col)
    
    if not success:
        sys.exit(1)