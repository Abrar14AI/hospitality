# main.py
"""
AtliQ Hospitality — Enterprise Chat-with-Data System
Entry point for Streamlit deployment.
"""

import subprocess
import sys

if __name__ == "__main__":
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        "frontend/dashboard.py",
        "--server.port=8501",
        "--server.headless=true"
    ])