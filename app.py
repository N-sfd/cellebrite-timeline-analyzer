from src.dashboard import *
import subprocess
import os

# run your streamlit dashboard
os.system("streamlit run src/dashboard.py --server.port 7860 --server.address 0.0.0.0")