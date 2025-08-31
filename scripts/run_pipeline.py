import os
from src.main import main

os.chdir(r"C:\Users\Bobbo\OneDrive\Desktop\Python\energy_data_repo")

if __name__ == "__main__":
    SOURCE = "elexon"  # 'entsoe' or 'elexon'
    START_DATE = "2023-01-01"  # YYYY-MM-DD
    END_DATE = "2023-01-05"  # YYYY-MM-DD
    QUERY = None  # Optional, e.g., 'generation_per_type'

    success = main(SOURCE, START_DATE, END_DATE, QUERY)


