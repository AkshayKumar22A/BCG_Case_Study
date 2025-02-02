# BCG Case Study: Traffic Accident Analysis

![Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue?style=for-the-badge&logo=python)

A Spark-based application to answer 10 key analytical questions.

---

## Overview
This project processes motor vehicle crash data to derive insights using Apache Spark. It performs 10 distinct analyses as specified in the BCG case study.

## Features
- **Analytical **: Application should perform below analysis and store the results for each analysis.
  1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
  2.	Analysis 2: How many two wheelers are booked for crashes? 
  3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
  4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
  5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
  6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
  7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
  8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
  9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
  10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

- **Modular Code**  
  Separates data loading, analysis, and configuration management.
- **Config-Driven**  
  Input/output paths and analyses list defined in `config.yml`.
- **Logging**  
  Detailed execution logs stored in `case_study.log`.
- **Spark Optimized**  
  Uses DataFrame APIs (no Spark SQL) for distributed processing.

## Dataset
The analysis uses six CSV files:
- `Primary_Person_use.csv`  
- `Units_use.csv`  
- `Damages_use.csv`  
- `Charges_use.csv`  
- `Endorse_use.csv`  
- `Restrict_use.csv`  

**Note**: Place all files in `data/input/` (see [Configuration](#configuration)).

## Prerequisites
- Python 3.8+
- Apache Spark 3.x
- Java 8/11
- Libraries: PySpark, PyYAML

## Install Dependencies:
bash
  pip install pyspark pyyaml

## Installation & Setup
1. **Clone Repository**:
   ```bash
   git clone https://github.com/your-username/bcg-case-study.git
   cd bcg-case-study

## Usage
Run the pipeline:
bash
spark-submit run_analysis.py --config config/config.yml
spark-submit --master "local[*]" --driver-memory 4G run_analysis.py --config config/config.yml
  Arguments:
  --config: Path to config file (default: config/config.yml)

## Project Structure
  bcg-case-study/
  ├── src/
  │   ├── __pycache__
  │   ├── analysis.py          # Analysis logic
  │   ├── config_manager.py    # Config loader
  │   ├── data_loader.py       # Data ingestion
  │   └── logger.py            # Logging setup
  ├── run_analysis.py          # Main pipeline
  ├── config/
  │   └── config.yml           # Configuration
  ├── data/                    # Input CSVs
  │   └── Primary_Person_use.csv
  │   └── Units_use.csv
  │   └── Damages_use.csv
  │   └── Charges_use.csv
  │   └── Endorse_use.csv
  │   └── Restrict_use.csv
  ├── output/                  # Analysis results
  ├── case_study.log           # Execution logs
  └── Readme.md                # Readme File

## Output
  Results are saved as CSV files in data/output/analysis_*/. Each folder corresponds to one analysis.

Contact: akshaykumar22a@gmail.com
GitHub: @AkshayKumar22A
