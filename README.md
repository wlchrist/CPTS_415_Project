# CPTS 415 Project - Movie Ratings Dataset

Project repository for Team OSHD for CPTS_415 at WSU

## Overview

This project processes IMDb movie data to create an expanded movie ratings dataset for analysis. The dataset has been expanded from 6 rows to over 60,000 rows while maintaining compatibility with existing analysis scripts.

## Dataset Information

- **Total rows**: 60,508
- **Unique users**: 60,646
- **Unique movies**: 1,004
- **Unique genres**: 23
- **Average rating**: 3.68
- **File size**: ~2.3MB

## Files

### Core Scripts
- `cleansing.py` - Data cleaning and validation
- `parser.py` - Main parsing script that orchestrates the pipeline
- `transformation.py` - Transforms data into document format
- `reduction.py` - Data reduction utilities

### Data Processing
- `process_imdb_data.py` - Expands ratings.csv with IMDb movie data
- `ratings.csv` - Main dataset (expanded from IMDb data)
- `movie_documents.json` - Generated output from transformation

### Configuration
- `.gitignore` - Excludes virtual environment and large data files
- `Project Milestone 2 Template - Team Submission.pdf` - Project requirements

## Setup Instructions

### Prerequisites
- Python 3.x
- Virtual environment support

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/wlchrist/CPTS_415_Project.git
   cd CPTS_415_Project
   ```

2. **Create virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install pandas
   ```

4. **Download IMDb dataset** (optional - for further expansion)
   - Download "title.basics.tsv.gz" from [IMDb datasets](https://datasets.imdbws.com/)
   - Extract and place "IMDb Title Basics.tsv" in project folder

## Usage

### Running Existing Scripts

```bash
# Activate virtual environment
source venv/bin/activate

# Run data cleaning
python cleansing.py

# Run main parser (generates movie_documents.json)
python parser.py

# Run transformation
python transformation.py

# Run reduction
python reduction.py
```

### Expanding the Dataset

To add more movies from IMDb data:

```bash
# Ensure IMDb Title Basics.tsv is in project folder
python process_imdb_data.py
```

This will:
- Process the IMDb TSV file in chunks
- Filter for movies only
- Generate additional ratings
- Expand ratings.csv while preserving format

## Data Format

The `ratings.csv` file maintains the following format:
```csv
userID,movieID,title,genre,rating
1,101,The Matrix,Sci-Fi,5
2,102,Titanic,Romance,4
```

## Project Structure

```
CPTS_415_Project/
├── cleansing.py              # Data cleaning
├── parser.py                 # Main parser
├── transformation.py         # Data transformation
├── reduction.py              # Data reduction
├── process_imdb_data.py      # IMDb data processing
├── ratings.csv               # Main dataset
├── movie_documents.json      # Generated output
├── .gitignore               # Git configuration
└── README.md                # This file
```

## Team Members

Team OSHD - CPTS 415 at WSU

## License

This project is for educational purposes as part of CPTS 415 coursework.
