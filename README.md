# Movie ETL Project

## Overview
This project extracts movie and rating data, enriches it using the OMDb API, and loads it into a relational database.  
The pipeline includes:
- CSV extraction
- Data cleaning and transformation
- API enrichment
- Database loading
- SQL analysis queries

---

## How to Run the Project

### 1. Install Dependencies

### 2. Run Schema (only once)
Create database & tables:


### 3. Run ETL Script


---

## 🏗 Design Choices
- Used mySQL for simplicity.
- Normalized genres using a junction table.
- OMDb API enriches each movie with:
  - Director
  - Plot
  - Box Office
- SQLAlchemy manages database interaction.


## Assumptions :
- CSV files are correctly formatted.
- OMDb API may not return data for all movies.
- No duplication check is required by assignment.

---

## ⚠ Challenges & Solutions : 
### 1. Missing OMDb responses  
**Solution:** Fallback null values to maintain pipeline flow.

### 2. Genre Normalization  
**Solution:** Splitting `genres` into a mapping table.

### 3. Box Office Conversion  
**Solution:** Clean "$" and commas → convert to integer.

---

## Files Included
- `etl.py`
- `schema.sql`
- `queries.sql`
- `README.md`
- `requirements.txt`
- CSV sample files (movies.csv, ratings.csv)
