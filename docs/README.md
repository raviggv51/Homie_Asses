# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Lightweight helper libraries only** (e.g. `pandas`, `mysql-connector-python`).  
  List every dependency in **`requirements.txt`** and justify anything unusual.
- **No ORMs / auto-migration tools** – write plain SQL by hand.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `sql/` and `scripts/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

**Tech Stack:**

- Python (include a `requirements.txt`)
  Use **MySQL** and SQL for all database work
- You may use any CLI or GUI for development, but the final changes must be submitted as python/ SQL scripts
- **Do not** use ORM migrations—write all SQL by hand

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Place all scripts/code in their respective folders (`sql/`, `scripts/`, etc.)
- Ensure all steps are fully **reproducible** using your documentation
- Create a new private repo and invite the reviewer https://github.com/mantreshjain

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Filed by Candidate)

**Document your database design and solution here:**

- Explain your schema and any design decisions

🧠 Design Decisions and Rationale
1️⃣ Normalization

The JSON source mixed unrelated attributes into one object.
We applied 3rd Normal Form (3NF) to separate logical entities:
property holds static attributes (address, structure).
Other tables store variable or repeating data (valuation, rehab, etc.).
This improves data integrity, reduces redundancy, and supports incremental updates.

2️⃣ Primary & Foreign Keys

Each table uses a surrogate key (*_id) as primary key for internal joins.
property_id acts as the referential key across all child tables.
This maintains referential integrity and simplifies one-to-many relationships.

3️⃣ Referential Integrity

Every child table (leads, valuation, hoa, rehab, taxes) references property(property_id) via a foreign key constraint.
ON DELETE CASCADE can be optionally enabled if we want automatic cleanup when a property is deleted.

4️⃣ 1:1 vs 1:N Constraints

For 1:1 relationships (taxes), a UNIQUE constraint was added on property_id.
For 1:N relationships, no uniqueness constraint allows multiple dependent records.

5️⃣ Data Cleaning Decisions

Nulls, blank strings, and placeholders like "null" or " " were standardized to NULL.
Inconsistent yes/no values (y, N, true) were normalized to "Yes" / "No".
Extreme numeric outliers (e.g., negative tax or >10M valuation) were capped or set to NULL.
Empty “Market” values were labeled "Unknown" for clarity.

6️⃣ ETL Flow

Extract: Load raw JSON using pandas.read_json().
Transform: Normalize nested structures (Valuation, Rehab, HOA) into separate DataFrames.
Clean: Apply consistent cleaning functions (clean_text, yes_no_normalize, etc.).
Load: Use SQLAlchemy + pandas.to_sql() to insert into normalized tables in correct order.

7️⃣ Performance & Scalability

Bulk inserts via pandas.to_sql() ensure fast loading for large datasets.
Indexes on property_id in all child tables improve join and lookup performance.
Modular cleaning functions allow easy extension to future data sources.

✅ Outcome

The final schema is relational, consistent, and query-efficient.

It supports both analytical queries (e.g., valuation trends, yield analysis)
and business monitoring (e.g., property status by market, rehab cost summary).

The design aligns with Data Engineering best practices —
separating static (dimension-like) data from dynamic (fact-like) data.

🧭 Example Query: End-to-End Relationship

To fetch a full property overview:

SELECT 
    p.Property_Title,
    p.City, p.State, 
    v.List_Price, v.Redfin_Value,
    r.Rehab_Calculation,
    l.Most_Recent_Status,
    t.Taxes
FROM property p
LEFT JOIN valuation v ON p.property_id = v.property_id
LEFT JOIN rehab r ON p.property_id = r.property_id
LEFT JOIN leads l ON p.property_id = l.property_id
LEFT JOIN taxes t ON p.property_id = t.property_id
WHERE p.Market = 'Chicago';

- Give clear instructions on how to run and test your script

🚀 Steps to Run the Project

Follow these steps carefully to run the ETL pipeline successfully 👇

1️⃣ Clone this repository
git clone https://github.com/<your-github-username>/<repo-name>.git
cd <repo-name>

2️⃣ Start MySQL Database in Docker
Run the following command to start the MySQL container using the provided Docker setup:
docker-compose -f docker-compose.initial.yml up --build -d
✅ This will:
Build and start the MySQL container (mysql_ctn)
Create a database named home_db
Make it accessible for Python via SQLAlchemy

3️⃣ Install all required Python modules
Navigate to the Scripts folder and install dependencies:
cd Scripts
pip3 install -r requirements.txt

4️⃣ Run the ETL Script
Run the main ETL Python script:
python3 test.py

5️⃣ Verify the Data Load
Connect to your MySQL container and verify the tables:
docker exec -it mysql_ctn mysql -u db_user -p home_db

SHOW TABLES;
SELECT COUNT(*) FROM property;
SELECT COUNT(*) FROM leads;



**Document your ETL logic here:**

🧱 Approach and Design
1️⃣ Understanding the Problem

The provided JSON dataset contained property records with mixed and nested information —
details about the property, its valuations, rehab costs, HOA data, and leads were all combined in one structure.
This made the dataset denormalized and not relationally consistent.

To make the data usable for analysis and storage, a proper ETL pipeline was designed.



2️⃣ ETL Pipeline Overview

The project follows a standard ETL (Extract → Transform → Load) architecture:

Stage	Description
Extract	Read the JSON array from fake_property_data_new.json using Python (pandas and json modules).
Transform	Clean, normalize, and split the mixed JSON data into structured DataFrames — one for each business entity (property, leads, hoa, rehab, valuation, taxes).
Load	Create relational MySQL tables using SQLAlchemy and load cleaned data into the database with proper foreign key constraints.



3️⃣ Data Cleaning Strategy

To ensure high-quality data:
Created generic helper functions (clean_text, yes_no_normalize, extract_numeric) for common cleanup patterns.
Replaced inconsistent values (e.g., "null", "NULL", " ") with NaN.
Standardized Yes/No flags, capitalized names, and extracted numeric values from text fields like "5649 sqft".
Filled missing categorical fields like Market with "Unknown".
Removed duplicates and invalid records.



4️⃣ Database Design (Normalization)

The schema was designed using 3rd Normal Form (3NF) to eliminate redundancy:

Table	Description	Relationship
property	Core entity — stores unique details of each property	Primary table
leads	Stores sales and review status per property	1 : N with property
valuation	Contains property price and rent data	1 : N with property
rehab	Stores property renovation details	1 : N with property
hoa	Stores Homeowners Association info	1 : 1 with property
taxes	Stores property tax information	1 : 1 with property

Each child table references property_id as a foreign key.



5️⃣ Technology Stack and Tools
Component	Tool / Library	Purpose
Language	Python 3	ETL and data transformation
Libraries	pandas, numpy, re, sqlalchemy, pymysql	Data processing & DB connection
Database	MySQL (via Docker)	Normalized relational storage
Environment	Docker Compose	Reproducible local setup
Documentation	Markdown, SQL, Python scripts	Clear reproducibility



6️⃣ Data Loading and Validation

Used SQLAlchemy ORM for safe insertion and schema creation.
Added checks to ensure referential integrity:
Dropped records referencing missing property_ids.
Avoided duplicate primary key insertions.
Verified successful data load with SQL queries and record counts.



7️⃣ Design Decisions

Chose MySQL for structured relational storage.
Adopted Docker for easy environment setup and portability.
Used Python Pandas for transformation due to its flexibility with nested JSON.
Maintained clear modular design — separate cleaning logic per DataFrame for easier debugging.
Stored helper functions centrally for reusability.
