# Databricks SQL: End-to-End Airline Data Analytics Platform

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=databricks&logoColor=white)

## 🎯 Project Overview

Built a **production-grade data lakehouse** on Databricks implementing the **Medallion Architecture** (Bronze → Silver → Gold) for airline operations analytics. This project demonstrates expertise in **cloud data engineering**, **real-time data processing**, and **AI-powered analytics**.

### Key Highlights
- ✅ Ingested **400K+ flight records** using streaming tables
- ✅ Implemented **3-layer medallion architecture** with data quality checks
- ✅ Built **AI/BI dashboards** with natural language querying (Genie)
- ✅ Applied **AI classification** using Databricks AI Functions
- ✅ Orchestrated **end-to-end workflows** with Lakeflow Jobs
- ✅ Established **data governance** with Unity Catalog (PK/FK constraints)

---

## 🏗️ Architecture

### Medallion Lakehouse Architecture
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  Raw Data   │     │  Cleansed   │     │ Aggregated  │
│  Streaming  │     │   + PK/FK   │     │  Business   │
└─────────────┘     └─────────────┘     └─────────────┘
      ▲                    ▲                    ▲
      │                    │                    │
   CSV Files      Materialized Views     AI/BI Dashboards
```

### Tech Stack
- **Platform:** Databricks Lakehouse (Unity Catalog)
- **Compute:** Serverless SQL Warehouses
- **Storage:** Delta Lake with streaming tables
- **Orchestration:** Lakeflow Jobs
- **Analytics:** Databricks SQL Editor, AI/BI Genie
- **Governance:** Unity Catalog (row-level, column-level permissions)

---

## 📊 Datasets

**Source:** Airline operations data (2007-2024)
- **Flights:** 400K+ records (origin, destination, delays, carriers)
- **Airports:** 300+ US airports (city, state, IATA codes)
- **Lookup Codes:** Airline carrier mappings

---

## 🚀 Implementation Steps

### 1. Data Ingestion (Bronze Layer)
Created **streaming tables** for continuous data ingestion:
- `airports_bronze_st`
- `flights_bronze_st`
- `lookupcodes_bronze_st`

**SQL:**
```sql
CREATE OR REFRESH STREAMING TABLE flights_bronze_st AS
SELECT * FROM STREAM read_files('/Volumes/.../flights');
```

### 2. Data Transformation (Silver Layer)
Built **materialized views** with:
- Data validation (null checks)
- Primary/Foreign key constraints
- Schema standardization

**Example:**
```sql
CREATE MATERIALIZED VIEW flights_silver_mv (
  FlightNum, Origin, Dest, Year, Month, ...,
  CONSTRAINT fk_flights FOREIGN KEY (UniqueCarrier) 
    REFERENCES lookupcodes_silver_mv(UniqueCode)
) AS SELECT ... FROM flights_bronze_st WHERE FlightNum IS NOT NULL;
```

### 3. Business Intelligence (Gold Layer)
Created aggregated views for analytics:
```sql
CREATE MATERIALIZED VIEW airports_by_city_mv AS
SELECT City, COUNT(*) AS number_of_airports
FROM airports_silver_mv
GROUP BY City;
```

### 4. AI Integration
Applied **AI classification** using Databricks AI Functions:
```sql
SELECT *, ai_classify(Description, 
  ARRAY('Full Service', 'Low Cost', 'Regional', 'Charter')
) AS airline_category
FROM lookupcodes_silver_mv;
```

### 5. Workflow Automation
Orchestrated end-to-end pipeline with **Lakeflow Jobs**:
1. Refresh streaming tables (Bronze)
2. Replace materialized views (Silver)
3. Check alerts (threshold monitoring)
4. Refresh dashboards (Gold)

---

## 📈 Dashboards & Analytics

### Interactive AI/BI Dashboard
Built multi-page dashboard featuring:
- **Flight delay analysis** by carrier, route, time period
- **Cross-filtering** for interactive exploration
- **Parameter filters** for dynamic filtering
- **Natural language queries** via Databricks Genie

![Dashboard Screenshot](screenshots/dashboard_overview.png)

### Databricks Genie (Natural Language Analytics)
Enabled business users to query data using plain English:
- "Which airlines are delayed the most? Top 5 by average delay"
- "How many flights from Boston were delayed in March 2008?"
- Genie auto-generates SQL + visualizations

![Genie Conversation](screenshots/genie_conversation.png)

---

## 🔐 Data Governance

### Unity Catalog Implementation
- **Catalog-level:** `dbsql_<firstname>_<lastname>`
- **Schema-level:** `demo`
- **Table-level:** Row/column permissions via Unity Catalog
- **Audit logging:** Query history tracked for 30 days

### Data Quality
- Primary key constraints (airports, lookupcodes)
- Foreign key constraints (flights → lookupcodes)
- Null validation in Silver layer
- AI-generated documentation for all objects

---

## 📊 Query Performance

### Optimization Techniques
- **Materialized views** for expensive aggregations
- **Streaming tables** for incremental processing
- **Query profiling** to identify bottlenecks
- **Serverless compute** for auto-scaling

**Sample Query Profile:**
![Query Profile](screenshots/query_profile.png)

---

## 🎯 Key Learnings

1. **Medallion Architecture** provides clear separation of concerns (raw → cleansed → aggregated)
2. **Streaming tables** enable real-time ingestion without complex code
3. **AI/BI Genie** democratizes data access for non-technical users
4. **Unity Catalog** simplifies governance at scale
5. **Lakeflow Jobs** orchestrates complex workflows with minimal configuration

---

## 📂 Repository Contents

- **`sql_queries/`** - All SQL code for tables, views, and analytics
- **`screenshots/`** - Dashboard visuals and architecture diagrams
- **`architecture/`** - System design and data flow diagrams

---

## 🛠️ Skills Demonstrated

- ✅ **Data Engineering:** ETL/ELT pipelines, streaming ingestion
- ✅ **SQL Mastery:** Advanced queries, window functions, CTEs
- ✅ **Data Modeling:** Star schema, PK/FK relationships
- ✅ **Cloud Platforms:** Databricks, Delta Lake, Unity Catalog
- ✅ **Data Governance:** Access control, audit logging, lineage tracking
- ✅ **AI Integration:** LLM-powered analytics, AI Functions
- ✅ **Workflow Orchestration:** Lakeflow Jobs, scheduling, alerting

---

## 📧 Contact

**Sahitya Gantala**  
Senior Data Platform Engineer  
📧 sahityagantalausa@gmail.com  
🔗 [LinkedIn](https://linkedin.com/in/sahityagantala)  
📍 Buffalo, NY

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

*Built as part of Databricks SQL Workshop (March 2026)*
```

---

## 📸 **SCREENSHOTS TO INCLUDE**

### **Must-have screenshots (5 minimum):**

**1. Dashboard Overview** (`dashboard_overview.png`)
- Your Flight Data Dashboard showing multiple visualizations
- **How to get:** Screenshot entire dashboard canvas

**2. Genie Conversation** (`genie_conversation.png`)
- Example of you asking Genie questions + responses
- **How to get:** Screenshot Genie chat with 2-3 Q&A exchanges

**3. Data Lineage** (`data_lineage.png`)
- Entity Relationship Diagram or lineage graph
- **How to get:** Catalog Explorer → Lineage tab → Screenshot

**4. Workflow/Lakeflow** (`workflow_lakeflow.png`)
- Your Lakeflow Job showing task dependencies
- **How to get:** Jobs → Your workflow → Screenshot DAG

**5. Query Profile** (`query_profile.png`)
- Performance metrics from query history
- **How to get:** Query History → Select query → Query Profile → Screenshot

---

## 🔗 **PART 2: LINKEDIN POST**

### **LinkedIn Post Template (Copy-Paste & Customize):**
```
🚀 Just completed an end-to-end Data Lakehouse project on Databricks! 

Over the past week, I built a production-grade airline analytics platform implementing the Medallion Architecture (Bronze → Silver → Gold layers) with 400K+ flight records.

🔧 What I built:
✅ Real-time data ingestion using Databricks Streaming Tables
✅ 3-layer data transformation pipeline (Bronze → Silver → Gold)
✅ AI-powered analytics with Databricks Genie (natural language queries!)
✅ Interactive AI/BI Dashboard with cross-filtering & parameters
✅ End-to-end workflow orchestration using Lakeflow Jobs
✅ Data governance with Unity Catalog (PK/FK constraints, permissions)

📊 Key technical highlights:
- Applied AI classification using Databricks AI Functions
- Built materialized views for query optimization
- Implemented streaming tables for incremental processing
- Created entity relationship diagrams for data modeling
- Set up automated alerts and dashboard refresh workflows

💡 Biggest takeaway: Databricks makes it incredibly easy to build 
production-grade data platforms WITHOUT complex infrastructure 
management. The AI/BI capabilities (especially Genie) truly 
democratize data access for non-technical stakeholders.

This project reinforced my skills in:
#DataEngineering #SQL #CloudComputing #Databricks #DataAnalytics 
#MachineLearning #ETL #DataPipelines

Full project code + documentation on GitHub:
[PASTE YOUR GITHUB LINK HERE]

📧 Open to connecting with fellow data engineers and exploring 
opportunities in data platform engineering!

#OpenToWork #DataEngineer #Databricks #LakehouseArchitecture
```

---

### **LinkedIn Post Variations (Pick ONE):**

**Option 1: Technical Focus** (for data engineers/recruiters)
```
🔥 Built a scalable data lakehouse on Databricks implementing 
real-time streaming + AI-powered analytics!

Tech stack: Databricks SQL | Delta Lake | Unity Catalog | PySpark 
| AI/BI Genie | Lakeflow Jobs

Processed 400K+ airline records through Bronze → Silver → Gold 
medallion architecture with full data governance.

Highlights:
✅ Streaming ingestion at scale
✅ Materialized views for performance
✅ AI classification with LLM functions
✅ Natural language analytics (Genie)
✅ Automated workflows + alerting

Project on GitHub: [LINK]

#DataEngineering #Databricks #OpenToWork
```

---

**Option 2: Business Value Focus** (for hiring managers)
```
📊 Just shipped a data analytics platform that lets business users 
query data using plain English—no SQL required!

Built on Databricks with AI/BI Genie, this airline analytics system 
enables stakeholders to ask questions like:

"Which airlines have the most delays?"
"Show me flight patterns for March 2008"

...and get instant visualizations + insights.

Behind the scenes: 400K+ records, streaming ingestion, 3-layer 
data architecture, automated quality checks, and full governance.

This is the future of self-serve analytics. Excited to bring these 
skills to a data engineering team!

Code + docs: [GITHUB LINK]

#DataAnalytics #DataEngineering #OpenToWork #AI #BusinessIntelligence
```

---

**Option 3: Learning Journey Focus** (relatable/authentic)
```
💻 Week of March 11: Completed an intensive Databricks workshop and 
built my first production-grade lakehouse!

Coming from a data engineering background (ex-Deloitte), I wanted 
to validate my Databricks skills with hands-on work.

What I built:
- Real-time data pipeline (streaming tables)
- Multi-layer architecture (Bronze/Silver/Gold)
- AI-powered dashboards with natural language queries
- Automated workflows with monitoring & alerts

The best part? Databricks makes complex data engineering accessible 
without sacrificing enterprise-grade capabilities.

Already applying these learnings to my job search—looking for 
senior data engineering roles where I can build scalable platforms!

GitHub repo: [LINK]

#DataEngineering #CareerDevelopment #OpenToWork #Databricks
```

---

## 🎯 **LINKEDIN POSTING STRATEGY**

### **When to post:**
- **Best time:** Tuesday-Thursday, 8-10 AM EST
- **Why:** Highest LinkedIn engagement during business hours

### **How to maximize reach:**

**1. Use hashtags strategically:**
- **Industry tags:** #DataEngineering, #Databricks, #DataAnalytics
- **Job search tags:** #OpenToWork, #Hiring, #JobSearch
- **Tech tags:** #SQL, #Python, #CloudComputing, #AI
- **Max:** 10-15 hashtags

**2. Tag relevant companies/people:**
- Databricks official account: @Databricks
- Recruiters you're connected with
- HCL, TCS, Infosys, Cognizant accounts (if relevant)

**3. Engage immediately after posting:**
- Reply to every comment within 1 hour
- Like and comment on 5-10 other posts
- LinkedIn algorithm favors active engagement

**4. Pin the post:**
- After posting, click "..." → "Pin to top of profile"
- Keeps it visible for recruiters visiting your profile

---

## 📝 **ADD TO RESUME**

### **Update "Projects" Section:**
```
PROJECTS

Airline Analytics Data Lakehouse | Databricks SQL (March 2026)
- Built end-to-end data platform processing 400K+ flight records using 
  Medallion Architecture (Bronze → Silver → Gold layers)
- Implemented streaming ingestion, materialized views, and AI classification 
  using Databricks AI Functions
- Created interactive AI/BI dashboards with natural language querying (Genie) 
  enabling self-serve analytics for business users
- Orchestrated automated workflows with Lakeflow Jobs including alerts, 
  monitoring, and dashboard refresh
- Established data governance with Unity Catalog (primary/foreign keys, 
  row-level permissions, audit logging)
- Tech Stack: Databricks SQL, Delta Lake, Unity Catalog, SQL, Python
- GitHub: github.com/sahityagantala/databricks-sql-airline-analytics
```

---

## ✅ **EXECUTION CHECKLIST**

### **Day 1 (Today - March 11):**
- [ ] Create GitHub repository
- [ ] Upload README.md (use template above)
- [ ] Create folder structure (sql_queries/, screenshots/, architecture/)
- [ ] Take 5-7 screenshots from Databricks workspace
- [ ] Upload screenshots to GitHub

### **Day 2 (Tomorrow - March 12):**
- [ ] Upload SQL queries to `sql_queries/` folder
- [ ] Create simple architecture diagram (use draw.io or Canva)
- [ ] Add LICENSE file (MIT License)
- [ ] Final README review

### **Day 3 (March 13):**
- [ ] Write LinkedIn post (choose template above)
- [ ] Post on LinkedIn (Tuesday 8-10 AM EST = best time)
- [ ] Engage with comments immediately
- [ ] Pin post to profile
- [ ] Update resume with project

---

## 🎯 **WHY THIS WORKS FOR YOUR JOB SEARCH**

### **For Indian Consulting Firms (TCS, Infosys, HCL, Cognizant):**
✅ Databricks is HIGH DEMAND for their clients  
✅ Shows you're current with modern data stack  
✅ Proves hands-on skills, not just resume claims  
✅ GitHub = professional credibility

### **For Recruiter Conversations:**
When recruiters ask: "What have you been doing since graduation?"

**Before:** "Studying and applying for jobs"  
**After:** "Built a production Databricks lakehouse (link to GitHub)"

### **For Resume:**
Fills "recent experience" gap between:
- Deloitte (Aug 2024 end) → Masters (Dec 2025) → Now (March 2026)

---

## 💡 **ADDITIONAL TIPS**

### **1. Add to Portfolio Site (if you have one):**
- Embed dashboard screenshots
- Link to GitHub
- Write 2-3 sentence summary

### **2. Mention in Cover Letters:**
```
"Recently completed a production-grade Databricks lakehouse project 
(link: github.com/...) demonstrating hands-on expertise in streaming 
data, AI/BI analytics, and data governance—skills directly applicable 
to [Company]'s data engineering team."
