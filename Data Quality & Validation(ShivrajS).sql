-- Data Quality & Validation
-- Question 1 : Define Data Quality in the context of ETL pipelines. Why is it more than just data cleaning?
/*
What is Data Quality in ETL?
Data Quality in the context of ETL pipelines refers to the accuracy, consistency, completeness, reliability,
and timeliness of data being extracted, transformed, and loaded into a target system (like a data warehouse).
It ensures that the data is fit for purpose—meaning it can be trusted for analytics, reporting, and 
decision-making.

Why Data Quality is More Than Just Data Cleaning
Many people equate data quality with simply “cleaning” data (removing duplicates, fixing formats, handling missing
values). But in ETL, data quality is a broader discipline that goes beyond cleaning: accuracy, consistency, 
completeness, reliability, and timeliness 

				
-- Question 2 :  Explain why poor data quality leads to misleading dashboards and incorrect decisions.
 Poor data quality directly undermines the reliability of dashboards and the decisions based on them. Here’s why:
Inaccurate Metrics, Incomplete or Missing Data, Inconsistent Data Across Sources, Delayed or Outdated Data	
Executives and analysts trust dashboards as single sources of truth.

Poor data quality means the “truth” is distorted, leading to:
Overestimating profits or underestimating risks.
Misallocating resources (e.g., marketing spend, staffing).
Failing to detect problems early (e.g., rising churn, declining sales).	
Dashboards are only as good as the data behind them. If data quality is poor, the visualizations look 
convincing but tell the wrong story—causing leaders to make decisions based on illusion rather than reality.			


-- Question 3 : What is duplicate data? Explain three causes in ETL pipelines.

Duplicate data refers to records that appear more than once in a dataset, either fully identical or 
partially overlapping. In ETL pipelines, duplicates reduce data quality, inflate metrics, and can lead 
to misleading analytics or reporting.
Three Common Causes of Duplicate Data in ETL Pipelines
Multiple Source Systems
When data is extracted from different systems, the same entity may be recorded multiple times.
Example: A customer exists in both the CRM and ERP system, leading to duplicate entries in the warehouse.

Improper Join or Merge Logic
During transformation, incorrect SQL joins or merge rules can create duplicates.
Example: Joining Orders and Customers tables without proper keys may duplicate rows if customers have 
multiple matching records.

Incremental Loads Without Deduplication
In incremental extraction, if new data is appended without checking for existing records, duplicates accumulate.
Example: Daily sales extracts include overlapping transactions, and the ETL job appends them without validation.

Duplicates distort KPIs (e.g., customer counts, revenue totals).
They increase storage costs and slow down queries.
They undermine trust in dashboards and analytics.	


-- Question 4 : Differentiate between exact, partial, and fuzzy duplicates.

Exact Duplicates
Definition: Records that are completely identical across all fields.
Cause: Multiple inserts of the same record, or repeated extraction from source systems.
Impact: Inflates counts and metrics (e.g., customer totals, sales).

Partial Duplicates
Definition: Records that share some fields but differ in others.
Cause: Inconsistent data entry, updates not synchronized across systems.
Impact: Creates ambiguity—hard to tell which record is correct or most recent.

Fuzzy Duplicates
Definition: Records that are not identical but represent the same entity, 
detected through similarity rather than exact matches.
Cause: Typos, spelling variations, different formats (e.g., “NY” vs “New York”).
Impact: Requires advanced matching techniques (fuzzy logic, string similarity, machine learning) 
to identify and resolve.

-- Question 5 : Why should data validation be performed during transformation rather than after loading?

Data validation is the process of checking whether extracted data meets business rules, quality standards,
and consistency requirements. In ETL pipelines, validation is most effective during the transformation stage, 
rather than after loading into the target system. Here’s why: Prevents Propagation of Errors, Reduces Cost 
of Correction, Ensures Business Rule Compliance, Improves Data Trustworthiness.

-- Question 6 : Explain how business rules help in validating data accuracy. Give an example.

Business rules are predefined conditions or logic that reflect how an organization expects its data to behave. 
In ETL pipelines, they act as filters and checkpoints to ensure that extracted and transformed data is 
accurate, consistent, and aligned with business requirements.
Business Rules Help Validate Data Accuracy: Enforce Consistency, Ensure Logical Accuracy, 
Maintain Referential Integrity, Support Compliance.
*/

CREATE TABLE Customers_Master (
    CustomerID VARCHAR(10) PRIMARY KEY,
    CustomerName VARCHAR(50),
    City VARCHAR(50)
);

CREATE TABLE Sales_Transactions (
    Txn_ID INT PRIMARY KEY,
    Customer_ID VARCHAR(10),
    Customer_Name VARCHAR(50),
    Product_ID VARCHAR(10),
    Quantity INT,
    Txn_Amount INT,
    Txn_Date DATE,
    City VARCHAR(50)
);
INSERT  INTO Customers_Master VALUES ('C101','Rahul Mehta','Mumbai'), ('C102','Anjali Rao','Bengaluru'),
('C103','Suresh lyer','Chennai'), ('C104','Neha Singh','Delhi');

INSERT INTO Sales_Transactions VALUES 
(201,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai'),
(202,'C102','Anjali Rao','P12',1,1500,'2025-12-01','Bengaluru'),
(203,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai'),
(204,'C103','Suresh lyer','P13',3,6000,'2025-12-02','Chennai'),
(205,'C104','Neha Singh','P14',NULL,2500,'2025-12-02','Delhi'),
(206,'C105','N/A','P15',1,NULL,'2025-12-03','Pune'),
(207,'C106','Amit Verma','P16',1,1800,NULL,'Pune'),
(208,'C101','Rahul Mehta','P11',2,4000,'2025-12-01','Mumbai');

-- Question 7 : Write an SQL query on to list all duplicate keys and their counts using the business key (Customer_ID + Product_ID + Txn_Date + Txn_Amount )

SELECT 
    Customer_ID, 
    Product_ID, 
    Txn_Date, 
    Txn_Amount, 
    COUNT(*) as Dup_Count
FROM Sales_Transactions
GROUP BY Customer_ID, Product_ID, Txn_Date, Txn_Amount
HAVING COUNT(*) > 1;

-- Question 8 : Enforcing Referential Integrity Identify Sales_Transactions.Customer_ID values that violate referential integrity when joined with Customers_Master and write a query to detect such violations.
SELECT DISTINCT st.Customer_ID
FROM Sales_Transactions st
LEFT JOIN Customers_Master cm ON st.Customer_ID = cm.CustomerID
WHERE cm.CustomerID IS NULL;