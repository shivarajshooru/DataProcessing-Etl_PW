-- Data Loading (ETL)
/*
Q1. DATA UNDERSTANDING: Identify all data quality issues.
ANSWER: The following data quality issues are present in the dataset.
- Duplicate Records: Order_ID '0101' appears twice.
- Missing Values: Order_ID '0102' has a 'Null' Sales_Amount.
- Data Type Mismatch: Order_ID '0104' contains text ('Three Thousand') instead of numbers.
- Inconsistent Date Formats: Mixed formats like DD-MM-YYYY ('12-01-2024') and YYYY/MM/DD ('2024/01/18') are used.

Q2. PRIMARY KEY VALIDATION 
a) Is the dataset violating the Primary Key rule? YES.
b) Which record(s) cause this violation? Order_ID '0101' because it is a duplicate.

Q3. MISSING VALUE ANALYSIS.
- Column(s) with missing values: Sales_Amount.
a) Affected records: Order_ID '0102'.
b) Risk: Loading nulls can lead to incorrect financial totals and may cause system crashes if the column has a 'NOT NULL' constraint.

Q4. DATA TYPE VALIDATION.
a) Failed records: Order_ID '0104' ('Three Thousand').
b) SQL Impact: Loading this into a DECIMAL column will cause an "Invalid numeric value" error and the load will fail.

Q5. DATE FORMAT CONSISTENCY.
a) Date formats: DD-MM-YYYY (e.g., 12-01-2024) and YYYY/MM/DD (e.g., 2024/01/18).
b) Problem: Inconsistent formats prevent correct sorting and cause errors in reporting tools during filtering.

Q6. LOAD READINESS DECISION.
a) Load directly? NO.
b) Justification: 1. Duplicate Primary Keys.
2. Null values in critical fields, 
3. Data type mismatch.

Q7. PRE-LOAD VALIDATION CHECKLIST.
- Uniqueness Check: Ensure Order_ID is unique.
- Null Value Check: Identify mandatory fields with missing values.
- Data Type Check: Ensure Sales_Amount is numeric.
- Date Format Validation: Verify consistent date structures.

Q8. CLEANING STRATEGY.
- Step 1: Remove or merge duplicate records for Order_ID '0101'.
- Step 2: Handle the Null value for Order_ID '0102'.
- Step 3: Convert the string 'Three Thousand' to the numeric value 3000.
- Step 4: Transform all dates into a standardized format (YYYY-MM-DD).

Q9. LOADING STRATEGY SELECTION.
a) Choice: Incremental Load.
b) Justification: It is more efficient for daily sales data as it only loads new or changed records.

Q10. BI IMPACT SCENARIO.
a) Incorrect Result: Total Sales KPI will be higher than actual due to duplicate counting.
b) Misleading records: '0101' (duplicates) and '0104' (incorrect type).
c) BI Limit: BI tools simply display data; they do not automatically detect logic or entry errors without pre-defined rules.
*/

-- Query to find duplicate Order_IDs
SELECT Order_ID, COUNT(*) 
FROM Sales_Data 
GROUP BY Order_ID 
HAVING COUNT(*) > 1;

-- Query to detect non-numeric Sales_Amount
SELECT * FROM Sales_Data 
WHERE Sales_Amount REGEXP '[^0-9]';

-- Query to detect NULL values
SELECT * FROM Sales_Data 
WHERE Sales_Amount IS NULL;