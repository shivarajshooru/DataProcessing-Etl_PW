/* ================================================================================
PART 1: THEORY SECTION 
================================================================================

1. What are the most common reasons for missing data in ETL pipelines?
Ans.
• Source system issues → Data not captured due to faulty sensors, human error, or
incomplete forms.
• Schema changes upstream → Column names or structures change, breaking
extraction.
• Integration errors → API failures, connection timeouts, or mismatched formats during
extraction.
• Data corruption → Loss during transfer because of network latency or hardware failures.
• Workflow bottlenecks → Jobs fail mid-process due to resource limits or poor
orchestration.
• Business rules → Certain fields intentionally left blank (e.g., optional attributes).

2. Why is blindly deleting rows with missing values considered a bad practice in ETL?
Ans.
• Loss of valuable information → Entire records are discarded even if most fields are
valid.
• Bias in analysis → If missingness is systematic (e.g., certain regions or customer
groups), deletion skews results.
• Reduced sample size → Smaller datasets weaken statistical power and model
accuracy.
• Business impact → Important insights (like why data is missing) are lost, which could
itself be meaningful.

3. Q3. Explain the difference between:
• Listwise deletion
• Column deletion
Also mention one scenario where each is appropriate.
Ans.
Listwise Deletion
• Removes entire rows if any required field is missing.
• Preserves schema but reduces dataset size.
• Scenario: Customer survey data where multiple answers are missing → better to drop
the whole record to avoid incomplete profiles.
Column Deletion
• Removes entire columns if they have too many missing values.
• Preserves row count but loses that variable.
• Scenario: A dataset where “Middle Name” is missing for 95% of customers → dropping
the column avoids clutter without harming analysis.

4. Why is median imputation preferred over mean imputation for skewed data such as
income?
Ans.
• Mean imputation → Sensitive to extreme values (outliers). In skewed distributions like
income, a few very high salaries can inflate the mean, making it unrepresentative.
• Median imputation → More robust because the median is the middle value, unaffected
by outliers. It better reflects the “typical” case in skewed datasets.
• Example: If most incomes are around 50,000 but a few are above 1,000,000, the mean
will be distorted, while the median stays close to the majority.

5. What is forward fill and in what type of dataset is it most useful?
Ans.
• Forward Fill (FFILL):
A technique where missing values are replaced with the last valid observation carried
forward.
• Usefulness:
o Best suited for time-series or sequential datasets (e.g., monthly sales, stock
prices, sensor readings).
o Assumes continuity — the last known value is a reasonable proxy until a new
one appears.
• Example:
If sales data for March is missing but February had 12,000, forward fill assigns March =
12,000 until April’s actual figure arrives.

6. Why should flagging missing values be done before imputation in an ETL workflow?
Ans.
• Preserves information about missingness → The fact that a value is missing can itself
carry business meaning (e.g., customers not disclosing income).
• Avoids loss of transparency → Once imputation is applied, you can’t distinguish
between original and filled values unless flagged beforehand.
• Supports better analysis → Analysts can study patterns of missingness separately (e.g.,
which regions or groups have more missing data).
• Improves model accuracy → Machine learning models can use the flag as an additional
feature, helping them account for missingness bias.

7. Consider a scenario where income is missing for many customers. How can this
missingness itself provide business insights?
Ans.
• Customer reluctance → Missing income may indicate customers are unwilling to
disclose sensitive financial details, signaling trust or privacy concerns.
• Segment behavior → Certain demographics or regions may have higher missingness,
revealing cultural or socio-economic differences.
• Product targeting → If high-value customers avoid sharing income, it may suggest they
don’t see relevance in providing it, guiding product design or survey strategy.
• Operational gaps → Consistent missingness could highlight flaws in data collection


*/

-- ================================================================================
-- PART 2: PRACTICAL SECTION (Runnable Code)
-- ================================================================================

-- --------------------------------------------------------------------------------
-- SETUP: Reset Data
-- --------------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS temp_customers;
CREATE TEMPORARY TABLE temp_customers (
    Customer_ID INT, Name VARCHAR(50), City VARCHAR(50), 
    Monthly_Sales INT, Income INT, Region VARCHAR(50)
);

INSERT INTO temp_customers VALUES 
(101, 'Rahul', 'Mumbai', 12000, 65000, 'West'),
(102, 'Anjali', 'Bengaluru', NULL, NULL, 'South'),
(103, 'Suresh', 'Chennai', 15000, 72000, 'South'),
(104, 'Neha', 'Delhi', NULL, NULL, 'North'),
(105, 'Amit', 'Pune', 18000, 58000, NULL),
(106, 'Karan', 'Ahmedabad', NULL, 61000, 'West'),
(107, 'Pooja', 'Kolkata', 14000, NULL, 'East'),
(108, 'Riya', 'Jaipur', 16000, 69000, 'North');


-- --------------------------------------------------------------------------------
-- [Q8. Listwise Deletion]
-- Task: Remove all rows where Region is missing.
-- --------------------------------------------------------------------------------

-- Task 1: Identify affected rows (Corrected Line)
SELECT * FROM temp_customers WHERE Region IS NULL;

-- Task 3: Count records to be lost
SELECT COUNT(*) AS Total_Records_Lost FROM temp_customers WHERE Region IS NULL;

-- Task 2: Perform Deletion
DELETE FROM temp_customers WHERE Region IS NULL;

-- Show Dataset after deletion
SELECT * FROM temp_customers AS Q8_Final_Dataset;


-- --------------------------------------------------------------------------------
-- [Q9. Imputation - Forward Fill]
-- Task: Handle missing Monthly_Sales using Forward Fill.
-- Why Forward Fill is suitable here?
-- Answer: Monthly sales data usually follows a trend. Forward fill assumes the 
-- value hasn't changed, which is safer than averaging in time-series data.
-- --------------------------------------------------------------------------------

-- Reset Data for Q9
TRUNCATE TABLE temp_customers;
INSERT INTO temp_customers VALUES 
(101, 'Rahul', 'Mumbai', 12000, 65000, 'West'), (102, 'Anjali', 'Bengaluru', NULL, NULL, 'South'),
(103, 'Suresh', 'Chennai', 15000, 72000, 'South'), (104, 'Neha', 'Delhi', NULL, NULL, 'North'),
(105, 'Amit', 'Pune', 18000, 58000, NULL), (106, 'Karan', 'Ahmedabad', NULL, 61000, 'West'),
(107, 'Pooja', 'Kolkata', 14000, NULL, 'East'), (108, 'Riya', 'Jaipur', 16000, 69000, 'North');

-- Logic: Forward Fill using Variables
SET @last_sales = 0;

UPDATE temp_customers 
SET Monthly_Sales = (
    CASE 
        WHEN Monthly_Sales IS NOT NULL THEN @last_sales := Monthly_Sales 
        ELSE @last_sales 
    END
)
ORDER BY Customer_ID;

SELECT * FROM temp_customers AS Q9_Final_Dataset;


-- --------------------------------------------------------------------------------
-- [Q10. Flagging Missing Data]
-- Task: Create a flag column for missing Income.
-- --------------------------------------------------------------------------------

-- Reset Data for Q10
TRUNCATE TABLE temp_customers;
INSERT INTO temp_customers VALUES 
(101, 'Rahul', 'Mumbai', 12000, 65000, 'West'), (102, 'Anjali', 'Bengaluru', NULL, NULL, 'South'),
(103, 'Suresh', 'Chennai', 15000, 72000, 'South'), (104, 'Neha', 'Delhi', NULL, NULL, 'North'),
(105, 'Amit', 'Pune', 18000, 58000, NULL), (106, 'Karan', 'Ahmedabad', NULL, 61000, 'West'),
(107, 'Pooja', 'Kolkata', 14000, NULL, 'East'), (108, 'Riya', 'Jaipur', 16000, 69000, 'North');

-- Task 1: Add Flag Column
ALTER TABLE temp_customers ADD COLUMN Income_Missing_Flag INT;

-- Task 2: Set Flag (1 if missing, 0 if present)
UPDATE temp_customers 
SET Income_Missing_Flag = CASE WHEN Income IS NULL THEN 1 ELSE 0 END;

-- Task 3 & 4: Show updated dataset and Count
SELECT * FROM temp_customers AS Q10_Final_Dataset;

SELECT COUNT(*) as Total_Missing_Incomes 
FROM temp_customers 
WHERE Income_Missing_Flag = 1;

-- Final Step: Enable Safe Update Mode
SET SQL_SAFE_UPDATES = 1;
