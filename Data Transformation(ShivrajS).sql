/* ================================================================================
PART 1: THEORY SECTION (Commented Out)
================================================================================
[Question 1: Define Data Transformation]
Answer: Data Transformation is the process of converting data from one format 
or structure into another format suitable for the target system. 

[Question 2: Data Cleaning Activities]
Answer:
1. Removing duplicate records.
2. Handling missing values.
3. Correcting structural errors.
4. Filtering outliers.

[Question 3: Normalization vs Standardization]
Answer: Normalization scales data to a range (0-1). Standardization transforms 
data to have a mean of 0 and standard deviation of 1.

[Question 7: Data Integration vs Mapping]
Answer: Data Integration combines data from multiple sources. Data Mapping links 
source fields to target fields.

[Question 8: Z-score vs Min-Max Scaling]
Answer: Z-score is preferred when outliers exist because Min-Max scaling compresses 
data too much if extreme values are present.
*/

-- ================================================================================
-- PART 2: PRACTICAL SECTION (Runnable Code)
-- ================================================================================

-- ------------------------------------------------------------------
-- [Question 4: Handling Missing Age]
-- ------------------------------------------------------------------

-- Step 1: Create dummy data
DROP TEMPORARY TABLE IF EXISTS temp_users_age;
CREATE TEMPORARY TABLE temp_users_age (id INT, age INT);
INSERT INTO temp_users_age VALUES (1, 25), (2, NULL), (3, 35), (4, NULL), (5, 30);

-- Step 2: Calculate Average and store in a Variable (@avg_age)
SET @avg_age = (SELECT AVG(age) FROM temp_users_age);

-- Step 3: Update NULL values using the variable
UPDATE temp_users_age 
SET age = @avg_age 
WHERE age IS NULL;

-- Step 4: Show Final Result
SELECT * FROM temp_users_age;


-- ------------------------------------------------------------------
-- [Question 5: Gender Standardization]
-- ------------------------------------------------------------------

DROP TEMPORARY TABLE IF EXISTS temp_gender;
CREATE TEMPORARY TABLE temp_gender (id INT, raw_gender VARCHAR(10));
INSERT INTO temp_gender VALUES (1, 'M'), (2, 'male'), (3, 'Female'), (4, 'f'), (5, 'MALE');

SELECT 
    id, 
    raw_gender,
    CASE 
        WHEN raw_gender IN ('M', 'male', 'MALE') THEN 'Male'
        WHEN raw_gender IN ('F', 'Female', 'f') THEN 'Female'
        ELSE 'Other'
    END AS Standardized_Gender
FROM temp_gender;


-- ------------------------------------------------------------------
-- [Question 6: One-Hot Encoding]
-- ------------------------------------------------------------------

DROP TEMPORARY TABLE IF EXISTS temp_colors;
CREATE TEMPORARY TABLE temp_colors (id INT, color VARCHAR(10));
INSERT INTO temp_colors VALUES (1, 'Red'), (2, 'Blue'), (3, 'Green'), (4, 'Red');

SELECT 
    id,
    color,
    IF(color = 'Red', 1, 0) AS Is_Red,
    IF(color = 'Blue', 1, 0) AS Is_Blue,
    IF(color = 'Green', 1, 0) AS Is_Green
FROM temp_colors;

-- Step End: Re-enable Safe Update Mode (Optional but good practice)
SET SQL_SAFE_UPDATES = 1;
