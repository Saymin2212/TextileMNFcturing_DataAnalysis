Create database MNF_Analytics;
use MNF_Analytics;
select * from  manufacturingdata_analytics;
ALTER TABLE `MNF_Analytic`
RENAME TO manufacturingdata_analytics;

#1  Join with machine info to find the highest rejection rate machines per department
SELECT 
    m.`Department Name`,
    mi.`Machine Code`,
    mi.`Operation Name`,
    ROUND(AVG(CAST(REPLACE(m.`Rejection Rate`, '%', '') AS DECIMAL(10,2))), 2) AS Avg_Rejection
FROM manufacturingdata_analytics m
JOIN (
    SELECT DISTINCT `Machine Code`, `Operation Name`
    FROM manufacturingdata_analytics
) mi
    ON m.`Machine Code` = mi.`Machine Code`
GROUP BY m.`Department Name`, mi.`Machine Code`, mi.`Operation Name`
ORDER BY Avg_Rejection DESC;

#2 Window function to calculate running cumulative production quantity per buyer
SELECT 
    Buyer,
    `Doc Date`,
    SUM(`Produced Qty`) OVER (
        PARTITION BY Buyer 
        ORDER BY `Doc Date`
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Produced
FROM manufacturingdata_analytics
ORDER BY Buyer, `Doc Date`;



#3 Subquery to find buyers whose average cost per unit is above the overall average
SELECT 
    Buyer, 
    ROUND(AVG(`Cost per Unit`), 2) AS Avg_Cost_Per_Unit
FROM manufacturingdata_analytics
GROUP BY Buyer
HAVING AVG(`Cost per Unit`) > (
    SELECT AVG(`Cost per Unit`) 
    FROM manufacturingdata_analytics
);


 #4 Find the top 3 buyers in each year by total production quantity
WITH RankedBuyers AS (
    SELECT 
        Buyer,
        YEAR,
        SUM(TotalQty) AS Yearly_Total_Qty,
        RANK() OVER (PARTITION BY YEAR ORDER BY SUM(TotalQty) DESC) AS rnk
    FROM manufacturingdata_analytics
    GROUP BY Buyer, YEAR
)
SELECT Buyer, YEAR, Yearly_Total_Qty
FROM RankedBuyers
WHERE rnk <= 3
ORDER BY YEAR, Yearly_Total_Qty DESC;


#5 Calculate cumulative production quantity per buyer across months (running total)
SELECT 
    Buyer,
    `month-yr`,
    SUM(`Produced Qty`) AS Monthly_Produced,
    SUM(SUM(`Produced Qty`)) OVER (
        PARTITION BY Buyer 
        ORDER BY STR_TO_DATE(`month-yr`, '%y-%b')
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cumulative_Produced
FROM manufacturingdata_analytics
GROUP BY Buyer, `month-yr`
ORDER BY Buyer, STR_TO_DATE(`month-yr`, '%y-%b');



#6 Identify buyers whose rejection rate improved every quarter within a year
WITH quarterly_rates AS (
    SELECT 
        Buyer,
        `YEAR`,
        `QUARTER`,
        ROUND(AVG(CAST(REPLACE(`Rejection Rate`, '%', '') AS DECIMAL(5,2))), 2) AS Avg_Rejection
    FROM manufacturingdata_analytics
    GROUP BY Buyer, `YEAR`, `QUARTER`
),
ranked AS (
    SELECT 
        Buyer,
        `YEAR`,
        `QUARTER`,
        Avg_Rejection,
        LAG(Avg_Rejection) OVER (PARTITION BY Buyer, `YEAR` ORDER BY `QUARTER`) AS Prev_Quarter_Rate
    FROM quarterly_rates
)
SELECT Buyer, `YEAR`, COUNT(*) AS Improving_Quarters
FROM ranked
WHERE Prev_Quarter_Rate IS NOT NULL
  AND Avg_Rejection < Prev_Quarter_Rate
GROUP BY Buyer, `YEAR`
ORDER BY Improving_Quarters DESC;



#7 Find the top 5 customers with the highest total cost in each year
WITH YearlyCustomerCost AS (
    SELECT 
        YEAR(`Doc Date`) AS Year,
        `Cust Name`,
        SUM(`Cost per Unit` * `Produced Qty`) AS Total_Cost
    FROM manufacturingdata_analytics
    GROUP BY YEAR(`Doc Date`), `Cust Name`
)
SELECT *
FROM (
    SELECT 
        Year,
        `Cust Name`,
        Total_Cost,
        RANK() OVER (PARTITION BY Year ORDER BY Total_Cost DESC) AS rnk
    FROM YearlyCustomerCost
) ranked
WHERE rnk <= 5
ORDER BY Year, Total_Cost DESC;

#8 Calculate the month-over-month % change in production quantity per department
WITH MonthlyData AS (
    SELECT 
        `Department Name`,
        DATE_FORMAT(`Doc Date`, '%Y-%m') AS Month,
        SUM(`Produced Qty`) AS Monthly_Qty
    FROM manufacturingdata_analytics
    GROUP BY `Department Name`, DATE_FORMAT(`Doc Date`, '%Y-%m')
)
SELECT 
    `Department Name`,
    Month,
    Monthly_Qty,
    ROUND(
        (Monthly_Qty - LAG(Monthly_Qty) OVER (PARTITION BY `Department Name` ORDER BY Month))
        / NULLIF(LAG(Monthly_Qty) OVER (PARTITION BY `Department Name` ORDER BY Month), 0) * 100,
        2
    ) AS MoM_Percent_Change
FROM MonthlyData
ORDER BY `Department Name`, Month;

 #9 Detect buyers with increasing production quantity for 3 consecutive months
WITH MonthlyBuyerQty AS (
    SELECT 
        Buyer,
        DATE_FORMAT(`Doc Date`, '%Y-%m') AS Month,
        SUM(`Produced Qty`) AS Qty
    FROM manufacturingdata_analytics
    GROUP BY Buyer, DATE_FORMAT(`Doc Date`, '%Y-%m')
),
Ranked AS (
    SELECT *,
        LAG(Qty, 1) OVER (PARTITION BY Buyer ORDER BY Month) AS prev1,
        LAG(Qty, 2) OVER (PARTITION BY Buyer ORDER BY Month) AS prev2
    FROM MonthlyBuyerQty
)
SELECT Buyer, Month, Qty
FROM Ranked
WHERE Qty > prev1 AND prev1 > prev2
ORDER BY Buyer, Month;

#10 Find the rejection rate variance per department and highlight the top 3
SELECT 
    `Department Name`,
    ROUND(VAR_POP(CAST(REPLACE(`Rejection Rate`, '%', '') AS DECIMAL(10,2))), 2) AS Rejection_Variance
FROM manufacturingdata_analytics
GROUP BY `Department Name`
ORDER BY Rejection_Variance DESC
LIMIT 3;

#11 Find the rejection rate trend (increase/decrease) per buyer between first and last month in the dataset
WITH BuyerRates AS (
    SELECT 
        Buyer,
        DATE_FORMAT(`Doc Date`, '%Y-%m') AS Month,
        AVG(CAST(REPLACE(`Rejection Rate`, '%', '') AS DECIMAL(10,2))) AS Avg_Rejection
    FROM manufacturingdata_analytics
    GROUP BY Buyer, DATE_FORMAT(`Doc Date`, '%Y-%m')
),
FirstLast AS (
    SELECT 
        Buyer,
        FIRST_VALUE(Avg_Rejection) OVER (PARTITION BY Buyer ORDER BY Month) AS First_Rate,
        LAST_VALUE(Avg_Rejection) OVER (PARTITION BY Buyer ORDER BY Month 
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Last_Rate
    FROM BuyerRates
)
SELECT DISTINCT Buyer,
       First_Rate,
       Last_Rate,
       CASE 
           WHEN Last_Rate > First_Rate THEN 'Increased'
           WHEN Last_Rate < First_Rate THEN 'Decreased'
           ELSE 'No Change'
       END AS Trend
FROM FirstLast
ORDER BY Buyer;


#12 Join Across Departments & Machines
SELECT 
    m.`Department Name`,
    mi.`Machine Code`,
    mi.`Operation Name`,
    ROUND(AVG(CAST(REPLACE(m.`Rejection Rate`, '%', '') AS DECIMAL(5,2))), 2) AS Avg_Rejection,
    SUM(m.`Produced Qty`) AS Total_Produced,
    SUM(m.`Rejected Qty`) AS Total_Rejected
FROM manufacturingdata_analytics m
JOIN (
    SELECT DISTINCT `Machine Code`, `Operation Name`
    FROM manufacturingdata_analytics
) mi 
    ON m.`Machine Code` = mi.`Machine Code`
GROUP BY m.`Department Name`, mi.`Machine Code`, mi.`Operation Name`
ORDER BY Avg_Rejection DESC;


#13 Seasonal Trend (Year + Month)
SELECT 
    `YEAR`,
    `MONTH`,
    SUM(`Produced Qty`) AS Total_Produced,
    SUM(`Rejected Qty`) AS Total_Rejected,
    ROUND(AVG(CAST(REPLACE(`Rejection Rate`, '%', '') AS DECIMAL(5,2))), 2) AS Avg_Rejection
FROM manufacturingdata_analytics
GROUP BY `YEAR`, `MONTH`
ORDER BY `YEAR`, 
    FIELD(`MONTH`, 'January','February','March','April','May','June','July','August','September','October','November','December');

