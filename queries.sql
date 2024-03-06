1) **Total Sales by Product**
SELECT
    p.ProductID,
    p.Name AS ProductName,
    SUM(sf.Quantity * sf.Price) AS TotalSalesAmount
FROM
    Product_Dim p
JOIN
    Sales_Fact sf ON p.ProductID = sf.ProductID
GROUP BY
    p.ProductID, p.Name
ORDER BY
    TotalSalesAmount DESC;

2) **Sales by Month and Channel** 
SELECT
    EXTRACT(MONTH FROM d.Date) AS Month,
    r.Channel,
    SUM(sf.Quantity) AS TotalQuantity,
    SUM(sf.Quantity * sf.Price) AS TotalSalesAmount
FROM
    Sales_Fact sf
JOIN
    Date_Dim d ON sf.Date = d.Date
JOIN
    Retailer_Dim r ON sf.RetailerID = r.RetailerID
GROUP BY
    EXTRACT(MONTH FROM d.Date), r.Channel
ORDER BY
    Month, Channel;


3)**Top Selling Product by Category for Each Retailer**

WITH RankedProducts AS (
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        p.Category,
        r.Name AS RetailerName,
        SUM(sf.Quantity * sf.Price) AS TotalSalesAmount,
        ROW_NUMBER() OVER (PARTITION BY r.RetailerID, p.Category ORDER BY SUM(sf.Quantity * sf.Price) DESC) AS Rank
    FROM
        Product_Dim p
    JOIN
        Sales_Fact sf ON p.ProductID = sf.ProductID
    JOIN
        Retailer_Dim r ON sf.RetailerID = r.RetailerID
    GROUP BY
        p.ProductID, p.Name, p.Category, r.Name
)
SELECT
    ProductID,
    ProductName,
    Category,
    RetailerName,
    TotalSalesAmount
FROM
    RankedProducts
WHERE
    Rank = 1;
