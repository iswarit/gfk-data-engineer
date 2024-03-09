-- 1) Total Sales by Product

SELECT
    p.Name,
    SUM(f.Price * f.Quantity) AS TotalSales
FROM
    Sales_Fact f
    JOIN Product_Dim p ON f.ProductID = p.ProductID
GROUP BY
    p.Name
ORDER BY
    TotalSales DESC;

-- 2) Sales by Month and Channel

SELECT
    EXTRACT(MONTH FROM d.Date) AS Month,
    r.Channel,
    SUM(f.Price * f.Quantity) AS TotalSales,
    SUM(f.Quantity) AS TotalQuantity
FROM
    Sales_Fact f
    JOIN Date_Dim d ON f.Date = d.Date
    JOIN Retailer_Dim r ON f.RetailerID = r.RetailerID
GROUP BY
    EXTRACT(MONTH FROM d.Date),
    r.Channel
ORDER BY
    Month,
    Channel;

-- 3) Top Selling Product by Category for Each Retailer

WITH ranking AS (
    SELECT
        r.Name AS RetailerName,
        p.Category,
        p.Name AS ProductName,
        SUM(f.Price * f.Quantity) AS TotalSales,
        RANK() OVER (PARTITION BY r.Name, p.Category ORDER BY SUM(f.Price * f.Quantity) DESC) AS Rank
    FROM
        Sales_Fact f
        JOIN Product_Dim p ON f.ProductID = p.ProductID
        JOIN Retailer_Dim r ON f.RetailerID = r.RetailerID
    GROUP BY
        r.Name,
        p.Category,
        p.Name
)
SELECT
    RetailerName,
    Category,
    ProductName,
    TotalSales
FROM
    ranking
WHERE
    Rank = 1
ORDER BY
    RetailerName,
    Category;
