from typing import List, Dict, Any, Generator
import os
import psycopg2
from contextlib import contextmanager
import logging
import csv
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@contextmanager
def database_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    connection = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "sales"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "mysecretpassword"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )
    try:
        yield connection
    finally:
        connection.close()

def read_csv_data(file_path: str) -> List[Dict[str, Any]]:
    data = []
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def clean_price(value: str) -> float:
    # Remove non-numeric characters except the decimal point
    cleaned_value = ''.join(c for c in value if c.isdigit() or c == '.')
    return float(cleaned_value) if cleaned_value else 0.0

def clean_value(key: str, value: str) -> Any:
    if key == "Price":
        return clean_price(value)
    elif key == "Date":
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
    else:
        return value

def clean_data(data: List[Dict[str, str]], id_fields: List[str] = ['ProductID', 'SaleID', 'RetailerID']) -> List[Dict[str, Any]]:
    cleaned_data = []
    seen = set()

    for row in data:
        cleaned_row = {key: clean_value(key, value) for key, value in row.items()}
        identifier = tuple(sorted(cleaned_row.items()))

        if all(cleaned_row.get(field) is not None and str(cleaned_row.get(field)).isdigit() for field in id_fields):
            if identifier not in seen:
                seen.add(identifier)
                cleaned_data.append(cleaned_row)

    return cleaned_data

def validate_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [row for row in data if all(value is not None for value in row.values())]

def transform_data(raw_data: List[Dict[str, Any]]):
    product_dim = []
    retailer_dim = []
    date_dim = []
    sales_fact = []

    for row in raw_data:
        product = {'ProductID': row['ProductID'], 'Name': row['ProductName'], 'Brand': row['Brand'], 'Category': row['Category']}
        if product not in product_dim:
            product_dim.append(product)

        retailer = {'RetailerID': row['RetailerID'], 'Name': row['RetailerName'], 'Channel': row['Channel'], 'Location': row['Location']}
        if retailer not in retailer_dim:
            retailer_dim.append(retailer)

        date = {'Date': row['Date'], 'Day': row['Date'].day, 'Month': row['Date'].month, 'Year': row['Date'].year, 
                'Quarter': (row['Date'].month - 1) // 3 + 1, 'DayOfWeek': row['Date'].strftime('%A'), 
                'WeekOfYear': row['Date'].isocalendar()[1]}
        if date not in date_dim:
            date_dim.append(date)

        sale = {'ProductID': row['ProductID'], 'RetailerID': row['RetailerID'], 'Date': row['Date'], 'Quantity': row['Quantity'], 'Price': row['Price']}
        sales_fact.append(sale)

    return product_dim, retailer_dim, date_dim, sales_fact

def publish_data(product_dim: List[Dict[str, Any]], retailer_dim: List[Dict[str, Any]], date_dim: List[Dict[str, Any]], sales_fact: List[Dict[str, Any]]):
    with database_connection() as conn:
        with conn.cursor() as cur:
            for product in product_dim:
                cur.execute("INSERT INTO Product_Dim (Name, Brand, Category) VALUES (%s, %s, %s)", 
                            (product['Name'], product['Brand'], product['Category']))

            for retailer in retailer_dim:
                cur.execute("INSERT INTO Retailer_Dim (Name, Channel, Location) VALUES (%s, %s, %s)", 
                            (retailer['Name'], retailer['Channel'], retailer['Location']))

            for date in date_dim:
                cur.execute("INSERT INTO Date_Dim (Date, Day, Month, Year, Quarter, DayOfWeek, WeekOfYear) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                            (date['Date'], date['Day'], date['Month'], date['Year'], date['Quarter'], date['DayOfWeek'], date['WeekOfYear']))

            for sale in sales_fact:
                cur.execute("INSERT INTO Sales_Fact (ProductID, RetailerID, Date, Quantity, Price) VALUES (%s, %s, %s, %s, %s)", 
                            (sale['ProductID'], sale['RetailerID'], sale['Date'], sale['Quantity'], sale['Price']))