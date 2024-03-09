from typing import List, Dict, Any, Generator
import csv
import os
import psycopg2
from contextlib import contextmanager
import logging
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
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def clean_price(value: str) -> float:
    """
    Clean price values, removing currency symbols and converting to float.
    """
    # Remove non-numeric characters except the decimal point
    cleaned_value = ''.join(c for c in value if c.isdigit() or c == '.')

    return float(cleaned_value) if cleaned_value else 0.0

def clean_value(key: str, value: str) -> Any:
    """
    Clean data based on key, including handling of date formats.
    """
    if key == "Price":
        return clean_price(value)
    elif key == "Date":
        try:
            return datetime.strptime(value, "%d-%m-%y").date()
        except ValueError:
            logging.warning(f"Invalid date format: {value}")
            return None
    else:
        return value

def clean_data(data: List[Dict[str, str]], id_fields: List[str] = ['ProductID', 'SaleID', 'RetailerID']) -> List[Dict[str, Any]]:
    """
    Clean data by applying specific cleaning logic based on the key of each item.
    Log and exclude rows where any of the specified ID fields are missing or not integers.

    Parameters:
    - data: The list of data rows to clean.
    - id_fields: The list of ID fields to validate for each row.

    Returns:
    - The cleaned data as a list of dictionaries.
    """
    cleaned_data: List[Dict[str, Any]] = []
    seen: set = set()

    for row in data:
        cleaned_row = {key: clean_value(key, value) for key, value in row.items()}

        identifier = tuple(sorted(cleaned_row.items()))

        if identifier not in seen:
            seen.add(identifier)

            # Check if any ID fields are missing or not integers
            if all(cleaned_row.get(field) and cleaned_row[field].isdigit() for field in id_fields):
                cleaned_data.append(cleaned_row)
            else:
                logging.warning(f"Invalid row: {row}. Skipping...")

    return cleaned_data

def validate_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    valid_data = []
    for row in data:
        quantity = row.get('Quantity')
        price = row.get('Price')
        date = row.get('Date')

        if quantity and quantity > 0 and price and price >= 0 and date:
            valid_data.append(row)
        else:
            logging.warning(f"Invalid row: {row}. Skipping...")

    return valid_data

def transform_data(raw_data: List[Dict[str, Any]]):
    product_dim = []
    retailer_dim = []
    date_dim = []
    sales_fact = []

    product_set = set()
    retailer_set = set()
    date_set = set()

    for row in raw_data:
        product_id = row['ProductID']
        product_name = row['ProductName']
        brand = row['Brand']
        category = row['Category']

        if (product_id, product_name, brand, category) not in product_set:
            product_dim.append({
                'ProductID': product_id,
                'Name': product_name,
                'Brand': brand,
                'Category': category
            })
            product_set.add((product_id, product_name, brand, category))

        retailer_id = row['RetailerID']
        retailer_name = row['RetailerName']
        channel = row['Channel']
        location = row['Location']

        if (retailer_id, retailer_name, channel, location) not in retailer_set:
            retailer_dim.append({
                'RetailerID': retailer_id,
                'Name': retailer_name,
                'Channel': channel,
                'Location': location
            })
            retailer_set.add((retailer_id, retailer_name, channel, location))

        date_value = row['Date']
        if date_value not in date_set:
            date_dim.append({
                'Date': date_value,
                'Day': date_value.day,
                'Month': date_value.month,
                'Year': date_value.year,
                'Quarter': (date_value.month - 1) // 3 + 1,
                'DayOfWeek': date_value.strftime('%A'),
                'WeekOfYear': date_value.isocalendar()[1]
            })
            date_set.add(date_value)

        sales_fact.append({
            'SaleID': row['SaleID'],
            'ProductID': product_id,
            'RetailerID': retailer_id,
            'Date': date_value,
            'Quantity': int(row['Quantity']),
            'Price': float(row['Price'])
        })

    return product_dim, retailer_dim, date_dim, sales_fact

def publish_data(
    product_dim: List[Dict[str, Any]],
    retailer_dim: List[Dict[str, Any]],
    date_dim: List[Dict[str, Any]],
    sales_fact: List[Dict[str, Any]],
) -> None:
    with database_connection() as conn:
        with conn.cursor() as cur:
            # Truncate tables
            cur.execute("TRUNCATE TABLE Product_Dim, Retailer_Dim, Date_Dim, Sales_Fact RESTART IDENTITY;")

            # Insert data into Product_Dim
            insert_query = """
                INSERT INTO Product_Dim (Name, Brand, Category)
                VALUES (%s, %s, %s)
                RETURNING ProductID;
            """
            product_ids = {}
            for product in product_dim:
                cur.execute(insert_query, (product['Name'], product['Brand'], product['Category']))
                product_id = cur.fetchone()[0]
                product_ids[(product['Name'], product['Brand'], product['Category'])] = product_id

            # Insert data into Retailer_Dim
            insert_query = """
                INSERT INTO Retailer_Dim (Name, Channel, Location)
                VALUES (%s, %s, %s)
                RETURNING RetailerID;
            """
            retailer_ids = {}
            for retailer in retailer_dim:
                cur.execute(insert_query, (retailer['Name'], retailer['Channel'], retailer['Location']))
                retailer_id = cur.fetchone()[0]
                retailer_ids[(retailer['Name'], retailer['Channel'], retailer['Location'])] = retailer_id

            # Insert data into Date_Dim
            insert_query = """
                INSERT INTO Date_Dim (Date, Day, Month, Year, Quarter, DayOfWeek, WeekOfYear)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (Date) DO NOTHING;
            """
            for date_record in date_dim:
                cur.execute(insert_query, (
                    date_record['Date'], date_record['Day'], date_record['Month'], date_record['Year'],
                    date_record['Quarter'], date_record['DayOfWeek'], date_record['WeekOfYear']
                ))

            # Insert data into Sales_Fact
            insert_query = """
                INSERT INTO Sales_Fact (ProductID, RetailerID, Date, Quantity, Price)
                VALUES (%s, %s, %s, %s, %s);
            """
            for sale in sales_fact:
                product_id = product_ids[(sale['ProductID'], product_dim[int(sale['ProductID']) - 1]['Name'], product_dim[int(sale['ProductID']) - 1]['Brand'])]
                retailer_id = retailer_ids[(sale['RetailerID'], retailer_dim[int(sale['RetailerID']) - 1]['Name'], retailer_dim[int(sale['RetailerID']) - 1]['Channel'])]
                cur.execute(insert_query, (product_id, retailer_id, sale['Date'], sale['Quantity'], sale['Price']))

            conn.commit()
