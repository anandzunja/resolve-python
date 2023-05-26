import csv
import json
import os
from collections import defaultdict

from typing import List, Dict

def process_data(input_dir: str, output_file: str):
    try:
        # Read customers data
        customers = read_customers_data(input_dir)

        # Read transactions data
        transactions = read_transactions_data(input_dir)

        # Read products data
        products = read_products_data(input_dir)

        # Process the data and generate the desired output
        output_data = generate_output(customers, transactions, products)

        # Write the output to a JSON file
        write_output(output_data, output_file)
        print("Data processing completed successfully!")
    except Exception as e:
        print("Error occurred during data processing:", str(e))


def read_customers_data(input_dir: str) -> Dict[str, int]:
    customers_data = {}
    customers_file = os.path.join(input_dir, 'customers.csv')
    try:
        with open(customers_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                customer_id = row['customer_id']
                loyalty_score = int(row['loyalty_score'])
                customers_data[customer_id] = loyalty_score
    except FileNotFoundError:
        print(f"Customers file not found: {customers_file}")
    except Exception as e:
        print("Error occurred while reading customers data:", str(e))
    return customers_data


def read_transactions_data(input_dir: str) -> List[Dict]:
    transactions_data = []
    transactions_dir = os.path.join(input_dir, 'transactions')
    try:
        for filename in os.listdir(transactions_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(transactions_dir, filename)
                with open(file_path, 'r') as file:
                    for line in file:
                        transaction = json.loads(line)
                        transactions_data.append(transaction)
    except FileNotFoundError:
        print(f"Transactions directory not found: {transactions_dir}")
    except Exception as e:
        print("Error occurred while reading transactions data:", str(e))
    return transactions_data


def read_products_data(input_dir: str) -> Dict[str, str]:
    products_data = {}
    products_file = os.path.join(input_dir, 'products.csv')
    try:
        with open(products_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                product_id = row['product_id']
                product_category = row['product_category']
                products_data[product_id] = product_category
    except FileNotFoundError:
        print(f"Products file not found: {products_file}")
    except Exception as e:
        print("Error occurred while reading products data:", str(e))
    return products_data


def generate_output(customers: Dict[str, int], transactions: List[Dict], products: Dict[str, str]) -> List[Dict]:
    customer_purchase_counts = defaultdict(int)
    output_data = []

    try:
        for transaction in transactions:
            customer_id = transaction['customer_id']
            basket = transaction['basket']

            for item in basket:
                product_id = item['product_id']
                product_category = products.get(product_id)

                if product_category:
                    customer_purchase_counts[(customer_id, product_category)] += 1

        for (customer_id, product_category), purchase_count in customer_purchase_counts.items():
            loyalty_score = customers.get(customer_id, 0)
            output_data.append({
                'customer_id': customer_id,
                'loyalty_score': loyalty_score,
                'product_id': product_id,
                'product_category': product_category,
                'purchase_count': purchase_count
            })
    except Exception as e:
        print("Error occurred while generating output:", str(e))

    return output_data


def write_output(output_data: List[Dict], output_file: str):
    try:
        with open(output_file, 'w') as file:
            json.dump(output_data, file)
    except Exception as e:
        print("Error occurred while writing output:", str(e))


if __name__ == '__main__':
    input_dir = '../input_data/starter'
    output_file = 'output.json'
    process_data(input_dir, output_file)
