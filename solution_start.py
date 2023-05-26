import csv
import json
import os
from collections import defaultdict
from typing import Dict, List, Any


def process_data(input_location: str, output_location: str) -> None:
    """
    Process the input data and generate the output data.

    Args:
        input_location (str): The path to the input data directory.
        output_location (str): The path to the output data directory.

    Returns:
        None
    """
    try:
        customers = load_customers(input_location)
        products = load_products(input_location)
        transactions = load_transactions(input_location)

        customer_data = aggregate_customer_data(customers, transactions)
        output_data = generate_output_data(customer_data, products)
        save_output_data(output_data, output_location)

    except Exception as e:
        print(f"An error occurred while processing the data: {str(e)}")


def load_customers(input_location: str) -> Dict[str, Dict[str, int]]:
    """
    Load the customers data from the input directory.

    Args:
        input_location (str): The path to the input data directory.

    Returns:
        Dict[str, Dict[str, int]]: A dictionary containing the customer data.
    """
    customers = {}
    try:
        with open(os.path.join(input_location, 'customers.csv'), mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                customers[row['customer_id']] = {
                    'loyalty_score': int(row['loyalty_score']),
                    'purchase_count': 0
                }
    except FileNotFoundError:
        raise Exception("Customers data file not found")
    except Exception as e:
        raise Exception(f"An error occurred while loading customers data: {str(e)}")

    return customers


def load_products(input_location: str) -> Dict[str, Dict[str, str]]:
    """
    Load the products data from the input directory.

    Args:
        input_location (str): The path to the input data directory.

    Returns:
        Dict[str, Dict[str, str]]: A dictionary containing the product data.
    """
    products = {}
    try:
        with open(os.path.join(input_location, 'products.csv'), mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                products[row['product_id']] = {
                    'product_category': row['product_category']
                }
    except FileNotFoundError:
        raise Exception("Products data file not found")
    except Exception as e:
        raise Exception(f"An error occurred while loading products data: {str(e)}")

    return products


def load_transactions(input_location: str) -> List[Dict[str, Any]]:
    """
    Load the transactions data from the input directory.

    Args:
        input_location (str): The path to the input data directory.

    Returns:
        List[Dict[str, Any]]: A list containing the transaction data.
    """
    transactions = []
    try:
        transaction_files = os.listdir(os.path.join(input_location, 'transactions'))
        for file_name in transaction_files:
            with open(os.path.join(input_location, 'transactions', file_name), mode='r') as file:
                for line in file:
                    transaction = json.loads(line)
                    transactions.append(transaction)
    except FileNotFoundError:
        raise Exception("Transactions data directory not found")
    except Exception as e:
        raise Exception(f"An error occurred while loading transactions data: {str(e)}")

    return transactions


def aggregate_customer_data(customers: Dict[str, Dict[str, int]],
                            transactions: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    """
    Aggregate the customer data based on the transactions.

    Args:
        customers (Dict[str, Dict[str, int]]): A dictionary containing the customer data.
        transactions (List[Dict[str, Any]]): A list containing the transaction data.

    Returns:
        Dict[str, Dict[str, int]]: A dictionary containing the aggregated customer data.
    """
    customer_data = customers.copy()  # Create a copy to avoid modifying the original dictionary

    for transaction in transactions:
        customer_id = transaction.get('customer_id')
        if customer_id in customer_data:
            customer_data[customer_id]['purchase_count'] += 1
        else:
            customer_data[customer_id] = {'loyalty_score': 0, 'purchase_count': 1}

    return customer_data


def generate_output_data(customer_data: Dict[str, Dict[str, int]],
                         products: Dict[str, Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Generate the output data based on the aggregated customer data and product data.

    Args:
        customer_data (Dict[str, Dict[str, int]]): A dictionary containing the aggregated customer data.
        products (Dict[str, Dict[str, str]]): A dictionary containing the product data.

    Returns:
        List[Dict[str, Any]]: A list containing the output data.
    """
    output_data = []

    for customer_id, data in customer_data.items():
        loyalty_score = data.get('loyalty_score')
        purchase_count = data.get('purchase_count')

        for _ in range(purchase_count):
            output_record = {
                'customer_id': customer_id,
                'loyalty_score': loyalty_score,
                'product_id': '',
                'product_category': '',
                'purchase_count': 0
            }
            output_data.append(output_record)

    for transaction in transactions:
        customer_id = transaction.get('customer_id')
        product_id = transaction.get('product_id')
        product_category = products.get(product_id, {}).get('product_category')

        for output_record in output_data:
            if output_record['customer_id'] == customer_id and output_record['product_id'] == '':
                output_record['product_id'] = product_id
                output_record['product_category'] = product_category
                output_record['purchase_count'] += 1
                break

    return output_data


def save_output_data(output_data: List[Dict[str, Any]], output_location: str) -> None:
    """
    Save the output data to the output location.

    Args:
        output_data (List[Dict[str, Any]]): A list containing the output data.
        output_location (str): The path to the output data directory.

    Returns:
        None
    """
    os.makedirs(output_location, exist_ok=True)
    output_file = os.path.join(output_location, 'output.csv')

    try:
        with open(output_file, mode='w', newline='') as file:
            fieldnames = ['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_data)
    except Exception as e:
        raise Exception(f"An error occurred while saving output data: {str(e)}")

