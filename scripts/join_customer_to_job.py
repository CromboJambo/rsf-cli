#!/usr/bin/env python3
"""
Join Customer Orders to Job Orders
Tests join strategies based on analysis results
"""

import csv
from pathlib import Path
from collections import defaultdict


def load_csv(filepath, encoding='utf-8'):
    """Load CSV with Python's flexible parser"""
    rows = []
    with open(filepath, 'r', encoding=encoding) as f:
        reader = csv.reader(f, delimiter='\t', quotechar='"')
        
        if filepath.endswith('CustomerOrders.csv'):
            # UTF-16 for customer orders
            try:
                content = Path(filepath).read_text(encoding='utf-16')
            except:
                content = Path(filepath).read_text(encoding='latin-1')
            
            lines = content.replace('\r\n', '\n').split('\n')
            if lines and lines[-1].strip() == '':
                lines.pop()
            
            cleaned_lines = [line.replace('\r', ' ').replace('\n', ' ') for line in lines if line.strip()]
            from io import StringIO
            reader = csv.reader(StringIO('\n'.join(cleaned_lines)), delimiter='\t', quotechar='"')
        
        header = next(reader)
        rows = list(reader)
    
    return header, rows


def join_by_customer(customer_header, customer_rows, job_header, job_rows):
    """Test join on 'Customer' column"""
    
    cust_idx = customer_header.index('Customer')
    job_idx = job_header.index('Customer')
    
    # Build lookup tables
    customer_by_name = defaultdict(list)
    for row in customer_rows:
        name = row[cust_idx] if cust_idx < len(row) else ''
        customer_by_name[name].append(row)
    
    job_by_customer = defaultdict(list)
    for row in job_rows:
        cust = row[job_idx] if job_idx < len(row) else ''
        job_by_customer[cust].append(row)
    
    # Perform inner join
    joined_rows = []
    unmatched_customers = []
    
    for customer_name, cust_orders in customer_by_name.items():
        jobs = job_by_customer.get(customer_name, [])
        
        if jobs:
            # Create all combinations (customer order × matching jobs)
            for order in cust_orders:
                for job in jobs:
                    joined_rows.append((order, job))
        else:
            unmatched_customers.extend(cust_orders)
    
    return joined_rows, unmatched_customers


def main():
    """Run join simulation"""
    print("="*80)
    print("JOIN SIMULATION: Customer Orders → Job Orders")
    print("="*80)
    
    # Load data (using clean UTF-8 versions where available)
    customer_header, customer_rows = load_csv(
        "data/customer_orders_clean_utf8.csv", 
        encoding='utf-8'
    )
    
    job_header, job_rows = load_csv(
        "data/job_orders_perfectly_clean.csv", 
        encoding='utf-8'
    )
    
    print(f"\nLoaded:")
    print(f"  Customer Orders: {len(customer_header)} columns × {len(customer_rows):,} rows")
    print(f"  Job Orders:      {len(job_header)} columns × {len(job_rows):,} rows")
    
    # Test join on 'Customer' column (best match found)
    print("\n" + "-"*80)
    print("Testing Join Key: 'Customer'")
    print("-"*80)
    
    joined_rows, unmatched = join_by_customer(
        customer_header, customer_rows, 
        job_header, job_rows
    )
    
    # Analyze results
    unique_customers_with_jobs = len(set(
        order[customer_header.index('Customer')] 
        for order in joined_rows[:len(customer_rows)]  # Just count once per customer
    ))
    
    print(f"\nResults:")
    print(f"  Total matched combinations: {len(joined_rows):,}")
    print(f"  Customers with jobs: ~{unique_customers_with_jobs:,}")
    print(f"  Unmatched customers: {len(unmatched):,} orders")
    
    # Show sample matches
    print("\n" + "="*80)
    print("SAMPLE JOIN RESULTS (First 5 combinations)")
    print("="*80)
    
    for i, (order, job) in enumerate(joined_rows[:5], 1):
        cust_name = order[customer_header.index('Customer')] if customer_header.index('Customer') < len(order) else ''
        
        # Get relevant fields from each file
        order_fields = [f'{field}' for field in order[:3]]  # First 3 columns
        
        print(f"\n{i}. Customer: {cust_name}")
        print(f"   Order ref/ID: {order_fields[0] if len(order_fields) > 0 else 'N/A'}")
        print(f"   → Matches with Job Orders...")


if __name__ == "__main__":
    main()
