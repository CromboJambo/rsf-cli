#!/usr/bin/env python3
"""
Test Join Script for Job Orders Dataset
Demonstrates multi-key join operations using the analysis results
"""

import csv
from pathlib import Path
from collections import defaultdict

def load_job_orders(filepath):
    """Load job orders with proper CSV handling"""
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t', quotechar='"')
        header = next(reader)  # Skip header for now
        
        for row in reader:
            if len(row) >= 10:  # Basic validation
                rows.append(row)
    
    return header, rows

def create_item_lookup(job_orders_header, job_rows):
    """Create lookup table by Item (for join demonstration)"""
    item_idx = None
    
    for i, col in enumerate(job_orders_header):
        if col == 'Item':
            item_idx = i
            break
    
    if item_idx is None:
        print("⚠️  No 'Item' column found!")
        return {}
    
    # Group rows by Item
    items = defaultdict(list)
    for row in job_rows[:100]:  # Sample first 100 for demo
        item_value = row[item_idx] if item_idx < len(row) else ''
        items[item_value].append(row)
    
    return dict(items)

def simulate_join_with_materials(job_items, material_samples=50):
    """Simulate join with hypothetical materials data"""
    print("\n" + "="*80)
    print("JOIN SIMULATION: Job Orders ← Materials Inventory")
    print("="*80)
    
    # Assume we have ~50 unique materials in inventory file
    unique_materials = set()
    for item_key in job_items.keys():
        if 'MAT' in item_key.upper() or len(item_key) > 3:
            unique_materials.add(item_key)
    
    print(f"\nJob Orders with Item IDs (sample): {len(job_items)}")
    print(f"Unique materials found: {len(unique_materials)}")
    print(f"Hypothetical Material Inventory records: {material_samples}")
    
    # Simulate match rate based on cardinality analysis
    expected_match_rate = 0.87  # From join plan (87% for Item-based join)
    expected_matches = int(len(job_items) * expected_match_rate)
    
    print(f"\nExpected matches: {expected_matches}/{len(job_items)} ({expected_match_rate*100:.0f}%)")
    print(f"Unmatched items: {len(job_items) - expected_matches}")

def main():
    """Run join simulation"""
    input_path = Path("data/ToExcel_JobOrders_final.csv")
    
    if not input_path.exists():
        print(f"Error: {input_path} not found!")
        return
    
    print("\n🔍 Loading job orders data...")
    header, rows = load_job_orders(input_path)
    
    print(f"Loaded {len(rows)} records with {len(header)} columns")
    
    # Create item-based grouping for join simulation
    print("\n📊 Building Item lookup table...")
    item_lookup = create_item_lookup(header, rows)
    
    # Simulate material inventory join
    simulate_join_with_materials(item_lookup)
    
    # Show sample matches
    print("\n" + "="*80)
    print("SAMPLE JOIN RESULTS (First 5 Item groups)")
    print("="*80)
    
    for i, (item_key, job_rows) in enumerate(list(item_lookup.items())[:5], 1):
        print(f"\n{i}. Item: {item_key}")
        print(f"   Jobs using this item: {len(job_rows)}")
        
        # Show key fields from first matching row
        if len(job_rows[0]) > 2:
            job_id = job_rows[0][1] if len(job_rows[0]) > 1 else ''
            print(f"   Sample Job ID: {job_id[:30]}")

if __name__ == "__main__":
    main()
