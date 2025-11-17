#!/usr/bin/env python3
"""
Script to generate algorithm results and performance metrics for Milestone 3.
This script runs the algorithms and captures their outputs.
"""

import pandas as pd
import json
import time
from pathlib import Path
from reduction import reduce
from cleansing import cleanse
from transformation import transform
from parser import parse

def generate_processing_results():
    """Generate results from data processing pipeline"""
    print("=" * 60)
    print("DATA PROCESSING PIPELINE RESULTS")
    print("=" * 60)
    
    # Check if ratings.csv exists
    if not Path("ratings.csv").exists():
        print("ERROR: ratings.csv not found. Please run process_imdb_data.py first.")
        return None
    
    # Load data
    print("\n1. Loading data from ratings.csv...")
    df = pd.read_csv("ratings.csv")
    print(f"   Initial dataset size: {len(df)} rows")
    print(f"   Unique users: {df['userID'].nunique()}")
    print(f"   Unique movies: {df['movieID'].nunique()}")
    
    # Reduction
    print("\n2. Applying reduction (max_users=100, max_movies=500)...")
    start_time = time.time()
    reduced_df = reduce(df, max_users=100, max_movies=500)
    reduction_time = time.time() - start_time
    print(f"   Reduction time: {reduction_time:.4f} seconds")
    print(f"   Reduced dataset size: {len(reduced_df)} rows")
    print(f"   Unique users after reduction: {reduced_df['userID'].nunique()}")
    print(f"   Unique movies after reduction: {reduced_df['movieID'].nunique()}")
    
    # Cleansing
    print("\n3. Applying cleansing...")
    start_time = time.time()
    cleansed_df = cleanse(reduced_df)
    cleansing_time = time.time() - start_time
    print(f"   Cleansing time: {cleansing_time:.4f} seconds")
    print(f"   Cleaned dataset size: {len(cleansed_df)} rows")
    print(f"   Rows removed: {len(reduced_df) - len(cleansed_df)}")
    
    # Transformation
    print("\n4. Applying transformation...")
    start_time = time.time()
    documents = transform(cleansed_df)
    transformation_time = time.time() - start_time
    print(f"   Transformation time: {transformation_time:.4f} seconds")
    print(f"   Number of user documents created: {len(documents)}")
    
    # Sample output
    print("\n5. Sample transformed documents (first 2):")
    for i, doc in enumerate(documents[:2]):
        print(f"\n   Document {i+1}:")
        print(f"   User ID: {doc['userID']}")
        print(f"   Number of ratings: {len(doc['ratings'])}")
        if doc['ratings']:
            print(f"   First rating: Movie {doc['ratings'][0]['movieID']} - {doc['ratings'][0]['title']} ({doc['ratings'][0]['rating']} stars)")
    
    # Save results
    results = {
        "pipeline_performance": {
            "reduction_time_seconds": round(reduction_time, 4),
            "cleansing_time_seconds": round(cleansing_time, 4),
            "transformation_time_seconds": round(transformation_time, 4),
            "total_time_seconds": round(reduction_time + cleansing_time + transformation_time, 4)
        },
        "dataset_statistics": {
            "initial_rows": len(df),
            "reduced_rows": len(reduced_df),
            "cleansed_rows": len(cleansed_df),
            "documents_created": len(documents),
            "unique_users_initial": int(df['userID'].nunique()),
            "unique_movies_initial": int(df['movieID'].nunique()),
            "unique_users_final": int(cleansed_df['userID'].nunique()),
            "unique_movies_final": int(cleansed_df['movieID'].nunique())
        },
        "sample_documents": documents[:3]  # First 3 documents as samples
    }
    
    with open("algorithm_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print("\n6. Results saved to algorithm_results.json")
    
    return results

def generate_sample_outputs():
    """Generate sample outputs for documentation"""
    print("\n" + "=" * 60)
    print("SAMPLE ALGORITHM OUTPUTS")
    print("=" * 60)
    
    if not Path("ratings.csv").exists():
        print("ERROR: ratings.csv not found.")
        return
    
    # Run parser to generate documents
    print("\nRunning parser with max_users=10, max_movies=20...")
    docs = parse("ratings.csv", max_users=10, max_movies=20)
    
    print(f"\nGenerated {len(docs)} user documents")
    print("\nSample document structure:")
    if docs:
        print(json.dumps(docs[0], indent=2))

if __name__ == "__main__":
    try:
        results = generate_processing_results()
        generate_sample_outputs()
        print("\n" + "=" * 60)
        print("RESULTS GENERATION COMPLETE")
        print("=" * 60)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

