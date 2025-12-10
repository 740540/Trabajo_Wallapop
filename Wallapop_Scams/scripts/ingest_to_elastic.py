#!/usr/bin/env python3
"""
Elasticsearch Bulk Ingestion Script
Loads enriched JSON data into Elasticsearch
"""

from elasticsearch import Elasticsearch, helpers
import json
import sys
import os

# Configuration
ES_HOST = "http://localhost:9200"
LAB_NUMBER = "001"  # Change to your lab number
INDEX_ALIAS = f"lab{LAB_NUMBER}.wallapop"


def load_json_lines(filepath: str):
    """Load JSON lines file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def bulk_ingest(es: Elasticsearch, filepath: str, batch_size: int = 500):
    """
    Bulk ingest data to Elasticsearch
    
    Args:
        es: Elasticsearch client
        filepath: Path to JSON lines file
        batch_size: Number of documents per batch
    """
    def generate_actions():
        """Generator for bulk API actions"""
        for doc in load_json_lines(filepath):
            yield {
                "_index": INDEX_ALIAS,
                "_id": doc.get("id"),
                "_source": doc
            }
    
    print(f"📤 Starting bulk ingestion to {INDEX_ALIAS}...")
    
    try:
        # Use helpers.bulk with better error reporting
        success_count = 0
        error_count = 0
        errors = []
        
        for ok, result in helpers.streaming_bulk(
            es,
            generate_actions(),
            chunk_size=batch_size,
            raise_on_error=False,
            max_retries=3
        ):
            if ok:
                success_count += 1
            else:
                error_count += 1
                errors.append(result)
                if len(errors) <= 5:  # Only print first 5 errors
                    print(f"\n⚠ Error: {result}")
        
        print(f"\n✓ Ingestion complete!")
        print(f"   Successfully indexed: {success_count}")
        print(f"   Failed: {error_count}")
        
        if errors:
            print(f"\n❌ Sample errors (showing first 5):")
            for i, error in enumerate(errors[:5], 1):
                print(f"\n{i}. {error}")
        
        return success_count, errors
        
    except Exception as e:
        print(f"✗ Bulk ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 0, []



def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python ingest_to_elastic.py <enriched_json_file>")
        print("\nExample:")
        print("  python ingest_to_elastic.py data/wallapop_motorbikes_20251210_enriched.json")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    if not os.path.exists(filepath):
        print(f"✗ File not found: {filepath}")
        sys.exit(1)
    
    print("=" * 60)
    print("Elasticsearch Bulk Ingestion")
    print("=" * 60)
    print(f"\nFile: {filepath}")
    print(f"Target index: {INDEX_ALIAS}\n")
    
    # Connect to Elasticsearch
    try:
        es = Elasticsearch([ES_HOST])
        es.info()
        print("✓ Connected to Elasticsearch\n")
    except Exception as e:
        print(f"✗ Could not connect to Elasticsearch: {e}")
        sys.exit(1)
    
    # Ingest data
    success, failed = bulk_ingest(es, filepath)
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
