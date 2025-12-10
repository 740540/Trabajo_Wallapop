#!/usr/bin/env python3
"""
Elasticsearch Index Setup
Creates index template, ILM policy, and initial index
"""

from elasticsearch import Elasticsearch
import requests
import json

# Configuration - ADJUST THESE
ES_HOST = "http://localhost:9200"
LAB_NUMBER = "001"
INDEX_ALIAS = f"lab{LAB_NUMBER}.wallapop"
ILM_POLICY_NAME = f"lab{LAB_NUMBER}-wallapop-rotation"
TEMPLATE_NAME = f"lab{LAB_NUMBER}-wallapop-template"


def create_ilm_policy(es: Elasticsearch):
    """Create Index Lifecycle Management policy using direct HTTP"""
    policy = {
        "policy": {
            "phases": {
                "hot": {
                    "actions": {
                        "rollover": {
                            "max_size": "1gb",
                            "max_age": "1d"
                        }
                    }
                },
                "delete": {
                    "min_age": "30d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
    }
    
    try:
        url = f"{ES_HOST}/_ilm/policy/{ILM_POLICY_NAME}"
        response = requests.put(url, json=policy, headers={"Content-Type": "application/json"})
        
        if response.status_code in [200, 201]:
            print(f"✓ Created ILM policy: {ILM_POLICY_NAME}")
        else:
            print(f"⚠ ILM policy creation: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"⚠ ILM policy creation: {e}")


def create_index_template(es: Elasticsearch):
    """Create index template with mappings"""
    template = {
        "index_patterns": [f"{INDEX_ALIAS}-*"],
        "template": {
            "settings": {
                "index.lifecycle.name": ILM_POLICY_NAME,
                "index.lifecycle.rollover_alias": INDEX_ALIAS,
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                    "price": {"type": "double"},
                    "currency": {"type": "keyword"},
                    "seller_id": {"type": "keyword"},
                    "category_id": {"type": "keyword"},
                    "web_slug": {"type": "keyword"},
                    
                    "location": {
                        "properties": {
                            "geo": {"type": "geo_point"},
                            "city": {"type": "keyword"},
                            "postal_code": {"type": "keyword"},
                            "region": {"type": "keyword"}
                        }
                    },
                    
                    "timestamps": {
                        "properties": {
                            "created_at": {"type": "date"},
                            "modified_at": {"type": "date"},
                            "crawl_timestamp": {"type": "date"}
                        }
                    },
                    
                    # Fixed taxonomy mapping to support nested objects
                    "taxonomy": {
                        "type": "nested",
                        "properties": {
                            "id": {"type": "integer"},
                            "name": {"type": "keyword"},
                            "icon": {"type": "keyword"}
                        }
                    },
                    
                    "enrichment": {
                        "properties": {
                            "price": {"type": "double"},
                            "relative_price_index": {"type": "float"},
                            "risk_score": {"type": "integer"},
                            "risk_factors": {"type": "keyword"},
                            "suspicious_keywords": {"type": "keyword"},
                            "has_suspicious_keywords": {"type": "boolean"},
                            "seller_items_today": {"type": "integer"}
                        }
                    }
                }
            }
        }
    }
    
    try:
        es.indices.put_index_template(name=TEMPLATE_NAME, body=template)
        print(f"✓ Created index template: {TEMPLATE_NAME}")
    except Exception as e:
        print(f"⚠ Template creation: {e}")


def create_initial_index(es: Elasticsearch):
    """Create the first backing index with write alias"""
    initial_index = f"{INDEX_ALIAS}-000001"
    
    try:
        if not es.indices.exists(index=initial_index):
            es.indices.create(
                index=initial_index,
                body={
                    "aliases": {
                        INDEX_ALIAS: {
                            "is_write_index": True
                        }
                    }
                }
            )
            print(f"✓ Created initial index: {initial_index}")
            print(f"✓ Write alias: {INDEX_ALIAS}")
        else:
            print(f"ℹ Index {initial_index} already exists")
    except Exception as e:
        print(f"⚠ Index creation: {e}")


def main():
    """Main setup function"""
    print("=" * 60)
    print("Elasticsearch Setup for Wallapop Motorbike Monitoring")
    print("=" * 60)
    print(f"\nIndex alias: {INDEX_ALIAS}")
    print(f"ILM policy: {ILM_POLICY_NAME}")
    print(f"Template: {TEMPLATE_NAME}\n")
    
    # Connect to Elasticsearch
    try:
        es = Elasticsearch(
            [ES_HOST],
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        
        # Test connection
        info = es.info()
        print(f"✓ Connected to Elasticsearch {info['version']['number']}\n")
    except Exception as e:
        print(f"✗ Could not connect to Elasticsearch: {e}")
        return
    
    # Create components
    print("Creating ILM policy...")
    create_ilm_policy(es)
    
    print("\nCreating index template...")
    create_index_template(es)
    
    print("\nCreating initial index...")
    create_initial_index(es)
    
    print("\n" + "=" * 60)
    print("✓ Setup complete!")
    print(f"\nYou can now ingest data to: {INDEX_ALIAS}")
    print("=" * 60)


if __name__ == "__main__":
    main()
