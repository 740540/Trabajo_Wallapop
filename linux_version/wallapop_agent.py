#!/usr/bin/env python3
"""
Wallapop Motorbike Fraud Detection Agent
All-in-one: Poll → Enrich → Ingest to Elasticsearch
"""

import requests
import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Optional
from statistics import mean
import time
import hashlib

# ============================================================================
# CONFIGURATION
# ============================================================================

# Wallapop API
API_URL = "https://api.wallapop.com/api/v3/search"
HEADERS = {
    "Host": "api.wallapop.com",
    "X-DeviceOS": "0"
}

# Elasticsearch
ES_HOST = os.environ.get("ES_HOST", "http://192.168.153.2:9200")
LAB_NUMBER = "001"
INDEX_ALIAS = f"lab{LAB_NUMBER}.wallapop"

# Search Config
MOTORBIKE_CATEGORY_ID = "14000"
SEARCH_LOCATION = {
    "latitude": 41.648823,   # Zaragoza (change to your city)
    "longitude": -0.889085
}

MOTORBIKE_KEYWORDS = [
    "yamaha", "honda", "kawasaki", "suzuki", "ktm",
    "bmw", "ducati", "triumph", "harley", "moto"
]

# Clothing filter keywords
CLOTHING_KEYWORDS = [
    "casco", "guante", "chaqueta", "pantalón", "pantalon", "botas",
    "alforja", "mochila", "chaleco", "protector", "cubremanos",
    "candado", "antirrobo", "baul", "maleta", "caballete"
]

# Risk keywords (categorized)
RISK_KEYWORDS = {
    "CRITICAL_LEGAL": ["sin papeles", "sin documentacion", "no papeles"],
    "CRITICAL_INTEGRITY": ["sin itv", "para piezas", "despiece"],
    "CRITICAL_FRAUD": ["robo", "importacion", "procedencia dudosa"],
    "GENERAL_URGENCY": ["urgente", "solo hoy", "rapido"],
    "GENERAL_PRICE": ["ganga", "chollo", "muy barato"]
}

# Paths
OUTPUT_DIR = "data"
BACKUP_ENABLED = True  # Save JSON backup to disk


# ============================================================================
# STEP 1: DATA COLLECTION (POLLING)
# ============================================================================

class WallapopCollector:
    """Handles data collection from Wallapop API"""
    
    def fetch_all_pages(self, keywords: Optional[str] = None) -> List[Dict]:
        """Fetch all pages with pagination"""
        all_items = []
        offset = 0
        limit = 50
        page = 1
        
        params = {
            "source": "search_box",
            "category_id": MOTORBIKE_CATEGORY_ID,
            "latitude": SEARCH_LOCATION["latitude"],
            "longitude": SEARCH_LOCATION["longitude"],
            "time_filter": "today",
            "order_by": "newest",
            "offset": offset,
            "limit": limit
        }
        
        if keywords:
            params["keywords"] = keywords
        
        while True:
            params["offset"] = offset
            
            try:
                response = requests.get(API_URL, params=params, headers=HEADERS, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                items = data.get("data", {}).get("section", {}).get("payload", {}).get("items", [])
                
                if not items:
                    break
                
                all_items.extend(items)
                print(f"    Page {page}: +{len(items)} items")
                
                if len(items) < limit:
                    break
                
                offset += limit
                page += 1
                time.sleep(0.5)
                
            except Exception as e:
                print(f"    ⚠ Page {page} error: {e}")
                break
        
        return all_items
    
    def collect_all(self) -> List[Dict]:
        """Collect from all keywords and deduplicate"""
        all_items = []
        seen_ids = set()
        
        for keyword in MOTORBIKE_KEYWORDS:
            print(f"\n  🔍 Keyword: '{keyword}'")
            items = self.fetch_all_pages(keywords=keyword)
            
            for item in items:
                item_id = item.get("id")
                if item_id and item_id not in seen_ids:
                    all_items.append(item)
                    seen_ids.add(item_id)
        
        print(f"\n  ✓ Total unique items: {len(all_items)}")
        return all_items
    
    def filter_clothing(self, items: List[Dict]) -> tuple:
        """Remove clothing and accessories"""
        filtered = []
        removed = 0
        
        for item in items:
            text = f"{item.get('title', '')} {item.get('description', '')}".lower()
            if not any(kw in text for kw in CLOTHING_KEYWORDS):
                filtered.append(item)
            else:
                removed += 1
        
        return filtered, removed


# ============================================================================
# STEP 2: DATA ENRICHMENT
# ============================================================================

class WallapopEnricher:
    """Enriches items with risk scores and metadata"""
    
    def detect_suspicious_keywords(self, text: str) -> tuple:
        """Returns (keywords_found, categories_triggered)"""
        if not text:
            return [], set()
        
        text_lower = text.lower()
        found_keywords = []
        found_categories = set()
        
        for category, keywords in RISK_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text_lower:
                    found_keywords.append(keyword)
                    found_categories.add(category)
        
        return found_keywords, found_categories
    
    def calculate_risk_score(self, item: Dict, prices: List[float], 
                            seller_counts: Dict, found_categories: set) -> int:
        """Calculate risk score 0-100"""
        score = 0
        price = item.get("price", {}).get("amount", 0)
        
        # 1. Keyword-based risk (30 points)
        if any(cat in found_categories for cat in ["CRITICAL_LEGAL", "CRITICAL_INTEGRITY", "CRITICAL_FRAUD"]):
            score += 30
        elif any(cat in found_categories for cat in ["GENERAL_URGENCY", "GENERAL_PRICE"]):
            score += 15
        
        # 2. Price-based risk (40 points)
        if prices:
            avg_price = mean(prices)
            if price and price < avg_price * 0.4:
                score += 40
            elif price and price < avg_price * 0.6:
                score += 20
        
        # 3. Seller behavior (20 points)
        seller_id = item.get("user_id")
        seller_count = seller_counts.get(seller_id, 0)
        if seller_count > 10:
            score += 20
        elif seller_count > 5:
            score += 10
        
        # 4. Description quality (10 points)
        description = item.get("description", "")
        if len(description) < 50:
            score += 10
        
        return min(score, 100)
    
    def enrich_all(self, items: List[Dict]) -> List[Dict]:
        """Enrich all items"""
        # Calculate statistics
        prices = [item.get("price", {}).get("amount") for item in items 
                 if item.get("price", {}).get("amount")]
        prices = [p for p in prices if p and p > 0]
        
        seller_counts = {}
        for item in items:
            seller_id = item.get("user_id")
            if seller_id:
                seller_counts[seller_id] = seller_counts.get(seller_id, 0) + 1
        
        enriched_items = []
        
        for item in items:
            # Detect keywords
            text = f"{item.get('title', '')} {item.get('description', '')}"
            found_kw, found_cat = self.detect_suspicious_keywords(text)
            
            # Calculate risk
            risk_score = self.calculate_risk_score(item, prices, seller_counts, found_cat)
            
            # Calculate relative price
            price = item.get("price", {}).get("amount", 0)
            avg_price = mean(prices) if prices else 1
            relative_price = round(price / avg_price, 2) if avg_price > 0 else 1.0
            
            # Build enriched item
            enriched = {
                "id": item.get("id"),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "price": price,
                "currency": item.get("currency", "EUR"),
                "seller_id": item.get("user_id"),
                "category_id": item.get("category_id"),
                "web_slug": item.get("web_slug"),
                
                "location": self._normalize_location(item),
                "timestamps": self._normalize_timestamps(item),
                "taxonomy": item.get("taxonomy", []),
                
                "enrichment": {
                    "price": price,
                    "relative_price_index": relative_price,
                    "risk_score": risk_score,
                    "risk_factors": list(found_cat),
                    "suspicious_keywords": list(set(found_kw)),
                    "has_suspicious_keywords": len(found_kw) > 0,
                    "seller_items_today": seller_counts.get(item.get("user_id"), 0)
                }
            }
            
            enriched_items.append(enriched)
        
        return enriched_items
    
    def _normalize_location(self, item: Dict) -> Dict:
        """Normalize location data"""
        location = item.get("location", {})
        lat = location.get("latitude")
        lon = location.get("longitude")
        
        result = {
            "city": location.get("city", ""),
            "postal_code": location.get("postal_code", ""),
            "region": location.get("region", "")
        }
        
        if lat is not None and lon is not None:
            try:
                result["geo"] = {
                    "lat": float(lat),
                    "lon": float(lon)
                }
            except (ValueError, TypeError):
                pass
        
        return result
    
    def _normalize_timestamps(self, item: Dict) -> Dict:
        """Normalize timestamps"""
        def to_iso(ts):
            if not ts:
                return None
            if isinstance(ts, str):
                return ts
            if isinstance(ts, (int, float)):
                if ts > 10000000000:
                    ts = ts / 1000
                return datetime.utcfromtimestamp(ts).isoformat() + 'Z'
            return None
        
        return {
            "created_at": to_iso(item.get("created_at")),
            "modified_at": to_iso(item.get("modified_at")),
            "crawl_timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        }


# ============================================================================
# STEP 3: ELASTICSEARCH INGESTION
# ============================================================================

class ElasticsearchIngester:
    """Handles bulk ingestion to Elasticsearch"""
    
    def __init__(self, es_host: str, index_alias: str):
        self.es_host = es_host
        self.index_alias = index_alias
    
    def bulk_ingest(self, items: List[Dict]) -> tuple:
        """Ingest items using bulk API"""
        if not items:
            return 0, 0
        
        # Build bulk request
        bulk_body = []
        for item in items:
            # Action line
            action = {"index": {"_index": self.index_alias, "_id": item.get("id")}}
            bulk_body.append(json.dumps(action))
            # Document line
            bulk_body.append(json.dumps(item, ensure_ascii=False))
        
        bulk_data = '\n'.join(bulk_body) + '\n'
        
        # Send to Elasticsearch
        try:
            response = requests.post(
                f"{self.es_host}/_bulk",
                data=bulk_data.encode('utf-8'),
                headers={"Content-Type": "application/x-ndjson"},
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Count successes and failures
            success = 0
            errors = 0
            
            for item_result in result.get("items", []):
                if "index" in item_result:
                    if item_result["index"].get("status") in [200, 201]:
                        success += 1
                    else:
                        errors += 1
            
            return success, errors
            
        except Exception as e:
            print(f"✗ Bulk ingestion error: {e}")
            return 0, len(items)


# ============================================================================
# STEP 4: BACKUP TO DISK (Optional)
# ============================================================================

def save_backup(items: List[Dict], output_dir: str = OUTPUT_DIR):
    """Save enriched data to daily JSON file"""
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    filename = os.path.join(output_dir, f"wallapop_motorbikes_{today}_enriched.json")
    
    with open(filename, 'w', encoding='utf-8') as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    print(f"  ✓ Backup saved: {filename}")


# ============================================================================
# MAIN AGENT EXECUTION
# ============================================================================

def main():
    """Main agent: Poll → Enrich → Ingest"""
    print("=" * 70)
    print("🤖 WALLAPOP MOTORBIKE FRAUD DETECTION AGENT")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Elasticsearch: {ES_HOST}")
    print(f"Target index: {INDEX_ALIAS}")
    print("=" * 70)
    
    # Step 1: COLLECT
    print("\n[1/4] 📡 COLLECTING DATA FROM WALLAPOP...")
    collector = WallapopCollector()
    items = collector.collect_all()
    
    if not items:
        print("✗ No items collected. Exiting.")
        return
    
    # Step 1b: FILTER
    print("\n[2/4] 🧹 FILTERING CLOTHING/ACCESSORIES...")
    filtered_items, removed_count = collector.filter_clothing(items)
    print(f"  ✓ Removed: {removed_count} clothing items")
    print(f"  ✓ Remaining: {len(filtered_items)} motorbikes")
    
    if not filtered_items:
        print("✗ No motorbikes after filtering. Exiting.")
        return
    
    # Step 2: ENRICH
    print("\n[3/4] 🔧 ENRICHING DATA...")
    enricher = WallapopEnricher()
    enriched_items = enricher.enrich_all(filtered_items)
    print(f"  ✓ Enriched: {len(enriched_items)} items")
    
    # Calculate stats
    high_risk = sum(1 for item in enriched_items 
                    if item["enrichment"]["risk_score"] >= 60)
    print(f"  ⚠ High-risk items (score ≥60): {high_risk}")
    
    # Step 3: INGEST
    print("\n[4/4] 📤 INGESTING TO ELASTICSEARCH...")
    ingester = ElasticsearchIngester(ES_HOST, INDEX_ALIAS)
    success, errors = ingester.bulk_ingest(enriched_items)
    
    print(f"  ✓ Successfully indexed: {success}")
    if errors > 0:
        print(f"  ✗ Failed: {errors}")
    
    # Step 4: BACKUP (optional)
    if BACKUP_ENABLED:
        print("\n[BACKUP] 💾 SAVING TO DISK...")
        save_backup(enriched_items)
    
    # Summary
    print("\n" + "=" * 70)
    print("✅ AGENT RUN COMPLETE")
    print(f"   Items collected: {len(items)}")
    print(f"   Items ingested: {success}")
    print(f"   High-risk items: {high_risk}")
    print(f"   Elasticsearch: {ES_HOST}/_cat/indices?v")
    print(f"   Kibana: http://192.168.153.2:5601")
    print("=" * 70)


if __name__ == "__main__":
    main()
