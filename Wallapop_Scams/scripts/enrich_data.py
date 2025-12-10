#!/usr/bin/env python3
"""
Data Enrichment Script
Adds fraud detection features to collected Wallapop data
"""

import json
import os
import re
from typing import Dict, List, Tuple
from statistics import median, mean
from datetime import datetime


class MotorbikeEnricher:
    """Enriches motorbike listings with fraud detection features"""
    
    def __init__(self, keywords_file: str = "config/suspicious_keywords.json"):
        # Load suspicious keywords
        with open(keywords_file, 'r', encoding='utf-8') as f:
            keywords_data = json.load(f)
            
        self.suspicious_keywords = []
        for category in keywords_data.values():
            self.suspicious_keywords.extend(category)
        
        self.all_keywords_data = keywords_data
        
    def extract_price(self, item: Dict) -> float:
        """Extract price from item"""
        if "price" in item:
            if isinstance(item["price"], dict):
                return float(item["price"].get("amount", 0))
            return float(item["price"])
        return 0.0
    
    def detect_suspicious_keywords(self, text: str) -> List[str]:
        """
        Detect suspicious keywords in text
        
        Args:
            text: Text to analyze (title + description)
            
        Returns:
            List of matched suspicious keywords
        """
        text_lower = text.lower()
        found_keywords = []
        
        for keyword in self.suspicious_keywords:
            if keyword.lower() in text_lower:
                found_keywords.append(keyword)
        
        return found_keywords
    
    def calculate_price_features(self, items: List[Dict]) -> Dict:
        """
        Calculate price statistics for the dataset
        
        Returns:
            Dictionary with price statistics
        """
        prices = [self.extract_price(item) for item in items]
        prices = [p for p in prices if p > 0]
        
        if not prices:
            return {"median": 0, "mean": 0, "min": 0, "max": 0}
        
        return {
            "median": median(prices),
            "mean": mean(prices),
            "min": min(prices),
            "max": max(prices)
        }
    
    def count_seller_items(self, items: List[Dict]) -> Dict[str, int]:
        """Count items per seller"""
        seller_counts = {}
        
        for item in items:
            seller_id = item.get("user_id") or item.get("userid") or "unknown"
            seller_counts[seller_id] = seller_counts.get(seller_id, 0) + 1
        
        return seller_counts
    
    def calculate_risk_score(self, item: Dict, enrichment: Dict, 
                            price_stats: Dict, seller_counts: Dict) -> Tuple[int, List[str]]:
        """
        Calculate risk score (0-100) based on multiple signals
        
        Returns:
            Tuple of (risk_score, risk_factors)
        """
        score = 0
        risk_factors = []
        
        price = enrichment["price"]
        
        # 1. Price-based signals (max 40 points)
        if price_stats["median"] > 0:
            price_ratio = price / price_stats["median"]
            
            if price_ratio < 0.3:  # Extremely low price
                score += 40
                risk_factors.append("extremely_low_price")
            elif price_ratio < 0.5:  # Very low price
                score += 30
                risk_factors.append("very_low_price")
            elif price_ratio < 0.7:  # Low price
                score += 15
                risk_factors.append("low_price")
        
        # 2. Suspicious keywords (max 30 points)
        suspicious_kw = enrichment.get("suspicious_keywords", [])
        
        if any(kw in self.all_keywords_data["motorbike_specific"] 
               for kw in suspicious_kw):
            score += 25
            risk_factors.append("motorbike_fraud_keywords")
        elif any(kw in self.all_keywords_data["general_fraud"] 
                 for kw in suspicious_kw):
            score += 15
            risk_factors.append("general_fraud_keywords")
        
        # 3. Seller behavior (max 20 points)
        seller_id = item.get("user_id") or item.get("userid") or "unknown"
        seller_item_count = seller_counts.get(seller_id, 0)
        
        if seller_item_count > 10:
            score += 20
            risk_factors.append("high_volume_seller")
        elif seller_item_count > 5:
            score += 10
            risk_factors.append("medium_volume_seller")
        
        # 4. Description quality (max 10 points)
        description = item.get("description", "")
        if len(description) < 20:
            score += 10
            risk_factors.append("short_description")
        
        # Cap at 100
        score = min(score, 100)
        
        return score, risk_factors
    
    def enrich_item(self, item: Dict, price_stats: Dict, 
                   seller_counts: Dict) -> Dict:
        """
        Add enrichment fields to a single item
        
        Returns:
            Enriched item dictionary
        """
        # Extract base fields
        item_id = item.get("id", "unknown")
        title = item.get("title", "")
        description = item.get("description", "")
        price = self.extract_price(item)
        
        # Combine text for keyword analysis
        full_text = f"{title} {description}"
        
        # Detect suspicious keywords
        suspicious_kw = self.detect_suspicious_keywords(full_text)
        
        # Build enrichment object
        enrichment = {
            "price": price,
            "suspicious_keywords": suspicious_kw,
            "has_suspicious_keywords": len(suspicious_kw) > 0,
            "relative_price_index": (price / price_stats["median"] 
                                    if price_stats["median"] > 0 else 0)
        }
        
        # Calculate risk score
        risk_score, risk_factors = self.calculate_risk_score(
            item, enrichment, price_stats, seller_counts
        )
        
        enrichment["risk_score"] = risk_score
        enrichment["risk_factors"] = risk_factors
        
        # Add seller stats
        seller_id = item.get("user_id") or item.get("userid") or "unknown"
        enrichment["seller_items_today"] = seller_counts.get(seller_id, 0)
        
        # Normalize item structure for Elasticsearch
        normalized_item = {
            "id": item_id,
            "title": title,
            "description": description,
            "price": price,
            "currency": item.get("currency", "EUR"),
            "seller_id": seller_id,
            "category_id": item.get("category_id") or item.get("categoryid"),
            "web_slug": item.get("web_slug") or item.get("webslug"),
            
            # Location
            "location": self._normalize_location(item),
            
            # Timestamps
            "timestamps": self._normalize_timestamps(item),
            
            # Taxonomy
            "taxonomy": item.get("taxonomy", []),
            
            # Enrichment
            "enrichment": enrichment
        }
        
        return normalized_item
    
    def _normalize_location(self, item: Dict) -> Dict:
        """Normalize location data"""
        location = item.get("location", {})
        
        lat = location.get("latitude")
        lon = location.get("longitude")
        
        result = {
            "city": location.get("city", ""),
            "postal_code": location.get("postal_code") or location.get("postalcode", ""),
            "region": location.get("region", "")
        }
        
        # Add geopoint if coordinates exist
        if lat is not None and lon is not None:
            result["geo"] = {
                "lat": float(lat),
                "lon": float(lon)
            }
        
        return result
    
    def _normalize_timestamps(self, item: Dict) -> Dict:
        """Normalize timestamp fields"""
        created_at = item.get("created_at") or item.get("createdat")
        modified_at = item.get("modified_at") or item.get("modifiedat")
        crawl_ts = item.get("crawl_timestamp")
        
        def normalize_timestamp(ts):
            """Convert timestamp to ISO format"""
            if not ts:
                return None
            
            # If milliseconds since epoch
            if isinstance(ts, (int, float)) and ts > 1000000000000:
                ts = ts / 1000
            
            if isinstance(ts, (int, float)):
                return datetime.utcfromtimestamp(ts).isoformat() + 'Z'
            
            return ts
        
        return {
            "created_at": normalize_timestamp(created_at),
            "modified_at": normalize_timestamp(modified_at),
            "crawl_timestamp": crawl_ts or datetime.utcnow().isoformat() + 'Z'
        }
    
    def enrich_file(self, input_file: str, output_file: str):
        """
        Enrich an entire daily file
        
        Args:
            input_file: Path to raw data file
            output_file: Path to save enriched data
        """
        print(f"📖 Reading {input_file}...")
        
        # Load all items
        items = []
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    items.append(json.loads(line))
        
        print(f"✓ Loaded {len(items)} items")
        
        # Calculate global statistics
        print("📊 Calculating statistics...")
        price_stats = self.calculate_price_features(items)
        seller_counts = self.count_seller_items(items)
        
        print(f"   Price median: €{price_stats['median']:.2f}")
        print(f"   Price range: €{price_stats['min']:.2f} - €{price_stats['max']:.2f}")
        print(f"   Unique sellers: {len(seller_counts)}")
        
        # Enrich each item
        print("🔧 Enriching items...")
        enriched_items = []
        for item in items:
            enriched = self.enrich_item(item, price_stats, seller_counts)
            enriched_items.append(enriched)
        
        # Save enriched data
        print(f"💾 Saving to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in enriched_items:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        # Print statistics
        high_risk_count = sum(1 for item in enriched_items 
                             if item["enrichment"]["risk_score"] >= 60)
        
        print(f"\n✓ Enrichment complete!")
        print(f"   High-risk items (score ≥ 60): {high_risk_count}")
        print(f"   Saved to: {output_file}")


def main():
    """Main execution"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python enrich_data.py <input_file> [output_file]")
        print("\nExample:")
        print("  python enrich_data.py data/wallapop_motorbikes_20251210.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    # Generate output filename
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    else:
        base = os.path.splitext(input_file)[0]
        output_file = f"{base}_enriched.json"
    
    enricher = MotorbikeEnricher()
    enricher.enrich_file(input_file, output_file)


if __name__ == "__main__":
    main()
