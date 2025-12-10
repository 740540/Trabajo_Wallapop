#!/usr/bin/env python3
"""
Wallapop Motorbike Poller
Collects motorbike listings from Wallapop API and saves to daily JSON files
"""

import requests
import json
import os
from datetime import datetime
from typing import List, Dict, Optional
import time

# Configuration
API_URL = "https://api.wallapop.com/api/v3/search"
HEADERS = {
    "Host": "api.wallapop.com",
    "X-DeviceOS": "0"
}

# Motorbike category ID (adjust based on actual Wallapop structure)
MOTORBIKE_CATEGORY_ID = "14000"  # Motors -> Motorbikes

# Search configuration
SEARCH_CONFIG = {
    "source": "search_box",
    "categoryid": MOTORBIKE_CATEGORY_ID,
    "latitude": 40.4168,  # Madrid coordinates (adjust for your region)
    "longitude": -3.7038,
    "timefilter": "today",  # Only today's listings
    "min_sale_price": 500,  # Minimum reasonable motorbike price
    "max_sale_price": 50000,  # Maximum to filter out noise
    "order_by": "newest"
}

# Keywords to search (optional - remove if you want all motorbikes)
MOTORBIKE_KEYWORDS = [
    "yamaha",
    "honda",
    "kawasaki",
    "suzuki",
    "ktm",
    "bmw",
    "ducati",
    "triumph",
    "harley",
    "moto"
]

OUTPUT_DIR = "data"


class WallapopPoller:
    """Handles polling of Wallapop API for motorbike listings"""
    
    def __init__(self, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def fetch_items(self, keywords: Optional[str] = None, 
                   max_retries: int = 3) -> List[Dict]:
        """
        Fetch items from Wallapop API
        
        Args:
            keywords: Optional search keywords
            max_retries: Number of retry attempts
            
        Returns:
            List of item dictionaries
        """
        params = SEARCH_CONFIG.copy()
        
        if keywords:
            params["keywords"] = keywords
            
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    API_URL,
                    params=params,
                    headers=HEADERS,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Extract items from response (adjust path based on actual structure)
                # Common paths: data.section.payload.items or search_objects
                items = []
                
                # Try different possible response structures
                if "data" in data and "section" in data["data"]:
                    items = data["data"]["section"].get("payload", {}).get("items", [])
                elif "search_objects" in data:
                    items = data["search_objects"]
                elif "data" in data:
                    items = data.get("data", [])
                else:
                    items = data.get("items", [])
                
                print(f"✓ Fetched {len(items)} items for keywords: {keywords or 'all'}")
                return items
                
            except requests.exceptions.RequestException as e:
                print(f"⚠ Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"✗ Failed to fetch items after {max_retries} attempts")
                    return []
                    
        return []
    
    def fetch_item_details(self, item_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific item
        
        Args:
            item_id: Wallapop item ID
            
        Returns:
            Item details dictionary or None
        """
        try:
            url = f"https://api.wallapop.com/api/v3/items/{item_id}"
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"⚠ Could not fetch details for item {item_id}: {e}")
            return None
    
    def collect_all_items(self, use_keywords: bool = True) -> List[Dict]:
        """
        Collect all motorbike items, optionally using multiple keyword searches
        
        Args:
            use_keywords: Whether to search with specific keywords
            
        Returns:
            List of all collected items
        """
        all_items = []
        seen_ids = set()
        
        if use_keywords:
            for keyword in MOTORBIKE_KEYWORDS:
                items = self.fetch_items(keywords=keyword)
                
                # Deduplicate items
                for item in items:
                    item_id = item.get("id")
                    if item_id and item_id not in seen_ids:
                        all_items.append(item)
                        seen_ids.add(item_id)
                
                time.sleep(1)  # Be respectful to the API
        else:
            # Single search without keywords
            all_items = self.fetch_items()
        
        print(f"\n✓ Total unique items collected: {len(all_items)}")
        return all_items
    
    def save_daily_file(self, items: List[Dict]) -> str:
        """
        Save items to a daily JSON file (one JSON object per line)
        
        Args:
            items: List of item dictionaries
            
        Returns:
            Path to saved file
        """
        today = datetime.utcnow().strftime("%Y%m%d")
        filename = os.path.join(self.output_dir, f"wallapop_motorbikes_{today}.json")
        
        with open(filename, 'w', encoding='utf-8') as f:
            for item in items:
                # Add collection timestamp
                item['crawl_timestamp'] = datetime.utcnow().isoformat() + 'Z'
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        print(f"✓ Saved {len(items)} items to {filename}")
        return filename


def main():
    """Main execution function"""
    print("=" * 60)
    print("Wallapop Motorbike Poller")
    print("=" * 60)
    
    poller = WallapopPoller()
    
    # Collect items
    print("\n🔍 Collecting motorbike listings...")
    items = poller.collect_all_items(use_keywords=True)
    
    if items:
        # Save to daily file
        print("\n💾 Saving to daily file...")
        filepath = poller.save_daily_file(items)
        print(f"\n✓ Success! Data saved to: {filepath}")
    else:
        print("\n✗ No items collected")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
