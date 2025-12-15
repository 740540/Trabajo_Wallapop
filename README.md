***

# **Wallapop Motorbike Fraud Detection System**

**Network Operations Centre with Elastic Stack**

Automated fraud detection system for Wallapop motorbike listings using Elasticsearch, Kibana, and Python.

***

## **Table of Contents**

- [Overview](#overview)
- [Architecture](#architecture)
- [Installation Methods](#installation-methods)
  - [Method 1: Docker Compose (Python Direct Ingestion)](#method-1-docker-compose-python-direct-ingestion)
  - [Method 2: Linux Native Install (with Logstash)](#method-2-linux-native-install-with-logstash)
- [Configuration](#configuration)
- [Usage](#usage)
- [Dashboards and Visualizations](#dashboards-and-visualizations)
- [Troubleshooting](#troubleshooting)

***

## **Overview**

This project implements an automated fraud detection system for Wallapop motorbike listings. It:

- **Collects** motorbike listings from Wallapop API
- **Enriches** data with risk scoring based on suspicious keywords and price anomalies
- **Stores** data in Elasticsearch with rotation policies
- **Visualizes** fraud patterns in Kibana dashboards
- **Alerts** via ElastAlert2 (Docker only)

**Key Features:**
- Risk scoring algorithm (0-100)
- Geo-location mapping of suspicious listings
- Keyword-based fraud detection
- Price anomaly detection
- Automated alerting (Docker version)

***

## **Architecture**

### **Docker Version (Simplified)**

```
┌──────────────────────────────────────────────┐
│  Python Script (Local Machine)               │
│  - Collects from Wallapop API               │
│  - Enriches data (risk score calculation)    │
│  - Directly ingests to Elasticsearch        │
└────────────────┬─────────────────────────────┘
                 │ HTTP POST (Bulk API)
                 ▼
┌──────────────────────────────────────────────┐
│  Docker Compose Stack                        │
│                                              │
│  ┌────────────────────┐                      │
│  │ Elasticsearch      │                      │
│  │ :9200              │                      │
│  └────────┬───────────┘                      │
│           │                                  │
│  ┌────────▼───────────┐                      │
│  │ Kibana             │                      │
│  │ :5601              │                      │
│  └────────────────────┘                      │
│                                              │
│  ┌────────────────────┐                      │
│  │ ElastAlert2        │                      │
│  │ (Optional)         │                      │
│  └────────────────────┘                      │
└──────────────────────────────────────────────┘
```

### **Linux Version (Traditional Pipeline)**

```
┌──────────────────────────────────────────────┐
│  Ubuntu Desktop                              │
│                                              │
│  ┌──────────────────┐                        │
│  │ Python Poller    │                        │
│  │ (Raw data)       │                        │
│  └────────┬─────────┘                        │
│           │                                  │
│           │ Writes JSON locally              │
│           ▼                                  │
│  ┌──────────────────┐                        │
│  │ Logstash         │ ◄─ Runs on Desktop    │
│  │ - Reads files    │                        │
│  │ - Enriches data  │                        │
│  │ - Risk scoring   │                        │
│  └────────┬─────────┘                        │
│           │                                  │
│           │ HTTPS (authentication)           │
└───────────┼──────────────────────────────────┘
            │
            ▼
┌──────────────────────────────────────────────┐
│  Server (192.168.153.2)                      │
│                                              │
│  ┌────────────────────┐                      │
│  │ Elasticsearch      │                      │
│  │ :9200 (HTTPS)      │                      │
│  │ user: elastic      │                      │
│  │ password: ****     │                      │
│  └────────┬───────────┘                      │
│           │                                  │
│  ┌────────▼───────────┐                      │
│  │ Kibana             │                      │
│  │ :5601              │                      │
│  └────────────────────┘                      │
└──────────────────────────────────────────────┘
```

### **Key Difference**

| Aspect | Docker Version | Linux Version |
|--------|----------------|---------------|
| **Location** | All on same machine | Desktop → Server |
| **Logstash** | ❌ Not used | ✅ Runs on **Desktop** (Ubuntu) |
| **Enrichment** | Python script | Logstash filters |
| **Connection** | Local (HTTP) | Remote HTTPS with auth |
| **ElastAlert** | ✅ Included | ❌ Not included |
| **Complexity** | Lower | Higher |

***

## **Installation Methods**

### **Method 1: Docker Compose (Python Direct Ingestion)**

#### **Advantages**
✅ **No Logstash needed** - simpler architecture  
✅ Fast setup (< 10 minutes)  
✅ Data enrichment in Python (easier to modify)  
✅ All services on one machine  
✅ Includes ElastAlert2 for alerting  

#### **Prerequisites**

```bash
# Install Docker and Docker Compose
sudo apt update
sudo apt install docker.io docker-compose python3-pip
sudo usermod -aG docker $USER
# Log out and back in

# Install Python dependencies
pip3 install elasticsearch requests
```

#### **Setup**

**1. Clone Repository**

```bash
git clone https://github.com/yourusername/wallapop-fraud-detection.git
cd wallapop-fraud-detection/docker
```

**2. Files Structure**

```
docker/
├── docker-compose.yml           # Docker services definition
├── poller_wallapop.py          # Data collection + enrichment
├── ingest_to_elastic.py        # Bulk ingestion script
├── run_pipeline.sh             # Automated workflow
└── elastalert/                 # Alerting configuration
    ├── config/
    │   └── config.yaml
    └── rules/
        ├── high_risk_motorbike.yaml
        ├── low_price_motorbike.yaml
        └── suspicious_keywords.yaml
```

**3. Start Services**

```bash
# Start Elasticsearch, Kibana, and ElastAlert
docker-compose up -d

# Wait for services
sleep 30

# Run collection and ingestion
./run_pipeline.sh
```

**4. Automate**

```bash
crontab -e
# Add: 0 */2 * * * ~/wallapop-fraud-detection/docker/run_pipeline.sh
```

***

### **Method 2: Linux Native Install (with Logstash)**

#### **Advantages**
✅ Production-ready pipeline  
✅ Logstash filters for data transformation[1]
✅ Better performance for high volume  
✅ Separation: Desktop collects, Server stores  
✅ Follows SNMP lab methodology  

#### **Uses Logstash Filters**

**Yes, the Linux version uses Logstash filters (`mutate`, `ruby`) for enrichment**, exactly like the SNMP practice.[1]

#### **Important: Logstash Runs on Desktop (Ubuntu)**

Unlike the SNMP practice where Logstash runs on the server, in this setup:[1]
- **Logstash runs on Ubuntu Desktop** (where data is collected)
- **Connects to remote Elasticsearch** on server via HTTPS with authentication
- **No ElastAlert2** in this version

***

#### **Prerequisites**

**Server:** Ubuntu/Debian 22.04+ with 4GB+ RAM  
**Desktop:** Ubuntu with Logstash installed

***

#### **Installation Steps**

### **Server Setup (192.168.153.2)**

**1. Install Elasticsearch and Kibana**

```bash
# Add repository
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

# Install
sudo apt update
sudo apt install elasticsearch kibana

# Start services
sudo systemctl enable --now elasticsearch
sudo systemctl enable --now kibana

# Save elastic password!
# Reset if needed:
# sudo /usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic
```

**2. Configure Elasticsearch**

Elasticsearch is configured with security enabled (HTTPS):
- Port: 9200
- Authentication: elastic user + password
- Certificate: `/etc/elasticsearch/http_ca.crt`

**3. Configure Kibana**

Edit `/etc/kibana/kibana.yml`:

```yaml
server.port: 5601
server.host: "0.0.0.0"
elasticsearch.hosts: ["https://localhost:9200"]
elasticsearch.username: "kibana_system"
elasticsearch.password: "YOUR_KIBANA_PASSWORD"
elasticsearch.ssl.certificateAuthorities: ["/etc/elasticsearch/http_ca.crt"]
```

**4. Create Elasticsearch Structure**

```bash
# Clone repo on server
git clone https://github.com/yourusername/wallapop-fraud-detection.git
cd wallapop-fraud-detection/linux/server

# Run setup
./setup_elasticsearch.sh
```

This creates:
- ILM policy (`elasticsearch/ilm_policy.json`)
- Index template (`elasticsearch/index_template.json`)
- Initial index (`elasticsearch/create_index.json`)

***

### **Desktop Setup (Ubuntu)**

**1. Install Logstash**

```bash
# Add Elastic repository
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

# Install
sudo apt update
sudo apt install logstash
```

**2. Clone Repository**

```bash
git clone https://github.com/yourusername/wallapop-fraud-detection.git
cd wallapop-fraud-detection/linux/desktop
```

**3. Files Structure**

```
linux/desktop/
├── poller_wallapop.py                # Data collection (raw)
├── logstash/
│   ├── 10.input.wallapop.conf        # File input
│   ├── 20.filter.wallapop.conf       # Filters (mutate, ruby)
│   └── 30.output.elastic.conf        # Remote ES output (HTTPS + auth)
└── data/                             # Output directory
```

**4. Copy Logstash Configuration**

```bash
# Copy configs
sudo cp logstash/*.conf /etc/logstash/conf.d/

# Create data directory
mkdir -p ~/wallapop-agent/data
```

**5. Configure Logstash Output**

Edit `/etc/logstash/conf.d/30.output.elastic.conf`:

**Key difference from SNMP practice:** Connects to **remote server with authentication**

```ruby
output {
  if "wallapop" in [tags] {
    elasticsearch {
      hosts => ["https://192.168.153.2:9200"]  # Remote server
      user => "elastic"                         # Authentication
      password => "YOUR_ELASTIC_PASSWORD"       # Authentication
      
      ssl => true
      cacert => "/path/to/http_ca.crt"         # Copy from server
      ssl_certificate_verification => true
      
      index => "%{[orgName]}.%{[eventType]}"
      
      id => "30.output.elastic"
    }
  }
}
```

**6. Copy CA Certificate from Server**

```bash
# On desktop
scp user@192.168.153.2:/etc/elasticsearch/http_ca.crt ~/wallapop-agent/
sudo cp ~/wallapop-agent/http_ca.crt /etc/logstash/
```

**7. Start Logstash**

```bash
sudo systemctl enable --now logstash

# Check logs
sudo tail -f /var/log/logstash/logstash-plain.log
```

**8. Run Poller**

```bash
cd ~/wallapop-fraud-detection/linux/desktop
chmod +x poller_wallapop.py

# Test
./poller_wallapop.py
# Outputs to: data/wallapop_YYYYMMDD.json

# Logstash automatically processes and sends to server
```

**9. Automate**

```bash
crontab -e
# Add: 0 */2 * * * ~/wallapop-fraud-detection/linux/desktop/poller_wallapop.py
```

***

## **Configuration**

### **Logstash Configuration (Linux Only)**

#### **Pipeline Files**

Located in `linux/desktop/logstash/`:

**1. `10.input.wallapop.conf`**
- File input plugin
- Reads JSON from `~/wallapop-agent/data/`
- Adds initial tags and fields

**2. `20.filter.wallapop.conf`**
- **Mutate filter**: Add metadata fields[1]
- **Ruby filter**: Calculate risk score, normalize geo_point[1]
- **Mutate filter**: Remove unnecessary fields[1]

**Uses same filter approach as SNMP practice**[1]

**3. `30.output.elastic.conf`**
- Elasticsearch output
- **Remote connection**: `https://192.168.153.2:9200`
- **Authentication**: `user` and `password` (not in SNMP practice)[1]
- **SSL**: `cacert` for certificate verification
- Index pattern: `lab001.wallapop`

**Key Difference from SNMP Practice:**
```ruby
# SNMP (local)[file:61]:
hosts => ["https://localhost:9200"]

# Wallapop (remote with auth):
hosts => ["https://192.168.153.2:9200"]
user => "elastic"
password => "YOUR_PASSWORD"
```

***

### **Python Scripts**

#### **Docker Version**

**1. `poller_wallapop.py`**
- Collects from Wallapop API
- **Enriches data** (risk scoring in Python)
- Saves to `data/wallapop_YYYYMMDD_enriched.json`

**2. `ingest_to_elastic.py`**
- Reads enriched JSON
- Bulk ingests to Elasticsearch
- Uses `helpers.streaming_bulk()`

**3. `run_pipeline.sh`**
- Wrapper script
- Runs poller → ingest

***

#### **Linux Version**

**1. `poller_wallapop.py`** (on Desktop)
- Collects from Wallapop API
- **No enrichment** (done by Logstash)
- Saves raw JSON to `data/`
- Logstash watches this directory

***

## **Usage**

### **Docker Version**

```bash
cd docker

# Start services (Elasticsearch, Kibana, ElastAlert)
docker-compose up -d

# Run collection and ingestion
./run_pipeline.sh

# Check results
curl http://localhost:9200/lab001.wallapop/_count

# Access Kibana
# http://localhost:5601

# Stop
docker-compose down
```

***

### **Linux Version**

```bash
# On desktop - check Logstash
sudo systemctl status logstash

# On desktop - run poller
cd ~/wallapop-fraud-detection/linux/desktop
./poller_wallapop.py

# Logstash automatically processes and sends to server

# On server - verify
curl -u elastic:PASSWORD \
  --cacert /etc/elasticsearch/http_ca.crt \
  "https://localhost:9200/lab001.wallapop/_count"

# On desktop - view Logstash logs
sudo tail -f /var/log/logstash/logstash-plain.log
```

***

## **Dashboards and Visualizations**

### **Create Data View**

1. **Kibana → Stack Management → Data Views**
2. Pattern: `lab001.wallapop*`
3. Time field: `timestamps.crawl_timestamp`

### **Recommended Visualizations**

1. **Metric Cards**: Total listings, high-risk count, % fraud
2. **Line Chart**: Risk score trends over time
3. **Geo Map**: Suspicious listings by location (filter: `enrichment.risk_score >= 60`)
4. **Tag Cloud**: Most common suspicious keywords
5. **Data Table**: High-risk listings for investigation

**See `dashboards/` directory for exported JSON definitions.**

***

## **ElastAlert2 (Docker Only)**

### **Configuration Files**

Located in `docker/elastalert/`:

- `config/config.yaml` - Main configuration
- `rules/high_risk_motorbike.yaml` - Alert on risk_score >= 60
- `rules/low_price_motorbike.yaml` - Alert on price anomalies
- `rules/suspicious_keywords.yaml` - Alert on critical keywords

**Already included in `docker-compose.yml`** - just configure email/Slack webhooks in rule files.

**Note:** ElastAlert2 is **not included in Linux version** for simplicity.

***

## **Troubleshooting**

### **Docker Issues**

```bash
# Clean restart
docker-compose down -v
docker-compose up -d

# Check logs
docker-compose logs elasticsearch
docker-compose logs elastalert

# Verify connectivity
curl http://localhost:9200
```

### **Linux Issues**

#### **Desktop (Logstash)**

```bash
# Check Logstash service
sudo systemctl status logstash

# View logs
sudo tail -f /var/log/logstash/logstash-plain.log

# Test config
sudo /usr/share/logstash/bin/logstash -t -f /etc/logstash/conf.d/

# Check if files are being read
ls -la ~/wallapop-agent/data/
```

#### **Server (Elasticsearch)**

```bash
# Check service
sudo systemctl status elasticsearch

# View logs
sudo journalctl -u elasticsearch -f

# Test connectivity from desktop
curl -u elastic:PASSWORD \
  --cacert /path/to/http_ca.crt \
  "https://192.168.153.2:9200"
```

### **Authentication Issues**

```bash
# On desktop - test connection
curl -u elastic:YOUR_PASSWORD \
  --cacert /etc/logstash/http_ca.crt \
  "https://192.168.153.2:9200"

# Should return cluster info

# If certificate error, check:
# 1. CA cert copied correctly
# 2. Path in Logstash config matches
```

***

## **Project Files**

### **Docker Version**

| File | Purpose |
|------|---------|
| `docker-compose.yml` | ES, Kibana, ElastAlert |
| `poller_wallapop.py` | Collection + enrichment |
| `ingest_to_elastic.py` | Bulk ingestion |
| `run_pipeline.sh` | Automated workflow |
| `elastalert/` | Alert rules |

### **Linux Version**

| File | Location | Purpose |
|------|----------|---------|
| `poller_wallapop.py` | Desktop | Collection only |
| `10.input.wallapop.conf` | Desktop | File input |
| `20.filter.wallapop.conf` | Desktop | **Filters (mutate, ruby)**[1] |
| `30.output.elastic.conf` | Desktop | Remote ES output (**with auth**) |
| `setup_elasticsearch.sh` | Server | ILM, template, index setup |

***

## **Comparison Summary**

| Aspect | Docker (Python) | Linux (Logstash) |
|--------|----------------|------------------|
| **Architecture** | All on one machine | Desktop → Server |
| **Logstash Location** | ❌ Not used | ✅ Desktop (Ubuntu) |
| **ES Connection** | Local HTTP | **Remote HTTPS + auth** |
| **Enrichment** | Python | Logstash filters[1] |
| **ElastAlert** | ✅ Included | ❌ Not included |
| **Setup** | 10 min | 30-60 min |
| **Best For** | Dev/Testing/Single machine | Production/Distributed |

***

## **References**

- GitHub Repository: [Link to your repo]
- SNMP Lab Practice (P3)[1]
- Elastic Documentation: https://www.elastic.co/guide/

***

## **License**

Academic project - Universidad de Zaragoza

***