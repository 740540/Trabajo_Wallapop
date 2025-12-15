# Wallapop Motorbike Fraud Detection Agent

Automated agent that polls Wallapop, enriches data, and ingests to Elasticsearch.

## Installation

```
# Install dependencies
pip3 install -r requirements.txt

# Install systemd service
sudo cp systemd/wallapop-agent.service /etc/systemd/system/
sudo cp systemd/wallapop-agent.timer /etc/systemd/system/

# Edit paths in service file
sudo nano /etc/systemd/system/wallapop-agent.service
# Change: User=youruser, WorkingDirectory=/your/path

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable wallapop-agent.timer
sudo systemctl start wallapop-agent.timer
```

## Usage

```
# Manual run
./run_agent.sh

# Check timer status
systemctl list-timers | grep wallapop

# View logs
sudo journalctl -u wallapop-agent.service -f
```

## Configuration

Edit `wallapop_agent.py`:
- `ES_HOST`: Elasticsearch URL
- `SEARCH_LOCATION`: Your city coordinates
- `MOTORBIKE_KEYWORDS`: Search terms
```

***

## **Installation Steps**

```bash
# 1. Create project
cd ~
mkdir wallapop-agent
cd wallapop-agent

# 2. Create structure
mkdir -p config data logs systemd

# 3. Copy files
# (Copy wallapop_agent.py, run_agent.sh, systemd files)

# 4. Make executable
chmod +x wallapop_agent.py run_agent.sh

# 5. Test manually
./run_agent.sh

# 6. Install systemd
sudo cp systemd/*.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable wallapop-agent.timer
sudo systemctl start wallapop-agent.timer

# 7. Verify
systemctl status wallapop-agent.timer
sudo journalctl -u wallapop-agent.service -f
```

***

## **Summary**

**One script does everything**: `wallapop_agent.py`  
**Runs every 30 minutes** via systemd timer  
**No manual steps** needed after setup  

This is your "agent" - fully automated! 🤖