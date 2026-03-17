#!/bin/bash

# ============================================
# Script  Monitoring WiFi
# ============================================

# Warna untuk output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  Uninstall Monitoring WiFi Service   ${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# 1. Matikan service yang sedang jalan
echo -e "${YELLOW}[1/6] Menghentikan service monitoring-wifi...${NC}"
sudo systemctl stop monitoring-wifi
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Service berhasil dihentikan${NC}"
else
    echo -e "${RED}✗ Service tidak ditemukan atau sudah berhenti${NC}"
fi
echo ""

# 2. Matikan fitur auto-start
echo -e "${YELLOW}[2/6] Menonaktifkan auto-start service...${NC}"
sudo systemctl disable monitoring-wifi
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Auto-start berhasil dinonaktifkan${NC}"
else
    echo -e "${RED}✗ Gagal menonaktifkan auto-start${NC}"
fi
echo ""

# 3. Hapus file service
echo -e "${YELLOW}[3/6] Menghapus file service...${NC}"
sudo rm -f /etc/systemd/system/monitoring-wifi.service
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ File service berhasil dihapus${NC}"
else
    echo -e "${RED}✗ File service tidak ditemukan${NC}"
fi
echo ""

# 4. Reload daemon
echo -e "${YELLOW}[4/6] Reload systemd daemon...${NC}"
sudo systemctl daemon-reload
echo -e "${GREEN}✓ Daemon reloaded${NC}"
echo ""

# 5. Reset failed
echo -e "${YELLOW}[5/6] Reset failed services...${NC}"
sudo systemctl reset-failed
echo -e "${GREEN}✓ Failed services direset${NC}"
echo ""

# 6. Hapus folder
echo -e "${YELLOW}[6/6] Menghapus folder monitoring-wifi...${NC}"
cd /root/
sudo rm -rf monitoring-wifi
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Folder berhasil dihapus${NC}"
else
    echo -e "${RED}✗ Gagal menghapus folder${NC}"
fi
echo ""

# Selesai
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Uninstall Selesai!                  ${NC}"
echo -e "${GREEN}========================================${NC}"
