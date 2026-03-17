#!/bin/bash

# ============================================
# Script Auto-Install Monitoring WiFi
# Jalankan: chmod +x install.sh && ./install.sh
# ============================================

set -e  # Exit jika ada error

echo "=========================================="
echo "  🚀 Memulai Instalasi Monitoring WiFi"
echo "=========================================="

# 1. Update package list
echo "📦 [1/9] Updating package list..."
apt update -qq

# 2. Install unzip
echo "📦 [2/9] Installing unzip..."
apt install unzip -y -qq

# 3. Install Python & dependencies
echo "📦 [3/9] Installing Python3, pip, venv, curl, wget..."
apt install python3 python3-pip python3-venv curl wget -y -qq

# 4. Buat direktori
echo "📁 [4/9] Creating directory /root/monitoring-wifi..."
mkdir -p /root/monitoring-wifi

# 5. Masuk ke direktori
echo "📂 [5/9] Changing to working directory..."
cd /root/monitoring-wifi

# 6. Buat virtual environment
echo "🐍 [6/9] Creating Python virtual environment..."
python3 -m venv venv

# 7. Install Python packages dalam venv
echo "⬇️  [7/9] Installing Python packages (flask, psutil, requests, routeros_api, icmplib, flask-compress, gunicorn)..."
source venv/bin/activate
pip install --quiet flask psutil requests routeros_api icmplib flask-compress gunicorn
deactivate

# 8. Download file monitoring
echo "⬇️  [8/9] Downloading monitoring.zip..."
wget -q --show-progress https://github.com/Skysurver007/NMS/raw/refs/heads/main/monitoring.zip

# 9. Extract dan cleanup
echo "📂 [9/9] Extracting and cleaning up..."
unzip -q monitoring.zip
rm -f monitoring.zip

echo ""
echo "=========================================="
echo "  ✅ Instalasi SELESAI!"
echo "=========================================="
echo ""
echo "📍 Lokasi: /root/monitoring-wifi"
echo "🐍 Virtual env: venv/"
echo ""
echo "🔧 Cara menjalankan:"
echo "   cd /root/monitoring-wifi"
echo "   source venv/bin/activate"
echo "   python app.py  # atau gunicorn"
echo ""
echo "=========================================="
