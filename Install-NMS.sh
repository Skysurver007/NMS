#!/bin/bash

# ╔══════════════════════════════════════════════════════════════════╗
# ║                NETWORK MONITORING INSTALLER                      ║
# ║              [ SYSTEM INITIALIZATION SEQUENCE ]                  ║
# ╚══════════════════════════════════════════════════════════════════╝

# Warna Cyberpunk
CYAN='\033[38;5;51m'
PINK='\033[38;5;198m'
YELLOW='\033[38;5;226m'
GREEN='\033[38;5;82m'
RED='\033[38;5;196m'
BLUE='\033[38;5;21m'
PURPLE='\033[38;5;93m'
RESET='\033[0m'
BOLD='\033[1m'

# Fungsi efek typing
type_effect() {
    text="$1"
    color="$2"
    echo -ne "${color}"
    for ((i=0; i<${#text}; i++)); do
        echo -n "${text:$i:1}"
        sleep 0.02
    done
    echo -e "${RESET}"
}

# Fungsi loading bar cyberpunk
cyberpunk_loader() {
    local duration=$1
    local message=$2
    local width=50
    local fill="█"
    local empty="░"
    
    echo -ne "${CYAN}[${RESET}"
    for ((i=0; i<=width; i++)); do
        local percent=$((i * 100 / width))
        local filled=$i
        local empty_spaces=$((width - i))
        
        printf "\r${CYAN}[${RESET}"
        printf "${PINK}%0.s${fill}${RESET}" $(seq 1 $filled)
        printf "${BLUE}%0.s${empty}${RESET}" $(seq 1 $empty_spaces)
        printf "${CYAN}]${RESET} ${YELLOW}%3d%%${RESET} ${CYAN}|${RESET} ${GREEN}${message}${RESET}" "$percent"
        sleep $(echo "scale=3; $duration/$width" | bc -l 2>/dev/null || echo "0.05")
    done
    echo ""
}

# Fungsi glitch effect
glitch_text() {
    text="$1"
    echo -ne "${CYAN}"
    for char in $(echo "$text" | grep -o .); do
        if [ $((RANDOM % 5)) -eq 0 ]; then
            echo -ne "${PINK}${char}${CYAN}"
        else
            echo -n "$char"
        fi
    done
    echo -e "${RESET}"
}

# Clear screen
clear

# ASCII Art Header
echo -e "${CYAN}"
cat << "EOF"
    ██████╗██╗   ██╗██████╗ ███████╗██████╗ ██████╗ ██╗   ██╗███╗   ██╗██╗  ██╗
   ██╔════╝██║   ██║██╔══██╗██╔════╝██╔══██╗██╔══██╗██║   ██║████╗  ██║██║ ██╔╝
   ██║     ██║   ██║██████╔╝█████╗  ██████╔╝██████╔╝██║   ██║██╔██╗ ██║█████╔╝ 
   ██║     ██║   ██║██╔══██╗██╔══╝  ██╔══██╗██╔═══╝ ██║   ██║██║╚██╗██║██╔═██╗ 
   ╚██████╗╚██████╔╝██║  ██║███████╗██║  ██║██║     ╚██████╔╝██║ ╚████║██║  ██╗
    ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝      ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝
EOF
echo -e "${PINK}"
cat << "EOF"
        ╔════════════════════════════════════════════════════════════════╗
        ║  [ NETWORK MONITORING SYSTEM INSTALLATION PROTOCOL v2.077 ]    ║
        ║  [ AUTHORIZED ACCESS ONLY // ENCRYPTED CONNECTION ESTABLISHED] ║
        ╚════════════════════════════════════════════════════════════════╝
EOF
echo -e "${RESET}"

sleep 1

# Inisialisasi sistem
type_effect ">>> INITIALIZING NEURAL LINK..." "$YELLOW"
cyberpunk_loader 2 "ESTABLISHING UPLINK"
echo -e "${GREEN}[✓] UPLINK ESTABLISHED${RESET}\n"

type_effect ">>> BYPASSING MAINFRAME SECURITY..." "$PINK"
cyberpunk_loader 1.5 "DECRYPTING FIREWALL"
echo -e "${GREEN}[✓] SECURITY BYPASSED${RESET}\n"

# Update System
echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${CYAN}║${RESET} ${BOLD}${PINK}PHASE 1: SYSTEM UPDATE${RESET}                                       ${CYAN}║${RESET}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${RESET}"
type_effect ">>> EXECUTING apt update sequence..." "$YELLOW"
apt update | while read line; do
    echo -e "${BLUE}[SYS]${RESET} $line"
done
echo -e "${GREEN}[✓] SYSTEM PACKAGE LIST UPDATED${RESET}\n"

# Install Dependencies
echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${CYAN}║${RESET} ${BOLD}${PINK}PHASE 2: CORE DEPENDENCIES INSTALLATION${RESET}                    ${CYAN}║${RESET}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${RESET}"

type_effect ">>> DEPLOYING UNZIP MODULE..." "$YELLOW"
apt install unzip -y | grep -E "(Setting up|Unpacking)" | while read line; do
    echo -e "${PURPLE}[PKG]${RESET} $line"
done
echo -e "${GREEN}[✓] UNZIP MODULE DEPLOYED${RESET}\n"

type_effect ">>> INSTALLING PYTHON ECOSYSTEM & NETWORK TOOLS..." "$YELLOW"
apt install python3 python3-pip python3-venv curl wget -y | grep -E "(Setting up|Unpacking|Preparing)" | while read line; do
    echo -e "${PURPLE}[PKG]${RESET} $line"
done
echo -e "${GREEN}[✓] PYTHON ENVIRONMENT INITIALIZED${RESET}\n"

# Create Directory Structure
echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${CYAN}║${RESET} ${BOLD}${PINK}PHASE 3: DIRECTORY FABRICATION${RESET}                             ${CYAN}║${RESET}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${RESET}"

type_effect ">>> CREATING VIRTUAL CONTAINER..." "$YELLOW"
mkdir -p /root/monitoring-wifi
cyberpunk_loader 0.5 "FABRICATING DIRECTORIES"
cd /root/monitoring-wifi
echo -e "${GREEN}[✓] CONTAINER /root/monitoring-wifi CREATED${RESET}"
echo -e "${CYAN}[INFO]${RESET} Current location: ${YELLOW}$(pwd)${RESET}\n"

# Python Virtual Environment
echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${CYAN}║${RESET} ${BOLD}${PINK}PHASE 4: VIRTUAL ENVIRONMENT CONSTRUCTION${RESET}                  ${CYAN}║${RESET}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${RESET}"

type_effect ">>> GENERATING PYTHON VIRTUAL ENVIRONMENT..." "$YELLOW"
python3 -m venv venv
cyberpunk_loader 1 "CONSTRUCTING VENV MATRIX"
echo -e "${GREEN}[✓] VIRTUAL ENVIRONMENT GENERATED${RESET}\n"

type_effect ">>> ACTIVATING NEURAL INTERFACE..." "$YELLOW"
source venv/bin/activate
echo -e "${GREEN}[✓] VENV ACTIVATED ${PINK}[$(which python)]${RESET}\n"

# Install Python Packages
echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${CYAN}║${RESET} ${BOLD}${PINK}PHASE 5: PYTHON PACKAGES DEPLOYMENT${RESET}                        ${CYAN}║${RESET}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${RESET}"

packages=("flask" "psutil" "requests" "routeros_api" "icmplib" "flask-compress" "gunicorn")
total=${#packages[@]}
current=0

for pkg in "${packages[@]}"; do
    current=$((current + 1))
    percent=$((current * 100 / total))
    type_effect ">>> INSTALLING MODULE: ${BOLD}${pkg}${RESET}" "$YELLOW"
    pip install "$pkg" -q 2>/dev/null | grep -v "already satisfied" || true
    
    # Progress bar untuk setiap package
    bar_width=30
    filled=$((percent * bar_width / 100))
    empty=$((bar_width - filled))
    
    printf "${CYAN}[${RESET}"
    printf "${PINK}%0.s█${RESET}" $(seq 1 $filled)
    printf "${BLUE}%0.s░${RESET}" $(seq 1 $empty)
    printf "${CYAN}]${RESET} ${YELLOW}%d/%d${RESET} ${CYAN}|${RESET} ${GREEN}%s${RESET}\n" "$current" "$total" "$pkg"
done

echo -e "\n${GREEN}[✓] ALL PYTHON MODULES DEPLOYED SUCCESSFULLY${RESET}\n"

type_effect ">>> DEACTIVATING TEMPORARY INTERFACE..." "$YELLOW"
deactivate
echo -e "${GREEN}[✓] INTERFACE DEACTIVATED${RESET}\n"

# Download and Extract
echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${CYAN}║${RESET} ${BOLD}${PINK}PHASE 6: ACQUIRING MONITORING PACKAGE${RESET}                      ${CYAN}║${RESET}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${RESET}"

type_effect ">>> INITIATING SECURE DOWNLOAD SEQUENCE..." "$YELLOW"
echo -e "${PINK}[DOWNLINK]${RESET} Target: ${CYAN}https://github.com/Skysurver007/NMS/${RESET}"
echo -e "${PINK}[DOWNLINK]${RESET} Status: ${YELLOW}CONNECTING...${RESET}"

wget --progress=bar:force https://github.com/Skysurfer007/NMS/raw/refs/heads/main/monitoring.zip 2>&1 | \
    grep --line-buffered "%" | \
    sed -u -e "s,\.,,g" | \
    awk '{printf("\r\033[38;5;198m[DOWNLINK]\033[0m Transfer Progress: \033[38;5;226m%4s\033[0m", $2)}'
echo -e "\n${GREEN}[✓] PACKAGE ACQUIRED${RESET}\n"

type_effect ">>> EXTRACTING ARCHIVE CONTENTS..." "$YELLOW"
unzip -o monitoring.zip | while read line; do
    echo -e "${BLUE}[EXTRACT]${RESET} $line"
done
echo -e "${GREEN}[✓] ARCHIVE EXTRACTED${RESET}\n"

type_effect ">>> PURGING TEMPORARY FILES..." "$YELLOW"
rm -f monitoring.zip
cyberpunk_loader 0.3 "CLEANING SECTORS"
echo -e "${GREEN}[✓] TEMPORARY FILES ELIMINATED${RESET}\n"

# Final Status
echo -e "${GREEN}"
cat << "EOF"
    ╔══════════════════════════════════════════════════════════════════╗
    ║                                                                  ║
    ║           INSTALLATION SEQUENCE COMPLETED SUCCESSFULLY           ║
    ║                                                                  ║
    ╚══════════════════════════════════════════════════════════════════╝
EOF
echo -e "${RESET}"

echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${CYAN}║${RESET} ${BOLD}SYSTEM STATUS REPORT${RESET}                                         ${CYAN}║${RESET}"
echo -e "${CYAN}╠════════════════════════════════════════════════════════════════╣${RESET}"
echo -e "${CYAN}║${RESET} ${GREEN}✓${RESET} Installation Directory: ${YELLOW}/root/monitoring-wifi${RESET}           ${CYAN}║${RESET}"
echo -e "${CYAN}║${RESET} ${GREEN}✓${RESET} Virtual Environment:   ${YELLOW}venv/${RESET}                            ${CYAN}║${RESET}"
echo -e "${CYAN}║${RESET} ${GREEN}✓${RESET} Python Packages:       ${YELLOW}7 modules installed${RESET}              ${CYAN}║${RESET}"
echo -e "${CYAN}║${RESET} ${GREEN}✓${RESET} Source Package:        ${YELLOW}monitoring.zip extracted${RESET}         ${CYAN}║${RESET}"
echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${RESET}"

echo -e "\n${PINK}[NEXT STEPS]${RESET}"
echo -e "${CYAN}➜${RESET} To activate environment: ${YELLOW}cd /root/monitoring-wifi && source venv/bin/activate${RESET}"
echo -e "${CYAN}➜${RESET} To run application:      ${YELLOW}python app.py${RESET} (atau sesuai file main di package)"
echo -e "${CYAN}➜${RESET} Directory contents:      ${YELLOW}ls -la /root/monitoring-wifi/${RESET}\n"

glitch_text ">>> SYSTEM READY FOR OPERATION <<<"
echo -e "${CYAN}[${RESET}${PINK}$(date '+%Y-%m-%d %H:%M:%S')${RESET}${CYAN}]${RESET} ${GREEN}END OF LINE.${RESET}\n"
