import platform
import uuid
import hashlib
import hmac
import base64
import json
import os

# --- OBFUSCATED SECURITY FRAGMENTS (Anti-Grep) ---
_k1 = "Peycell-NMS"
_k2 = "Super-Secret"
_k3 = "Key-2025"
_salt = "NMS-PRO-SECURED"

def _get_integrity_hash():
    """Generates a hash of the main app file to detect tampering (Anti-Crack)."""
    try:
        app_path = os.path.join(os.path.dirname(__file__), 'app.py')
        if os.path.exists(app_path):
            with open(app_path, 'rb') as f:
                # Use a chunked read to be memory efficient but cover the whole file
                h = hashlib.sha256()
                while True:
                    chunk = f.read(8192)
                    if not chunk: break
                    h.update(chunk)
                return h.hexdigest()[:16].upper()
    except: pass
    return "ORIGINAL-V341"

def _get_secret():
    """Reconstructs the secret key at runtime to avoid static analysis."""
    return f"{_k1}-{_k2}-{_k3}".encode()

def _get_system_uuid():
    try:
        sys_type = platform.system()
        if sys_type == 'Windows':
            import subprocess
            try:
                out = subprocess.check_output('wmic csproduct get uuid', shell=True, stderr=subprocess.DEVNULL).decode().split('\n')
                for line in out:
                    u = line.strip()
                    if u and 'UUID' not in u and 'Default' not in u and '000000' not in u:
                        return u
            except: pass
            
            # Fallback for Windows: Registry MachineGuid (Very stable)
            try:
                import winreg
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography")
                guid, _ = winreg.QueryValueEx(key, "MachineGuid")
                return guid
            except: pass
        elif sys_type == 'Linux':
            # 1. Linux Product UUID (Best for Proxmox/VMware/KVM)
            paths = ['/sys/class/dmi/id/product_uuid', '/sys/class/dmi/id/board_serial']
            for p in paths:
                if os.path.exists(p):
                    try:
                        with open(p, 'r') as f:
                            val = f.read().strip()
                            if val and '000000' not in val: return val
                    except: pass
            
            # 2. Armbian/Raspberry Pi Serial (ARM devices)
            arm_paths = ['/proc/device-tree/serial-number', '/sys/class/sunxi_info/sys_info']
            for p in arm_paths:
                if os.path.exists(p):
                    try:
                        with open(p, 'r') as f:
                            return f.read().strip()
                    except: pass
            
            # 3. /proc/cpuinfo Serial fallback
            if os.path.exists('/proc/cpuinfo'):
                try:
                    with open('/proc/cpuinfo', 'r') as f:
                        for line in f:
                            if 'Serial' in line:
                                return line.split(':')[-1].strip()
                except: pass
    except: pass
    return None

def get_machine_id_legacy():
    """Old machine ID logic used in previous versions (Hostname + MAC)."""
    node = platform.node()
    mac = uuid.getnode()
    mid = f"{node}-{mac}"
    hash_id = hashlib.md5(mid.encode()).hexdigest().upper()
    return f"{hash_id[:4]}-{hash_id[4:8]}-{hash_id[8:12]}-{hash_id[12:16]}"

def get_machine_id():
    # ... (rest of the new robust logic)
    # 1. Try System UUID (Best for VM/Proxmox stability)
    mid = _get_system_uuid()
    
    # 2. Try OS Specific Machine ID
    if not mid:
        # ... (rest of search logic same as before)
        if os.path.exists('/etc/machine-id'):
            try:
                with open('/etc/machine-id', 'r') as f: mid = f.read().strip()
            except: pass
            
    # 3. Last Fallback: Hostname + MAC
    if not mid:
        node = platform.node()
        mac = uuid.getnode()
        mid = f"{node}-{mac}"
        
    # --- TAHAN BANTING UPGRADE (Stable Machine ID) ---
    # We remove integrity hash so the Machine ID stays the same even if app.py is edited.
    combined = f"{mid}-{_salt}"
    
    hash_id = hashlib.md5(combined.encode()).hexdigest().upper()
    formatted_id = f"{hash_id[:4]}-{hash_id[4:8]}-{hash_id[8:12]}-{hash_id[12:16]}"
    return formatted_id

def sign_data(data_string):
    signature = hmac.new(_get_secret(), data_string.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(signature).decode().rstrip('=')

def generate_license(machine_id, client_name, license_type="ENTERPRISE"):
    payload = {
        "mid": machine_id,
        "cli": client_name,
        "typ": license_type
    }
    payload_str = json.dumps(payload, separators=(',', ':'))
    payload_b64 = base64.urlsafe_b64encode(payload_str.encode()).decode().rstrip('=')
    signature = sign_data(payload_b64)
    full_license = f"{payload_b64}.{signature}"
    return full_license

def verify_license(license_key):
    try:
        if not license_key or "." not in license_key:
            return False, "Format lisensi salah."
        payload_b64, signature = license_key.split('.', 1)
        expected_signature = sign_data(payload_b64)
        if not hmac.compare_digest(signature, expected_signature):
            return False, "Signature tidak valid (Lisensi Palsu)."
        padding = len(payload_b64) % 4
        if padding: 
            payload_b64 += '=' * (4 - padding)
        payload_str = base64.urlsafe_b64decode(payload_b64).decode()
        data = json.loads(payload_str)
        # --- UNIVERSAL MULTI-LAYER VERIFICATION (V3.4.1) ---
        # 1. New Secure Mode (Integrity + Hardware + Salt)
        if data.get('mid') == get_machine_id():
            return True, data
            
        # 2. Robust Hardware Mode (UUID / CPU Serial / etc - No Integrity)
        # This covers existing 3.4.x users on Proxmox/ARM who don't edit code.
        hw_id = _get_system_uuid()
        if hw_id:
            h = hashlib.md5(hw_id.encode()).hexdigest().upper()
            hw_formatted = f"{h[:4]}-{h[4:8]}-{h[8:12]}-{h[12:16]}"
            if data.get('mid') == hw_formatted:
                data['is_robust_legacy'] = True
                return True, data

        # 3. Exhaustive Legacy Mode (All physical MAC addresses)
        # Handles shifting interfaces in VMs and older NMS versions.
        import psutil
        try:
            node = platform.node()
            all_mids = [f"{node}-{uuid.getnode()}"]
            for _, snics in psutil.net_if_addrs().items():
                for snic in snics:
                    addr = snic.address.replace(':','').replace('-','')
                    if len(addr) == 12:
                        try:
                            all_mids.append(f"{node}-{int(addr, 16)}")
                        except: pass
            
            for m_test in set(all_mids):
                h = hashlib.md5(m_test.encode()).hexdigest().upper()
                legacy_fmt = f"{h[:4]}-{h[4:8]}-{h[8:12]}-{h[12:16]}"
                if data.get('mid') == legacy_fmt:
                    data['is_legacy'] = True
                    return True, data
        except: pass

        # 4. Emergency Fallback: If mid is literally MAC address or Hostname
        raw_mid = platform.node().upper()
        if data.get('mid') == raw_mid:
            return True, data

        # 5. Legacy v3.4.1 Mode (Integrity Hash + Hardware + Salt)
        # This ensures old clients who already activated don't get locked out.
        try:
            old_integrity = _get_integrity_hash()
            # We use the same 'mid' (Hardware ID) found in step 1 of get_machine_id()
            hw_mid = _get_system_uuid() or f"{platform.node()}-{uuid.getnode()}"
            combined_old = f"{hw_mid}-{old_integrity}-{_salt}"
            h_old = hashlib.md5(combined_old.encode()).hexdigest().upper()
            v341_fmt = f"{h_old[:4]}-{h_old[4:8]}-{h_old[8:12]}-{h_old[12:16]}"
            if data.get('mid') == v341_fmt:
                data['is_v341_legacy'] = True
                return True, data
        except: pass

        return False, f"Lisensi ini untuk mesin berbeda ({data.get('mid')})."

        return False, f"Lisensi ini untuk mesin berbeda ({data.get('mid')})."
    except Exception as e:
        return False, f"Error verifikasi: {str(e)}"
