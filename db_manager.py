import sqlite3
import json
import os
import threading

class DBManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # WAL mode improves performance on slow storage (STB S905W)
        # by reducing disk write operations.
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        return conn

    def _init_db(self):
        with self.lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            
            # Table for all nodes (server, odps, clients, extra_routers)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL, -- 'server', 'odp', 'client', 'router'
                    name TEXT NOT NULL,
                    coordinates TEXT, -- Saved as JSON string "[lat, lng]"
                    parent_id TEXT,
                    data TEXT -- All other attributes saved as JSON string
                )
            ''')
            
            # Index for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_type ON nodes(type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_parent ON nodes(parent_id)')
            
            conn.commit()
            conn.close()

    def save_full_topology(self, topology_dict):
        """
        Saves a full topology dictionary (compatible with topology.json structure).
        """
        with self.lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            try:
                # Clear old data to ensure consistency with a "full save"
                cursor.execute('DELETE FROM nodes')
                
                # 1. Server
                server = topology_dict.get('server', {})
                if server:
                    cursor.execute(
                        'INSERT INTO nodes (id, type, name, coordinates, data) VALUES (?, ?, ?, ?, ?)',
                        (
                            server.get('id', 'server_utama'),
                            'server',
                            server.get('name', 'SERVER'),
                            json.dumps(server.get('coordinates', [0, 0])),
                            json.dumps({k: v for k, v in server.items() if k not in ['id', 'type', 'name', 'coordinates']})
                        )
                    )
                
                # 2. ODPs
                for odp in topology_dict.get('odps', []):
                    cursor.execute(
                        'INSERT INTO nodes (id, type, name, coordinates, data) VALUES (?, ?, ?, ?, ?)',
                        (
                            odp.get('id'),
                            'odp',
                            odp.get('name', ''),
                            json.dumps(odp.get('coordinates', [0, 0])),
                            json.dumps({k: v for k, v in odp.items() if k not in ['id', 'type', 'name', 'coordinates']})
                        )
                    )
                
                # 3. Clients
                for client in topology_dict.get('clients', []):
                    cursor.execute(
                        'INSERT INTO nodes (id, type, name, coordinates, parent_id, data) VALUES (?, ?, ?, ?, ?, ?)',
                        (
                            client.get('id'),
                            'client',
                            client.get('name', ''),
                            json.dumps(client.get('coordinates', [0, 0])),
                            client.get('parent_id'),
                            json.dumps({k: v for k, v in client.items() if k not in ['id', 'type', 'name', 'coordinates', 'parent_id']})
                        )
                    )
                
                # 4. Extra Routers
                for router in topology_dict.get('extra_routers', []):
                    cursor.execute(
                        'INSERT INTO nodes (id, type, name, coordinates, data) VALUES (?, ?, ?, ?, ?)',
                        (
                            router.get('id'),
                            'router',
                            router.get('name', ''),
                            json.dumps(router.get('coordinates', [0, 0])),
                            json.dumps({k: v for k, v in router.items() if k not in ['id', 'type', 'name', 'coordinates']})
                        )
                    )
                
                conn.commit()
                return True
            except Exception as e:
                print(f"[DB ERROR] save_full_topology: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()

    def load_full_topology(self):
        """
        Loads all nodes and reconstructs the topology.json structure.
        """
        with self.lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            
            res = {
                "server": {},
                "odps": [],
                "clients": [],
                "extra_routers": []
            }
            
            cursor.execute('SELECT id, type, name, coordinates, parent_id, data FROM nodes')
            rows = cursor.fetchall()
            
            for row in rows:
                node_id, node_type, name, coords_raw, parent_id, data_raw = row
                
                # Parse JSON fields
                try:
                    coords = json.loads(coords_raw) if coords_raw else [0, 0]
                except:
                    coords = [0, 0]
                
                try:
                    data = json.loads(data_raw) if data_raw else {}
                except:
                    data = {}
                
                # Build object
                obj = {"id": node_id, "name": name, "coordinates": coords}
                obj.update(data)
                
                if node_type == 'server':
                    res['server'] = obj
                elif node_type == 'odp':
                    res['odps'].append(obj)
                elif node_type == 'client':
                    obj['parent_id'] = parent_id
                    res['clients'].append(obj)
                elif node_type == 'router':
                    res['extra_routers'].append(obj)
            
            conn.close()
            return res

    def apply_bulk_updates(self, updates):
        """
        Updates multiple nodes efficiently in a single transaction.
        'updates' should be a list of dicts, each with an 'id' and fields to update.
        """
        if not updates: return True
        
        with self.lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            try:
                for upd in updates:
                    node_id = upd.get('id')
                    if not node_id: continue
                    
                    # 1. Get current data
                    cursor.execute('SELECT data FROM nodes WHERE id = ?', (node_id,))
                    row = cursor.fetchone()
                    if not row: continue
                    
                    try:
                        data = json.loads(row[0]) if row[0] else {}
                    except:
                        data = {}
                    
                    # 2. Update specific fields
                    changed = False
                    for k, v in upd.items():
                        if k == 'id': continue
                        if k == 'name':
                            cursor.execute('UPDATE nodes SET name = ? WHERE id = ?', (v, node_id))
                        elif k == 'coordinates':
                            cursor.execute('UPDATE nodes SET coordinates = ? WHERE id = ?', (json.dumps(v), node_id))
                        elif k == 'parent_id':
                            cursor.execute('UPDATE nodes SET parent_id = ? WHERE id = ?', (v, node_id))
                        else:
                            if data.get(k) != v:
                                data[k] = v
                                changed = True
                    
                    if changed:
                        cursor.execute('UPDATE nodes SET data = ? WHERE id = ?', (json.dumps(data), node_id))
                
                conn.commit()
                return True
            except Exception as e:
                print(f"[DB ERROR] apply_bulk_updates: {e}")
                conn.rollback()
                return False
            finally:
                conn.close()
