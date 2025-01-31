import socket
import requests
import time
from typing import Optional

class CoordinatorDiscovery:
    MCAST_GRP = '224.1.1.1'
    MCAST_PORT = 25565
    
    def __init__(self):
        self.sock = self._setup_socket()
        
    def _setup_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self.MCAST_PORT))
        # Join the multicast group
        mreq = socket.inet_aton(self.MCAST_GRP) + socket.inet_aton('0.0.0.0')
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        return sock
    
    def find_coordinator(self) -> Optional[str]:
        print(f"\033[92mSearching for coordinator\033[0m")
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                if data == b"Coordinator Discovery":
                    print(f"Found coordinator at {addr[0]}")
                    return addr[0]
            except Exception as e:
                print(f"\033[91mError during discovery: {e}\033[0m")
                time.sleep(1)
    
    def register_with_coordinator(self, coordinator_ip: str):
        local_ip = socket.gethostbyname(socket.gethostname())
        try:
            response = requests.post(
                f"http://{coordinator_ip}:8081/register",
                params={"worker_ip": local_ip},
                timeout=5
            )
            response.raise_for_status()
            print(f"\033[92mSuccessfully registered with coordinator at {coordinator_ip}\033[0m")
        except Exception as e:
            print(f"\033[91mFailed to register with coordinator: {e}\033[0m")
            raise
    
    def close(self):
        self.sock.close()

def search_and_register():
    discovery = CoordinatorDiscovery()
    try:
        coordinator_ip = discovery.find_coordinator()
        if coordinator_ip:
            discovery.register_with_coordinator(coordinator_ip)
    finally:
        discovery.close() 