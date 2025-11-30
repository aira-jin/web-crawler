import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import random

# --- CONFIGURATION ---
SERVER_IP = "10.2.13.18"  # <--- The Worker looks for the NS here
PORT = 9090
WORKER_ID = f"Node-{random.randint(1000,9999)}"

def crawl_page(url):
    try:
        # Simple Politeness & Request
        time.sleep(0.5) 
        resp = requests.get(url, headers={'User-Agent': 'Bot/1.0'}, timeout=5)
        if resp.status_code != 200: return None, []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        title = soup.title.string.strip() if soup.title else "No Title"
        links = [urljoin(url, a['href']) for a in soup.find_all('a', href=True)]
        return title, links
    except Exception as e:
        return None, []

def main():
    print(f"[{WORKER_ID}] Contacting Name Server at {SERVER_IP}...")
    
    try:
        # 1. Ask Name Server for the Master's address
        ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
        uri = ns.lookup("crawler_master")
        print(f"[{WORKER_ID}] Found Master at: {uri}")
        
        # 2. Connect to Master
        master = Pyro5.api.Proxy(uri)
        master._pyroBind() # Test connection
        print(f"[{WORKER_ID}] Connected! Asking for tasks...")
        
        while True:
            task = master.get_task(WORKER_ID)
            
            if task == "STOP": break
            if task == "WAIT": 
                time.sleep(1)
                continue
                
            print(f"[{WORKER_ID}] Crawling: {task}")
            res = crawl_page(task)
            
            if res and res[0]:
                master.submit_result(WORKER_ID, task, res[0], res[1])

    except Exception as e:
        print(f"[ERROR] {e}")
        print("Make sure Name Server AND Master are running on 10.2.13.18")

if __name__ == "__main__":
    main()