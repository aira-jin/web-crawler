import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import random

# --- CONFIGURATION ---
SERVER_IP = "10.2.13.18"
PORT = 9090
WORKER_ID = f"Node-{random.randint(1000,9999)}"

def crawl_page(url):
    try:
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
        ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
        uri = ns.lookup("crawler_master")
        print(f"[{WORKER_ID}] Found Master at: {uri}")
        
        master = Pyro5.api.Proxy(uri)
        master._pyroBind()
        print(f"[{WORKER_ID}] Connected! Asking for tasks...")
        
        while True:
            # 1. Get Task
            try:
                task = master.get_task(WORKER_ID)
            except Pyro5.errors.ConnectionClosedError:
                print(f"[{WORKER_ID}] Master went offline (Shutdown). Exiting.")
                break

            if task == "STOP": 
                print(f"[{WORKER_ID}] Received STOP signal.")
                break
            if task == "WAIT": 
                time.sleep(1)
                continue
                
            print(f"[{WORKER_ID}] Crawling: {task}")
            
            # 2. Do the work
            res = crawl_page(task)
            
            # 3. Submit Result (Safely)
            if res and res[0]:
                try:
                    master.submit_result(WORKER_ID, task, res[0], res[1])
                except Pyro5.errors.ConnectionClosedError:
                    print(f"[{WORKER_ID}] Tried to submit, but Master is gone. Exiting.")
                    break

    except Exception as e:
        print(f"[ERROR] {e}")
        print("Make sure Name Server AND Master are running on 10.2.13.18")

if __name__ == "__main__":
    main()