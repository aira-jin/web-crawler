import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import random
import os
import threading
import math

# --- CONFIGURATION ---
SERVER_IP = "10.2.13.18"
PORT = 9090
WORKER_ID = f"Node-{random.randint(1000,9999)}"

HEADERS = {
    'User-Agent': 'DLSU_Distributed_Crawler/1.0 (Student Project)'
}

# --- NODE STATISTICS ---
NODE_STATS = {
    "success_count": 0, # Successful fetches (HTML + Files)
    "error_count": 0,   # Timeouts, 404s, Network Errors
    "active_threads": 0
}
STATS_LOCK = threading.Lock()

# --- HELPER FUNCTIONS ---

def is_downloadable(url):
    return url.lower().endswith((
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        ".zip", ".rar", ".jpg", ".jpeg", ".png", ".gif", ".mp3", ".mp4"
    ))

def extract_description(soup):
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        return desc_tag["content"].strip()
    p = soup.find("p")
    if p:
        return p.get_text(strip=True)[:100] + "..."
    return "No title available"

def crawl_page(url):
    try:
        time.sleep(random.uniform(0.1, 0.5))
        
        # 1. Check for Files
        if is_downloadable(url):
            filename = os.path.basename(urlparse(url).path)
            with STATS_LOCK: NODE_STATS["success_count"] += 1
            return f"[FILE] {filename}", []

        # 2. Download HTML
        response = requests.get(url, headers=HEADERS, timeout=10)
        
        # 3. Check for Errors (404, 500, etc.)
        if response.status_code != 200:
            with STATS_LOCK: NODE_STATS["error_count"] += 1
            return None, []
            
        if "text/html" not in response.headers.get("Content-Type", ""):
            # Technically a success (we reached the server), but skipped content
            with STATS_LOCK: NODE_STATS["success_count"] += 1
            return "[SKIPPED] Non-HTML content", []

        # 4. Parse & Count Success
        soup = BeautifulSoup(response.text, 'html.parser')
        title = extract_description(soup)
        
        links = []
        for tag in soup.find_all('a', href=True):
            absolute_link = urljoin(url, tag['href'])
            links.append(absolute_link)
            
        with STATS_LOCK: NODE_STATS["success_count"] += 1
            
        return title, links
        
    except Exception as e:
        # Network errors, Timeouts, DNS failures
        with STATS_LOCK: NODE_STATS["error_count"] += 1
        return None, []

# --- THREAD LOGIC ---
def run_thread_loop(master_uri, thread_index):
    with STATS_LOCK: NODE_STATS["active_threads"] += 1
    
    master = Pyro5.api.Proxy(master_uri) 
    current_id = f"{WORKER_ID}-{thread_index}"

    while True:
        try:
            task = master.get_task(current_id)
        except Pyro5.errors.ConnectionClosedError:
            print(f"[{current_id}] Master went offline. Stopping.")
            break
        except Exception:
            time.sleep(2)
            try: master = Pyro5.api.Proxy(master_uri)
            except: break
            continue

        if task == "STOP": 
            break
        if task == "WAIT": 
            time.sleep(1)
            continue
            
        res = crawl_page(task)
        
        if res and res[0]:
            try:
                master.submit_result(current_id, task, res[0], res[1])
            except:
                break
    
    master._pyroRelease()
    with STATS_LOCK: NODE_STATS["active_threads"] -= 1

# --- MAIN LOOP ---

def main():
    print(f"[{WORKER_ID}] Contacting Name Server at {SERVER_IP}...")
    start_time = time.time()
    
    try:
        ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
        uri = ns.lookup("crawler_master")
        print(f"[{WORKER_ID}] Found Master at: {uri}")
        
        with Pyro5.api.Proxy(uri) as master:
            try:
                config = master.get_config()
                num_threads = config.get("threads", 1)
            except:
                num_threads = 1
        
        print(f"[{WORKER_ID}] Initializing {num_threads} threads...")
        
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=run_thread_loop, args=(uri, i))
            t.daemon = True
            t.start()
            threads.append(t)
            time.sleep(0.1)
            
        for t in threads:
            t.join()
            
        # --- PERFORMANCE REPORT ---
        duration_mins = (time.time() - start_time) / 60
        if duration_mins == 0: duration_mins = 0.01 
        
        successes = NODE_STATS["success_count"]
        errors = NODE_STATS["error_count"]
        total_ops = successes + errors
        
        # Calculate Metrics
        ppm = total_ops / duration_mins
        success_rate = (successes / total_ops * 100) if total_ops > 0 else 0
        
        print(f"\n[{WORKER_ID}] WORKER STOPPED. Final Metrics:")
        print("="*40)
        print(f" Threads Used:   {num_threads}")
        print(f" Throughput:     {math.floor(ppm*100)/100:.2f} pages/min")
        print(f" Success Rate:   {success_rate:.1f}%")
        print("="*40)

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    main()