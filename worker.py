import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import random
import os
import threading

# --- CONFIGURATION ---
SERVER_IP = "10.2.13.18"
PORT = 9090
WORKER_ID = f"Node-{random.randint(1000,9999)}"

HEADERS = {
    'User-Agent': 'DLSU_Distributed_Crawler/1.0 (Student Project)'
}

# --- NODE STATISTICS ---
NODE_STATS = {
    "html_scraped": 0,
    "files_found": 0,
    "links_found": 0,
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
            with STATS_LOCK: NODE_STATS["files_found"] += 1
            return f"[FILE] {filename}", []

        # 2. Download HTML
        response = requests.get(url, headers=HEADERS, timeout=10)
        if "text/html" not in response.headers.get("Content-Type", ""):
            return "[SKIPPED] Non-HTML content", []

        if response.status_code != 200:
            return None, []

        # 3. Parse & Count
        soup = BeautifulSoup(response.text, 'html.parser')
        title = extract_description(soup)
        
        links = []
        for tag in soup.find_all('a', href=True):
            absolute_link = urljoin(url, tag['href'])
            links.append(absolute_link)
            
        # Update Stats
        with STATS_LOCK: 
            NODE_STATS["html_scraped"] += 1
            NODE_STATS["links_found"] += len(links)
            
        return title, links
        
    except Exception as e:
        return None, []

# --- THREAD LOGIC ---
def run_thread_loop(master_uri, thread_index):
    """Runs the crawling logic inside a thread."""
    
    # Register as Active
    with STATS_LOCK: NODE_STATS["active_threads"] += 1
    
    # Create Thread-Local Proxy
    master = Pyro5.api.Proxy(master_uri) 
    current_id = f"{WORKER_ID}-{thread_index}"

    while True:
        try:
            task = master.get_task(current_id)
        except Pyro5.errors.ConnectionClosedError:
            print(f"[{current_id}] Master went offline. Stopping.")
            break
        except Exception as e:
            time.sleep(2)
            # Try to reconnect silently
            try: master = Pyro5.api.Proxy(master_uri)
            except: break
            continue

        if task == "STOP": 
            print(f"[{current_id}] Received STOP signal.")
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
    
    # De-Register
    master._pyroRelease()
    with STATS_LOCK: NODE_STATS["active_threads"] -= 1

# --- MAIN LOOP ---

def main():
    print(f"[{WORKER_ID}] Contacting Name Server at {SERVER_IP}...")
    
    try:
        ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
        uri = ns.lookup("crawler_master")
        print(f"[{WORKER_ID}] Found Master at: {uri}")
        
        # Fetch Config
        with Pyro5.api.Proxy(uri) as master:
            try:
                config = master.get_config()
                num_threads = config.get("threads", 1)
            except:
                num_threads = 1
        
        print(f"[{WORKER_ID}] Initializing {num_threads} threads...")
        
        # Start Worker Threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=run_thread_loop, args=(uri, i))
            t.daemon = True
            t.start()
            threads.append(t)
            time.sleep(0.1)
            
        # Wait for threads to finish (Block until STOP signal received)
        for t in threads:
            t.join()
            
        # --- FINAL REPORT ---
        print(f"\n[{WORKER_ID}] WORKER STOPPED. Generating Report...")
        print("="*40)
        print(f"Node Statistics for {WORKER_ID}")
        print("="*40)
        
        total_processed = NODE_STATS["html_scraped"] + NODE_STATS["files_found"]
        
        print(f"Number of Threads Used: {num_threads}")
        print(f"Total URLs Processed: {total_processed}")
        print(f"HTML Pages Scraped: {NODE_STATS['html_scraped']}")
        print(f"Files/Media Detected: {NODE_STATS['files_found']}")
        print(f"Unique URLs Discovered (Links Extracted): {NODE_STATS['links_found']}")
        print("="*40)

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    main()