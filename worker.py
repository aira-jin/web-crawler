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
        if is_downloadable(url):
            filename = os.path.basename(urlparse(url).path)
            return f"[FILE] {filename}", []

        response = requests.get(url, headers=HEADERS, timeout=10)
        if "text/html" not in response.headers.get("Content-Type", ""):
            return "[SKIPPED] Non-HTML content", []

        if response.status_code != 200:
            return None, []

        soup = BeautifulSoup(response.text, 'html.parser')
        title = extract_description(soup)
        
        links = []
        for tag in soup.find_all('a', href=True):
            absolute_link = urljoin(url, tag['href'])
            links.append(absolute_link)
        return title, links
    except Exception as e:
        # print(f"[{WORKER_ID}] Error on {url}: {e}")
        return None, []

# --- THREAD LOGIC ---
def run_thread_loop(master_uri, thread_index):  # <--- CHANGED: Takes URI, not object
    """Runs the crawling logic inside a thread."""
    
    # 1. Create a NEW Proxy for this specific thread
    # This fixes the "calling thread is not owner" error
    master = Pyro5.api.Proxy(master_uri) 
    
    current_id = f"{WORKER_ID}-{thread_index}"
    print(f"[{current_id}] Thread started.")

    while True:
        try:
            task = master.get_task(current_id)
        except Pyro5.errors.ConnectionClosedError:
            print(f"[{current_id}] Master went offline. Exiting.")
            break
        except Exception as e:
            # Re-connect if proxy breaks
            print(f"[{current_id}] Connection error: {e}. Reconnecting...")
            time.sleep(2)
            master = Pyro5.api.Proxy(master_uri)
            continue

        if task == "STOP": 
            # print(f"[{current_id}] Received STOP signal.")
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
    
    # Clean up proxy
    master._pyroRelease()

# --- MAIN LOOP ---

def main():
    print(f"[{WORKER_ID}] Contacting Name Server at {SERVER_IP}...")
    
    try:
        # 1. Locate Name Server & Master
        ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
        uri = ns.lookup("crawler_master")
        print(f"[{WORKER_ID}] Found Master at: {uri}")
        
        # 2. Get Config (Using a temporary proxy)
        with Pyro5.api.Proxy(uri) as master:
            try:
                config = master.get_config()
                num_threads = config.get("threads", 1)
            except:
                num_threads = 1
        
        print(f"[{WORKER_ID}] Spawning {num_threads} threads...")
        
        # 3. Launch Threads (Pass the URI, not the proxy object)
        threads = []
        for i in range(num_threads):
            # Pass 'uri' string, so thread creates its own connection
            t = threading.Thread(target=run_thread_loop, args=(uri, i))
            t.daemon = True
            t.start()
            threads.append(t)
            time.sleep(0.1)
            
        for t in threads:
            t.join()

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    main()