import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import random
import os

# --- CONFIGURATION ---
PORT = 9090
WORKER_ID = f"Node-{random.randint(1000,9999)}"

HEADERS = {
    'User-Agent': 'DLSU_Distributed_Crawler/1.0 (Student Project)'
}

# --- HELPER FUNCTIONS ---

def is_downloadable(url):
    """Checks if the URL is a file based on extension."""
    return url.lower().endswith((
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        ".zip", ".rar", ".jpg", ".jpeg", ".png", ".gif", ".mp3", ".mp4"
    ))

def extract_description(soup):
    """Prioritizes Title > Meta Description > First Paragraph."""
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
    """Downloads page, handles files, and extracts links."""
    try:
        time.sleep(random.uniform(0.1, 0.5))
        
        # 1. Check if it's a static file (PDF, JPG, etc.)
        if is_downloadable(url):
            filename = os.path.basename(urlparse(url).path)
            return f"[FILE] {filename}", []

        # 2. Download HTML
        response = requests.get(url, headers=HEADERS, timeout=10)
        
        if "text/html" not in response.headers.get("Content-Type", ""):
            return "[SKIPPED] Non-HTML content", []

        if response.status_code != 200:
            return None, []

        # 3. Parse Content
        soup = BeautifulSoup(response.text, 'html.parser')
        title = extract_description(soup)
        
        links = []
        for tag in soup.find_all('a', href=True):
            absolute_link = urljoin(url, tag['href'])
            links.append(absolute_link)
            
        return title, links
        
    except Exception as e:
        print(f"[{WORKER_ID}] Error on {url}: {e}")
        return None, []

# --- MAIN LOOP ---

def main():
    print(f"[{WORKER_ID}] Searching for Master (Broadcasting)...")
    
    try:
        # 1. Locate Name Server via Broadcast
        ns = Pyro5.api.locate_ns(port=PORT) 
        
        # 2. Look up the Master object
        uri = ns.lookup("crawler_master")
        print(f"[{WORKER_ID}] Found Master at: {uri}")
        
        master = Pyro5.api.Proxy(uri)
        master._pyroBind()
        print(f"[{WORKER_ID}] Connected! Asking for tasks...")
        
        while True:
            # 3. Get Task
            try:
                task = master.get_task(WORKER_ID)
            except Pyro5.errors.ConnectionClosedError:
                print(f"[{WORKER_ID}] Master went offline (Shutdown). Exiting.")
                break
            except Exception as e:
                # Catch generic errors to allow clean exit on disconnect
                print(f"[{WORKER_ID}] Connection lost: {e}")
                break

            if task == "STOP": 
                print(f"[{WORKER_ID}] Received STOP signal.")
                break
            if task == "WAIT": 
                time.sleep(1)
                continue
                
            print(f"[{WORKER_ID}] Crawling: {task}")
            
            # 4. Do the work
            res = crawl_page(task)
            
            # 5. Submit Result
            if res and res[0]:
                try:
                    master.submit_result(WORKER_ID, task, res[0], res[1])
                except Pyro5.errors.ConnectionClosedError:
                    print(f"[{WORKER_ID}] Tried to submit, but Master is gone. Exiting.")
                    break

    except Exception as e:
        print(f"[ERROR] Could not find Master: {e}")
        print("Ensure Master is running and network is set to Bridged Adapter (if using VMs).")

if __name__ == "__main__":
    main()