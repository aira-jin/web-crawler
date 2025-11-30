import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import random
import os

# --- CONFIGURATION ---
SERVER_IP = "10.2.13.18"  # <--- Your VM IP (Connection Logic)
PORT = 9090
WORKER_ID = f"Node-{random.randint(1000,9999)}"

# (Feature Restored: Custom Headers to look legitimate)
HEADERS = {
    'User-Agent': 'DLSU_Distributed_Crawler/1.0 (Student Project)'
}

# --- HELPER FUNCTIONS (Restored from Old Version) ---

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
        # (Feature Restored: Random Politeness Delay)
        time.sleep(random.uniform(0.5, 1.5))
        
        # 1. Check if it's a static file (PDF, JPG, etc.)
        if is_downloadable(url):
            filename = os.path.basename(urlparse(url).path)
            # Returns a special tag so Master knows it's a file
            return f"[FILE] {filename}", []

        # 2. Download HTML
        response = requests.get(url, headers=HEADERS, timeout=10)
        
        # (Feature Restored: Content-Type Check)
        if "text/html" not in response.headers.get("Content-Type", ""):
            return "[SKIPPED] Non-HTML content", []

        if response.status_code != 200:
            return None, []

        # 3. Parse Content
        soup = BeautifulSoup(response.text, 'html.parser')
        title = extract_description(soup) # Uses the smarter extraction
        
        links = []
        for tag in soup.find_all('a', href=True):
            absolute_link = urljoin(url, tag['href'])
            links.append(absolute_link)
            
        return title, links
        
    except Exception as e:
        print(f"[{WORKER_ID}] Error on {url}: {e}")
        return None, []

# --- MAIN LOOP (Uses the New Connection Logic) ---

def main():
    print(f"[{WORKER_ID}] Contacting Name Server at {SERVER_IP}...")
    
    try:
        # 1. Locate Name Server & Master (The New Way)
        ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
        uri = ns.lookup("crawler_master")
        print(f"[{WORKER_ID}] Found Master at: {uri}")
        
        master = Pyro5.api.Proxy(uri)
        master._pyroBind()
        print(f"[{WORKER_ID}] Connected! Asking for tasks...")
        
        while True:
            # 2. Get Task (With Shutdown Safety)
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
            
            # 3. Do the work (Uses the Restored Logic)
            res = crawl_page(task)
            
            # 4. Submit Result (With Shutdown Safety)
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