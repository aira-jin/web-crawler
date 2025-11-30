import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import random
import os

# --- CONFIGURATION ---
MASTER_URI = input("Enter Master URI (e.g., PYRO:crawler_master@192.168.1.X:9090): ").strip()
WORKER_ID = f"Node-{random.randint(1000,9999)}"

HEADERS = {
    'User-Agent': 'DLSU_Distributed_Crawler/1.0 (Student Project)'
}

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
        # Politeness Delay
        time.sleep(random.uniform(0.5, 1.5))
        
        # 1. Check if it's a static file
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

def main():
    print(f"[{WORKER_ID}] Connecting to Master...")
    try:
        master = Pyro5.api.Proxy(MASTER_URI)
        
        while True:
            task = master.get_task(WORKER_ID)
            
            if task == "STOP":
                print(f"[{WORKER_ID}] STOP signal received. Exiting.")
                break
            
            if task == "WAIT":
                time.sleep(1)
                continue
            
            print(f"[{WORKER_ID}] Processing: {task}")
            result = crawl_page(task)
            
            if result and result[0]:
                master.submit_result(WORKER_ID, task, result[0], result[1])
            else:
                # Even if failed, report back so we don't hang? 
                # For this simple logic, we just skip.
                pass
                
    except Exception as e:
        print(f"[{WORKER_ID}] Critical Error: {e}")

if __name__ == "__main__":
    main()