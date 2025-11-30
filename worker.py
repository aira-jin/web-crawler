# worker.py
import Pyro5.api
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import random
import sys

# --- CONFIGURATION ---
# PASTE THE URI FROM THE MASTER HERE
MASTER_URI = input("Enter Master URI (e.g., PYRO:crawler_master@192.168.1.X:9090): ")
WORKER_ID = f"Node-{random.randint(1000,9999)}"

# Headers to be polite and identify ourselves
HEADERS = {
    'User-Agent': 'DLSU_Student_Project_Bot/1.0 (Educational Purpose)'
}

def crawl_page(url):
    """Downloads and parses a single page."""
    try:
        # Politeness Delay [Industry Standard]
        time.sleep(random.uniform(1.0, 2.0)) 
        
        response = requests.get(url, headers=HEADERS, timeout=5)
        if response.status_code != 200:
            return None, []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string.strip() if soup.title else "No Title"
        
        links = []
        for tag in soup.find_all('a', href=True):
            absolute_link = urljoin(url, tag['href'])
            # Basic filtering to avoid non-html resources
            if not any(ext in absolute_link for ext in ['.pdf', '.jpg', '.png', '.css']):
                links.append(absolute_link)
                
        return title, links
        
    except Exception as e:
        print(f"[Error] Failed to crawl {url}: {e}")
        return None, []

def main():
    print(f"[{WORKER_ID}] Connecting to Master...")
    master = Pyro5.api.Proxy(MASTER_URI)
    
    try:
        while True:
            # 1. Ask Master for work
            task = master.get_task(WORKER_ID)
            
            if task == "STOP":
                print(f"[{WORKER_ID}] Received STOP signal. Exiting.")
                break
            
            if task == "WAIT":
                print(f"[{WORKER_ID}] Queue empty. Waiting...")
                time.sleep(2)
                continue
                
            # 2. Perform the work
            print(f"[{WORKER_ID}] Crawling: {task}")
            title, links = crawl_page(task)
            
            # 3. Report back
            if title:
                master.submit_result(WORKER_ID, task, title, links)
                
    except Pyro5.errors.ConnectionClosedError:
        print(f"[{WORKER_ID}] Lost connection to Master. Shutting down.")
    except Exception as e:
        print(f"[{WORKER_ID}] Unexpected error: {e}")

if __name__ == "__main__":
    main()