# master.py
import Pyro5.api
import socket
import queue
import csv
import time
import threading
from urllib.parse import urljoin, urlparse

# --- CONFIGURATION ---
TARGET_DOMAIN = "dlsu.edu.ph"
START_URL = "https://www.dlsu.edu.ph"
OUTPUT_FILE = "crawl_results.csv"
STATS_FILE = "crawl_summary.txt"

@Pyro5.api.expose
class CrawlMaster:
    def __init__(self, duration_minutes):
        self.url_queue = queue.Queue()
        self.url_queue.put(START_URL)
        self.visited = set([START_URL])
        self.start_time = time.time()
        self.end_time = self.start_time + (duration_minutes * 60)
        self.lock = threading.Lock()
        
        # Initialize CSV with headers
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['url', 'title', 'timestamp'])
        
        print(f"[Master] Initialized. Crawling for {duration_minutes} minutes.")

    def get_task(self, worker_id):
        """Called by Worker to get a URL to crawl."""
        # 1. Check Time Limit
        if time.time() > self.end_time:
            return "STOP"
        
        # 2. Check if Queue is Empty
        try:
            url = self.url_queue.get(block=False)
            print(f"[Master] Dispatching {url} to Worker {worker_id}")
            return url
        except queue.Empty:
            # If empty, tell worker to wait briefly (the queue might refill)
            return "WAIT"

    def submit_result(self, worker_id, source_url, title, found_links):
        """Called by Worker to return results."""
        with self.lock:
            # 1. Log the successful crawl to CSV
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([source_url, title, time.time()])
            
            # 2. Process new links (URL Frontier Logic)
            new_count = 0
            for link in found_links:
                # Ensure we only crawl the target domain (Scope Restriction)
                if TARGET_DOMAIN in urlparse(link).netloc:
                    if link not in self.visited:
                        self.visited.add(link)
                        self.url_queue.put(link)
                        new_count += 1
            
            print(f"[Master] Worker {worker_id} finished. Found {len(found_links)} links ({new_count} new).")

    def generate_report(self):
        """Generates the summary text file required by the project."""
        elapsed = (time.time() - self.start_time) / 60
        with open(STATS_FILE, 'w') as f:
            f.write(f"--- Crawl Summary ---\n")
            f.write(f"Total Unique Pages Accessed: {len(self.visited)}\n")
            f.write(f"Total Duration: {elapsed:.2f} minutes\n")
            f.write(f"List of Visited URLs:\n")
            for url in self.visited:
                f.write(f"{url}\n")
        print("[Master] Report generated.")

def main():
    # Ask for duration input
    try:
        minutes = int(input("Enter duration in minutes: "))
    except ValueError:
        minutes = 5 # Default
        
    # get IP
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)

    # Start Pyro Daemon with IP
    daemon = Pyro5.api.Daemon(host=local_ip) 
    uri = daemon.register(CrawlMaster(minutes), "crawler_master")
    
    print(f"\n[SYSTEM READY] Master URI: {uri}")
    print(f"--> COPY THIS URI TO YOUR WORKER NODES <--\n")
    
    # Run the server loop
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print("\n[Master] Shutting down...")

if __name__ == "__main__":
    main()