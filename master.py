import Pyro5.api
import queue
import csv
import time
import threading
from urllib.parse import urlparse

# --- CONFIGURATION ---
SERVER_IP = "10.2.13.18"  # <--- Your VM IP
PORT = 9090

# --- CRAWLER SETTINGS ---
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
        self.crawled_data = {} 
        self.start_time = time.time()
        self.end_time = self.start_time + (duration_minutes * 60)
        self.lock = threading.Lock()
        
        # Init CSV
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['url', 'title', 'timestamp'])
        
        print(f"[Master] Initialized. Duration: {duration_minutes} mins.")

    def get_task(self, worker_id):
        if time.time() > self.end_time: return "STOP"
        try:
            url = self.url_queue.get(block=False)
            print(f"[Master] Sent {url} -> {worker_id}")
            return url
        except queue.Empty: return "WAIT"

    def submit_result(self, worker_id, source_url, title, found_links):
        with self.lock:
            self.crawled_data[source_url] = title
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([source_url, title, time.time()])
            
            count = 0
            for link in found_links:
                if TARGET_DOMAIN in urlparse(link).netloc and link not in self.visited:
                    self.visited.add(link)
                    self.url_queue.put(link)
                    count += 1
            print(f"[Master] {worker_id} returned {count} new links.")

    def generate_report(self):
        print(f"[Master] Saving report to {STATS_FILE}...")
        with open(STATS_FILE, 'w') as f:
            f.write(f"Total URLs: {len(self.crawled_data)}\n")

def main():
    try:
        minutes = int(input("Enter duration (mins): "))
    except ValueError: minutes = 5

    crawler = CrawlMaster(minutes)
    
    # 1. Start Daemon on the specific IP
    daemon = Pyro5.api.Daemon(host=SERVER_IP)
    
    # 2. Locate the Name Server (Phonebook)
    print(f"[Master] Connecting to Name Server at {SERVER_IP}...")
    ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
    
    # 3. Register the Object
    uri = daemon.register(crawler)
    ns.register("crawler_master", uri) # Give it a name "crawler_master"
    
    print(f"[Master] Ready at {uri}")
    print("[Master] Waiting for workers...")

    # Background thread to handle shutdown
    threading.Thread(target=lambda: (time.sleep(minutes*60), daemon.shutdown()), daemon=True).start()
    
    daemon.requestLoop()
    crawler.generate_report()

if __name__ == "__main__":
    main()