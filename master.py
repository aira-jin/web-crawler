import Pyro5.api
import queue
import csv
import time
import threading
from urllib.parse import urlparse

# --- CONFIGURATION ---
SERVER_IP = "10.2.13.18"
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
        # 1. Check if time is up
        if time.time() > self.end_time:
            return "STOP"
        
        try:
            url = self.url_queue.get(block=False)
            print(f"[Master] Sent {url} -> {worker_id}")
            return url
        except queue.Empty:
            return "WAIT"

    def submit_result(self, worker_id, source_url, title, found_links):
        with self.lock:
            # Save data
            self.crawled_data[source_url] = title
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([source_url, title, time.time()])
            
            # Add new links
            count = 0
            for link in found_links:
                if TARGET_DOMAIN in urlparse(link).netloc and link not in self.visited:
                    self.visited.add(link)
                    self.url_queue.put(link)
                    count += 1
            print(f"[Master] {worker_id} returned {count} new links.")

    def generate_report(self):
        """Generates the advanced report required by the rubric."""
        elapsed = (time.time() - self.start_time) / 60
        
        # Calculate Stats
        file_count = 0
        html_count = 0
        for title in self.crawled_data.values():
            if title.startswith("[FILE]"):
                file_count += 1
            else:
                html_count += 1

        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            f.write("--- DISTRIBUTED CRAWL SUMMARY ---\n")
            f.write(f"Total Duration: {elapsed:.2f} minutes\n")
            f.write(f"Total URLs Processed: {len(self.crawled_data)}\n")
            f.write(f"HTML Pages Scraped: {html_count}\n")
            f.write(f"Files/Media Detected: {file_count}\n")
            f.write(f"Unique URLs Discovered (Queue size + Visited): {len(self.visited)}\n\n")
            f.write("--- LIST OF PROCESSED URLS ---\n")
            for url, title in self.crawled_data.items():
                f.write(f"{url}  [{title}]\n")
        
        print(f"[Master] Detailed report generated at {STATS_FILE}")

# --- NEW SHUTDOWN LOGIC ---
def monitor_exit(daemon, end_time):
    """Waits for time to expire, gives a grace period, then shuts down."""
    
    # 1. Wait for the main duration
    while time.time() < end_time:
        time.sleep(1)
        
    print("\n[Master] TIME LIMIT REACHED. Stopping new tasks...")
    print("[Master] Entering 15s GRACE PERIOD to allow workers to finish...")
    
    # 2. The Grace Period (Keep server alive so workers can submit last results)
    time.sleep(15)
    
    print("[Master] Grace period over. Shutting down now.")
    daemon.shutdown()

def main():
    try:
        minutes = int(input("Enter duration (mins): "))
    except ValueError: minutes = 5

    crawler = CrawlMaster(minutes)
    
    daemon = Pyro5.api.Daemon(host=SERVER_IP)
    
    print(f"[Master] Connecting to Name Server at {SERVER_IP}...")
    try:
        ns = Pyro5.api.locate_ns(host=SERVER_IP, port=PORT)
        uri = daemon.register(crawler)
        ns.register("crawler_master", uri)
    except Exception as e:
        print(f"[ERROR] Name Server not found: {e}")
        return

    print(f"[Master] Ready at {uri}")
    print("[Master] Waiting for workers...")

    # Start the "Polite" Shutdown Monitor
    threading.Thread(target=monitor_exit, args=(daemon, crawler.end_time), daemon=True).start()
    
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print("\n[Master] Force stopped by user.")
        
    crawler.generate_report()

if __name__ == "__main__":
    main()