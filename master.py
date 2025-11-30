import Pyro5.api
import queue
import csv
import time
import threading
import socket
from urllib.parse import urlparse

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
        
        # Store metadata for the final report
        self.crawled_data = {}  # {url: title}
        
        self.start_time = time.time()
        self.end_time = self.start_time + (duration_minutes * 60)
        self.lock = threading.Lock()
        
        # Initialize CSV
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['url', 'title', 'timestamp'])
        
        print(f"[Master] Initialized. Crawling for {duration_minutes} minutes.")

    def get_task(self, worker_id):
        if time.time() > self.end_time:
            return "STOP"
        
        try:
            url = self.url_queue.get(block=False)
            print(f"[Master] Dispatching {url} to {worker_id}")
            return url
        except queue.Empty:
            return "WAIT"

    def submit_result(self, worker_id, source_url, title, found_links):
        with self.lock:
            # 1. Save Result
            self.crawled_data[source_url] = title
            
            with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([source_url, title, time.time()])
            
            # 2. Add New Links to Queue
            new_count = 0
            for link in found_links:
                if TARGET_DOMAIN in urlparse(link).netloc:
                    if link not in self.visited:
                        self.visited.add(link)
                        self.url_queue.put(link)
                        new_count += 1
            
            print(f"[Master] {worker_id} finished. Found {len(found_links)} links ({new_count} new).")

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

def monitor_shutdown(daemon, end_time):
    """Background thread to kill the server when time is up."""
    while time.time() < end_time:
        time.sleep(1)
    print("\n[Master] Time limit reached. Shutting down daemon...")
    daemon.shutdown()

def main():
    try:
        minutes = int(input("Enter duration in minutes: "))
    except ValueError:
        minutes = 5

    # 1. Setup Network
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    # 2. Setup Objects
    crawler = CrawlMaster(minutes)
    daemon = Pyro5.api.Daemon(host=local_ip, port=9090)
    uri = daemon.register(crawler, "crawler_master")
    
    print(f"\n[SYSTEM READY] Master URI: {uri}")
    print(f"--> COPY THIS URI TO YOUR WORKER NODES <--\n")
    
    # 3. Start Shutdown Monitor (The Fix)
    monitor_thread = threading.Thread(target=monitor_shutdown, args=(daemon, crawler.end_time))
    monitor_thread.daemon = True # Kills thread if main program exits
    monitor_thread.start()

    # 4. Run Server Loop
    try:
        daemon.requestLoop() # Blocks until daemon.shutdown() is called
    except KeyboardInterrupt:
        print("\n[Master] Interrupted by user.")
    
    # 5. Generate Report (Runs after loop breaks)
    print("[Master] Shutdown complete. Generating report...")
    crawler.generate_report()

if __name__ == "__main__":
    main()