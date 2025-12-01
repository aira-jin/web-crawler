import Pyro5.api
import Pyro5.nameserver
import queue
import csv
import time
import threading
import socket
from urllib.parse import urlparse

# --- CONFIGURATION ---
PORT = 9090

# --- CRAWLER SETTINGS ---
OUTPUT_FILE = "crawl_results.csv"
STATS_FILE = "crawl_summary.txt"

# --- HELPER: GET LOCAL IP ---
def get_local_ip():
    """Auto-detects the machine's LAN IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

@Pyro5.api.expose
class CrawlMaster:
    def __init__(self, start_url, duration_minutes, num_nodes):
        # Dynamic Configuration
        self.start_url = start_url
        self.target_domain = urlparse(start_url).netloc.replace("www.", "") 
        self.num_nodes = num_nodes
        
        self.url_queue = queue.Queue()
        self.url_queue.put(self.start_url)
        self.visited = set([self.start_url])
        self.crawled_data = {} 
        self.start_time = time.time()
        self.end_time = self.start_time + (duration_minutes * 60)
        self.lock = threading.Lock()
        
        # Init CSV
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['url', 'title', 'timestamp'])
        
        print(f"[Master] Initialized.")
        print(f"         Target: {self.start_url} (Domain: {self.target_domain})")
        print(f"         Duration: {duration_minutes} mins")
        print(f"         Expected Nodes: {num_nodes}")

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
                if self.target_domain in urlparse(link).netloc and link not in self.visited:
                    self.visited.add(link)
                    self.url_queue.put(link)
                    count += 1
            print(f"[Master] {worker_id} returned {count} new links.")

    def generate_report(self):
        """Generates the advanced report required by the rubric."""
        elapsed = (time.time() - self.start_time) / 60
        
        file_count = 0
        html_count = 0
        for title in self.crawled_data.values():
            if title.startswith("[FILE]"):
                file_count += 1
            else:
                html_count += 1

        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            f.write("--- DISTRIBUTED CRAWL SUMMARY ---\n")
            f.write(f"Start URL: {self.start_url}\n")
            f.write(f"Target Domain: {self.target_domain}\n")
            f.write(f"Number of Nodes: {self.num_nodes}\n")
            f.write(f"Total Duration: {elapsed:.2f} minutes\n")
            f.write(f"Total URLs Processed: {len(self.crawled_data)}\n")
            f.write(f"HTML Pages Scraped: {html_count}\n")
            f.write(f"Files/Media Detected: {file_count}\n")
            f.write(f"Unique URLs Discovered: {len(self.visited)}\n\n")
            f.write("--- LIST OF PROCESSED URLS ---\n")
            for url, title in self.crawled_data.items():
                f.write(f"{url}  [{title}]\n")
        
        print(f"[Master] Detailed report generated at {STATS_FILE}")

# --- THREADED NAME SERVER ---
def start_nameserver(host, port):
    """Starts the Name Server loop (blocking) in a thread."""
    try:
        Pyro5.nameserver.start_ns_loop(host=host, port=port)
    except Exception as e:
        print(f"[NameServer Error] {e}")

# --- SHUTDOWN LOGIC ---
def monitor_exit(daemon, end_time):
    """Waits for time to expire, gives a grace period, then shuts down Master."""
    while time.time() < end_time:
        time.sleep(1)
        
    print("\n[Master] TIME LIMIT REACHED. Stopping new tasks...")
    print("[Master] Entering 15s GRACE PERIOD to allow workers to finish...")
    time.sleep(15)
    
    print("[Master] Grace period over. Shutting down now.")
    daemon.shutdown() 

def main():
    print("--- DISTRIBUTED CRAWLER CONFIG ---")
    
    # 1. Inputs
    start_url = input("1. Enter Start URL (default: https://www.dlsu.edu.ph): ").strip() or "https://www.dlsu.edu.ph"
    try:
        minutes = int(input("2. Enter Duration (mins): "))
    except ValueError:
        minutes = 5
        print("Defaulting to 5 minutes.")
    try:
        nodes = int(input("3. Enter Number of Nodes: "))
    except ValueError:
        nodes = 2
        print("Defaulting to 2 nodes.")

    # 2. Auto-Detect IP
    my_ip = get_local_ip()
    print(f"\n[Master] Detected LAN IP: {my_ip}")
    
    # 3. Start Name Server (Background Thread)
    print("[Master] Starting internal Name Server...")
    t_ns = threading.Thread(target=start_nameserver, args=(my_ip, PORT), daemon=True)
    t_ns.start()
    time.sleep(2) # Give it a moment to initialize
    
    # 4. Initialize Master Daemon
    crawler = CrawlMaster(start_url, minutes, nodes)
    daemon = Pyro5.api.Daemon(host=my_ip)
    uri = daemon.register(crawler)
    
    # 5. Register with the Name Server we just started
    try:
        ns = Pyro5.api.locate_ns(host=my_ip, port=PORT)
        ns.register("crawler_master", uri)
        print(f"[Master] Registered 'crawler_master' with Name Server.")
    except Exception as e:
        print(f"[ERROR] Could not register with Name Server: {e}")
        print("Ensure port 9090 is free.")
        return

    print(f"[Master] Ready at {uri}")
    print("[Master] Broadcasting existence... Waiting for workers...")

    # Start Shutdown Monitor
    threading.Thread(target=monitor_exit, args=(daemon, crawler.end_time), daemon=True).start()
    
    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print("\n[Master] Force stopped by user.")
        
    crawler.generate_report()

if __name__ == "__main__":
    main()