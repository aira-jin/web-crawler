# visualize.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def generate_charts():
    # 1. Load Data
    try:
        df = pd.read_csv('crawl_results.csv')
    except FileNotFoundError:
        print("Error: crawl_results.csv not found. Run the crawler first!")
        return

    # 2. Preprocessing
    # Convert timestamp to a readable datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    # Round to the nearest minute to group data
    df['minute'] = df['datetime'].dt.floor('T')

    # 3. Setup Style
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(12, 6))

    # 4. Create Visualization: Pages Crawled Over Time
    # This shows the "speed" of your distributed system
    data_per_minute = df.groupby('minute').size().reset_index(name='pages_count')
    
    sns.lineplot(data=data_per_minute, x='minute', y='pages_count', marker='o', color='green')
    
    plt.title('Distributed Crawler Performance: Pages Processed Per Minute', fontsize=16)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Pages Crawled', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # 5. Save and Show
    output_img = 'crawl_performance.png'
    plt.savefig(output_img)
    print(f"Chart saved to {output_img}")
    plt.show()

if __name__ == "__main__":
    generate_charts()