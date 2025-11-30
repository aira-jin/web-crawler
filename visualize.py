import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_charts():
    if not os.path.exists('crawl_results.csv'):
        print("Error: crawl_results.csv not found.")
        return

    # 1. Load Data
    df = pd.read_csv('crawl_results.csv')
    
    # 2. Convert Timestamps
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df['minute'] = df['datetime'].dt.floor('T') # Round to minute

    # 3. Plot Pages per Minute
    plt.figure(figsize=(10, 6))
    sns.set_theme(style="whitegrid")
    
    data_per_minute = df.groupby('minute').size().reset_index(name='pages_count')
    
    sns.lineplot(data=data_per_minute, x='minute', y='pages_count', marker='o', linewidth=2.5)
    
    plt.title('Distributed Crawler Velocity (Pages/Minute)', fontsize=16)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Pages Processed', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    plt.savefig('crawl_velocity_chart.png')
    print("Visualization saved as 'crawl_velocity_chart.png'")

if __name__ == "__main__":
    generate_charts()