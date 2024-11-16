import json
import re
import time
import matplotlib.pyplot as plt
import pandas as pd
from queue import Queue
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait

def clean_string_for_json_conversion(str):
    return re.sub(r'er\': \[.*?\]', 'er\': \'INVALID\'', str).replace('er\': None', 'er\': \'INVALID\'').replace(' b\'', ' \'').replace('\'', '\"')

def get_block(filename):
    with open(filename, 'r') as file:
        block_str = file.readline()
        cleaned_block_str = clean_string_for_json_conversion(block_str)
        block = json.loads(cleaned_block_str)

    return block

def extract_height_from_name(file):
    return int(file.replace('block_', '').replace('.txt', ''))

def _process_balance_chunk(chunk):
    local_counts = {'= 0': 0, '0 ~ 10000': 0, '10000 ~ 1000000': 0, '1000000 ~ 100000000': 0, '> 100000000': 0}

    for _, row in chunk.iterrows():
        try:
            balance = int(float(row['balance']))
            if balance == 0:
                local_counts['= 0'] += 1
            elif 0 < balance <= 10000:
                local_counts['0 ~ 10000'] += 1
            elif 10000 < balance <= 1000000:
                local_counts['10000 ~ 1000000'] += 1
            elif 1000000 < balance <= 100000000:
                local_counts['1000000 ~ 100000000'] += 1
            else:
                local_counts['> 100000000'] += 1
        except ValueError:
            continue

    return local_counts

def plot_balance_histogram(csv_file, chunk_size=1000000):
    accounts = {'= 0': 0, '0 ~ 10000': 0, '10000 ~ 1000000': 0, '1000000 ~ 100000000': 0, '> 100000000': 0}
    aggregation_queue = Queue(maxsize=10)

        # Function to aggregate results in a separate thread
    def aggregate_results():
        while True:
            local_counts = aggregation_queue.get()
            if local_counts == None:
                break
                
            local_counts = local_counts.result()
            for key, count in local_counts.items():
                accounts[key] += count

    # Start the aggregation thread
    with ThreadPoolExecutor(max_workers=1) as aggregator_executor:
        aggregator_future = [aggregator_executor.submit(aggregate_results)]

        with ProcessPoolExecutor() as executor:
            # Submit initial chunks to reach the max limit in memory
            for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc='Reading CSV in chunks'):
                aggregation_queue.put(executor.submit(_process_balance_chunk, chunk))
            
            aggregation_queue.put(None)
        
            wait(aggregator_future)
    
    balances = list(accounts.keys())
    frequencies = list(accounts.values())

    # Create a bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(balances, frequencies, color='skyblue', log=True)
    plt.xlabel('Balance (in SAT)')
    plt.ylabel('Number of users')
    plt.title('Bitcoin Balance Distribution')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.tight_layout()       # Adjust layout to prevent clipping

    # Show the plot
    plt.show()

def plot_redistribution_histogram(csv_file, chunk_size=1000000):
    redistribution_per_block = {}

    for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc="Reading CSV in chunks"):
        for _, row in chunk.iterrows():
            height = row['height']
            redistribution = row['redistribution']

            redistribution_per_block[height] = redistribution

    block_heights = list(redistribution_per_block.keys())
    block_fees = list(redistribution_per_block.values())

    # Create a bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(block_heights, block_fees, color='skyblue')
    plt.xlabel('Block Height')
    plt.ylabel('Redistribution per user (SAT)')
    plt.title('Bitcoin Block Fees by Block Height')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.tight_layout()       # Adjust layout to prevent clipping

    # Show the plot
    plt.show()