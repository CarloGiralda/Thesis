import json
import re
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from queue import Queue
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait

class DoubleDictionaryList:
    def __init__(self, dictionary={}, list=[]):
        self.dictionary = dictionary
        self.reverse_dictionary = {value: key for key, value in self.dictionary.items()}

        # lists have the same length
        self.list = np.array(list)
        self.len_list = len(self.list)

        self.invalid_elements = []
        self.elements_to_add = []

    def remove(self, key):
        # remove it from both dictionary and reverse dictionary
        index = self.dictionary.pop(key)
        self.reverse_dictionary.pop(index)

        self.invalid_elements.append(index)

        if index >= self.len_list:
            balance = self.elements_to_add[index - self.len_list]
        else:
            balance = self.list[index]

        return balance

    def add(self, address, balance):

        if len(self.invalid_elements) > 0:
            free_index = self.invalid_elements[0]

            if free_index >= self.len_list:
                self.elements_to_add[free_index - self.len_list] = balance
            else:
                self.list[free_index] = balance

            self.dictionary[address] = free_index
            self.reverse_dictionary[free_index] = address
            self.invalid_elements.pop(0)
        else:
            self.elements_to_add.append(balance)

            index = self.len_list + len(self.elements_to_add) - 1
            self.dictionary[address] = index
            self.reverse_dictionary[index] = address

    def update_balance(self, address, balance):
        index = self.dictionary[address]

        if index >= self.len_list:
            self.elements_to_add[index - self.len_list] = balance
        else:
            self.list[index] = balance

    def perform_addition(self):
        if len(self.elements_to_add) > 0:
            # add elements to first list, recompute the length of the first list, clear all elements added from list
            self.list = np.append(self.list, self.elements_to_add)
            self.len_list = len(self.list)
            self.elements_to_add.clear()

    def contains_key(self, key):
        return key in self.dictionary
    
    def get_balance(self, address):
        index = self.dictionary[address]

        if index >= self.len_list:
            balance = self.elements_to_add[index - self.len_list]
        else:
            balance = self.list[index]

        return balance

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

def distribute(extra_fee_amount, num_outputs):
    base, extra = divmod(extra_fee_amount, num_outputs)
    # Create an array filled with `base`
    result = np.full(num_outputs, base, dtype=np.int64)
    # Add 1 to the first `extra` elements
    if extra > 0:
        result[:extra] += 1
    return result

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

    plt.figure(figsize=(10, 6))
    plt.bar(balances, frequencies, color='skyblue', log=True)

    plt.xlabel('Balance (in SAT)')
    plt.ylabel('Number of users')
    plt.title('Bitcoin Balance Distribution')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.tight_layout()       # Adjust layout to prevent clipping

    plt.show()

def plot_linear_redistribution_histogram(csv_file, chunk_size=1000000):
    redistribution_per_block = {}

    for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc="Reading CSV in chunks"):
        for _, row in chunk.iterrows():
            height = row['height']
            redistribution = row['redistribution']

            redistribution_per_block[height] = redistribution

    block_heights = list(redistribution_per_block.keys())
    block_fees = list(redistribution_per_block.values())

    plt.figure(figsize=(10, 6))
    plt.bar(block_heights, block_fees, color='skyblue')
    
    plt.xlabel('Block Height')
    plt.ylabel('Redistribution per user (SAT)')
    plt.title('Bitcoin Block Fees by Block Height')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.tight_layout()       # Adjust layout to prevent clipping

    plt.show()

def plot_weight_based_metrics(csv_file, chunk_size=1000000, groups=1000):
    is_first_index = True
    first_index = 0

    block_heights = []

    total_redistribution_per_block = []
    max_redistribution_per_block = []
    min_redistribution_per_block = []
    perc_25_redistribution_per_block = []
    perc_50_redistribution_per_block = []
    perc_75_redistribution_per_block = []

    temp_height = np.array([0] * groups)
    temp0 = np.array([0] * groups)
    temp1 = np.array([0] * groups)
    temp2 = np.array([0] * groups)
    temp3 = np.array([0] * groups)
    temp4 = np.array([0] * groups)
    temp5 = np.array([0] * groups)

    for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc="Reading CSV in chunks"):
        for index, row in chunk.iterrows():
            # plus 2 is necessary because the blocks start at height 865003
            array_index = (index + 2) % groups
            if is_first_index:
                first_index = array_index
                is_first_index = False
            height = row['height']
            redistribution = row['redistribution']

            temp_height[array_index] = height
            redistribution = json.loads(redistribution)
            temp0[array_index] = redistribution[0]
            temp1[array_index] = redistribution[1]
            temp2[array_index] = redistribution[2]
            temp3[array_index] = redistribution[3]
            temp4[array_index] = redistribution[4]
            temp5[array_index] = redistribution[5]

            if height % groups == 0:
                # mask for excluding default values if the first_index % 1000 == 1 (the first index is not 001, but something else)
                mask = np.ones(groups, dtype=bool)
                if first_index % 1000 != 1:
                    for invalid_index in range(0, first_index):
                        mask[invalid_index] = False
                
                block_heights.append(f'{temp_height[first_index]} ~ {temp_height[array_index]}')
                total_redistribution_per_block.append(np.mean(temp0, where=mask))
                max_redistribution_per_block.append(np.mean(temp1, where=mask))
                min_redistribution_per_block.append(np.mean(temp2, where=mask))
                perc_25_redistribution_per_block.append(np.mean(temp3, where=mask))
                perc_50_redistribution_per_block.append(np.mean(temp4, where=mask))
                perc_75_redistribution_per_block.append(np.mean(temp5, where=mask))

                is_first_index = True

    # total redistribution
    plt.figure(figsize=(10, 6))
    plt.bar(block_heights, total_redistribution_per_block, color='skyblue')
    plt.xlabel('Block Height')
    plt.ylabel('Satoshis')
    plt.title('Total Redistribution by Block Height')
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.tight_layout()       # Adjust layout to prevent clipping

    plt.show()

    # percentile redistribution
    percentiles = {'Min': min_redistribution_per_block,
               '25th': perc_25_redistribution_per_block,
               '50th': perc_50_redistribution_per_block,
               '75th': perc_75_redistribution_per_block,
               'Max': max_redistribution_per_block}
    
    y = np.arange(len(block_heights))
    height = 0.175
    multiplier = 0

    _, ax = plt.subplots(layout='constrained', figsize=(10, 6))

    for attribute, measurement in percentiles.items():
        measurement = np.round(measurement, 1)
        offset = height * multiplier
        rects = ax.barh(y + offset, measurement, height, label=attribute)
        ax.bar_label(rects, padding=3, fontsize=8.5)
        multiplier += 1

    ax.set_xlabel('Satoshis')
    ax.set_title('Percentiles Redistribution by Block Height')
    ax.set_yticks(y + height, block_heights)
    ax.legend(loc='upper right', ncols=1)

    min_value = min(min_redistribution_per_block)
    max_value = max(max_redistribution_per_block)
    ax.set_xlim(min_value, max_value * 1.5)

    plt.show()

def _process_redistribution_chunk(chunk):
    local_redistributions = []

    for _, row in chunk.iterrows():
        redistribution = row['redistribution']
        local_redistributions.append(redistribution)

    return local_redistributions

def plot_balance_line(csv_file, chunk_size=1000000):
    redistributions = []

    aggregation_queue = Queue(maxsize=10)

    # Function to aggregate results in a separate thread
    def aggregate_results():
        while True:
            local_redistributions = aggregation_queue.get()
            if local_redistributions == None:
                break
                
            local_redistributions = local_redistributions.result()
            redistributions.extend(local_redistributions)

    # Start the aggregation thread
    with ThreadPoolExecutor(max_workers=1) as aggregator_executor:
        aggregator_future = [aggregator_executor.submit(aggregate_results)]

        with ProcessPoolExecutor() as executor:
            # Submit initial chunks to reach the max limit in memory
            for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc='Reading CSV in chunks'):
                aggregation_queue.put(executor.submit(_process_redistribution_chunk, chunk))
            
            aggregation_queue.put(None)
        
            wait(aggregator_future)

    redistributions.sort(reverse=True)
    
    plt.figure(figsize=(10, 6))
    plt.plot(redistributions)
    plt.ylabel('Redistribution')
    plt.yscale('log')
    plt.title('Total Redistribution per user')
    plt.show()