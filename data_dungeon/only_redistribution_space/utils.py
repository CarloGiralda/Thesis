import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from queue import Queue
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait

class DictionaryDoubleList:
    def __init__(self, dictionary={}, first_list=[], second_list=[]):
        self.dictionary = dictionary

        # lists have the same length
        self.first_list = np.array(first_list)
        self.len_first_list = len(self.first_list)
        self.second_list = np.array(second_list)
        self.len_second_list = len(self.second_list)

        self.invalid_elements = []
        self.elements_to_add_first_list = []
        self.elements_to_add_second_list = []

    def remove(self, key):
        index = self.dictionary.pop(key)
        self.invalid_elements.append(index)

        if index >= self.len_first_list:
            balance = self.elements_to_add_first_list[index - self.len_first_list]
            redistribution = self.elements_to_add_second_list[index - self.len_second_list]
        else:
            balance = self.first_list[index]
            redistribution = self.second_list[index]

        return balance, redistribution

    def add(self, address, balance, redistribution):

        if len(self.invalid_elements) > 0:
            free_index = self.invalid_elements[0]

            if free_index >= self.len_first_list:
                self.elements_to_add_first_list[free_index - self.len_first_list] = balance
                self.elements_to_add_second_list[free_index - self.len_second_list] = redistribution
            else:
                self.first_list[free_index] = balance
                self.second_list[free_index] = redistribution

            self.dictionary[address] = free_index
            self.invalid_elements.pop(0)
        else:
            self.elements_to_add_first_list.append(balance)
            self.elements_to_add_second_list.append(redistribution)
            self.dictionary[address] = self.len_first_list + len(self.elements_to_add_first_list) - 1

    def update_balance(self, address, balance):
        index = self.dictionary[address]

        if index >= self.len_first_list:
            self.elements_to_add_first_list[index - self.len_first_list] = balance
        else:
            self.first_list[index] = balance

    def update_redistribution(self, address, redistribution):
        index = self.dictionary[address]
        
        if index >= self.len_second_list:
            self.elements_to_add_second_list[index - self.len_second_list] = redistribution
        else:
            self.second_list[index] = redistribution

    def perform_addition(self):
        if len(self.elements_to_add_first_list) > 0:
            # add elements to first list, recompute the length of the first list, clear all elements added from list
            self.first_list = np.append(self.first_list, self.elements_to_add_first_list)
            self.len_first_list = len(self.first_list)
            self.elements_to_add_first_list.clear()
            # add elements to second list, recompute the length of the second list, clear all elements added from list
            self.second_list = np.append(self.second_list, self.elements_to_add_second_list)
            self.len_second_list = len(self.second_list)
            self.elements_to_add_second_list.clear()

    def contains_key(self, address):
        return address in self.dictionary
    
    def get_balance(self, address):
        index = self.dictionary[address]

        if index >= self.len_first_list:
            balance = self.elements_to_add_first_list[index - self.len_first_list]
        else:
            balance = self.first_list[index]

        return balance
    
    def get_redistribution(self, address):
        index = self.dictionary[address]

        if index >= self.len_first_list:
            redistribution = self.elements_to_add_second_list[index - self.len_first_list]
        else:
            redistribution = self.second_list[index]

        return redistribution
    
class DoubleDictionaryDoubleList:
    def __init__(self, dictionary={}, first_list=[], second_list=[]):
        self.dictionary = dictionary
        self.reverse_dictionary = {value: key for key, value in self.dictionary.items()}

        # lists have the same length
        self.first_list = np.array(first_list)
        self.len_first_list = len(self.first_list)
        self.second_list = np.array(second_list)
        self.len_second_list = len(self.second_list)

        self.invalid_elements = []
        self.elements_to_add_first_list = []
        self.elements_to_add_second_list = []

    def remove(self, key):
        # remove it from both dictionary and reverse dictionary
        index = self.dictionary.pop(key)
        self.reverse_dictionary.pop(index)

        self.invalid_elements.append(index)

        if index >= self.len_first_list:
            balance = self.elements_to_add_first_list[index - self.len_first_list]
            redistribution = self.elements_to_add_second_list[index - self.len_second_list]
        else:
            balance = self.first_list[index]
            redistribution = self.second_list[index]

        return balance, redistribution

    def add(self, address, balance, redistribution):

        if len(self.invalid_elements) > 0:
            free_index = self.invalid_elements[0]

            if free_index >= self.len_first_list:
                self.elements_to_add_first_list[free_index - self.len_first_list] = balance
                self.elements_to_add_second_list[free_index - self.len_second_list] = redistribution
            else:
                self.first_list[free_index] = balance
                self.second_list[free_index] = redistribution

            self.dictionary[address] = free_index
            self.reverse_dictionary[free_index] = address
            self.invalid_elements.pop(0)
        else:
            self.elements_to_add_first_list.append(balance)
            self.elements_to_add_second_list.append(redistribution)

            index = self.len_first_list + len(self.elements_to_add_first_list) - 1
            self.dictionary[address] = index
            self.reverse_dictionary[index] = address

    def update_balance(self, address, balance):
        index = self.dictionary[address]

        if index >= self.len_first_list:
            self.elements_to_add_first_list[index - self.len_first_list] = balance
        else:
            self.first_list[index] = balance

    def update_redistribution(self, address, redistribution):
        index = self.dictionary[address]
        
        if index >= self.len_second_list:
            self.elements_to_add_second_list[index - self.len_second_list] = redistribution
        else:
            self.second_list[index] = redistribution

    def perform_addition(self):
        if len(self.elements_to_add_first_list) > 0:
            # add elements to first list, recompute the length of the first list, clear all elements added from list
            self.first_list = np.append(self.first_list, self.elements_to_add_first_list)
            self.len_first_list = len(self.first_list)
            self.elements_to_add_first_list.clear()
            # add elements to second list, recompute the length of the second list, clear all elements added from list
            self.second_list = np.append(self.second_list, self.elements_to_add_second_list)
            self.len_second_list = len(self.second_list)
            self.elements_to_add_second_list.clear()

    def contains_key(self, address):
        return address in self.dictionary
    
    def get_balance(self, address):
        index = self.dictionary[address]

        if index >= self.len_first_list:
            balance = self.elements_to_add_first_list[index - self.len_first_list]
        else:
            balance = self.first_list[index]

        return balance
    
    def get_redistribution(self, address):
        index = self.dictionary[address]

        if index >= self.len_first_list:
            redistribution = self.elements_to_add_second_list[index - self.len_first_list]
        else:
            redistribution = self.second_list[index]

        return redistribution
    
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