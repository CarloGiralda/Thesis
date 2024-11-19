import queue
import time
import os
import numpy as np
import database.accounts_database as accounts_database
import database.multi_input_accounts_database as multi_input_accounts_database
from redistribution_space.utils import get_block, extract_height_from_name
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm

# number of readers for blocks
num_readers = 2

def multi_input_address_clustering(dir_sorted_blocks):
    file_queue = queue.Queue(maxsize=10)

    files = os.listdir(dir_sorted_blocks)
    files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
    files.sort()
    # delete the first 3 files (000, 001, 002) because the utxo has already them
    files = files[3:]

    len_files = len(files)

    prev_height = extract_height_from_name(files[0]) - 1
    lock = prev_height

    accounts_conn = accounts_database.create_connection()
    accounts = {key: int(value) for key, value in accounts_database.retrieve_all_accounts(accounts_conn)}

    multi_input_accounts_conn = multi_input_accounts_database.create_connection()
    multi_input_accounts_database.create_tables(multi_input_accounts_conn)

    user_index = 0

    def read_files(files, thread_number, dir_sorted_blocks):
        nonlocal file_queue
        nonlocal lock

        with tqdm(total=len(files) // num_readers, desc=f'Reader Thread {thread_number}') as reading_pbar:
            for i in range(thread_number, len(files), num_readers):
                height = extract_height_from_name(files[i])
                path_file = os.path.join(dir_sorted_blocks, files[i])

                block = get_block(path_file)

                # wait until the previous block is inserted in the queue
                while height - 1 != lock:
                    time.sleep(0.1)

                file_queue.put((height, block))
                
                lock = height

                reading_pbar.update(1)

    class UnionFind:
        def __init__(self):
            self.parent = {}
            self.rank = {}

        def find(self, x):
            if x not in self.parent:
                self.parent[x] = x
                self.rank[x] = 0
            # Path compression
            if self.parent[x] != x:
                self.parent[x] = self.find(self.parent[x])  # Recursively find the root
            return self.parent[x]

        def union(self, x, y):
            root_x = self.find(x)
            root_y = self.find(y)

            if root_x != root_y:
                if self.rank.get(root_x, 0) > self.rank.get(root_y, 0):
                    self.parent[root_y] = root_x
                elif self.rank.get(root_x, 0) < self.rank.get(root_y, 0):
                    self.parent[root_x] = root_y
                else:
                    self.parent[root_y] = root_x
                    self.rank[root_x] = self.rank.get(root_x, 0) + 1

    def process_blocks(accounts):
        nonlocal file_queue

        # Dictionary ---> address: index of the corresponding user in the list
        multi_input_addresses = UnionFind()
        
        def cluster_addresses(inputs):
            nonlocal multi_input_addresses
            input_addresses = []

            for input in inputs:
                sender = input['Sender']
                if isinstance(sender, list) or sender == 'INVALID' or sender is None:
                    continue
                if isinstance(sender, bytes):
                    sender = sender.decode('utf-8')

                input_addresses.append(sender)

            for i in range(len(input_addresses) - 1):
                multi_input_addresses.union(input_addresses[i], input_addresses[i + 1])

        with tqdm(total=len_files, desc=f'Processing blocks') as pbar:
            while True:
                item = file_queue.get()
                if item is None:
                    break  # End of files
                _, block = item

                for transaction in block['Transactions']:
                    inputs = transaction['Inputs']
                    cluster_addresses(inputs)

                pbar.update(1)

                file_queue.task_done()

        addresses = {}
        address_to_user = {}
        nonlocal user_index

        multi_input_accounts = {}
        # apply path compression to all addresses
        # at the same time build the addresses dictionary ---> address: user
        # address_to_root dictionary is used to find the link between an address and its corresponding user identifier
        for address in tqdm(multi_input_addresses.parent.keys(), desc='Building dictionaries'):
            root = multi_input_addresses.find(address)

            if root not in address_to_user:
                address_to_user[root] = user_index
                user_index += 1
            
            user = address_to_user[root]
            addresses[address] = user
            # update the balance of the user by adding the balance of the current address
            # if this is the first tie, then the balance is the balance of the root address
            multi_input_accounts[user] = multi_input_accounts.get(user, accounts.get(root, 0)) + accounts.get(address, 0)
        
        return addresses, multi_input_accounts

    with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

        futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

        futures_processors = [processors.submit(process_blocks, accounts)]

        # wait for all readers to complete
        wait(futures_readers)
        # there is only one processor (the main thread)
        file_queue.put(None)

        # wait for the processor to complete
        for future in as_completed(futures_processors):
            multi_input_addresses, multi_input_accounts = future.result()

    # insert all the addresses that did not make any transaction into the two dictionaries assuming that they do not own any other address
    for address, balance in tqdm(accounts.items(), desc='Finalizing'):
        if address not in multi_input_addresses:
            multi_input_addresses[address] = user_index
            multi_input_accounts[user_index] = balance
            user_index += 1
        
    multi_input_accounts_database.insert_many_addresses(multi_input_accounts_conn, list(multi_input_addresses.items()))
    multi_input_accounts_database.insert_many_accounts(multi_input_accounts_conn, list(multi_input_accounts.items()))