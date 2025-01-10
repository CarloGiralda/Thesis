import os
import csv
import queue
import time
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm
from redistribution_space.utils import get_block, extract_height_from_name, plot_linear_redistribution_histogram, plot_almost_equal_metrics, plot_weight_based_metrics
from database.accounts_database import create_connection, retrieve_all_accounts

# number of readers for blocks
num_readers = 2

file_queue = queue.Queue(maxsize=10)
lock = 0

def perform_input_output(address, payment, input_output, accounts):
    
    if address in accounts:
        balance = accounts[address]

    # it may happen that some addresses are not in accounts
    # they have not been used up until now or they have been used but consumed all their balance in the past
    else:
        balance = 0

    # compute the updated balance
    if input_output == 0:
        updated_balance = balance - payment
    else:
        updated_balance = balance + payment
    
    accounts[address] = updated_balance

    return accounts

def perform_block_transactions(block, accounts):

    for index, transaction in enumerate(block['Transactions']):
        # skip coinbase transaction
        if index == 0:
            continue

        for input in transaction['Inputs']:
            sender = input['Sender']
            payment = input['Value']

            if isinstance(sender, list):
                continue
            if isinstance(sender, bytes):
                sender = sender.decode('utf-8')

            accounts = perform_input_output(sender, payment, 0, accounts)

        for output in transaction['Outputs']:
            receiver = output['Receiver']
            payment = output['Value']

            if isinstance(receiver, list):
                continue
            if isinstance(receiver, bytes):
                receiver = receiver.decode('utf-8')

            accounts = perform_input_output(receiver, payment, 1, accounts)

    return accounts

def perform_coinbase_transaction(block, accounts):
    coinbase_transaction = block['Transactions'][0]

    for i in range(len(coinbase_transaction['Outputs'])):
        output = coinbase_transaction['Outputs'][i]

        receiver = output['Receiver']
        value = output['Value']

        if isinstance(receiver, list):
            continue
        if isinstance(receiver, bytes):
            receiver = receiver.decode('utf-8')

        accounts = perform_input_output(receiver, value, 1, accounts)
            
    return accounts

# each block is processed sequentially (and the corresponding accounts are updated)
def process_blocks(accounts, len_files):
    global file_queue

    with tqdm(total=len_files, desc=f'Processing blocks') as pbar:

        while True:
            item = file_queue.get()
            if item is None:
                break  # End of files
            _, block = item

            accounts = perform_block_transactions(block, accounts)

            accounts = perform_coinbase_transaction(block, accounts)

            pbar.update(1)

            file_queue.task_done()

    return accounts

def read_files(files, thread_number, dir_sorted_blocks):
    global file_queue
    global lock

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

def no_redistribution(dir_sorted_blocks, dir_results, metric_type):
    global file_queue
    global lock

    dir_results_folder = os.path.join(dir_results, metric_type, 'single_input')
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_no_redistribution.csv')

    if not os.path.exists(path_accounts):

        conn = create_connection()

        files = os.listdir(dir_sorted_blocks)
        files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
        files.sort()
        # delete the first 3 files (000, 001, 002) because the utxo has already them
        files = files[3:]

        len_files = len(files)

        print('Retrieving all accounts...')
        accounts = dict(retrieve_all_accounts(conn))

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, accounts, len_files)]

            # wait for all readers to complete
            wait(futures_readers)
            # there is only one processor (the main thread)
            file_queue.put(None)

            # wait for the processor to complete
            for future in as_completed(futures_processors):
                accounts = future.result()

        with open(path_accounts, 'w+') as file:
            csv_out = csv.writer(file)
            csv_out.writerow(['address','balance'])

            with tqdm(total=len(accounts), desc=f'Writing accounts') as pbar:
                for address, balance in accounts.items():
                    csv_out.writerow((address, balance))

                    pbar.update(1)