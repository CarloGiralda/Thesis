import os
import csv
import queue
import time
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm
from redistribution_space.utils import get_block, extract_height_from_name
from database.multi_input_accounts_database import create_connection, retrieve_all_accounts

# number of readers for blocks
num_readers = 2

file_queue = queue.Queue(maxsize=10)
lock = 0
# index used to give a different number to all the new users discovered
user_index = 0

def perform_input_output(address, payment, input_output, address_to_user, accounts):
    global user_index

    if address not in address_to_user:
        address_to_user[address] = user_index
        user_index += 1
    # addresses of transactions' outputs are not included in it
    user = address_to_user[address]

    if user in accounts:
        balance = accounts[user]
    
    # it may happen that some addresses are not in accounts
    # they have not been used up until now or they have been used but consumed all their balance in the past
    else:
        balance = 0

    # compute the updated balance
    if input_output == 0:
        updated_balance = balance - payment
    else:
        updated_balance = balance + payment

    accounts[user] = updated_balance

    return address_to_user, accounts

def perform_block_transactions(address_to_user, accounts, block):

    for index, transaction in enumerate(block['Transactions']):
        # skip coinbase transaction
        if index == 0:
            continue

        for input in transaction['Inputs']:
            sender = input['Sender']
            payment = input['Value']

            if sender == 'INVALID' or sender == 'UNKNOWN':
                continue

            address_to_user, accounts = perform_input_output(sender, payment, 0, address_to_user, accounts)

        for output in transaction['Outputs']:
            receiver = output['Receiver']
            payment = output['Value']

            if receiver == 'INVALID' or receiver == 'UNKNOWN':
                continue

            address_to_user, accounts = perform_input_output(receiver, payment, 1, address_to_user, accounts)

    return address_to_user, accounts

def perform_coinbase_transaction(block, accounts, address_to_user):
    coinbase_transaction = block['Transactions'][0]

    for i in range(len(coinbase_transaction['Outputs'])):
        output = coinbase_transaction['Outputs'][i]

        receiver = output['Receiver']
        value = output['Value']

        if receiver == 'INVALID' or receiver == 'UNKNOWN':
            continue

        address_to_user, accounts = perform_input_output(receiver, value, 1, address_to_user, accounts)
            
    return address_to_user, accounts

# each block is processed sequentially (and the corresponding accounts are updated)
def process_blocks(address_to_user, accounts, len_files):
    global file_queue

    with tqdm(total=len_files, desc=f'Processing blocks') as pbar:

        while True:
            item = file_queue.get()
            if item is None:
                break  # End of files
            _, block = item

            address_to_user, accounts = perform_block_transactions(address_to_user, accounts, block)

            address_to_user, accounts = perform_coinbase_transaction(block, accounts, address_to_user)

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

def multi_input_no_redistribution(dir_sorted_blocks, dir_results, metric_type):
    global file_queue
    global lock
    global user_index

    dir_results_folder = os.path.join(dir_results, metric_type, 'multi_input')
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_no_redistribution.csv')
    path_balances = os.path.join(dir_results_folder, f'balances_no_redistribution.csv')

    if not os.path.exists(path_accounts) or not os.path.exists(path_balances):

        conn = create_connection()

        files = os.listdir(dir_sorted_blocks)
        files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
        files.sort()
        # delete the first 3 files (000, 001, 002) because the utxo has already them
        files = files[3:]

        len_files = len(files)

        def retrieve_accounts_object(conn):
            print('Retrieving accounts from database...')
            # in this dictionary are saved all the addresses and their corresponding user identifier
            address_to_user = {}

            accounts = {}

            for address, user, balance in retrieve_all_accounts(conn):
                address_to_user[address] = user
                if user not in accounts:
                    accounts[user] = balance

            return accounts, address_to_user
        
        accounts, address_to_user = retrieve_accounts_object(conn)

        # set the user_index to the identifier of the user with the highest number of eligible_balances + 1
        user_index = max(address_to_user.values()) + 1

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, address_to_user, accounts, len_files)]

            # wait for all readers to complete
            wait(futures_readers)
            # there is only one processor (the main thread)
            file_queue.put(None)

            # wait for the processor to complete
            for future in as_completed(futures_processors):
                accounts = future.result()

        with open(path_accounts, 'w+') as file:
            csv_out = csv.writer(file)
            csv_out.writerow(['address','user'])

            with tqdm(total=len(address_to_user), desc=f'Writing accounts') as pbar:
                for address, user in address_to_user.items():
                    csv_out.writerow([address,user])

                    pbar.update(1)

        with open(path_balances, 'w+') as file:
            csv_out = csv.writer(file)
            csv_out.writerow(['user','balance'])

            with tqdm(total=len(accounts), desc=f'Writing balances') as pbar:
                for user, balance in accounts.items():
                    csv_out.writerow([user,balance])

                    pbar.update(1)