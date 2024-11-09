import os
import yaml
import csv
import gc
import queue
import time
import json
from concurrent.futures import ThreadPoolExecutor, wait
from tqdm import tqdm
from redistribution_space.utils import get_block, extract_height_from_name, plot_balance_histogram, plot_redistribution_histogram
from database.database import create_connection, retrieve_eligible_addresses, retrieve_address, retrieve_all_accounts

# Global variables that are not supposed to be modified

num_readers = 2
num_processors = 8
num_writers = 1

# Global variables supposed to be modified

file_queue = queue.Queue(maxsize=20)
dictionary_queue = queue.Queue()
lock = 0

def compute_redistribution(address, eligible_addresses, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses):
    value = 0

    if address in eligible_addresses:
        value = redistribution_for_eligible_subsequent_addresses[0]
        # redistribution has been performed for the address up until now, so it is not among the addresses that should receive the redistribution from the beginning anymore
        # if it is still eligible, then it will be inserted in the eligible_subsequent_addresses dictionary
        eligible_addresses.remove(address)
    elif address in eligible_subsequent_addresses:
        # addresses in eligible_subsequent_addresses are not eligible from the beginning,
        # so their redistribution is computed only from the number_of_file from which they are eligible
        number_of_file = eligible_subsequent_addresses[address]
        value = redistribution_for_eligible_subsequent_addresses[number_of_file]
        # redistribution has been performed for the address up until now
        # if it is still eligible, then it will be inserted again in the dictionary
        eligible_subsequent_addresses.pop(address)
    
    return value

def perform_block_transactions(updated_accounts, eligible_addresses, eligible_subsequent_addresses, minimum, maximum, block, number_of_file, redistribution_for_eligible_subsequent_addresses):
    for index, transaction in enumerate(block['Transactions']):

        # skip coinbase transaction
        if index == 0:
            continue

        for input in transaction['Inputs']:
            sender = input['Sender']

            if isinstance(sender, list):
                continue

            if isinstance(sender, bytes):
                sender = sender.decode('utf-8')

            # store balance, payment and redistribution
            balance = updated_accounts[sender]
            payment = input['Value']
            cumulative_redistribution = compute_redistribution(sender, eligible_addresses, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses)
            # compute the updated balance and assign it to the corresponding address
            updated_balance = balance - payment + cumulative_redistribution
            updated_accounts[sender] = updated_balance
            # only check whether it is eligible for redistribution or not (in this case, add it to eligible_subsequent_addresses)
            if minimum <= updated_balance <= maximum:
                # add the address to the eligible_subsequent_addresses dictionary (it is eligible from now on)
                # if it is already present (it was already eligible), then the number_of_file is updated, because the redistribution has just been performed on it
                eligible_subsequent_addresses[sender] = number_of_file

        for output in transaction['Outputs']:
            receiver = output['Receiver']

            if isinstance(receiver, list):
                continue

            if isinstance(receiver, bytes):
                receiver = receiver.decode('utf-8')

            # create a new key that corresponds to the receiver of an output
            # its value is the previous balance + the amount received
            # store balance, payment and redistribution
            balance = updated_accounts[receiver]
            payment = output['Value']
            cumulative_redistribution = compute_redistribution(receiver, eligible_addresses, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses)
            # compute the updated balance and assign it to the corresponding address
            updated_balance = balance + payment + cumulative_redistribution
            updated_accounts[receiver] = updated_balance
            if minimum <= updated_balance <= maximum:
                # add the address to the eligible_subsequent_addresses dictionary (it is eligible from now on)
                # if it is already present (it was already eligible), then the number_of_file is updated, because the redistribution has just been performed on it
                eligible_subsequent_addresses[receiver] = number_of_file

# each block is processed sequentially (and the corresponding accounts are updated)
# furthermore, in order to reduce the number of computations, in this phase, the redistribution is computed only for the accounts that are involved in transactions
# the redistribution to other accounts is performed afterwards
def process_blocks(updated_accounts, eligible_addresses, redistribution, len_files, minimum, maximum, percentage, type):
    number_of_file = 0
    # addresses that are not eligible at the beginning are saved in this dictionary, along with the height of the block at which they start to be eligible
    eligible_subsequent_addresses = {}
    # used for performance
    # dictionary that contains the sum of all redistribution computed from each file
    # {0: redistribution from first file to last one, 1: redistribution from second file to last one, ...}
    redistribution_for_eligible_subsequent_addresses = {i: 0 for i in range(len_files)}

    with tqdm(total=len_files, desc=f'Processing blocks') as pbar:

        while True:
            item = file_queue.get()
            if item is None:
                break  # End of files
            _, block = item

            perform_block_transactions(updated_accounts, eligible_addresses, eligible_subsequent_addresses, minimum, maximum, block, number_of_file, redistribution_for_eligible_subsequent_addresses)

            num_users = len(eligible_addresses) + len(eligible_subsequent_addresses)

            # fees payed by users
            fees = block['Fees']
            # total reward = block reward + fees
            total_reward = block['Reward']
            # block reward
            block_reward = total_reward - fees
            block_redistribution = fees * percentage

            if type == 'linear':
                # equal redistribution among all users with a positive balance
                redistribution_per_user = block_redistribution / num_users if num_users > 0 else 0
                redistribution[number_of_file] = redistribution_per_user

                # update the dictionary used for eligible_subsequent_addresses
                for i in range(number_of_file):
                    redistribution_for_eligible_subsequent_addresses[i] += redistribution_per_user
                redistribution_for_eligible_subsequent_addresses[number_of_file] = redistribution_per_user

            new_total_reward = block_reward + (fees - block_redistribution)
            # ratio between previous total reward and redistributed total reward
            ratio = new_total_reward / total_reward

            coinbase_transaction = block['Transactions'][0]
            for output in coinbase_transaction['Outputs']:
                receiver = output['Receiver']

                if isinstance(receiver, list):
                    continue

                if isinstance(receiver, bytes):
                    receiver = receiver.decode('utf-8')
                
                # perform the coinbase transaction, but taking into account the redistribution previously performed
                balance = updated_accounts[receiver]
                payment = output['Value'] * ratio
                cumulative_redistribution = compute_redistribution(receiver, eligible_addresses, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses)
                # compute the updated balance and assign it to the corresponding address
                updated_balance = balance + payment + cumulative_redistribution
                updated_accounts[receiver] = updated_balance
                if minimum <= updated_balance <= maximum:
                    # add the address to the eligible_subsequent_addresses dictionary (it is eligible from now on)
                    # if it is already present (it was already eligible), then the number_of_file is updated, because the redistribution has just been performed on it
                    eligible_subsequent_addresses[receiver] = number_of_file

            number_of_file += 1
            pbar.update(1)

    # apply redistribution to updated_accounts first
    for addr in updated_accounts.keys():
        cumulative_redistribution = compute_redistribution(addr, eligible_addresses, eligible_subsequent_addresses, redistribution)
        updated_accounts[addr] += cumulative_redistribution

def read_files(files, thread_number, dir_sorted_blocks):
    global lock
    global file_queue

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
    
def extract_updated_accounts(updated_accounts):
    global file_queue
    global dictionary_queue

    conn = create_connection()

    while True:

        item = file_queue.get()
        if item is None:
            file_queue.task_done()
            break  # End of files
        height, block = item

        for transaction in block['Transactions']:

            for input in transaction['Inputs']:
                sender = input['Sender']
                if not isinstance(sender, list):
                    if isinstance(sender, bytes):
                        sender = sender.decode('utf-8')
                    if sender not in updated_accounts:
                        address, balance = retrieve_address(conn, sender)
                        dictionary_queue.put((address, balance))
            
            for output in transaction['Outputs']:
                receiver = output['Receiver']
                if not isinstance(receiver, list):
                    if isinstance(receiver, bytes):
                        receiver = receiver.decode('utf-8')
                    if receiver not in updated_accounts:
                        address, balance = retrieve_address(conn, receiver)
                        dictionary_queue.put((address, balance))
        
        file_queue.task_done()
        
def save_updated_accounts(updated_accounts):
    global dictionary_queue

    while True:

        item = dictionary_queue.get()
        if item is None:
            dictionary_queue.task_done()
            break  # End of files
        address, balance = item
        if address not in updated_accounts:
            updated_accounts[address] = balance

        dictionary_queue.task_done()

def redistribute_fees(config_file, dir_sorted_blocks, dir_results):
    global lock
    global file_queue
    global dictionary_queue

    with open(config_file) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    
    percentage = cfg['fee']['percentage']
    type = cfg['redistribution']['type']
    minimum = cfg['redistribution']['minimum']
    maximum = cfg['redistribution']['maximum']

    folder = f'{percentage}_{type}_{minimum}_{maximum}'
    dir_results_folder = f'{dir_results}/{folder}'
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_{percentage}_{type}_{minimum}_{maximum}.csv')
    path_redistribution = os.path.join(dir_results_folder, f'redistribution_{percentage}_{type}_{minimum}_{maximum}.csv')

    if not os.path.exists(path_accounts) or not os.path.exists(path_redistribution):

        conn = create_connection()

        updated_accounts = {}

        files = os.listdir(dir_sorted_blocks)
        files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
        files.sort()
        # delete the first 3 files (000, 001, 002) because the utxo has already them
        files = files[3:]

        len_files = len(files)

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        path_updated_accounts = os.path.join(dir_results, 'updated_accounts.txt')

        start_time = time.time()

        if not os.path.exists(path_updated_accounts):

            with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=num_processors) as processors, ThreadPoolExecutor(max_workers=num_writers) as writers:

                for _ in range(num_writers):
                    writers.submit(save_updated_accounts, updated_accounts)

                futures_processors = [processors.submit(extract_updated_accounts, updated_accounts) for _ in range(num_processors)]

                # submit method with the lock mechanism is far faster than map method without lock mechanism
                futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

                # wait for all readers to complete
                wait(futures_readers)
                # terminate the processors
                for _ in range(num_processors):
                    file_queue.put(None)

                file_queue.join()

                # wait for all processors to complete
                wait(futures_processors)
                # terminate the writers
                for _ in range(num_writers):
                    dictionary_queue.put(None)

                dictionary_queue.join()

            with open(path_updated_accounts, 'w+') as file:
                # save the accounts that are going to be updated
                file.write(str(updated_accounts))

        else:
            with open(path_updated_accounts, 'r') as file:
                updated_accounts_str = file.readline().replace('None:', '\'INVALID\':').replace('\'', '\"')
                updated_accounts = json.loads(updated_accounts_str)

        end_time = time.time()
        print(f'Time for preprocessing: {end_time - start_time}')

        # addresses that are eligible at the beginning are saved in this set
        # addresses that become uneligible at some point are removed from this set, but at the same time the redistribution is transferred to them
        eligible_addresses = set(retrieve_eligible_addresses(conn, minimum, maximum))

        # pre-allocate a fixed size redistribution list
        redistribution = [0] * len_files

        if not file_queue.empty():
            print('Queue not empty')
            while not file_queue.empty():
                file_queue.get()

        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, updated_accounts, eligible_addresses, redistribution, len_files, minimum, maximum, percentage, type)]

            # wait for all readers to complete
            wait(futures_readers)
            # there is only one processor (the main thread)
            file_queue.put(None)

            # wait for the processor to complete
            wait(futures_processors)

        with open(path_redistribution, 'w+') as file:
            csv_out = csv.writer(file)
            csv_out.writerow(['height','redistribution'])
            with tqdm(total=len(redistribution), desc=f'Writing redistribution per block') as pbar:
                for index, red in enumerate(redistribution):
                    # the first file is 856003
                    csv_out.writerow([index + 856003, red])

                    pbar.update(1)

        # free RAM
        dictionary_queue = None
        file_queue = None

        gc.collect()

        already_saved_addresses = set()
        updated_accounts_list = list(updated_accounts.items())

        with open(path_accounts, 'w+') as file:
            csv_out = csv.writer(file)
            csv_out.writerow(['address','balance'])
            # save the accounts which have already received redistribution
            with tqdm(total=len(updated_accounts_list), desc=f'Writing updated accounts') as pbar:
                for row in updated_accounts_list:
                    csv_out.writerow(row)
                    # cache addresses of already updated accounts
                    already_saved_addresses.add(row[0])

                    pbar.update(1)

            # free RAM
            updated_accounts = None
            updated_accounts_list = None

            gc.collect()

            # perform redistribution on all other accounts
            total_redistribution = sum(redistribution)
            for index, accounts in enumerate(retrieve_all_accounts(conn)):
                with tqdm(total=1000000, desc=f'Writing {index}-th chunk of accounts') as pbar:
                    for account in accounts:
                        # the address must not be in already_saved_addresses (otherwise, it has already been processed)
                        # and it must not be in eligible_addresses (otherwise, it is not eligible for redistribution)
                        # if they were eligible at the beginning (and they were not affected by the transactions), then they are eligible at the end and they receive the full redistribution
                        if account[0] not in already_saved_addresses and account[0] in eligible_addresses:
                            csv_out.writerow([account[0], account[1] + total_redistribution])
                            # instead of adding it to the already_saved_addresses, it is more efficient to remove it from eligible_addresses
                            eligible_addresses.remove(account[0])
                        
                        # if they are not eligible and they have not been already saved, then simply save them
                        elif account[0] not in already_saved_addresses and account[0] not in eligible_addresses:
                            csv_out.writerow(account)
                            already_saved_addresses.add(account[0])

                        pbar.update(1)

        # free RAM
        eligible_addresses = None
        already_saved_addresses = None

        gc.collect()

    plot_balance_histogram(path_accounts)
    plot_redistribution_histogram(path_redistribution)