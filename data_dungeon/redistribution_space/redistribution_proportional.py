import os
import csv
import gc
import queue
import time
from concurrent.futures import ThreadPoolExecutor, wait
from tqdm import tqdm
from redistribution_space.utils import get_block, extract_height_from_name, plot_balance_histogram, plot_redistribution_histogram
from database.database import create_connection, retrieve_eligible_accounts, retrieve_non_eligible_accounts

# Global variables that are not supposed to be modified

num_readers = 2

# Global variables supposed to be modified

file_queue = queue.Queue(maxsize=20)
lock = 0

def compute_redistribution(address, eligible_accounts, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses):
    value = 0

    # if the address is in eligible_subsequent_addresses, then it has been already updated at least once
    if address in eligible_subsequent_addresses:
        # addresses in eligible_subsequent_addresses are not eligible from the beginning 
        # (or they are, but a partial redistribution has already been performed on them),
        # so their redistribution is computed only from the number_of_file from which they are eligible
        number_of_file = eligible_subsequent_addresses[address]
        value = redistribution_for_eligible_subsequent_addresses[number_of_file]
        # redistribution has been performed for the address up until now
        # if it is still eligible, then it will be inserted again in the dictionary
        del eligible_subsequent_addresses[address]
    # if the address is not in eligible_subsequent_addresses, then it has not been updated yet
    # so the redistribution is computed from the beginning (if it is eligible)
    elif address in eligible_accounts:
        value = redistribution_for_eligible_subsequent_addresses[0]
    
    return value

def perform_input_output(address, payment, input_output, cumulative_redistribution, eligible_accounts, non_eligible_accounts, eligible_subsequent_addresses, minimum, maximum, number_of_file):
    
    if address in eligible_accounts:
        balance = eligible_accounts[address]
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment + cumulative_redistribution
        else:
            updated_balance = balance + payment + cumulative_redistribution
        # if the address is not eligible anymore, then remove it from eligible_accounts and add it to non_eligible_accounts
        if updated_balance < minimum or updated_balance > maximum:
            del eligible_accounts[address]
            non_eligible_accounts[address] = updated_balance
        # otherwise, update the balance and update eligible_subsequent_addresses
        else:
            eligible_accounts[address] = updated_balance
            # add the address to the eligible_subsequent_addresses dictionary (it is eligible from now on)
            # if it is already present (it was already eligible), then the number_of_file is updated, because the redistribution has just been performed on it
            eligible_subsequent_addresses[address] = number_of_file

    elif address in non_eligible_accounts:
        balance = non_eligible_accounts[address]
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment + cumulative_redistribution
        else:
            updated_balance = balance + payment + cumulative_redistribution
        # if the address is eligible now, then add it to eligible_accounts and remove it from non_eligible_accounts, other than updating eligible_subsequent_addresses
        if minimum <= updated_balance <= maximum:
            del non_eligible_accounts[address]
            eligible_accounts[address] = updated_balance
            # add the address to the eligible_subsequent_addresses dictionary (it is eligible from now on)
            # if it is already present (it was already eligible), then the number_of_file is updated, because the redistribution has just been performed on it
            eligible_subsequent_addresses[address] = number_of_file
        # otherwise, simply update the balance
        else:
            non_eligible_accounts[address] = updated_balance
    # it may happen that some addresses are not in neither eligible_accounts nor non_eligible_accounts 
    # they have not been used up until now or they have been used but consumed all their balance in the past
    else:
        balance = 0
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment + cumulative_redistribution
        else:
            updated_balance = balance + payment + cumulative_redistribution
        # if the address is eligible now, then add it to eligible_accounts and remove it from non_eligible_accounts, other than updating eligible_subsequent_addresses
        if minimum <= updated_balance <= maximum:
            eligible_accounts[address] = updated_balance
            # add the address to the eligible_subsequent_addresses dictionary (it is eligible from now on)
            # if it is already present (it was already eligible), then the number_of_file is updated, because the redistribution has just been performed on it
            eligible_subsequent_addresses[address] = number_of_file
        # otherwise, simply update the balance
        else:
            non_eligible_accounts[address] = updated_balance

def perform_block_transactions(eligible_accounts, non_eligible_accounts, eligible_subsequent_addresses, minimum, maximum, block, number_of_file, redistribution_for_eligible_subsequent_addresses):
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

            cumulative_redistribution = compute_redistribution(sender, eligible_accounts, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses)
            perform_input_output(sender, payment, 0, cumulative_redistribution, eligible_accounts, non_eligible_accounts, eligible_subsequent_addresses, minimum, maximum, number_of_file)

        for output in transaction['Outputs']:
            receiver = output['Receiver']
            payment = output['Value']

            if isinstance(receiver, list):
                continue
            if isinstance(receiver, bytes):
                receiver = receiver.decode('utf-8')

            cumulative_redistribution = compute_redistribution(receiver, eligible_accounts, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses)
            perform_input_output(receiver, payment, 1, cumulative_redistribution, eligible_accounts, non_eligible_accounts, eligible_subsequent_addresses, minimum, maximum, number_of_file)

# each block is processed sequentially (and the corresponding accounts are updated)
# furthermore, in order to reduce the number of computations, in this phase, the redistribution is computed only for the accounts that are involved in transactions
# the redistribution to other accounts is performed afterwards
def process_blocks(eligible_accounts, non_eligible_accounts, redistribution, len_files, minimum, maximum, percentage, type):
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

            perform_block_transactions(eligible_accounts, non_eligible_accounts, eligible_subsequent_addresses, minimum, maximum, block, number_of_file, redistribution_for_eligible_subsequent_addresses)

            num_users = len(eligible_accounts)

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
                payment = output['Value'] * ratio

                if isinstance(receiver, list):
                    continue
                if isinstance(receiver, bytes):
                    receiver = receiver.decode('utf-8')

                cumulative_redistribution = compute_redistribution(receiver, eligible_accounts, eligible_subsequent_addresses, redistribution_for_eligible_subsequent_addresses)
                perform_input_output(receiver, payment, 1, cumulative_redistribution, eligible_accounts, non_eligible_accounts, eligible_subsequent_addresses, minimum, maximum, number_of_file)

            number_of_file += 1
            pbar.update(1)

    # apply redistribution to eligible_accounts first (in order to free RAM by removing eligible_subsequent_addresses)
    for addr in eligible_accounts.keys():
        cumulative_redistribution = compute_redistribution(addr, eligible_accounts, eligible_subsequent_addresses, redistribution)
        eligible_accounts[addr] += cumulative_redistribution

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

def redistribution_proportional(dir_sorted_blocks, dir_results, type, percentage, minimum, maximum):
    global lock
    global file_queue

    folder = f'{percentage}_{minimum}_{maximum}'
    dir_results_folder = f'{dir_results}/{type}/{folder}'
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_{percentage}_{minimum}_{maximum}.csv')
    path_redistribution = os.path.join(dir_results_folder, f'redistribution_{percentage}_{minimum}_{maximum}.csv')

    if not os.path.exists(path_accounts) or not os.path.exists(path_redistribution):

        conn = create_connection()

        files = os.listdir(dir_sorted_blocks)
        files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
        files.sort()
        # delete the first 3 files (000, 001, 002) because the utxo has already them
        files = files[3:]

        len_files = len(files)

        # addresses that are eligible at the beginning are saved in this set
        # addresses that become uneligible at some point are removed from this set, but at the same time the redistribution is transferred to them
        eligible_accounts = {key: value for key, value in retrieve_eligible_accounts(conn, minimum, maximum)}
        non_eligible_accounts = {key: value for key, value in retrieve_non_eligible_accounts(conn, minimum, maximum)}

        # pre-allocate a fixed size redistribution list
        redistribution = [0] * len_files

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, eligible_accounts, non_eligible_accounts, redistribution, len_files, minimum, maximum, percentage, type)]

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

        with open(path_accounts, 'w+') as file:
            csv_out = csv.writer(file)
            csv_out.writerow(['address','balance'])

            # save the accounts which have already received redistribution
            with tqdm(total=len(eligible_accounts), desc=f'Writing eligible accounts') as pbar:
                for key, value in eligible_accounts.items():
                    csv_out.writerow((key, value))

                    pbar.update(1)

            # save accounts that are not eligible
            with tqdm(total=len(non_eligible_accounts), desc=f'Writing non-eligible accounts') as pbar:
                for key, value in non_eligible_accounts.items():
                    csv_out.writerow((key, value))

                    pbar.update(1)
    
    eligible_accounts = None
    non_eligible_accounts = None
    gc.collect()

    plot_balance_histogram(path_accounts)
    plot_redistribution_histogram(path_redistribution)