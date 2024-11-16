import os
import csv
import gc
import queue
import time
import math
import numpy as np
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm
from redistribution_space.utils import get_block, extract_height_from_name, plot_balance_histogram, plot_redistribution_histogram
from database.database import create_connection, retrieve_eligible_accounts, retrieve_non_eligible_accounts

# number of readers for blocks
num_readers = 2

file_queue = queue.Queue(maxsize=10)
lock = 0

def perform_input_output(address, payment, input_output, 
                         eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                         minimum, maximum, len_eligible_balances):
    
    if address in eligible_addresses:
        index = eligible_addresses[address]
        if index >= len_eligible_balances:
            balance = elements_to_add[index - len_eligible_balances]
        else:
            balance = eligible_balances[index]
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment
        
        # if the address is still eligible, then update the balance, eligible_subsequent_addresses and eligible_accounts_near_non_eligibility (if its balance is higher than the threshold)
        if minimum <= updated_balance <= maximum:
            if index >= len_eligible_balances:
                elements_to_add[index - len_eligible_balances] = updated_balance
            else:
                eligible_balances[index] = updated_balance
        # otherwise, remove the address from eligible_accounts and eligible_accounts_near_non_eligibility (if it was part of it)
        # add it to non_eligible_accounts
        else:
            del eligible_addresses[address]
            invalid_balances.append(index)
            non_eligible_accounts[address] = updated_balance

    elif address in non_eligible_accounts:
        balance = non_eligible_accounts[address]
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment
        # if the address is eligible now, then add it to eligible_accounts (and to eligible_accounts_near_non_eligibility, if it satisfies the threshold)
        # update eligible_subsequent_addresses
        # remove it from non_eligible_accounts
        if minimum <= updated_balance <= maximum:
            del non_eligible_accounts[address]
            # if there is at least one invalid balance, then use it
            if len(invalid_balances) > 0:
                free_index = invalid_balances[0]
                if free_index >= len_eligible_balances:
                    elements_to_add[free_index - len_eligible_balances] = updated_balance
                else:
                    eligible_balances[free_index] = updated_balance
                eligible_addresses[address] = free_index
                invalid_balances.pop(0)
            else:
                elements_to_add.append(updated_balance)
                eligible_addresses[address] = len_eligible_balances + len(elements_to_add) - 1

        # otherwise, simply update the balance
        else:
            non_eligible_accounts[address] = updated_balance
    # it may happen that some addresses are not in neither eligible_accounts nor non_eligible_accounts 
    # they have not been used up until now or they have been used but consumed all their balance in the past
    else:
        balance = 0
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment
        # if the address is eligible now, then add it to eligible_accounts (and to eligible_accounts_near_non_eligibility, if it satisfies the threshold)
        # update eligible_subsequent_addresses
        # remove it from non_eligible_accounts
        if minimum <= updated_balance <= maximum:
            # if there is at least one invalid balance, then use it
            if len(invalid_balances) > 0:
                free_index = invalid_balances[0]
                if free_index >= len_eligible_balances:
                    elements_to_add[free_index - len_eligible_balances] = updated_balance
                else:
                    eligible_balances[free_index] = updated_balance
                eligible_addresses[address] = free_index
                invalid_balances.pop(0)
            else:
                elements_to_add.append(updated_balance)
                eligible_addresses[address] = len_eligible_balances + len(elements_to_add) - 1

        # otherwise, simply update the balance
        else:
            non_eligible_accounts[address] = updated_balance

    return eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add

def perform_block_transactions(eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                               minimum, maximum, block):
    # number of eligible accounts
    len_eligible_balances = len(eligible_balances)

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

            eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add = perform_input_output(
                sender, payment, 0, 
                eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                minimum, maximum, len_eligible_balances)

        for output in transaction['Outputs']:
            receiver = output['Receiver']
            payment = output['Value']

            if isinstance(receiver, list):
                continue
            if isinstance(receiver, bytes):
                receiver = receiver.decode('utf-8')

            eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add = perform_input_output(
                receiver, payment, 1, 
                eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                minimum, maximum, len_eligible_balances)

    return eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add
            
def perform_redistribution(type, amount, percentage, block, number_of_file,
                            redistribution, eligible_balances, elements_to_add):
    # fees payed by users
    fees = block['Fees']
    # total reward = block reward + fees
    total_reward = block['Reward']
    # block reward
    block_reward = total_reward - fees

    if amount == 'fees':
        max_block_redistribution = fees * percentage
    elif amount == 'block_reward':
        max_block_redistribution = block_reward * percentage
    elif amount == 'total_reward':
        max_block_redistribution = total_reward * percentage
    else:
        max_block_redistribution = total_reward
    
    if len(elements_to_add) > 0:
        eligible_balances = np.append(eligible_balances, elements_to_add)
        elements_to_add.clear()
    num_users = len(eligible_balances)
    
    if type == 'linear':

        redistribution_per_user = int(math.floor(max_block_redistribution / num_users)) if num_users > 0 else 0
        redistribution[number_of_file] = redistribution_per_user
        actual_block_redistribution = redistribution_per_user * num_users

        if redistribution_per_user > 0:
            eligible_balances += redistribution_per_user

    new_total_reward = total_reward - actual_block_redistribution
    # ratio between previous total reward and redistributed total reward
    ratio = new_total_reward / total_reward

    return redistribution, eligible_balances, elements_to_add, ratio

# each block is processed sequentially (and the corresponding accounts are updated)
# furthermore, in order to reduce the number of computations, in this phase, the redistribution is computed only for the accounts that are involved in transactions
# the redistribution to other accounts is performed afterwards
def process_blocks(eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, redistribution, 
                   len_files, minimum, maximum, percentage, type, amount):
    global file_queue
    number_of_file = 0

    with tqdm(total=len_files, desc=f'Processing blocks') as pbar:
        elements_to_add = []

        while True:
            item = file_queue.get()
            if item is None:
                break  # End of files
            _, block = item

            eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add = perform_block_transactions(
                eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                minimum, maximum, block)

            redistribution, eligible_balances, elements_to_add, ratio = perform_redistribution(
                type, amount, percentage, block, number_of_file, 
                redistribution, eligible_balances, elements_to_add)

            # to not waste the SATs of the approximation (math.floor), their number is kept in this variable
            excessive = 0.0
            coinbase_transaction = block['Transactions'][0]
            for i in range(len(coinbase_transaction['Outputs'])):
                output = coinbase_transaction['Outputs'][i]
                receiver = output['Receiver']
                exact_payment = output['Value'] * ratio
                payment = int(math.floor(exact_payment))
                excessive += exact_payment - payment

                if isinstance(receiver, list):
                    continue
                if isinstance(receiver, bytes):
                    receiver = receiver.decode('utf-8')

                # the SATs approximated by math.floor are added to the last output
                if i == len(coinbase_transaction['Outputs']) - 1:
                    payment += int(excessive)

                eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add = perform_input_output(
                    receiver, payment, 1, 
                    eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                    minimum, maximum, len(eligible_balances))

            number_of_file += 1
            pbar.update(1)

            file_queue.task_done()

    return eligible_addresses, eligible_balances, non_eligible_accounts, redistribution

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

def redistribution_paradise(dir_sorted_blocks, dir_results, type, percentage, amount, minimum, maximum):
    global file_queue
    global lock

    folder = f'{percentage}_{minimum}_{maximum}'
    dir_results_folder = f'{dir_results}/{type}/{folder}'
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_{percentage}_{amount}_{minimum}_{maximum}.csv')
    path_redistribution = os.path.join(dir_results_folder, f'redistribution_{percentage}_{amount}_{minimum}_{maximum}.csv')

    if not os.path.exists(path_accounts) or not os.path.exists(path_redistribution):

        conn = create_connection()

        files = os.listdir(dir_sorted_blocks)
        files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
        files.sort()
        # delete the first 3 files (000, 001, 002) because the utxo has already them
        files = files[3:]

        len_files = len(files)

        print('Retrieving eligible accounts from database...')
        eligible_accounts = retrieve_eligible_accounts(conn, minimum, maximum)
        len_eligible_accounts = len(eligible_accounts)
        eligible_addresses = {}
        eligible_balances = np.array([0] * len_eligible_accounts)
        # because removal from eligible_balances would be expensive (indeed, each value is referenced by one value in eligible_addresses)
        # we use a list to keep track of invalid positions (they can be used if new addresses are added)
        invalid_balances = []
        for index, (key, value) in enumerate(eligible_accounts):
            eligible_balances[index] = int(value)
            eligible_addresses[key] = index

        eligible_accounts = None

        print('Retrieving non eligible accounts from database...')
        non_eligible_accounts = {key: int(value) for key, value in retrieve_non_eligible_accounts(conn, minimum, maximum)}

        # pre-allocate a fixed size redistribution list
        redistribution = [0] * len_files

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, eligible_addresses, eligible_balances, invalid_balances, non_eligible_accounts, redistribution, len_files, minimum, maximum, percentage, type, amount)]

            # wait for all readers to complete
            wait(futures_readers)
            # there is only one processor (the main thread)
            file_queue.put(None)

            # wait for the processor to complete
            for future in as_completed(futures_processors):
                eligible_addresses, eligible_balances, non_eligible_accounts, redistribution = future.result()

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
            with tqdm(total=len(eligible_addresses), desc=f'Writing eligible accounts') as pbar:
                for address, index in eligible_addresses.items():
                    balance = eligible_balances[index]
                    csv_out.writerow((address, balance))

                    pbar.update(1)

            # save accounts that are not eligible
            with tqdm(total=len(non_eligible_accounts), desc=f'Writing non-eligible accounts') as pbar:
                for key, value in non_eligible_accounts.items():
                    csv_out.writerow((key, value))

                    pbar.update(1)
    
        eligible_addresses = None
        eligible_balances = None
        invalid_balances = None
        non_eligible_accounts = None
        gc.collect()

    plot_balance_histogram(path_accounts)
    plot_redistribution_histogram(path_redistribution)