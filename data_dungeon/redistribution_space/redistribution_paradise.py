import os
import csv
import queue
import time
import math
import numpy as np
import numpy_minmax
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm
from redistribution_space.utils import DoubleDictionaryList, get_block, extract_height_from_name, distribute, plot_balance_histogram, plot_linear_redistribution_histogram, plot_weight_based_metrics
from database.accounts_database import create_connection, retrieve_eligible_accounts, retrieve_non_eligible_accounts

# number of readers for blocks
num_readers = 2

file_queue = queue.Queue(maxsize=10)
lock = 0

def perform_input_output(address, payment, input_output, 
                         eligible_accounts, non_eligible_accounts, 
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output):
    
    if eligible_accounts.contains_key(address):
        balance = eligible_accounts.get_balance(address)

        # compute the updated balance
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output
        
        # if the address is still eligible, then update the balance
        if redistribution_minimum <= updated_balance <= redistribution_maximum:
            eligible_accounts.update_balance(address, updated_balance)
        # otherwise, remove the address from eligible_accounts
        # add it to non_eligible_accounts
        else:
            _ = eligible_accounts.remove(address)
            non_eligible_accounts[address] = updated_balance

    elif address in non_eligible_accounts:
        balance = non_eligible_accounts[address]

        # compute the updated balance
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output

        # if the address is eligible now, then add it to eligible_accounts
        # remove it from non_eligible_accounts
        if redistribution_minimum <= updated_balance <= redistribution_maximum:
            del non_eligible_accounts[address]
            eligible_accounts.add(address, updated_balance)
        # otherwise, simply update the balance
        else:
            non_eligible_accounts[address] = updated_balance

    # it may happen that some addresses are not in eligible_accounts nor non_eligible_accounts
    # they have not been used up until now or they have been used but consumed all their balance in the past
    else:
        balance = 0

        # compute the updated balance
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output

        # if the address is eligible now, then add it to eligible_accounts
        # remove it from non_eligible_accounts
        if redistribution_minimum <= updated_balance <= redistribution_maximum:
            eligible_accounts.add(address, updated_balance)
        # otherwise, simply update the balance
        else:
            non_eligible_accounts[address] = updated_balance

    return eligible_accounts, non_eligible_accounts

def perform_block_transactions(eligible_accounts, non_eligible_accounts,  
                               redistribution_minimum, redistribution_maximum, block, extra_fee_amount, extra_fee_percentage):

    total_extra_fee_percentage = 0
    # extra fee computed for each output if the fee is a percentage
    extra_fee_percentage_per_output = 0

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

            eligible_accounts, non_eligible_accounts = perform_input_output(
                sender, payment, 0, 
                eligible_accounts, non_eligible_accounts, 
                redistribution_minimum, redistribution_maximum, 0, 0)
        
        num_outputs = len(transaction['Outputs'])
        extra_fee_per_output = distribute(extra_fee_amount, num_outputs)

        for output_index, output in enumerate(transaction['Outputs']):
            receiver = output['Receiver']
            payment = output['Value']

            if extra_fee_percentage > 0.0:
                extra_fee_percentage_per_output = math.ceil(extra_fee_percentage * payment)
                total_extra_fee_percentage += extra_fee_percentage_per_output

            if isinstance(receiver, list):
                continue
            if isinstance(receiver, bytes):
                receiver = receiver.decode('utf-8')

            eligible_accounts, non_eligible_accounts = perform_input_output(
                receiver, payment, 1, 
                eligible_accounts, non_eligible_accounts, 
                redistribution_minimum, redistribution_maximum, extra_fee_per_output[output_index], extra_fee_percentage_per_output)

    return eligible_accounts, non_eligible_accounts, total_extra_fee_percentage
            
def perform_redistribution(redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee,
                            redistribution, eligible_accounts, non_eligible_accounts):
    # fees payed by users
    fees = block['Fees']
    # total reward = block reward + fees
    total_reward = block['Reward']
    # block reward
    block_reward = total_reward - fees

    if redistribution_amount == 'fees':
        max_block_redistribution = math.floor(fees * redistribution_percentage)
    elif redistribution_amount == 'block_reward':
        max_block_redistribution = math.floor(block_reward * redistribution_percentage)
    elif redistribution_amount == 'total_reward':
        max_block_redistribution = math.floor(total_reward * redistribution_percentage)
    else:
        max_block_redistribution = 0

    max_redistribution = max_block_redistribution + total_extra_fee
    
    eligible_accounts.perform_addition()
    
    eligible_balances = eligible_accounts.list
    invalid_eligible_balances = eligible_accounts.invalid_elements

    len_eligible_balances = len(eligible_balances)

    # mask array to select only the valid balances (not all of them are valid)
    mask = np.ones(len_eligible_balances, dtype=bool)
    mask[invalid_eligible_balances] = False

    num_users = len_eligible_balances - len(invalid_eligible_balances)
    # if the percentage of users is less than 100%, then update the mask to exclude the top (1 - redistribution_user_percentage)%
    if redistribution_user_percentage < 1.0:
        valid_balances = eligible_balances[mask]
        k = math.floor(num_users * redistribution_user_percentage)
        threshold = np.partition(valid_balances, k)[k]

        mask = (eligible_balances < threshold) & mask

        num_users = np.count_nonzero(mask)

    if redistribution_type == 'no_redistribution':

        actual_redistribution = 0
    
    elif redistribution_type == 'equal':

        redistribution_per_user = int(math.floor(max_redistribution / num_users)) if num_users > 0 else 0
        redistribution[number_of_file] = redistribution_per_user
        actual_redistribution = redistribution_per_user * num_users

        if redistribution_per_user > 0:
            if redistribution_user_percentage < 1.0:
                eligible_accounts.list[mask] += redistribution_per_user
            else:
                eligible_accounts.list += redistribution_per_user

    elif redistribution_type == 'weight_based':

        # compute the inverse of the eligible balances
        # by doing this, the lowest balance will have the highest weight, while the highest balance will have the lowest weight
        inverse_weights = np.zeros_like(eligible_balances, dtype=float)
        np.divide(1, eligible_balances, out=inverse_weights, where=mask)
        # normalize the weights
        total_weight = np.sum(inverse_weights)
        inverse_weights /= total_weight
        # compute the redistributed amount for each user
        redistributed_amounts = (inverse_weights * max_redistribution).astype(int)

        # because the previous operations rounded down the values, something is left
        difference = max_redistribution - np.sum(redistributed_amounts)
        # assign the remaining in the most equal way possible
        remaining = distribute(difference, num_users)
        redistributed_amounts[mask] += remaining

        actual_redistribution = max_redistribution

        masked_redistributed_amounts = redistributed_amounts[mask]

        min_red, max_red = numpy_minmax.minmax(masked_redistributed_amounts)

        percentiles = [25, 50, 75]
        indices = [int(np.ceil((p / 100) * num_users)) - 1 for p in percentiles]
        unique_indices = np.unique(indices)
        partitioned_data = np.partition(masked_redistributed_amounts, unique_indices)
        perc_25_redistribution, perc_50_redistribution, perc_75_redistribution = [partitioned_data[idx] for idx in indices]

        redistribution[number_of_file] = [actual_redistribution, max_red, min_red, 
                                          perc_25_redistribution, perc_50_redistribution, perc_75_redistribution]
        
        eligible_accounts.list += redistributed_amounts

    # it cannot happen that an account becomes non-eligible through redistribution by having a balance lower than the minimum
    filtered_indices = np.argwhere(eligible_accounts.list[mask] > redistribution_maximum).flatten()

    if len(filtered_indices) > 0:
        # map to original indices
        original_indices = np.flatnonzero(mask)[filtered_indices]

        for index in original_indices:
            address = eligible_accounts.reverse_dictionary[index]
            balance = eligible_accounts.remove(address)
            non_eligible_accounts[address] = balance

    # redistribution coming from the block (fees, block reward or total reward), not coming from extra fees
    block_redistribution = actual_redistribution
    if total_extra_fee > 0 and actual_redistribution > max_block_redistribution:
        block_redistribution = max_block_redistribution

    new_total_reward = total_reward - block_redistribution
    # ratio between previous total reward and redistributed total reward
    ratio = new_total_reward / total_reward

    return redistribution, eligible_accounts, non_eligible_accounts, ratio

# each block is processed sequentially (and the corresponding accounts are updated)
# furthermore, in order to reduce the number of computations, in this phase, the redistribution is computed only for the accounts that are involved in transactions
# the redistribution to other accounts is performed afterwards
def process_blocks(eligible_accounts, non_eligible_accounts, redistribution, 
                   len_files, redistribution_minimum, redistribution_maximum, redistribution_percentage, redistribution_type, redistribution_amount, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage):
    global file_queue
    number_of_file = 0

    with tqdm(total=len_files, desc=f'Processing blocks') as pbar:

        while True:
            item = file_queue.get()
            if item is None:
                break  # End of files
            _, block = item

            eligible_accounts, non_eligible_accounts, total_extra_fee_percentage = perform_block_transactions(
                eligible_accounts, non_eligible_accounts, 
                redistribution_minimum, redistribution_maximum, block, extra_fee_amount, extra_fee_percentage)
            
            # total extra fee per block is equal to 
            # extra_fee_amount for each transaction multiplied by the number of transactions (minus the coinbase transaction) +
            # the amount sent multiplied by extra_fee_percentage
            total_extra_fee = extra_fee_amount * (len(block['Transactions']) - 1) + total_extra_fee_percentage

            redistribution, eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
                redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee,
                redistribution, eligible_accounts, non_eligible_accounts)
            
            coinbase_transaction = block['Transactions'][0]
            for i in range(len(coinbase_transaction['Outputs'])):
                output = coinbase_transaction['Outputs'][i]
                receiver = output['Receiver']
                exact_payment = output['Value'] * ratio
                payment = int(math.floor(exact_payment))

                if isinstance(receiver, list):
                    continue
                if isinstance(receiver, bytes):
                    receiver = receiver.decode('utf-8')

                if payment > 0:
                    eligible_accounts, non_eligible_accounts = perform_input_output(
                        receiver, payment, 1, 
                        eligible_accounts, non_eligible_accounts, 
                        redistribution_minimum, redistribution_maximum, 0, 0)

            number_of_file += 1
            pbar.update(1)

            file_queue.task_done()
    
    eligible_accounts.perform_addition()

    return eligible_accounts, non_eligible_accounts, redistribution

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

def redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, redistribution_percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage):
    global file_queue
    global lock

    folder = f'{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}'
    dir_results_folder = os.path.join(dir_results, 'normal', 'single_input', redistribution_type, redistribution_amount, folder)
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_{redistribution_percentage}.csv')
    path_redistribution = os.path.join(dir_results_folder, f'redistribution_{redistribution_percentage}.csv')

    if not os.path.exists(path_accounts) or not os.path.exists(path_redistribution):

        conn = create_connection()

        files = os.listdir(dir_sorted_blocks)
        files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
        files.sort()
        # delete the first 3 files (000, 001, 002) because the utxo has already them
        files = files[3:]

        len_files = len(files)

        def retrieve_eligible_accounts_object(conn, redistribution_minimum, redistribution_maximum):
            print('Retrieving eligible accounts from database...')
            eligible_accounts = retrieve_eligible_accounts(conn, redistribution_minimum, redistribution_maximum)
            len_eligible_accounts = len(eligible_accounts)
            eligible_addresses = {}
            eligible_balances = np.array([0] * len_eligible_accounts)
            # because removal from eligible_balances would be expensive (indeed, each value is referenced by one value in eligible_addresses)
            # we use a list to keep track of invalid positions (they can be used if new addresses are added)
            for index, (key, value) in enumerate(eligible_accounts):
                eligible_balances[index] = int(value)
                eligible_addresses[key] = index

            eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)
            return eligible_accounts
        
        eligible_accounts = retrieve_eligible_accounts_object(conn, redistribution_minimum, redistribution_maximum)

        print('Retrieving non eligible accounts from database...')
        non_eligible_accounts = {key: int(value) for key, value in retrieve_non_eligible_accounts(conn, redistribution_minimum, redistribution_maximum)}

        # pre-allocate a fixed size redistribution list
        redistribution = [0] * len_files

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, eligible_accounts, non_eligible_accounts, redistribution, len_files, redistribution_minimum, redistribution_maximum, redistribution_percentage, redistribution_type, redistribution_amount, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)]

            # wait for all readers to complete
            wait(futures_readers)
            # there is only one processor (the main thread)
            file_queue.put(None)

            # wait for the processor to complete
            for future in as_completed(futures_processors):
                eligible_accounts, non_eligible_accounts, redistribution = future.result()

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
            csv_out.writerow(['balance'])

            eligible_addresses = eligible_accounts.dictionary
            eligible_balances = eligible_accounts.list

            # save the accounts which have already received redistribution
            with tqdm(total=len(eligible_addresses), desc=f'Writing eligible accounts') as pbar:
                for address, index in eligible_addresses.items():
                    balance = eligible_balances[index]
                    csv_out.writerow([balance])

                    pbar.update(1)

            # save accounts that are not eligible
            with tqdm(total=len(non_eligible_accounts), desc=f'Writing non-eligible accounts') as pbar:
                for key, value in non_eligible_accounts.items():
                    csv_out.writerow([value])

                    pbar.update(1)

    # plot_balance_histogram(path_accounts)
    # if redistribution_type == 'equal':
    #     plot_linear_redistribution_histogram(path_redistribution)
    # elif redistribution_type == 'weight_based':
    #     plot_weight_based_metrics(path_redistribution)