import os
import csv
import queue
import time
import math
import numpy as np
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm
from only_redistribution_space.utils import DictionaryDoubleList, DoubleDictionaryDoubleList, plot_balance_line
from redistribution_space.utils import get_block, extract_height_from_name, distribute
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

        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output
        
        # if the address is still eligible, then update the balance, eligible_subsequent_addresses and eligible_accounts_near_non_eligibility (if its balance is higher than the threshold)
        if redistribution_minimum <= updated_balance <= redistribution_maximum:
            eligible_accounts.update_balance(address, updated_balance)
        else:
            # remove the element from eligible_accounts, but store the redistribution up until now
            _, redistribution = eligible_accounts.remove(address)
            # redistribution is assigned to non_eligible_accounts
            non_eligible_accounts.add(address, updated_balance, redistribution)

    elif non_eligible_accounts.contains_key(address):
        balance = non_eligible_accounts.get_balance(address)

        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output

        if redistribution_minimum <= updated_balance <= redistribution_maximum:
            # remove the element from non_eligible_accounts, but store the redistribution up until now
            _, redistribution = non_eligible_accounts.remove(address)
            # redistribution is assigned to eligible_accounts
            eligible_accounts.add(address, updated_balance, redistribution)
        # otherwise, simply update the balance
        else:
            non_eligible_accounts.update_balance(address, updated_balance)

    # it may happen that some addresses are not in neither eligible_accounts nor non_eligible_accounts 
    # they have not been used up until now or they have been used but consumed all their balance in the past
    else:
        balance = 0
        redistribution = 0
        
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output

        if redistribution_minimum <= updated_balance <= redistribution_maximum:
            eligible_accounts.add(address, updated_balance, redistribution)
        else:
            non_eligible_accounts.add(address, updated_balance, redistribution)

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

            if sender == 'INVALID' or sender == 'UNKNOWN':
                continue

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
                extra_fee_percentage_per_output = int(math.floor(extra_fee_percentage * payment))
                total_extra_fee_percentage += extra_fee_percentage_per_output

            if receiver == 'INVALID' or receiver == 'UNKNOWN':
                continue

            eligible_accounts, non_eligible_accounts = perform_input_output(
                receiver, payment, 1, 
                eligible_accounts, non_eligible_accounts, 
                redistribution_minimum, redistribution_maximum, extra_fee_per_output[output_index], extra_fee_percentage_per_output)
        
    return eligible_accounts, non_eligible_accounts, total_extra_fee_percentage

def perform_coinbase_transaction(block, block_redistribution, redistribution_minimum, redistribution_maximum, redistribution_amount, remaining_from_extra_fee,
                                 eligible_accounts, non_eligible_accounts):
    # fees payed by users
    fees = block['Fees']
    # total reward = block reward + fees
    total_reward = block['Reward']
    # block reward
    block_reward = total_reward - fees

    # block_redistribution is the amount of the block that has been redistributed (not including the extra fees)
    new_total_reward = total_reward - block_redistribution
    # ratio between previous total reward and redistributed total reward
    ratio = new_total_reward / total_reward
    
    coinbase_transaction = block['Transactions'][0]
    for i in range(len(coinbase_transaction['Outputs'])):
        output = coinbase_transaction['Outputs'][i]

        receiver = output['Receiver']
        value = output['Value']

        if receiver == 'INVALID' or receiver == 'UNKNOWN':
            continue

        # ratio between the original reward of the user and the total reward of the block
        # percentage of total reward received by a user
        # different from ratio, that is the percentage of total_reward left after redistribution
        second_ratio = value / total_reward

        payment = int(math.floor(value * ratio))

        # if the redistribution has not redistributed all the amount, then give it to the miners (according to the share of the original block that they had)
        additional_payment = int(math.floor(remaining_from_extra_fee * second_ratio))
        payment += additional_payment

        if payment > 0:

            eligible_accounts, non_eligible_accounts = perform_input_output(
                receiver, payment, 1, 
                eligible_accounts, non_eligible_accounts, 
                redistribution_minimum, redistribution_maximum, 0, 0)

            if redistribution_amount == 'fees':
                redistribution = (fees - block_redistribution) * second_ratio + additional_payment
            elif redistribution_amount == 'block_reward':
                redistribution = (block_reward - block_redistribution) * second_ratio + additional_payment
            else:
                redistribution = (total_reward - block_redistribution) * second_ratio + additional_payment

            redistribution = int(math.floor(redistribution))
                                    
            if eligible_accounts.contains_key(receiver):
                previous_redistribution = eligible_accounts.get_redistribution(receiver)
                total_redistribution = previous_redistribution + redistribution
                eligible_accounts.update_redistribution(receiver, total_redistribution)

            elif non_eligible_accounts.contains_key(receiver):
                previous_redistribution = non_eligible_accounts.get_redistribution(receiver)
                total_redistribution = previous_redistribution + redistribution
                non_eligible_accounts.update_redistribution(receiver, total_redistribution)

    return eligible_accounts, non_eligible_accounts
            
def perform_redistribution(redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
                           eligible_accounts, non_eligible_accounts, circular_queue_index):
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
    non_eligible_accounts.perform_addition()

    eligible_balances = eligible_accounts.first_list
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

    indices = np.flatnonzero(mask)

    if redistribution_type == 'no_redistribution':

        max_block_redistribution = 0
        redistribution_extra_fee = 0
    
    elif redistribution_type == 'equal':

        redistribution_per_user = max_redistribution // num_users if num_users > 0 else 0
        actual_redistribution = redistribution_per_user * num_users

        if redistribution_per_user > 0:
            if redistribution_user_percentage < 1.0:
                eligible_accounts.first_list[indices] += redistribution_per_user
                eligible_accounts.second_list[indices] += redistribution_per_user
            else:
                eligible_accounts.first_list += redistribution_per_user
                eligible_accounts.second_list += redistribution_per_user

        # overwrite the max_block_redistribution (that is going to be returned) if the actual redistribution is less than the supposed one
        if actual_redistribution < max_block_redistribution:
            max_block_redistribution = actual_redistribution

        redistribution_extra_fee = actual_redistribution - max_block_redistribution if actual_redistribution > max_block_redistribution else 0

    elif redistribution_type == 'no_minimum_equal':

        # in this case there is no limitation such as indivisible unit (as satoshi)
        redistribution_per_user = max_redistribution / num_users if num_users > 0 else 0

        if redistribution_per_user > 0:
            if redistribution_user_percentage < 1.0:
                eligible_accounts.first_list[indices] += redistribution_per_user
                eligible_accounts.second_list[indices] += redistribution_per_user
            else:
                eligible_accounts.first_list += redistribution_per_user
                eligible_accounts.second_list += redistribution_per_user

        redistribution_extra_fee = total_extra_fee

    elif redistribution_type == 'almost_equal':

        redistribution_per_user = max_redistribution // num_users if num_users > 0 else 0
        actual_redistribution = redistribution_per_user * num_users

        if redistribution_per_user > 0:
            if redistribution_user_percentage < 1.0:
                eligible_accounts.first_list[indices] += redistribution_per_user
                eligible_accounts.second_list[indices] += redistribution_per_user
            else:
                eligible_accounts.first_list += redistribution_per_user
                eligible_accounts.second_list += redistribution_per_user

        remaining = max_redistribution - actual_redistribution
        eligible_accounts.first_list[indices[:remaining]] += 1
        eligible_accounts.second_list[indices[:remaining]] += 1

        redistribution_extra_fee = total_extra_fee

    elif redistribution_type == 'circular_queue_equal':

        redistribution_per_user = max_redistribution // num_users if num_users > 0 else 0
        actual_redistribution = redistribution_per_user * num_users

        if redistribution_per_user > 0:
            if redistribution_user_percentage < 1.0:
                eligible_accounts.first_list[indices] += redistribution_per_user
                eligible_accounts.second_list[indices] += redistribution_per_user
            else:
                eligible_accounts.first_list += redistribution_per_user
                eligible_accounts.second_list += redistribution_per_user

        remaining = max_redistribution - actual_redistribution
        new_circular_queue_index = circular_queue_index + remaining

        # if the index exceeds the length of the list, then redistribute from the address corresponding to the index until the list is finished
        # then redistribute the remaining part to the first addresses in the list
        if new_circular_queue_index > eligible_accounts.len_first_list:
            eligible_accounts.first_list[indices[circular_queue_index:]] += 1
            eligible_accounts.second_list[indices[circular_queue_index:]] += 1

            circular_queue_index = new_circular_queue_index % eligible_accounts.len_first_list
            
            eligible_accounts.first_list[indices[:circular_queue_index]] += 1
            eligible_accounts.second_list[indices[:circular_queue_index]] += 1

        else:
            eligible_accounts.first_list[indices[circular_queue_index:new_circular_queue_index]] += 1
            eligible_accounts.second_list[indices[circular_queue_index:new_circular_queue_index]] += 1

            circular_queue_index = new_circular_queue_index

        redistribution_extra_fee = total_extra_fee

    elif redistribution_type == 'weight_based':

        # compute the inverse of the eligible balances
        # by doing this, the lowest balance will have the highest weight, while the highest balance will have the lowest weight
        inverse_weights = np.zeros_like(eligible_balances, dtype=float)
        inverse_weights[indices] = 1 / eligible_balances[indices]
        # normalize the weights
        total_weight = np.sum(inverse_weights)
        inverse_weights /= total_weight
        # compute the redistributed amount for each user
        redistributed_amounts = (inverse_weights * max_redistribution).astype(int)

        # because the previous operations rounded down the values, something is left
        difference = max_redistribution - np.sum(redistributed_amounts)
        if difference > 0:
            # assign the remaining in the most equal way possible
            remaining = distribute(difference, num_users)
            redistributed_amounts[indices] += remaining
        
        eligible_accounts.first_list += redistributed_amounts
        eligible_accounts.second_list += redistributed_amounts

        redistribution_extra_fee = total_extra_fee

    # it cannot happen that an account becomes non-eligible through redistribution by having a balance lower than the minimum
    filtered_indices = np.argwhere(eligible_accounts.first_list[indices] > redistribution_maximum).flatten()

    if len(filtered_indices) > 0:
        # map to original indices
        original_indices = indices[filtered_indices]

        for index in original_indices:
            address = eligible_accounts.reverse_dictionary[index]
            balance, redistribution = eligible_accounts.remove(address)
            non_eligible_accounts.add(address, balance, redistribution)

    remaining_from_extra_fee = total_extra_fee - redistribution_extra_fee

    return eligible_accounts, non_eligible_accounts, max_block_redistribution, remaining_from_extra_fee, circular_queue_index

# each block is processed sequentially (and the corresponding accounts are updated)
# furthermore, in order to reduce the number of computations, in this phase, the redistribution is computed only for the accounts that are involved in transactions
# the redistribution to other accounts is performed afterwards
def process_blocks(eligible_accounts, non_eligible_accounts, 
                   len_files, redistribution_minimum, redistribution_maximum, redistribution_percentage, redistribution_type, redistribution_amount, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage):
    global file_queue

    circular_queue_index = 0

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
            total_extra_fee = int(extra_fee_amount * (len(block['Transactions']) - 1) + total_extra_fee_percentage)

            eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
                redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee,
                eligible_accounts, non_eligible_accounts, circular_queue_index)
            
            eligible_accounts, non_eligible_accounts = perform_coinbase_transaction(
                block, block_redistribution, redistribution_minimum, redistribution_maximum, redistribution_amount, remaining_from_extra_fee,
                eligible_accounts, non_eligible_accounts)
                
            pbar.update(1)

            file_queue.task_done()

    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()

    return eligible_accounts, non_eligible_accounts

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

def only_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, redistribution_percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage):
    global file_queue
    global lock

    folder = f'{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}'
    dir_results_folder = os.path.join(dir_results, 'only_redistribution', 'single_input', redistribution_type, redistribution_amount, folder)
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_{redistribution_percentage}.csv')

    if not os.path.exists(path_accounts):

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
            redistribution_eligible_balances = np.array([0] * len_eligible_accounts)
            # because removal from eligible_balances would be expensive (indeed, each value is referenced by one value in eligible_addresses)
            # we use a list to keep track of invalid positions (they can be used if new addresses are added)
            for index, (key, value) in enumerate(eligible_accounts):
                eligible_balances[index] = int(value)
                eligible_addresses[key] = index

            if redistribution_type == 'no_minimum_equal':
                eligible_balances = eligible_balances.astype(float)
                redistribution_eligible_balances = redistribution_eligible_balances.astype(float)

            eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, redistribution_eligible_balances)
            return eligible_accounts
        
        eligible_accounts = retrieve_eligible_accounts_object(conn, redistribution_minimum, redistribution_maximum)

        def retrieve_non_eligible_accounts_object(conn, redistribution_minimum, redistribution_maximum):
            print('Retrieving non eligible accounts from database...')
            non_eligible_accounts = retrieve_non_eligible_accounts(conn, redistribution_minimum, redistribution_maximum)
            len_non_eligible_accounts = len(non_eligible_accounts)
            non_eligible_addresses = {}
            non_eligible_balances = np.array([0] * len_non_eligible_accounts)
            redistribution_non_eligible_balances = np.array([0] * len_non_eligible_accounts)
            # because removal from eligible_balances would be expensive (indeed, each value is referenced by one value in eligible_addresses)
            # we use a list to keep track of invalid positions (they can be used if new addresses are added)
            for index, (key, value) in enumerate(non_eligible_accounts):
                non_eligible_balances[index] = int(value)
                non_eligible_addresses[key] = index

            if redistribution_type == 'no_minimum_equal':
                non_eligible_balances = non_eligible_balances.astype(float)
                redistribution_non_eligible_balances = redistribution_non_eligible_balances.astype(float)
        
            non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, redistribution_non_eligible_balances)
            return non_eligible_accounts

        non_eligible_accounts = retrieve_non_eligible_accounts_object(conn, redistribution_minimum, redistribution_maximum)

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, eligible_accounts, non_eligible_accounts, len_files, redistribution_minimum, redistribution_maximum, redistribution_percentage, redistribution_type, redistribution_amount, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)]

            # wait for all readers to complete
            wait(futures_readers)
            # there is only one processor (the main thread)
            file_queue.put(None)

            # wait for the processor to complete
            for future in as_completed(futures_processors):
                eligible_accounts, non_eligible_accounts = future.result()

        with open(path_accounts, 'w+') as file:
            csv_out = csv.writer(file)
            csv_out.writerow(['redistribution', 'balance'])

            eligible_addresses = eligible_accounts.dictionary
            eligible_balances = eligible_accounts.first_list
            eligible_redistribution = eligible_accounts.second_list

            # save the accounts which have already received redistribution
            with tqdm(total=len(eligible_addresses), desc=f'Writing eligible accounts') as pbar:
                for address, index in eligible_addresses.items():
                    redistribution = eligible_redistribution[index]
                    balance = eligible_balances[index]
                    csv_out.writerow((redistribution, balance))

                    pbar.update(1)

            non_eligible_addresses = non_eligible_accounts.dictionary
            non_eligible_balances = non_eligible_accounts.first_list
            non_eligible_redistribution = non_eligible_accounts.second_list

            # save accounts that are not eligible
            with tqdm(total=len(non_eligible_addresses), desc=f'Writing non-eligible accounts') as pbar:
                for address, index in non_eligible_addresses.items():
                    redistribution = non_eligible_redistribution[index]
                    balance = non_eligible_balances[index]
                    csv_out.writerow((redistribution, balance))

                    pbar.update(1)

    # plot_balance_line(path_accounts)