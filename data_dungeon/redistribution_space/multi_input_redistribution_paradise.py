import os
import csv
import queue
import time
import math
import numpy_minmax
import numpy as np
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm
from redistribution_space.utils import get_block, extract_height_from_name, distribute, plot_balance_histogram, plot_linear_redistribution_histogram, plot_weight_based_metrics
from database.multi_input_accounts_database import create_connection, retrieve_eligible_accounts, retrieve_non_eligible_accounts

# number of readers for blocks
num_readers = 2

file_queue = queue.Queue(maxsize=10)
lock = 0
# index used to give a different number to all the new users discovered
user_index = 0

def perform_input_output(address, payment, input_output, 
                         address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                         minimum, maximum, len_eligible_balances, extra_fee_per_output, extra_fee_percentage_per_output):
    global user_index

    if address not in address_to_user:
        address_to_user[address] = user_index
        user_index += 1
    # addresses of transactions' outputs are not included in it
    user = address_to_user[address]

    if user in eligible_accounts:
        index = eligible_accounts[user]
        if index >= len_eligible_balances:
            balance = elements_to_add[index - len_eligible_balances]
        else:
            balance = eligible_balances[index]
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output
        
        # if the address is still eligible, then update the balance, eligible_subsequent_addresses and eligible_accounts_near_non_eligibility (if its balance is higher than the threshold)
        if minimum <= updated_balance <= maximum:
            if index >= len_eligible_balances:
                elements_to_add[index - len_eligible_balances] = updated_balance
            else:
                eligible_balances[index] = updated_balance
        # otherwise, remove the address from eligible_accounts and eligible_accounts_near_non_eligibility (if it was part of it)
        # add it to non_eligible_accounts
        else:
            del eligible_accounts[user]
            invalid_balances.append(index)
            non_eligible_accounts[user] = updated_balance

    elif user in non_eligible_accounts:
        balance = non_eligible_accounts[user]
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output
        # if the address is eligible now, then add it to eligible_accounts (and to eligible_accounts_near_non_eligibility, if it satisfies the threshold)
        # update eligible_subsequent_addresses
        # remove it from non_eligible_accounts
        if minimum <= updated_balance <= maximum:
            del non_eligible_accounts[user]
            # if there is at least one invalid balance, then use it
            if len(invalid_balances) > 0:
                free_index = invalid_balances[0]
                if free_index >= len_eligible_balances:
                    elements_to_add[free_index - len_eligible_balances] = updated_balance
                else:
                    eligible_balances[free_index] = updated_balance
                eligible_accounts[user] = free_index
                invalid_balances.pop(0)
            else:
                elements_to_add.append(updated_balance)
                eligible_accounts[user] = len_eligible_balances + len(elements_to_add) - 1

        # otherwise, simply update the balance
        else:
            non_eligible_accounts[user] = updated_balance

    # it may happen that some addresses are not in neither eligible_accounts nor non_eligible_accounts 
    # they have not been used up until now or they have been used but consumed all their balance in the past
    else:
        balance = 0
        # compute the updated balance and assign it to the corresponding address
        if input_output == 0:
            updated_balance = balance - payment
        else:
            updated_balance = balance + payment - extra_fee_per_output - extra_fee_percentage_per_output
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
                eligible_accounts[user] = free_index
                invalid_balances.pop(0)
            else:
                elements_to_add.append(updated_balance)
                eligible_accounts[user] = len_eligible_balances + len(elements_to_add) - 1

        # otherwise, simply update the balance
        else:
            non_eligible_accounts[user] = updated_balance

    return address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add

def perform_block_transactions(address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                               minimum, maximum, block, extra_fee_amount, extra_fee_percentage):
    # number of eligible accounts
    len_eligible_balances = len(eligible_balances)
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

            address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add = perform_input_output(
                sender, payment, 0, 
                address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                minimum, maximum, len_eligible_balances, 0)
            
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

            address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add = perform_input_output(
                receiver, payment, 1, 
                address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                minimum, maximum, len_eligible_balances, extra_fee_per_output[output_index], extra_fee_percentage_per_output)

    return address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, total_extra_fee_percentage
            
def perform_redistribution(type, amount, percentage, block, number_of_file, invalid_balances, total_extra_fee,
                            redistribution, eligible_balances, elements_to_add):
    # fees payed by users
    fees = block['Fees']
    # total reward = block reward + fees
    total_reward = block['Reward']
    # block reward
    block_reward = total_reward - fees

    if amount == 'fees':
        max_block_redistribution = math.floor(fees * percentage)
    elif amount == 'block_reward':
        max_block_redistribution = math.floor(block_reward * percentage)
    elif amount == 'total_reward':
        max_block_redistribution = math.floor(total_reward * percentage)
    else:
        max_block_redistribution = 0

    max_block_redistribution += total_extra_fee
    
    if len(elements_to_add) > 0:
        eligible_balances = np.append(eligible_balances, elements_to_add)
        elements_to_add.clear()
    num_users = len(eligible_balances)

    if type == 'no_redistribution':

        actual_block_redistribution = 0
    
    elif type == 'equal':

        redistribution_per_user = int(math.floor(max_block_redistribution / num_users)) if num_users > 0 else 0
        redistribution[number_of_file] = redistribution_per_user
        actual_block_redistribution = redistribution_per_user * num_users

        if redistribution_per_user > 0:
            eligible_balances += redistribution_per_user

    elif type == 'weight_based':
        len_eligible_balances = len(eligible_balances)

        # mask array to select only the valid balances (not all of them are valid)
        mask = np.ones(len_eligible_balances, dtype=bool)
        mask[invalid_balances] = False
        # compute the inverse of the eligible balances
        # by doing this, the lowest balance will have the highest weight, while the highest balance will have the lowest weight
        inverse_weights = np.where(mask, 1 / eligible_balances, 0)
        # normalize the weights
        total_weight = np.sum(inverse_weights)
        inverse_weights /= total_weight
        normalized_weights = inverse_weights
        # compute the redistributed amount for each user
        redistributed_amounts = (normalized_weights * max_block_redistribution).astype(int)

        # because the previous operations rounded down the values, something is left
        actual_block_redistribution = np.sum(redistributed_amounts)
        difference = max_block_redistribution - actual_block_redistribution
        # assign the remaining in the most equal way possible
        remaining = distribute(difference, len_eligible_balances)
        redistributed_amounts += remaining

        actual_block_redistribution = max_block_redistribution

        min_redistribution, max_redistribution = numpy_minmax.minmax(redistributed_amounts)
        
        partition = np.partition(redistributed_amounts, int(len(redistributed_amounts) * 75 / 100))
        percentiles = [25, 50, 75]
        percentile_values = []
        for p in percentiles:
            k = int(len(redistributed_amounts) * p / 100)
            percentile_values.append(partition[k])

        perc_25_redistribution, perc_50_redistribution, perc_75_redistribution = percentile_values

        redistribution[number_of_file] = [actual_block_redistribution, max_redistribution, min_redistribution, 
                                          perc_25_redistribution, perc_50_redistribution, perc_75_redistribution]
        
        eligible_balances += redistributed_amounts

    new_total_reward = total_reward - actual_block_redistribution
    # ratio between previous total reward and redistributed total reward
    ratio = new_total_reward / total_reward

    return redistribution, eligible_balances, elements_to_add, ratio

# each block is processed sequentially (and the corresponding accounts are updated)
# furthermore, in order to reduce the number of computations, in this phase, the redistribution is computed only for the accounts that are involved in transactions
# the redistribution to other accounts is performed afterwards
def process_blocks(address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, redistribution, 
                   len_files, minimum, maximum, percentage, type, amount, extra_fee_amount, extra_fee_percentage):
    global file_queue
    number_of_file = 0

    with tqdm(total=len_files, desc=f'Processing blocks') as pbar:
        elements_to_add = []

        while True:
            item = file_queue.get()
            if item is None:
                break  # End of files
            _, block = item

            address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, total_extra_fee_percentage = perform_block_transactions(
                address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                minimum, maximum, block, extra_fee_amount, extra_fee_percentage)

            # total extra fee per block is equal to 
            # extra_fee_amount for each transaction multiplied by the number of transactions (minus the coinbase transaction) +
            # the amount sent multiplied by extra_fee_percentage
            total_extra_fee = extra_fee_amount * (len(block['Transactions']) - 1) + total_extra_fee_percentage

            redistribution, eligible_balances, elements_to_add, ratio = perform_redistribution(
                type, amount, percentage, block, number_of_file, invalid_balances, total_extra_fee,
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

                address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add = perform_input_output(
                    receiver, payment, 1, 
                    address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, elements_to_add, 
                    minimum, maximum, len(eligible_balances), 0, 0)

            number_of_file += 1
            pbar.update(1)

            file_queue.task_done()

    if len(elements_to_add) > 0:
        print(elements_to_add)
        eligible_balances = np.append(eligible_balances, elements_to_add)
        elements_to_add.clear()

    return eligible_accounts, eligible_balances, non_eligible_accounts, redistribution

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

def multi_input_redistribution_paradise(dir_sorted_blocks, dir_results, type, percentage, amount, minimum, maximum, extra_fee_amount, extra_fee_percentage):
    global file_queue
    global lock
    global user_index

    folder = f'{percentage}_{minimum}_{maximum}_{extra_fee_amount}_{extra_fee_percentage}'
    dir_results_folder = f'{dir_results}/multi_input/{type}/{folder}'
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_{amount}.csv')
    path_redistribution = os.path.join(dir_results_folder, f'redistribution_{amount}.csv')

    if not os.path.exists(path_accounts) or not os.path.exists(path_redistribution):

        conn = create_connection()

        files = os.listdir(dir_sorted_blocks)
        files = [x for x in files if (x.endswith('.txt') and x.startswith('block_'))]
        files.sort()
        # delete the first 3 files (000, 001, 002) because the utxo has already them
        files = files[3:]

        len_files = len(files)

        # in this dictionary are saved all the addresses and their corresponding user identifier (both eligible and non-eligible)
        address_to_user = {}

        print('Retrieving eligible accounts from database...')
        # results are in a specific format ---> address, user, balance
        eligible_accounts = {}
        eligible_balances = []
        invalid_balances = []

        for address, user, balance in retrieve_eligible_accounts(conn, minimum, maximum):
            address_to_user[address] = user
            if user not in eligible_accounts:
                eligible_balances.append(balance)
                eligible_accounts[user] = len(eligible_balances) - 1
        # transform the eligible_balances into an array for faster computations
        eligible_balances = np.array(eligible_balances)

        print('Retrieving non eligible accounts from database...')
        non_eligible_accounts = {}
        for address, user, balance in retrieve_non_eligible_accounts(conn, minimum, maximum):
            address_to_user[address] = user
            if user not in non_eligible_accounts:
                non_eligible_accounts[user] = balance

        # set the user_index to the identifier of the user with the highest number of eligible_balances + 1
        user_index = max(eligible_accounts.keys()) + 1

        # pre-allocate a fixed size redistribution list
        redistribution = [0] * len_files

        # initialize the lock set to make the reader threads coordinate on the order of files
        prev_height = extract_height_from_name(files[0]) - 1
        lock = prev_height

        with ThreadPoolExecutor(max_workers=num_readers) as readers, ThreadPoolExecutor(max_workers=1) as processors:

            futures_readers = [readers.submit(read_files, files, i, dir_sorted_blocks) for i in range(num_readers)]

            futures_processors = [processors.submit(process_blocks, address_to_user, eligible_accounts, eligible_balances, invalid_balances, non_eligible_accounts, redistribution, len_files, minimum, maximum, percentage, type, amount, extra_fee_amount, extra_fee_percentage)]

            # wait for all readers to complete
            wait(futures_readers)
            # there is only one processor (the main thread)
            file_queue.put(None)

            # wait for the processor to complete
            for future in as_completed(futures_processors):
                eligible_accounts, eligible_balances, non_eligible_accounts, redistribution = future.result()

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
            csv_out.writerow(['user','balance'])

            # save the accounts which have already received redistribution
            with tqdm(total=len(eligible_accounts), desc=f'Writing eligible accounts') as pbar:
                for user, index in eligible_accounts.items():
                    balance = eligible_balances[index]
                    csv_out.writerow((user, balance))

                    pbar.update(1)

            # save accounts that are not eligible
            with tqdm(total=len(non_eligible_accounts), desc=f'Writing non-eligible accounts') as pbar:
                for user, balance in non_eligible_accounts.items():
                    csv_out.writerow((user, balance))

                    pbar.update(1)
    
        eligible_accounts = None
        eligible_balances = None
        invalid_balances = None
        non_eligible_accounts = None

    plot_balance_histogram(path_accounts)
    if type == 'equal':
        plot_linear_redistribution_histogram(path_redistribution)
    elif type == 'weight_based':
        plot_weight_based_metrics(path_redistribution)