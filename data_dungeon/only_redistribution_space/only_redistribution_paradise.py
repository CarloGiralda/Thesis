import os
import csv
import queue
import time
import math
import numpy as np
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from tqdm import tqdm
from redistribution_space.utils import get_block, extract_height_from_name, distribute, plot_balance_line
from database.accounts_database import create_connection, retrieve_eligible_accounts, retrieve_non_eligible_accounts

# number of readers for blocks
num_readers = 2

file_queue = queue.Queue(maxsize=10)
lock = 0

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

    def contains_address(self, address):
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

    def contains_address(self, address):
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



def perform_input_output(address, payment, input_output, 
                         eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output):

    if eligible_accounts.contains_address(address):
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

    elif non_eligible_accounts.contains_address(address):
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
            
def perform_redistribution(redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
                           eligible_accounts, non_eligible_accounts):
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

    max_block_redistribution += total_extra_fee
    
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

    if redistribution_type == 'no_redistribution':

        actual_block_redistribution = 0
    
    elif redistribution_type == 'equal':

        redistribution_per_user = int(math.floor(max_block_redistribution / num_users)) if num_users > 0 else 0
        actual_block_redistribution = redistribution_per_user * num_users

        if redistribution_per_user > 0:
            eligible_accounts.first_list[mask] += redistribution_per_user
            eligible_accounts.second_list[mask] += redistribution_per_user

    elif redistribution_type == 'weight_based':

        # compute the inverse of the eligible balances
        # by doing this, the lowest balance will have the highest weight, while the highest balance will have the lowest weight
        inverse_weights = np.zeros_like(eligible_balances, dtype=float)
        np.divide(1, eligible_balances, out=inverse_weights, where=mask)
        # normalize the weights
        total_weight = np.sum(inverse_weights)
        inverse_weights /= total_weight
        # compute the redistributed amount for each user
        redistributed_amounts = (inverse_weights * max_block_redistribution).astype(int)

        # because the previous operations rounded down the values, something is left
        actual_block_redistribution = np.sum(redistributed_amounts)
        difference = max_block_redistribution - actual_block_redistribution
        if difference > 0:
            # assign the remaining in the most equal way possible
            remaining = distribute(difference, num_users)
            redistributed_amounts[mask] += remaining

        actual_block_redistribution = max_block_redistribution
        
        eligible_accounts.first_list += redistributed_amounts
        eligible_accounts.second_list += redistributed_amounts

    # it cannot happen that an account becomes non-eligible through redistribution by having a balance lower than the minimum
    filtered_indices = np.argwhere(eligible_accounts.first_list[mask] > redistribution_maximum).flatten()

    if len(filtered_indices) > 0:
        # map to original indices
        original_indices = np.flatnonzero(mask)[filtered_indices]

        for index in original_indices:
            address = eligible_accounts.reverse_dictionary[index]
            balance, redistribution = eligible_accounts.remove(address)
            non_eligible_accounts.add(address, balance, redistribution)

    new_total_reward = total_reward - actual_block_redistribution
    # ratio between previous total reward and redistributed total reward
    ratio = new_total_reward / total_reward

    return eligible_accounts, non_eligible_accounts, ratio

# each block is processed sequentially (and the corresponding accounts are updated)
# furthermore, in order to reduce the number of computations, in this phase, the redistribution is computed only for the accounts that are involved in transactions
# the redistribution to other accounts is performed afterwards
def process_blocks(eligible_accounts, non_eligible_accounts, 
                   len_files, redistribution_minimum, redistribution_maximum, redistribution_percentage, redistribution_type, redistribution_amount, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage):
    global file_queue

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

            eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
                redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee,
                eligible_accounts, non_eligible_accounts)
            
            # fees payed by users
            fees = block['Fees']
            # total reward = block reward + fees
            total_reward = block['Reward']
            # block reward
            block_reward = total_reward - fees
            
            coinbase_transaction = block['Transactions'][0]
            for i in range(len(coinbase_transaction['Outputs'])):
                output = coinbase_transaction['Outputs'][i]

                receiver = output['Receiver']
                value = output['Value']

                exact_payment = value * ratio
                payment = int(math.floor(exact_payment))

                if payment > 0:

                    eligible_accounts, non_eligible_accounts = perform_input_output(
                        receiver, payment, 1, 
                        eligible_accounts, non_eligible_accounts, 
                        redistribution_minimum, redistribution_maximum, 0, 0)

                    # ratio between the original reward of the user and the total reward of the block
                    # percentage of total reward received by a user
                    # different from ratio, that is the percentage of total_reward left after redistribution
                    second_ratio = value / total_reward

                    if redistribution_amount == 'fees':
                        # exclude the amount received from the block reward
                        redistribution = payment - int(math.floor(block_reward * second_ratio))
                    elif redistribution_amount == 'block_reward':
                        # exclude the amount received from the fees
                        redistribution = payment - int(math.floor(fees * second_ratio))
                    else:
                        # include everything
                        redistribution = payment
                        
                    if eligible_accounts.contains_address(receiver):
                        previous_redistribution = eligible_accounts.get_redistribution(receiver)
                        total_redistribution = previous_redistribution + redistribution
                        eligible_accounts.update_redistribution(receiver, total_redistribution)

                    elif non_eligible_accounts.contains_address(receiver):
                        previous_redistribution = non_eligible_accounts.get_redistribution(receiver)
                        total_redistribution = previous_redistribution + redistribution
                        non_eligible_accounts.update_redistribution(receiver, total_redistribution)
                
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

    folder = f'{redistribution_percentage}_{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}'
    dir_results_folder = f'{dir_results}/only_redistribution/single_input/{redistribution_type}/{folder}'
    if not os.path.exists(dir_results_folder):
        os.makedirs(dir_results_folder)

    path_accounts = os.path.join(dir_results_folder, f'accounts_{redistribution_amount}.csv')

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
            csv_out.writerow(['redistribution'])

            eligible_addresses = eligible_accounts.dictionary
            eligible_redistribution = eligible_accounts.second_list

            # save the accounts which have already received redistribution
            with tqdm(total=len(eligible_addresses), desc=f'Writing eligible accounts') as pbar:
                for address, index in eligible_addresses.items():
                    redistribution = eligible_redistribution[index]
                    csv_out.writerow([redistribution])

                    pbar.update(1)

            non_eligible_addresses = non_eligible_accounts.dictionary
            non_eligible_redistribution = non_eligible_accounts.second_list

            # save accounts that are not eligible
            with tqdm(total=len(non_eligible_addresses), desc=f'Writing non-eligible accounts') as pbar:
                for address, index in non_eligible_addresses.items():
                    redistribution = non_eligible_redistribution[index]
                    csv_out.writerow([redistribution])

                    pbar.update(1)

    plot_balance_line(path_accounts)