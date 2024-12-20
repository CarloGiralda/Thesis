import numpy as np
from only_redistribution_space.utils import DoubleDictionaryDoubleList, DictionaryDoubleList
from only_redistribution_space.only_redistribution_paradise import *

def test_perform_input_output():
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_minimum = 10
    redistribution_maximum = 500
    extra_fee_per_output = 0
    extra_fee_percentage_per_output = 0

    address = 'bc12'
    payment = 10
    input_output = 0

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.first_list[0] == 90:
        print('Test passed: simulate an input')
    else:
        print('Test failed: simulate an input')
        return

    address = 'bc12'
    payment = 10
    input_output = 1

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)

    if eligible_accounts.first_list[0] == 100:
        print('Test passed: simulate an output')
    else:
        print('Test failed: simulate an output')
        return

    address = 'bc18'
    payment = 10
    input_output = 1

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.contains_key(address) and eligible_accounts.get_balance(address) == 10:
        print('Test passed: add address to eligible')
    else:
        print('Test failed: add address to eligible')
        return

    address = 'bc18'
    payment = 5
    input_output = 0

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if not eligible_accounts.contains_key(address) and non_eligible_accounts.contains_key(address) and non_eligible_accounts.get_balance(address) == 5:
        print('Test passed: address from eligible to non eligible')
    else:
        print('Test passed: address from eligible to non eligible')
        return

    address = 'bc15'
    payment = 100
    input_output = 0

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.contains_key(address) and not non_eligible_accounts.contains_key(address) and eligible_accounts.get_balance(address) == 401:
        print('Test passed: address from non eligible to eligible')
    else:
        print('Test passed: address from non eligible to eligible')
        return

    address = 'bc19'
    payment = 1010
    input_output = 1

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if non_eligible_accounts.contains_key(address) and non_eligible_accounts.get_balance(address) == 1010 and non_eligible_accounts.dictionary[address] == 0:
        print('Test passed: replace unused index')
    else:
        print('Test passed: replace unused index')
        return

    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()

    print('Final:')
    expected_dict = {'bc12': 0, 'bc13': 1, 'bc14': 2, 'bc15': 3}
    expected_first_list = np.array([100, 25, 320, 401])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_first_list}')
    expected_dict = {'bc16': 1, 'bc17': 2, 'bc18': 3, 'bc19': 0}
    expected_first_list = np.array([1010, 750, 1000, 5])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_first_list}')

def test_perform_block_transactions():
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_minimum = 10
    redistribution_maximum = 500
    extra_fee_per_output = 0
    extra_fee_percentage_per_output = 0.0

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 316227592, 'Fees': 3727592, 
             'Transactions': [
                 {'Inputs': [{'Sender': None, 'Value': 0}], 
                  'Outputs': [{'Receiver': 'bc12', 'Value': 700}, {'Receiver': 'INVALID', 'Value': 0}, {'Receiver': 'INVALID', 'Value': 0}]}, 
                  {'Inputs': [{'Sender': 'bc15', 'Value': 150}], 
                   'Outputs': [{'Receiver': 'bc17', 'Value': 150}]}, 
                    {'Inputs': [{'Sender': 'bc17', 'Value': 1000}], 
                     'Outputs': [{'Receiver': 'bc13', 'Value': 500}, {'Receiver': 'bc16', 'Value': 500}]
                     }
                     ]}
    
    # Test for simple block execution
    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
                               redistribution_minimum, redistribution_maximum, block, extra_fee_per_output, extra_fee_percentage_per_output)

    condition = total_extra_fee_percentage == 0.0 and eligible_accounts.get_balance('bc17') == 150
    if condition:
        print('Test passed: block execution')
    else:
        print('Test failed: block execution')
        return

    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()

    print('Post:')
    expected_dict = {'bc12': 0, 'bc14': 2, 'bc15': 3, 'bc17': 4}
    expected_first_list = np.array([100, 25, 320, 351, 150])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_first_list}')
    expected_dict = {'bc16': 1, 'bc13': 0}
    expected_first_list = np.array([525, 1250, 1150])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_first_list}')

    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    extra_fee_per_output = 10

    # Test for block execution with an extra fee that is fixed for the whole transaction
    # an equal fraction of the fee is subtracted to each output
    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
                               redistribution_minimum, redistribution_maximum, block, extra_fee_per_output, extra_fee_percentage_per_output)
    
    condition = eligible_accounts.get_balance('bc17') == 140 and non_eligible_accounts.get_balance('bc13') == 520
    if condition:
        print('Test passed: block execution with fixed extra fee')
    else:
        print('Test failed: block execution with fixed extra fee')
        return

    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()

    print('Post:')
    expected_dict = {'bc12': 0, 'bc14': 2, 'bc15': 3, 'bc17': 4}
    expected_first_list = np.array([100, 25, 320, 351, 140])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_first_list}')
    expected_dict = {'bc16': 1, 'bc13': 0}
    expected_first_list = np.array([520, 1245, 1140])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_first_list}')

    # Test for block execution with an extra fee that is a percentage of each output
    # the same percentage is subtracted to each output
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    extra_fee_per_output = 0
    extra_fee_percentage_per_output = 0.1

    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
                               redistribution_minimum, redistribution_maximum, block, extra_fee_per_output, extra_fee_percentage_per_output)
    
    condition = eligible_accounts.get_balance('bc17') == 135 and eligible_accounts.get_balance('bc13') == 475
    if condition:
        print('Test passed: block execution with percentage extra fee')
    else:
        print('Test failed: block execution with percentage extra fee')
        return

    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()

    print('Post:')
    expected_dict = {'bc12': 0, 'bc13': 1, 'bc14': 2, 'bc15': 3, 'bc17': 4}
    expected_first_list = np.array([100, 475, 320, 351, 135])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_first_list}')
    expected_dict = {'bc16': 1}
    expected_first_list = np.array([501, 1200, 1135])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_first_list}')

def test_perform_redistribution():
    # Test for equal redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra_fee = 0
    circular_queue_index = 0

    redistribution_type = 'equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and eligible_accounts.get_redistribution('bc12') == 202 and eligible_accounts.get_redistribution('bc13') == 202 and non_eligible_accounts.get_redistribution('bc14') == 203 and block_redistribution == 600 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: simple equal redistribution')
    else:
        print('Test failed: simple equal redistribution')
        return
    
    # Test for equal redistribution but with less satoshis than users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra_fee = 0

    redistribution_type = 'equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 4, 'Fees': 2
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 3 and len(non_eligible_accounts.dictionary) == 3 and eligible_accounts.get_redistribution('bc12') == 0 and eligible_accounts.get_redistribution('bc13') == 0 and eligible_accounts.get_redistribution('bc14') == 0 and block_redistribution == 0 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: equal redistribution')
    else:
        print('Test failed: equal redistribution')
        return
    
    # Test for equal redistribution with percentage on users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 0.5
    total_extra_fee = 0

    redistribution_type = 'equal'
    redistribution_amount = 'block_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and non_eligible_accounts.get_redistribution('bc13') == 1902 and block_redistribution == 1900 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: equal redistribution with percentage of users')
    else:
        print('Test failed: equal redistribution with percentage of users')
        return

    # Test for equal redistribution with percentage on users and an extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 0.75
    total_extra_fee = 15

    redistribution_type = 'equal'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 1 and len(non_eligible_accounts.dictionary) == 5 and non_eligible_accounts.get_redistribution('bc12') == 1259 and non_eligible_accounts.get_redistribution('bc13') == 1259 and block_redistribution == 2500 and remaining_from_extra_fee == 1
    if condition:
        print('Test passed: equal redistribution with percentage of users and an extra fee')
    else:
        print('Test failed: equal redistribution with percentage of users and an extra fee')
        return
    
    # Test for almost_equal redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra_fee = 0

    redistribution_type = 'almost_equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and eligible_accounts.get_redistribution('bc12') == 202 and eligible_accounts.get_redistribution('bc13') == 202 and non_eligible_accounts.get_redistribution('bc14') == 203 and block_redistribution == 600 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: simple almost equal redistribution')
    else:
        print('Test failed: simple almost equal redistribution')
        return
    
    # Test for almost_equal redistribution but with less satoshis than users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra_fee = 0

    redistribution_type = 'almost_equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 4, 'Fees': 2
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 3 and len(non_eligible_accounts.dictionary) == 3 and eligible_accounts.get_redistribution('bc12') == 1 and eligible_accounts.get_redistribution('bc13') == 0 and eligible_accounts.get_redistribution('bc14') == 0 and block_redistribution == 1 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: almost equal redistribution')
    else:
        print('Test failed: almost equal redistribution')
        return
    
    # Test for almost_equal redistribution with percentage on users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 0.5
    total_extra_fee = 0

    redistribution_type = 'almost_equal'
    redistribution_amount = 'block_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and non_eligible_accounts.get_redistribution('bc13') == 1902 and block_redistribution == 1900 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: almost equal redistribution with percentage of users')
    else:
        print('Test failed: almost equal redistribution with percentage of users')
        return

    # Test for almost_equal redistribution with percentage on users and an extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([2, 2, 3])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 0.75
    total_extra_fee = 15

    redistribution_type = 'almost_equal'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 1 and len(non_eligible_accounts.dictionary) == 5 and non_eligible_accounts.get_redistribution('bc12') == 1260 and non_eligible_accounts.get_redistribution('bc13') == 1259 and block_redistribution == 2500 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: almost equal redistribution with percentage of users and an extra fee')
    else:
        print('Test failed: almost equal redistribution with percentage of users and an extra fee')
        return
    
    # Test for no_minimum_equal redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1}
    eligible_balances = np.array([100.0, 25.0])
    eligible_redistributions = np.array([0.0, 0.0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501.0, 750.0, 1000.0])
    non_eligible_redistributions = np.array([100.0, 100.0, 100.0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra_fee = 0

    redistribution_type = 'no_minimum_equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1202
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 3 and eligible_accounts.get_redistribution('bc12') == 300.5 and eligible_accounts.get_redistribution('bc13') == 300.5 and block_redistribution == 601 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: equal redistribution with no minimum limit')
    else:
        print('Test failed: equal redistribution with no minimum limit')
        return
    
    # Test for circular_queue_equal redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0

    redistribution_type = 'circular_queue_equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 4
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    first_condition = len(eligible_accounts.dictionary) == 3 and len(non_eligible_accounts.dictionary) == 3 and eligible_accounts.get_redistribution('bc12') == 1 and eligible_accounts.get_redistribution('bc13') == 1 and eligible_accounts.get_redistribution('bc14') == 0 and block_redistribution == 2 and remaining_from_extra_fee == 0 and circular_queue_index == 2
    
    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)
    
    second_condition = len(eligible_accounts.dictionary) == 3 and len(non_eligible_accounts.dictionary) == 3 and eligible_accounts.get_redistribution('bc12') == 2 and eligible_accounts.get_redistribution('bc13') == 1 and eligible_accounts.get_redistribution('bc14') == 1 and block_redistribution == 2 and remaining_from_extra_fee == 0 and circular_queue_index == 1
    
    if first_condition and second_condition:
        print('Test passed: circular queue equal redistribution')
    else:
        print('Test failed: circular queue equal redistribution')
        return

    # Test for simple weight based redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 1000
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra_fee = 0

    redistribution_type = 'weight_based'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and eligible_accounts.get_redistribution('bc12') == 471 and non_eligible_accounts.get_redistribution('bc13') == 1882 and eligible_accounts.get_redistribution('bc14') == 147 and block_redistribution == 2500 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: simple weight based redistribution')
    else:
        print('Test failed: simple weight based redistribution')
        return

    # Test for weight based redistribution with percentage on users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 1000
    redistribution_percentage = 0.9
    redistribution_user_percentage = 0.75
    total_extra_fee = 0

    redistribution_type = 'weight_based'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and eligible_accounts.get_redistribution('bc12') == 900 and non_eligible_accounts.get_redistribution('bc13') == 3600 and eligible_accounts.get_redistribution('bc14') == 0 and block_redistribution == 4500 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: weight based redistribution with percentage on users')
    else:
        print('Test failed: weight based redistribution with percentage on users')
        return

    # Test for weight based redistribution with percentage on users and an extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 1000
    redistribution_percentage = 0.9
    redistribution_user_percentage = 0.75
    total_extra_fee = 503

    redistribution_type = 'weight_based'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, block_redistribution, remaining_from_extra_fee, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra_fee, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 1 and len(non_eligible_accounts.dictionary) == 5 and non_eligible_accounts.get_redistribution('bc12') == 1001 and non_eligible_accounts.get_redistribution('bc13') == 4002 and eligible_accounts.get_redistribution('bc14') == 0 and block_redistribution == 4500 and remaining_from_extra_fee == 0
    if condition:
        print('Test passed: weight based redistribution with percentage on users and an extra fee')
    else:
        print('Test failed: weight based redistribution with percentage on users and an extra fee')
        return

def test_perform_coinbase_transaction():
    # Test for coinbase transaction with fees
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200, 
             'Transactions': [
                 {'Inputs': [{'Sender': None, 'Value': 0}], 
                  'Outputs': [{'Receiver': 'bc12', 'Value': 4500}, {'Receiver': 'bc13', 'Value': 500}, {'Receiver': 'INVALID', 'Value': 0}]}, 
            ]}
    
    block_redistribution = 1000

    redistribution_maximum = 1000
    redistribution_minimum = 10
    redistribution_amount = 'fees'
    remaining_from_extra_fee = 0

    eligible_accounts, non_eligible_accounts = perform_coinbase_transaction(
        block, block_redistribution, redistribution_minimum, redistribution_maximum, redistribution_amount, remaining_from_extra_fee,
        eligible_accounts, non_eligible_accounts)
    
    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()
    
    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and non_eligible_accounts.get_redistribution('bc12') == 180 and eligible_accounts.get_redistribution('bc13') == 20 and non_eligible_accounts.get_balance('bc12') == 3700 and eligible_accounts.get_balance('bc13') == 425
    if condition:
        print('Test passed: coinbase transaction with fees')
    else:
        print('Test failed: coinbase transaction with fees')
        return
    
    # Test for coinbase transaction with block reward
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200, 
             'Transactions': [
                 {'Inputs': [{'Sender': None, 'Value': 0}], 
                  'Outputs': [{'Receiver': 'bc12', 'Value': 4500}, {'Receiver': 'bc13', 'Value': 500}, {'Receiver': 'INVALID', 'Value': 0}]}, 
            ]}
    
    block_redistribution = 3800

    redistribution_maximum = 1000
    redistribution_minimum = 10
    redistribution_amount = 'block_reward'

    eligible_accounts, non_eligible_accounts = perform_coinbase_transaction(
        block, block_redistribution, redistribution_minimum, redistribution_maximum, redistribution_amount, remaining_from_extra_fee,
        eligible_accounts, non_eligible_accounts)
    
    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()
    
    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and non_eligible_accounts.get_redistribution('bc12') == 0 and eligible_accounts.get_redistribution('bc13') == 0 and non_eligible_accounts.get_balance('bc12') == 1180 and eligible_accounts.get_balance('bc13') == 145
    if condition:
        print('Test passed: coinbase transaction with block reward')
    else:
        print('Test failed: coinbase transaction with block reward')
        return
    
    # Test for coinbase transaction with fees and an additional amount obtained from a not redistributed extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([100, 100, 100])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200, 
             'Transactions': [
                 {'Inputs': [{'Sender': None, 'Value': 0}], 
                  'Outputs': [{'Receiver': 'bc12', 'Value': 4500}, {'Receiver': 'bc13', 'Value': 500}, {'Receiver': 'INVALID', 'Value': 0}]}, 
            ]}
    
    block_redistribution = 1000

    redistribution_maximum = 1000
    redistribution_minimum = 10
    redistribution_amount = 'fees'
    remaining_from_extra_fee = 100

    eligible_accounts, non_eligible_accounts = perform_coinbase_transaction(
        block, block_redistribution, redistribution_minimum, redistribution_maximum, redistribution_amount, remaining_from_extra_fee,
        eligible_accounts, non_eligible_accounts)
    
    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()
    
    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and non_eligible_accounts.get_redistribution('bc12') == 270 and eligible_accounts.get_redistribution('bc13') == 30 and non_eligible_accounts.get_balance('bc12') == 3790 and eligible_accounts.get_balance('bc13') == 435
    if condition:
        print('Test passed: coinbase transaction with fees and an additional amount obtained from a not redistributed extra fee')
    else:
        print('Test failed: coinbase transaction with fees and an additional amount obtained from a not redistributed extra fee')
        return