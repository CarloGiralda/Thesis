import numpy as np
from only_redistribution_space.utils import DoubleDictionaryDoubleList, DictionaryDoubleList
from redistribution_space.adjusted_redistribution_paradise import *

def test_perform_input_output():
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 1, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_minimum = 10
    redistribution_maximum = 500
    extra_fee_per_output = 0
    extra_fee_percentage_per_output = 0

    address = 'bc12'
    payment = 10
    input_output = 0

    eligible_accounts, non_eligible_accounts, extra_from_redistribution = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.first_list[0] == 90 and extra_from_redistribution == 0:
        print('Test passed: simulate an input')
    else:
        print('Test failed: simulate an input')
        return

    address = 'bc12'
    payment = 10
    input_output = 1

    eligible_accounts, non_eligible_accounts, extra_from_redistribution = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)

    if eligible_accounts.first_list[0] == 100 and extra_from_redistribution == 0:
        print('Test passed: simulate an output')
    else:
        print('Test failed: simulate an output')
        return

    address = 'bc18'
    payment = 10
    input_output = 1

    eligible_accounts, non_eligible_accounts, extra_from_redistribution = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.contains_key(address) and eligible_accounts.get_balance(address) == 10:
        print('Test passed: add address to eligible')
    else:
        print('Test failed: add address to eligible')
        return
    
    address = 'bc15'
    payment = 100
    input_output = 0

    eligible_accounts, non_eligible_accounts, extra_from_redistribution = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.contains_key(address) and not non_eligible_accounts.contains_key(address) and eligible_accounts.get_balance(address) == 401 and extra_from_redistribution == 0:
        print('Test passed: address from non eligible to eligible')
    else:
        print('Test passed: address from non eligible to eligible')
        return

    address = 'bc13'
    payment = 20
    input_output = 0

    eligible_accounts, non_eligible_accounts, extra_from_redistribution = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if not eligible_accounts.contains_key(address) and non_eligible_accounts.contains_key(address) and non_eligible_accounts.get_balance(address) == 4 and extra_from_redistribution == 1:
        print('Test passed: address from eligible to non eligible')
    else:
        print('Test passed: address from eligible to non eligible')
        return

    address = 'bc19'
    payment = 300
    input_output = 1

    eligible_accounts, non_eligible_accounts, extra_from_redistribution = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if not non_eligible_accounts.contains_key(address) and eligible_accounts.get_balance(address) == 300 and eligible_accounts.dictionary[address] == 1:
        print('Test passed: replace unused index')
    else:
        print('Test passed: replace unused index')
        return

    eligible_accounts.perform_addition()

    print('Final:')
    expected_dict = {'bc12': 0, 'bc14': 2, 'bc18': 3, 'bc15': 4, 'bc19': 1}
    expected_list = np.array([100, 300, 320, 10, 401])
    expected_second_list = np.array([0, 0, 0, 0, 0])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}, {eligible_accounts.second_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}, {expected_second_list}')
    expected_dict = {'bc16': 1, 'bc17': 2, 'bc13': 0}
    expected_list = np.array([4, 750, 1000])
    expected_second_list = np.array([0, 0, 0])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}, {non_eligible_accounts.second_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_list}, {expected_second_list}')

def test_perform_block_transactions():
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
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
    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage, _ = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
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
    expected_list = np.array([100, 25, 320, 351, 150])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}')
    expected_dict = {'bc16': 1, 'bc13': 0}
    expected_list = np.array([525, 1250, 1150])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_list}')

    # Test for block execution with an extra fee that is fixed for the whole transaction
    # an equal fraction of the fee is subtracted to each output
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    extra_fee_per_output = 10

    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage, _ = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
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
    expected_list = np.array([100, 25, 320, 351, 140])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}')
    expected_dict = {'bc16': 1, 'bc13': 0}
    expected_list = np.array([520, 1245, 1140])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_list}')

    # Test for block execution with an extra fee that is a percentage of each output
    # the same percentage is subtracted to each output
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 0, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    extra_fee_per_output = 0
    extra_fee_percentage_per_output = 0.1

    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage, _ = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
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
    expected_list = np.array([100, 475, 320, 351, 135])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}')
    expected_dict = {'bc16': 1}
    expected_list = np.array([501, 1200, 1135])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_list}')

    # Test for block execution with a redistribution that is passed to the next address
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 5, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    extra_fee_per_output = 0
    extra_fee_percentage_per_output = 0.0

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 316227592, 'Fees': 3727592, 
             'Transactions': [
                 {'Inputs': [{'Sender': None, 'Value': 0}], 
                  'Outputs': [{'Receiver': 'bc12', 'Value': 700}, {'Receiver': 'INVALID', 'Value': 0}, {'Receiver': 'INVALID', 'Value': 0}]}, 
                  {'Inputs': [{'Sender': 'bc13', 'Value': 20}], 
                   'Outputs': [{'Receiver': 'bc17', 'Value': 150}]}, 
                    {'Inputs': [{'Sender': 'bc17', 'Value': 1000}], 
                     'Outputs': [{'Receiver': 'bc13', 'Value': 500}, {'Receiver': 'bc16', 'Value': 500}]
                     }
                     ]}

    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage, _ = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
                               redistribution_minimum, redistribution_maximum, block, extra_fee_per_output, extra_fee_percentage_per_output)
    
    condition = eligible_accounts.get_balance('bc17') == 150 and eligible_accounts.get_balance('bc13') == 500 and eligible_accounts.get_redistribution('bc13') == 0
    if condition:
        print('Test passed: block execution with redistribution passed')
    else:
        print('Test failed: block execution with redistribution passed')
        return

    eligible_accounts.perform_addition()
    non_eligible_accounts.perform_addition()

    print('Post:')
    expected_dict = {'bc12': 0, 'bc14': 2, 'bc17': 1, 'bc13': 3}
    expected_list = np.array([100, 150, 320, 500])
    expected_second_list = np.array([0, 0, 0, 0])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.first_list}, {eligible_accounts.second_list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}, {expected_second_list}')
    expected_dict = {'bc15': 0, 'bc16': 1}
    expected_list = np.array([501, 1250, 1150, 0])
    expected_second_list = np.array([0, 0, 0, 0])
    print(f'- Non eligible accounts: {non_eligible_accounts.dictionary}, {non_eligible_accounts.first_list}, {non_eligible_accounts.second_list}')
    print(f'- Expected non eligible accounts: {expected_dict}, {expected_list}, {expected_second_list}')

def test_perform_redistribution():
    # Test for equal redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 1, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra = 0
    circular_queue_index = 0

    redistribution_type = 'equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, max_block_redistribution, remaining_from_extra, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and eligible_accounts.get_balance('bc12') == 300 and eligible_accounts.get_balance('bc13') == 225 and non_eligible_accounts.get_balance('bc14') == 520 and (5000 - max_block_redistribution)/5000 == 0.88
    if condition:
        print('Test passed: simple equal redistribution')
    else:
        print('Test failed: simple equal redistribution')
        return

    # Test for equal redistribution with percentage on users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 1, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 0.5
    total_extra = 0

    redistribution_type = 'equal'
    redistribution_amount = 'block_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, max_block_redistribution, remaining_from_extra, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and non_eligible_accounts.get_balance('bc13') == 1925 and (5000 - max_block_redistribution)/5000 == 0.62
    if condition:
        print('Test passed: equal redistribution with percentage of users')
    else:
        print('Test failed: equal redistribution with percentage of users')
        return

    # Test for equal redistribution with percentage on users and an extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 1, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 0.75
    total_extra = 15

    redistribution_type = 'equal'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, max_block_redistribution, remaining_from_extra, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 1 and len(non_eligible_accounts.dictionary) == 5 and non_eligible_accounts.get_balance('bc12') == 1357 and non_eligible_accounts.get_balance('bc13') == 1282 and (5000 - max_block_redistribution)/5000 == 0.5
    if condition:
        print('Test passed: equal redistribution with percentage of users and an extra fee')
    else:
        print('Test failed: equal redistribution with percentage of users and an extra fee')
        return

    # Test for simple weight based redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 1, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 1000
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra = 0

    redistribution_type = 'weight_based'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, max_block_redistribution, remaining_from_extra, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and eligible_accounts.get_balance('bc12') == 570 and non_eligible_accounts.get_balance('bc13') == 1908 and eligible_accounts.get_balance('bc14') == 467 and (5000 - max_block_redistribution)/5000 == 0.5
    if condition:
        print('Test passed: simple weight based redistribution')
    else:
        print('Test failed: simple weight based redistribution')
        return

    # Test for weight based redistribution with percentage on users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 1, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 1000
    redistribution_percentage = 0.9
    redistribution_user_percentage = 0.75
    total_extra = 0

    redistribution_type = 'weight_based'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, max_block_redistribution, remaining_from_extra, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts.dictionary) == 4 and eligible_accounts.get_balance('bc12') == 1000 and non_eligible_accounts.get_balance('bc13') == 3625 and eligible_accounts.get_balance('bc14') == 320 and (5000 - max_block_redistribution)/5000 == 0.1
    if condition:
        print('Test passed: weight based redistribution with percentage on users')
    else:
        print('Test failed: weight based redistribution with percentage on users')
        return

    # Test for weight based redistribution with percentage on users and an extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_redistributions = np.array([0, 1, 0])
    eligible_accounts = DoubleDictionaryDoubleList(eligible_addresses, eligible_balances, eligible_redistributions)

    non_eligible_addresses = {'bc15': 0, 'bc16': 1, 'bc17': 2}
    non_eligible_balances = np.array([501, 750, 1000])
    non_eligible_redistributions = np.array([0, 0, 0])
    non_eligible_accounts = DictionaryDoubleList(non_eligible_addresses, non_eligible_balances, non_eligible_redistributions)

    redistribution_maximum = 1000
    redistribution_percentage = 0.9
    redistribution_user_percentage = 0.75
    total_extra = 503

    redistribution_type = 'weight_based'
    redistribution_amount = 'total_reward'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    eligible_accounts, non_eligible_accounts, max_block_redistribution, remaining_from_extra, circular_queue_index = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, total_extra, 
        eligible_accounts, non_eligible_accounts, circular_queue_index)

    condition = len(eligible_accounts.dictionary) == 1 and len(non_eligible_accounts.dictionary) == 5 and non_eligible_accounts.get_balance('bc12') == 1100 and non_eligible_accounts.get_balance('bc13') == 4028 and eligible_accounts.get_balance('bc14') == 320 and (5000 - max_block_redistribution)/5000 == 0.1
    if condition:
        print('Test passed: weight based redistribution with percentage on users and an extra fee')
    else:
        print('Test failed: weight based redistribution with percentage on users and an extra fee')
        return