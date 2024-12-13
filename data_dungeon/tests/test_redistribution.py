import numpy as np
from redistribution_space.utils import DoubleDictionaryList
from redistribution_space.redistribution_paradise import *

def test_perform_input_output():
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    redistribution_minimum = 10
    redistribution_maximum = 500
    extra_fee_per_output = 0
    extra_fee_percentage_per_output = 0

    address = 'bc12'
    payment = 10
    input_output = 0

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.list[0] == 90:
        print('Test passed: simulate an input')
    else:
        print('Test failed: simulate an input')
        return

    address = 'bc12'
    payment = 10
    input_output = 1

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)

    if eligible_accounts.list[0] == 100:
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
    
    address = 'bc15'
    payment = 100
    input_output = 0

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if eligible_accounts.contains_key(address) and not address in non_eligible_accounts and eligible_accounts.get_balance(address) == 401:
        print('Test passed: address from non eligible to eligible')
    else:
        print('Test passed: address from non eligible to eligible')
        return

    address = 'bc13'
    payment = 20
    input_output = 0

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if not eligible_accounts.contains_key(address) and address in non_eligible_accounts and non_eligible_accounts[address] == 5:
        print('Test passed: address from eligible to non eligible')
    else:
        print('Test passed: address from eligible to non eligible')
        return

    address = 'bc19'
    payment = 300
    input_output = 1

    eligible_accounts, non_eligible_accounts = perform_input_output(address, payment, input_output, eligible_accounts, non_eligible_accounts,
                         redistribution_minimum, redistribution_maximum, extra_fee_per_output, extra_fee_percentage_per_output)
    
    if not address in non_eligible_accounts and eligible_accounts.get_balance(address) == 300 and eligible_accounts.dictionary[address] == 1:
        print('Test passed: replace unused index')
    else:
        print('Test passed: replace unused index')
        return

    eligible_accounts.perform_addition()

    print('Final:')
    expected_dict = {'bc12': 0, 'bc14': 2, 'bc18': 3, 'bc15': 4, 'bc19': 1}
    expected_list = np.array([100, 300, 320, 10, 401])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}')
    expected_dict = {'bc16': 750, 'bc17': 1000, 'bc13': 5}
    print(f'- Non eligible accounts: {non_eligible_accounts}')
    print(f'- Expected non eligible accounts: {expected_dict}')

def test_perform_block_transactions():
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

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

    print('Post:')
    expected_dict = {'bc12': 0, 'bc14': 2, 'bc15': 3, 'bc17': 4}
    expected_list = np.array([100, 25, 320, 351, 150])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}')
    expected_dict = {'bc16': 1250, 'bc13': 525}
    print(f'- Non eligible accounts: {non_eligible_accounts}')
    print(f'- Expected non eligible accounts: {expected_dict}')

    # Test for block execution with an extra fee that is fixed for the whole transaction
    # an equal fraction of the fee is subtracted to each output
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    extra_fee_per_output = 10

    eligible_accounts, non_eligible_accounts, total_extra_fee_percentage = perform_block_transactions(eligible_accounts, non_eligible_accounts, 
                               redistribution_minimum, redistribution_maximum, block, extra_fee_per_output, extra_fee_percentage_per_output)
    
    condition = eligible_accounts.get_balance('bc17') == 140 and non_eligible_accounts['bc13'] == 520
    if condition:
        print('Test passed: block execution with fixed extra fee')
    else:
        print('Test failed: block execution with fixed extra fee')
        return

    eligible_accounts.perform_addition()

    print('Post:')
    expected_dict = {'bc12': 0, 'bc14': 2, 'bc15': 3, 'bc17': 4}
    expected_list = np.array([100, 25, 320, 351, 140])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}')
    expected_dict = {'bc16': 1245, 'bc13': 520}
    print(f'- Non eligible accounts: {non_eligible_accounts}')
    print(f'- Expected non eligible accounts: {expected_dict}')

    # Test for block execution with an extra fee that is a percentage of each output
    # the same percentage is subtracted to each output
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

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

    print('Post:')
    expected_dict = {'bc12': 0, 'bc13': 1, 'bc14': 2, 'bc15': 3, 'bc17': 4}
    expected_list = np.array([100, 475, 320, 351, 135])
    print(f'- Eligible accounts: {eligible_accounts.dictionary}, {eligible_accounts.list}')
    print(f'- Expected eligible accounts: {expected_dict}, {expected_list}')
    expected_dict = {'bc16': 1200}
    print(f'- Non eligible accounts: {non_eligible_accounts}')
    print(f'- Expected non eligible accounts: {expected_dict}')

def test_perform_redistribution():
    # Test for equal redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    redistribution = [0]
    number_of_file = 0

    redistribution_maximum = 500
    redistribution_percentage = 0.5
    redistribution_user_percentage = 1.0
    total_extra_fee = 0

    redistribution_type = 'equal'
    redistribution_amount = 'fees'

    block = {'Block Hash': '000000000000000000004988528F7BE1744D4F05E706E33DDB36963236FC3C41', 
             'Previous Block Hash': '00000000000000000000DF86517266E2DCE9766241C14FE224D5FED1C09F5F8D', 
             'Reward': 5000, 'Fees': 1200
    }

    redistribution, eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee, 
        redistribution, eligible_accounts, non_eligible_accounts)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts) == 4 and eligible_accounts.get_balance('bc12') == 300 and eligible_accounts.get_balance('bc13') == 225 and non_eligible_accounts['bc14'] == 520 and ratio == 0.88 and redistribution == [200]
    if condition:
        print('Test passed: simple equal redistribution')
    else:
        print('Test failed: simple equal redistribution')
        return

    # Test for equal redistribution with percentage on users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    redistribution = [0]
    number_of_file = 0

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

    redistribution, eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee, 
        redistribution, eligible_accounts, non_eligible_accounts)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts) == 4 and non_eligible_accounts['bc13'] == 1925 and ratio == 0.62 and redistribution == [1900]
    if condition:
        print('Test passed: equal redistribution with percentage of users')
    else:
        print('Test failed: equal redistribution with percentage of users')
        return

    # Test for equal redistribution with percentage on users and an extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    redistribution = [0]
    number_of_file = 0

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

    redistribution, eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee, 
        redistribution, eligible_accounts, non_eligible_accounts)

    condition = len(eligible_accounts.dictionary) == 1 and len(non_eligible_accounts) == 5 and non_eligible_accounts['bc12'] == 1357 and non_eligible_accounts['bc13'] == 1282 and ratio == 0.5 and redistribution == [1257]
    if condition:
        print('Test passed: equal redistribution with percentage of users and an extra fee')
    else:
        print('Test failed: equal redistribution with percentage of users and an extra fee')
        return

    # Test for simple weight based redistribution
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    redistribution = [0]
    number_of_file = 0

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

    redistribution, eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee, 
        redistribution, eligible_accounts, non_eligible_accounts)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts) == 4 and eligible_accounts.get_balance('bc12') == 571 and non_eligible_accounts['bc13'] == 1907 and eligible_accounts.get_balance('bc14') == 467 and ratio == 0.5 and redistribution[0][1] == 1882
    if condition:
        print('Test passed: simple weight based redistribution')
    else:
        print('Test failed: simple weight based redistribution')
        return

    # Test for weight based redistribution with percentage on users
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    redistribution = [0]
    number_of_file = 0

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

    redistribution, eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee, 
        redistribution, eligible_accounts, non_eligible_accounts)

    condition = len(eligible_accounts.dictionary) == 2 and len(non_eligible_accounts) == 4 and eligible_accounts.get_balance('bc12') == 1000 and non_eligible_accounts['bc13'] == 3625 and eligible_accounts.get_balance('bc14') == 320 and ratio == 0.1 and redistribution[0][1] == 3600
    if condition:
        print('Test passed: weight based redistribution with percentage on users')
    else:
        print('Test failed: weight based redistribution with percentage on users')
        return

    # Test for weight based redistribution with percentage on users and an extra fee
    eligible_addresses = {'bc12': 0, 'bc13': 1, 'bc14': 2}
    eligible_balances = np.array([100, 25, 320])
    eligible_accounts = DoubleDictionaryList(eligible_addresses, eligible_balances)

    non_eligible_accounts = {'bc15': 501, 'bc16': 750, 'bc17': 1000}

    redistribution = [0]
    number_of_file = 0

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

    redistribution, eligible_accounts, non_eligible_accounts, ratio = perform_redistribution(
        redistribution_type, redistribution_amount, redistribution_maximum, redistribution_percentage, redistribution_user_percentage, block, number_of_file, total_extra_fee, 
        redistribution, eligible_accounts, non_eligible_accounts)

    condition = len(eligible_accounts.dictionary) == 1 and len(non_eligible_accounts) == 5 and non_eligible_accounts['bc12'] == 1101 and non_eligible_accounts['bc13'] == 4027 and eligible_accounts.get_balance('bc14') == 320 and ratio == 0.1 and redistribution[0][1] == 4002
    if condition:
        print('Test passed: weight based redistribution with percentage on users and an extra fee')
    else:
        print('Test failed: weight based redistribution with percentage on users and an extra fee')
        return