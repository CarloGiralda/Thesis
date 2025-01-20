from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from utils import read_redistribution_csv_file, read_multi_input_redistribution_csv_file

metric_type = 'normal'
address_grouping = 'single_input'
redistribution_type = 'no_redistribution'
redistribution_amount = 'fees'
percentage = 0.0
user_percentage = 1.0
extra_fee_amount = 0
extra_fee_percentage = 0.0
minimum = 100000
maximum = 2100000000000000

directory = './result/WorkstationResults'

def main():
    if address_grouping == 'single_input':
        csv_file = f'{directory}/{metric_type}/{address_grouping}/{redistribution_type}/{redistribution_amount}/{minimum}_{maximum}_{user_percentage}_{extra_fee_amount}_{extra_fee_percentage}/accounts_{percentage}.csv' if redistribution_type != 'no_redistribution' else f'{directory}/{metric_type}/{address_grouping}/accounts_no_redistribution.csv'

        balances_array_sorted, total_sum = read_redistribution_csv_file(csv_file, percentage)
    
    else:
        csv_file_accounts = f'{directory}/{metric_type}/{address_grouping}/{redistribution_type}/{redistribution_amount}/{minimum}_{maximum}_{user_percentage}_{extra_fee_amount}_{extra_fee_percentage}/accounts_{percentage}.csv' if redistribution_type != 'no_redistribution' else f'{directory}/{metric_type}/{address_grouping}/accounts_no_redistribution.csv'
        csv_file_balances = f'{directory}/{metric_type}/{address_grouping}/{redistribution_type}/{redistribution_amount}/{minimum}_{maximum}_{user_percentage}_{extra_fee_amount}_{extra_fee_percentage}/balances_{percentage}.csv' if redistribution_type != 'no_redistribution' else f'{directory}/{metric_type}/{address_grouping}/balances_no_redistribution.csv'

        balances_array_sorted, total_sum = read_multi_input_redistribution_csv_file(csv_file_accounts, csv_file_balances, percentage)

    gini_coefficient = gini(balances_array_sorted, total_sum)
    nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)

    print(f'Gini coefficient: {gini_coefficient}')
    print(f'Nakamoto coefficient: {nakamoto_coefficient}')

if __name__ == '__main__':
    main()