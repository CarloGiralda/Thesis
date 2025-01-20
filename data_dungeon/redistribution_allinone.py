import os
from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from wealth_metrics.charts import plot_multiple_gini_coefficients, plot_multiple_nakamoto_coefficients
from utils import read_redistribution_csv_file

dir_results = './result/WorkstationResults' # Directory where to store the results

def main():
    metric_type = 'normal'
    addresses = 'single_input'
    extra_fee_amount = 0
    extra_fee_percentage = 0.0
    redistribution_minimum = 100000
    redistribution_maximum = 2100000000000000
    redistribution_user_percentage = 1.0

    indexes_to_redistribution_types = {0: 'equal\nfees', 1: 'circular_queue_equal\nfees', 2: 'weight_based\nfees', 3: 'weight_based\nblock_reward'}

    dir_general = os.path.join(dir_results, metric_type, addresses)

    ginis = []
    nakamotos = []

    gini_file = f'{dir_general}/gini_coefficient.png'
    nakamoto_file = f'{dir_general}/nakamoto_coefficient.png'

    for index, redistribution_type_amount in indexes_to_redistribution_types.items():
        redistribution_type, redistribution_amount = redistribution_type_amount.split('\n')
        print(f'Reading {redistribution_type} {redistribution_amount} redistribution files...')
        dir_files = os.path.join(dir_general, redistribution_type, redistribution_amount, f'{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}')
        
        ginis.append({})
        nakamotos.append({})
        for i in range(0, 11):
            percentage = i / 10

            csv_file = f'{dir_files}/accounts_{percentage}.csv'
            
            balances_array_sorted, total_sum = read_redistribution_csv_file(csv_file, percentage)
            
            gini_coefficient = gini(balances_array_sorted, total_sum)
            ginis[index][percentage] = gini_coefficient
            nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)
            nakamotos[index][percentage] = nakamoto_coefficient
            
    plot_multiple_gini_coefficients(ginis, gini_file, indexes_to_redistribution_types)
    plot_multiple_nakamoto_coefficients(nakamotos, nakamoto_file, indexes_to_redistribution_types)

if __name__ == '__main__':
    main()