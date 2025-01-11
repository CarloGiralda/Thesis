import os
from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from wealth_metrics.charts import plot_multiple_gini_coefficients, plot_multiple_nakamoto_coefficients
from utils import read_only_redistribution_csv_file

dir_results = './result/WorkstationResults' # Directory where to store the results

def main():
    metric_type = 'only_redistribution'
    addresses = 'single_input'
    extra_fee_amount = 0
    extra_fee_percentage = 0.0
    redistribution_amount = 'fees'
    redistribution_minimum = 0
    redistribution_maximum = 2100000000000000
    redistribution_user_percentage = 1.0

    redistribution_types = ['equal', 'almost_equal', 'no_minimum_equal', 'circular_queue_equal']

    dir_general = os.path.join(dir_results, metric_type, addresses)

    ginis = []
    nakamotos = []

    gini_file = f'{dir_general}/gini_coefficient.png'
    nakamoto_file = f'{dir_general}/nakamoto_coefficient.png'

    for index, redistribution_type in enumerate(redistribution_types):
        print(f'Reading {redistribution_type} redistribution files...')
        dir_files = os.path.join(dir_general, redistribution_type, redistribution_amount, f'{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}')
        
        ginis.append({})
        nakamotos.append({})
        for i in range(0, 11):
            percentage = i / 10

            csv_file = f'{dir_files}/accounts_{percentage}.csv'
            
            balances_array_sorted, total_sum = read_only_redistribution_csv_file(csv_file, percentage)
            
            gini_coefficient = gini(balances_array_sorted, total_sum)
            ginis[index][percentage] = gini_coefficient
            nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)
            nakamotos[index][percentage] = nakamoto_coefficient
            
    plot_multiple_gini_coefficients(ginis, gini_file)
    plot_multiple_nakamoto_coefficients(nakamotos, nakamoto_file)

if __name__ == '__main__':
    main()