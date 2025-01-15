import os
from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from wealth_metrics.charts import plot_gini_coefficient_for_taxation, plot_nakamoto_coefficient_for_taxation
from redistribution_space.redistribution_for_taxation import redistribution_for_taxation
from utils import read_redistribution_csv_file

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './result/WorkstationResults' # Directory where to store the results

def main():
    metric_type = 'normal'
    addresses = 'single_input'
    extra_fee_amount = 0
    redistribution_percentage = 0.0
    redistribution_type = 'weight_based'
    redistribution_amount = 'fees'
    redistribution_minimum = 100000
    redistribution_maximum = 2100000000000000
    redistribution_user_percentage = 1.0

    ginis = {}
    nakamotos = {}

    dir_files = os.path.join(dir_results, metric_type, addresses, redistribution_type, redistribution_amount, f'{redistribution_percentage}_{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}')

    gini_file = f'{dir_files}/gini_coefficient_{redistribution_type}_{redistribution_amount}.png'
    nakamoto_file = f'{dir_files}/nakamoto_coefficient_{redistribution_type}_{redistribution_amount}.png'

    for i in (10 ** p for p in range(1, 7)):
        fee_percentage = 1 / i

        print('Percentage: ', fee_percentage)

        csv_file = f'{dir_files}/accounts_{extra_fee_amount}_{fee_percentage}.csv'

        if addresses == 'single_input':
            redistribution_for_taxation(dir_sorted_blocks, dir_results, redistribution_type, redistribution_percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, fee_percentage)
        elif addresses == 'multi_input':
            pass

        balances_array_sorted, total_sum = read_redistribution_csv_file(csv_file, fee_percentage)
        
        gini_coefficient = gini(balances_array_sorted, total_sum)
        ginis[fee_percentage] = gini_coefficient
        nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)
        nakamotos[fee_percentage] = nakamoto_coefficient
    
    plot_gini_coefficient_for_taxation(ginis, gini_file)
    plot_nakamoto_coefficient_for_taxation(nakamotos, nakamoto_file)

if __name__ == '__main__':
    main()