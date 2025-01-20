import os
from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from wealth_metrics.charts import plot_gini_coefficient, plot_nakamoto_coefficient
from only_redistribution_space.only_redistribution_paradise import only_redistribution_paradise
from only_redistribution_space.multi_input_only_redistribution_paradise import multi_input_only_redistribution_paradise
from utils import read_only_redistribution_csv_file

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './result/WorkstationResults' # Directory where to store the results

def main():
    metric_type = 'only_redistribution'
    addresses = 'single_input'
    extra_fee_amount = 0
    extra_fee_percentage = 0.0
    redistribution_type = 'equal'
    redistribution_amount = 'block_reward'
    redistribution_minimum = 0
    redistribution_maximum = 2100000000000000
    redistribution_user_percentage = 1.0

    ginis = {}
    nakamotos = {}

    for i in range(0, 11):
        percentage = i / 10

        print('Percentage: ', percentage)

        dir_files = os.path.join(dir_results, metric_type, addresses, redistribution_type, redistribution_amount, f'{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}')
        csv_file = f'{dir_files}/accounts_{percentage}.csv'
        gini_file = f'{dir_files}/gini_coefficient_{redistribution_type}_{redistribution_amount}.png'
        nakamoto_file = f'{dir_files}/nakamoto_coefficient_{redistribution_type}_{redistribution_amount}.png'

        if addresses == 'single_input':
            only_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
        elif addresses == 'multi_input':
            multi_input_only_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
        
        balances_array_sorted, total_sum = read_only_redistribution_csv_file(csv_file, percentage)
        gini_coefficient = gini(balances_array_sorted, total_sum)
        ginis[percentage] = gini_coefficient
        nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)
        nakamotos[percentage] = nakamoto_coefficient
    
    plot_gini_coefficient(ginis, gini_file)
    plot_nakamoto_coefficient(nakamotos, nakamoto_file)

if __name__ == '__main__':
    main()