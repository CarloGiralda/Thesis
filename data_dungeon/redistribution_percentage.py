import os
from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from wealth_metrics.charts import plot_gini_coefficient, plot_nakamoto_coefficient
from redistribution_space.redistribution_paradise import redistribution_paradise
from redistribution_space.no_redistribution import no_redistribution
from redistribution_space.multi_input_no_redistribution import multi_input_no_redistribution
from redistribution_space.multi_input_redistribution_paradise import multi_input_redistribution_paradise
from utils import read_redistribution_csv_file, read_multi_input_redistribution_csv_file

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './result/WorkstationResults' # Directory where to store the results

def main():
    metric_type = 'r_normal'
    addresses = 'single_input'
    extra_fee_amount = 0
    extra_fee_percentage = 0.0
    redistribution_type = 'circular_queue_equal'
    redistribution_amount = 'fees'
    redistribution_minimum = 100000
    redistribution_maximum = 2100000000000000
    redistribution_user_percentage = 1.0

    if redistribution_type == 'no_redistribution' and addresses == 'single_input':
        no_redistribution(dir_sorted_blocks, dir_results, metric_type)

    elif redistribution_type == 'no_redistribution' and addresses == 'multi_input':
        multi_input_no_redistribution(dir_sorted_blocks, dir_results, metric_type)
    
    else:
        ginis = {}
        nakamotos = {}

        dir_files = os.path.join(dir_results, metric_type, addresses, redistribution_type, redistribution_amount, f'{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}')

        gini_file = f'{dir_files}/gini_coefficient_{redistribution_type}_{redistribution_amount}.png'
        nakamoto_file = f'{dir_files}/nakamoto_coefficient_{redistribution_type}_{redistribution_amount}.png'

        for i in range(10, 11):
            percentage = i / 10

            print('Percentage: ', percentage)

            csv_file = f'{dir_files}/accounts_{percentage}.csv'

            if addresses == 'single_input':
                redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
            
                balances_array_sorted, total_sum = read_redistribution_csv_file(csv_file, percentage)
            elif addresses == 'multi_input':
                multi_input_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
            
                balances_array_sorted, total_sum = read_multi_input_redistribution_csv_file(csv_file, percentage)
            
            gini_coefficient = gini(balances_array_sorted, total_sum)
            ginis[percentage] = gini_coefficient
            nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)
            nakamotos[percentage] = nakamoto_coefficient
        
        plot_gini_coefficient(ginis, gini_file)
        plot_nakamoto_coefficient(nakamotos, nakamoto_file)

if __name__ == '__main__':
    main()