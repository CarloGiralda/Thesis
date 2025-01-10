import os
from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from wealth_metrics.charts import plot_gini_coefficient, plot_nakamoto_coefficient
from redistribution_space.no_redistribution import no_redistribution
from redistribution_space.adjusted_redistribution_paradise import adjusted_redistribution_paradise
from utils import *

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = '/home/carlo/HDD/Shared/' # Directory where to store the results
config_file = './data_dungeon/only_redistribution_space/config.yaml'

def main():
    metric_type = 'adjusted_normal'
    addresses = 'single_input'
    extra_fee_amount = 0
    extra_fee_percentage = 0.0
    redistribution_type = 'no_redistribution'
    redistribution_amount = 'fees'
    redistribution_minimum = 100000
    redistribution_maximum = 2100000000000000
    redistribution_user_percentage = 1.0

    if redistribution_type == 'no_redistribution' and addresses == 'single_input':
        no_redistribution(dir_sorted_blocks, dir_results, metric_type)

    elif redistribution_type == 'no_redistribution' and addresses == 'multi_input':
        pass

    else:
        ginis = {}
        nakamotos = {}

        ginis_no_redistribution = {}
        nakamotos_no_redistribution = {}

        dir_files = os.path.join(dir_results, metric_type, addresses, redistribution_type, redistribution_amount, f'{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}')

        gini_file = f'{dir_files}/gini_coefficient_{redistribution_type}_{redistribution_amount}.png'
        nakamoto_file = f'{dir_files}/nakamoto_coefficient_{redistribution_type}_{redistribution_amount}.png'

        gini_no_redistribution_file = f'{dir_files}/gini_coefficient_no_redistribution_{redistribution_type}_{redistribution_amount}.png'
        nakamoto_no_redistribution_file = f'{dir_files}/nakamoto_coefficient_no_redistribution_{redistribution_type}_{redistribution_amount}.png'

        no_redistribution_file = os.path.join(dir_results, metric_type, addresses, 'accounts_no_redistribution.csv')
        if not os.path.exists(no_redistribution_file):
            print('Error:')
            print('Execute no redistribution before')
            return
        
        no_redistribution_addresses = read_no_redistribution_file(no_redistribution_file)

        for i in range(0, 11):
            percentage = i / 10

            print('Percentage: ', percentage)

            csv_file = f'{dir_files}/accounts_{percentage}.csv'

            if addresses == 'single_input':
                adjusted_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
            elif addresses == 'multi_input':
                pass

            balances_array_sorted, total_sum, no_redistribution_balances_array_sorted, no_redistribution_total_sum = read_csv_file(csv_file, percentage, no_redistribution_addresses)
            
            gini_coefficient = gini(balances_array_sorted, total_sum)
            ginis[percentage] = gini_coefficient
            nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)
            nakamotos[percentage] = nakamoto_coefficient

            gini_coefficient = gini(no_redistribution_balances_array_sorted, no_redistribution_total_sum)
            ginis_no_redistribution[percentage] = gini_coefficient
            nakamoto_coefficient = nakamoto(no_redistribution_balances_array_sorted, no_redistribution_total_sum)
            nakamotos_no_redistribution[percentage] = nakamoto_coefficient
        
        plot_gini_coefficient(ginis, gini_file)
        plot_nakamoto_coefficient(nakamotos, nakamoto_file)

        plot_gini_coefficient(ginis_no_redistribution, gini_no_redistribution_file)
        plot_nakamoto_coefficient(nakamotos_no_redistribution, nakamoto_no_redistribution_file)

if __name__ == '__main__':
    main()