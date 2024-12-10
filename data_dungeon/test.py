import numpy as np
import pandas as pd
from queue import Queue
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from wealth_metrics.gini_coefficient import gini
from wealth_metrics.nakamoto_coefficient import nakamoto
from wealth_metrics.charts import plot_gini_coefficient, plot_nakamoto_coefficient
from only_redistribution_space.only_redistribution_paradise import only_redistribution_paradise
from only_redistribution_space.multi_input_only_redistribution_paradise import multi_input_only_redistribution_paradise

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './results_HDD/' # Directory where to store the results
config_file = './data_dungeon/only_redistribution_space/config.yaml'

def _process_balance_chunk(chunk):
    # Extract balances and calculate the total sum in a vectorized manner
    local_balances = chunk['redistribution'].tolist()
    local_total_sum = chunk['redistribution'].sum()

    return local_balances, local_total_sum

def read_csv_file(csv_file, chunk_size=1000000):
    balances = []
    total_sum = 0

    aggregation_queue = Queue(maxsize=10)
            
    def aggregate_results():
        nonlocal total_sum

        while True:
            local_variables = aggregation_queue.get()
            if local_variables == None:
                break
                
            local_balances, local_total_sum = local_variables.result()
            balances.extend(local_balances)
            total_sum += local_total_sum

        return balances, total_sum
    
    with ThreadPoolExecutor(max_workers=1) as aggregator_executor:
        aggregator_future = [aggregator_executor.submit(aggregate_results)]
        
        with ProcessPoolExecutor() as processors:
            for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc='Reading CSV in chunks'):
                aggregation_queue.put(processors.submit(_process_balance_chunk, chunk))
            
            aggregation_queue.put(None)
            
            wait(aggregator_future)

    balances_array = np.array(balances, dtype=np.float64)
    balances_array_sorted = np.sort(balances_array)

    return balances_array_sorted, total_sum

def main():
    metric_type = 'only_redistribution'
    addresses = 'single_input'
    extra_fee_amount = 0
    extra_fee_percentage = 0.0
    redistribution_type = 'equal'
    redistribution_amount = 'fees'
    redistribution_minimum = 0
    redistribution_maximum = 2100000000000000
    redistribution_user_percentage = 1.0

    ginis = {}
    nakamotos = {}

    for i in range(0, 11):
        percentage = i / 10

        dir_results = f'./results_HDD/{metric_type}/{addresses}/{redistribution_type}/{redistribution_amount}_{redistribution_minimum}_{redistribution_maximum}_{redistribution_user_percentage}_{extra_fee_amount}_{extra_fee_percentage}'
        csv_file = f'{dir_results}/accounts_{percentage}.csv'
        gini_file = f'{dir_results}/gini_coefficient.png'
        nakamoto_file = f'{dir_results}/nakamoto_coefficient.png'

        if addresses == 'single_input':
            only_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
        elif addresses == 'multi_input':
            multi_input_only_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
        
        balances_array_sorted, total_sum = read_csv_file(csv_file)
        gini_coefficient = gini(balances_array_sorted, total_sum)
        ginis[i] = gini_coefficient
        nakamoto_coefficient = nakamoto(balances_array_sorted, total_sum)
        nakamotos[i] = nakamoto_coefficient
    
    plot_gini_coefficient(ginis, gini_file)
    plot_nakamoto_coefficient(nakamotos, nakamoto_file)

if __name__ == '__main__':
    main()