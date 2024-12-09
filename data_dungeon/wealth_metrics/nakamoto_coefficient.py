import numpy as np

def nakamoto_coefficient(balances_array_sorted, total_sum):
    balances_array_sorted = balances_array_sorted[::-1]

    cumulative_percentage = np.cumsum(balances_array_sorted) / total_sum

    nakamoto_coefficient = np.searchsorted(cumulative_percentage, 0.51) + 1

    return nakamoto_coefficient