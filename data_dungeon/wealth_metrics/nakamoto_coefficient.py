def nakamoto_coefficient(balances_array_sorted, total_sum):
    balances_array_sorted = balances_array_sorted[::-1]

    percentage = 0
    nakamoto_coefficient = 0
    while percentage <= 0.51:
        balance = balances_array_sorted[nakamoto_coefficient]
        percentage_balance = balance / total_sum
        percentage += percentage_balance
        nakamoto_coefficient += 1

    return nakamoto_coefficient