from tqdm import tqdm

def gini(balances_array_sorted, total_sum):
    n = balances_array_sorted.size
    coef_ = 2. / n
    const_ = (n + 1.) / n
    weighted_sum = sum([(i + 1) * yi for i, yi in tqdm(enumerate(balances_array_sorted))])
    return coef_ * weighted_sum / (total_sum) - const_