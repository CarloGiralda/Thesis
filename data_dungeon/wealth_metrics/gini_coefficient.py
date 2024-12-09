import numpy as np
from matplotlib import pyplot as plt
from tqdm import tqdm

def lorenz_curve(balances_array_sorted, total_sum, lorenz_curve_file):
    X_lorenz = balances_array_sorted.cumsum() / total_sum
    X_lorenz = np.insert(X_lorenz, 0, 0) 
    X_lorenz[0], X_lorenz[-1]

    _, ax = plt.subplots(layout='constrained', figsize = (10, 6))
    # plot Lorenz curve
    ax.plot(np.arange(X_lorenz.size)/(X_lorenz.size-1), X_lorenz, color='purple', label='Lorenz curve')
    # plot line of equality
    ax.plot([0,1], [0,1], color='k', label='Equality line')
    ax.legend(loc='upper left', ncols=1)
    ax.set_xlabel('Percentage of Population')
    ax.set_ylabel('Percentage of Wealth')
    plt.savefig(lorenz_curve_file)
    plt.show()

def gini(balances_array_sorted, total_sum):
    n = balances_array_sorted.size
    coef_ = 2. / n
    const_ = (n + 1.) / n
    weighted_sum = sum([(i + 1) * yi for i, yi in tqdm(enumerate(balances_array_sorted))])
    return coef_ * weighted_sum / (total_sum) - const_