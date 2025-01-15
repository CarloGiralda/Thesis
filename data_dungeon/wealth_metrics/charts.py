import numpy as np
from matplotlib import pyplot as plt

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

def plot_gini_coefficient(gini_coefficients, gini_file):
    percentages = list(gini_coefficients.keys())
    ginis = list(gini_coefficients.values())

    plt.figure(figsize=(10, 6))
    plt.plot(percentages, ginis)
    plt.xlabel('Percentage of redistribution')
    plt.ylabel('Gini coefficient')
    plt.tight_layout()
    plt.savefig(gini_file)

def plot_nakamoto_coefficient(nakamoto_coefficients, nakamoto_file):
    percentages = list(nakamoto_coefficients.keys())
    nakamotos = list(nakamoto_coefficients.values())

    plt.figure(figsize=(10, 6))
    plt.plot(percentages, nakamotos)
    plt.xlabel('Percentage of redistribution')
    plt.ylabel('Nakamoto coefficient')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig(nakamoto_file)

def plot_gini_coefficient_for_taxation(gini_coefficients, gini_file):
    percentages = list(gini_coefficients.keys())
    ginis = list(gini_coefficients.values())

    plt.figure(figsize=(10, 6))
    plt.plot(percentages, ginis)
    plt.xlabel('Percentage of taxation on each transaction')
    plt.ylabel('Gini coefficient')
    plt.xscale('log')
    plt.tight_layout()
    plt.savefig(gini_file)

def plot_nakamoto_coefficient_for_taxation(nakamoto_coefficients, nakamoto_file):
    percentages = list(nakamoto_coefficients.keys())
    nakamotos = list(nakamoto_coefficients.values())

    plt.figure(figsize=(10, 6))
    plt.plot(percentages, nakamotos)
    plt.xlabel('Percentage of taxation on each transaction')
    plt.ylabel('Nakamoto coefficient')
    plt.xscale('log')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig(nakamoto_file)

def plot_multiple_gini_coefficients(list_of_gini_coefficients, gini_file, indexes_to_redistribution_types):

    plt.figure(figsize=(10, 6))
    # plot each dictionary as a line
    for index, d in enumerate(list_of_gini_coefficients):
        x = list(d.keys())
        y = list(d.values())
        plt.plot(x, y, label=indexes_to_redistribution_types[index])

    plt.xlabel('Percentage of redistribution')
    plt.ylabel('Gini coefficient')
    plt.legend()
    plt.tight_layout()
    plt.savefig(gini_file)

def plot_multiple_nakamoto_coefficients(list_of_nakamoto_coefficients, nakamoto_file, indexes_to_redistribution_types):

    plt.figure(figsize=(10, 6))
    # plot each dictionary as a line
    for index, d in enumerate(list_of_nakamoto_coefficients):
        x = list(d.keys())
        y = list(d.values())
        plt.plot(x, y, label=indexes_to_redistribution_types[index])

    plt.xlabel('Percentage of redistribution')
    plt.ylabel('Nakamoto coefficient')
    plt.legend()
    plt.tight_layout()
    plt.savefig(nakamoto_file)