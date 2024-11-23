import math
from tqdm import tqdm

def gini_coefficient(balances_array_sorted, total_sum):
    num_balances = len(balances_array_sorted)
    balances_in_decile = math.ceil(num_balances / 10)

    wealth_distribution_decile = []
    
    sum = 0
    for index, balance in tqdm(enumerate(balances_array_sorted), desc='Wealth distribution computation'):
        sum += balance

        # check if the next number is the one that starts the new decile or if this is the last element of the array
        # this is done because the division for deciles is not perfect, so approximation must be taken into account
        if (index + 1) % balances_in_decile == 0 or index == num_balances - 1:
            wealth_decile = sum / total_sum
            wealth_distribution_decile.append(wealth_decile)

    print(f'Deciles: {wealth_distribution_decile}')

    perfect_lorenz_curve_area = 0.5
    # the area under this curve is computed by summing the rectangle and the triangle under each decile
    # the first decile has only a triangle
    actual_lorenz_curve_area = 0

    for i in tqdm(range(len(wealth_distribution_decile)), desc='Gini computation'):
        # decile division makes the base always equal to 0.1
        base = 0.1
        previous_height = 0
        if i > 0:
            previous_height = wealth_distribution_decile[i - 1]
        current_height = wealth_distribution_decile[i]

        rectangle_area = base * previous_height
        triangle_area = base * (current_height - previous_height) / 2
        area = rectangle_area + triangle_area

        actual_lorenz_curve_area += area

    area_between_lorenz_curves = perfect_lorenz_curve_area - actual_lorenz_curve_area
    gini_value = area_between_lorenz_curves / perfect_lorenz_curve_area

    return gini_value