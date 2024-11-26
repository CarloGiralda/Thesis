import yaml
from redistribution_space.redistribution_paradise import redistribution_paradise
from redistribution_space.multi_input_redistribution_paradise import multi_input_redistribution_paradise

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './results_HDD/' # Directory where to store the results
config_file = './data_dungeon/redistribution_space/config.yaml'

def main():
    with open(config_file) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    
    addresses = cfg['addresses']['grouping']
    extra_fee_amount = cfg['extra_fee']['amount_per_transaction']
    extra_fee_percentage = cfg['extra_fee']['percentage_per_transaction']
    redistribution_type = cfg['redistribution']['type']
    redistribution_percentage = cfg['redistribution']['from']['percentage']
    redistribution_amount = cfg['redistribution']['from']['amount']
    redistribution_minimum = cfg['redistribution']['to']['minimum']
    redistribution_maximum = cfg['redistribution']['to']['maximum']
    redistribution_user_percentage = cfg['redistribution']['to']['percentage']

    if addresses == 'single_input':
        redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, redistribution_percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)
    elif addresses == 'multi_input':
        multi_input_redistribution_paradise(dir_sorted_blocks, dir_results, redistribution_type, redistribution_percentage, redistribution_amount, redistribution_minimum, redistribution_maximum, redistribution_user_percentage, extra_fee_amount, extra_fee_percentage)

if __name__ == '__main__':
    main()