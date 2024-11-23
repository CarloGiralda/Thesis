import yaml
from redistribution_space.redistribution_paradise import redistribution_paradise
from redistribution_space.multi_input_redistribution_paradise import multi_input_redistribution_paradise

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './result/' # Directory where to store the results
config_file = './data_dungeon/redistribution_space/config.yaml'

def main():
    with open(config_file) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    
    addresses = cfg['addresses']['grouping']
    extra_fee_amount = cfg['extra_fee']['amount_per_transaction']
    type = cfg['redistribution']['type']
    percentage = cfg['redistribution']['percentage']
    amount = cfg['redistribution']['amount']
    minimum = cfg['redistribution']['minimum']
    maximum = cfg['redistribution']['maximum']

    if addresses == 'single_input':
        redistribution_paradise(dir_sorted_blocks, dir_results, type, percentage, amount, minimum, maximum, extra_fee_amount)
    elif addresses == 'multi_input':
        multi_input_redistribution_paradise(dir_sorted_blocks, dir_results, type, percentage, amount, minimum, maximum, extra_fee_amount)

if __name__ == '__main__':
    main()