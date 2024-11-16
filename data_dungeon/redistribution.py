import yaml
from data_dungeon.redistribution_space.redistribution_paradise import redistribution_paradise

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './result/' # Directory where to store the results
config_file = './data_dungeon/redistribution_space/config.yaml'

def main():
    with open(config_file) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    
    type = cfg['redistribution']['type']
    percentage = cfg['redistribution']['percentage']
    amount = cfg['redistribution']['amount']
    minimum = cfg['redistribution']['minimum']
    maximum = cfg['redistribution']['maximum']

    redistribution_paradise(dir_sorted_blocks, dir_results, type, percentage, amount, minimum, maximum)

if __name__ == '__main__':
    main()