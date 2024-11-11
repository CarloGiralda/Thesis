import yaml
from redistribution_space.redistribution_linear import redistribution_linear
from redistribution_space.redistribution_proportional import redistribution_proportional

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './result/' # Directory where to store the results
config_file = './data_dungeon/redistribution_space/config.yaml'

def main():
    with open(config_file) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    
    percentage = cfg['fee']['percentage']
    type = cfg['redistribution']['type']
    minimum = cfg['redistribution']['minimum']
    maximum = cfg['redistribution']['maximum']

    # if type == 'linear':
    #     redistribution_linear(dir_sorted_blocks, dir_results, type, percentage, minimum, maximum)
    # elif type == 'proportional':
    redistribution_proportional(dir_sorted_blocks, dir_results, type, percentage, minimum, maximum)

if __name__ == '__main__':
    main()