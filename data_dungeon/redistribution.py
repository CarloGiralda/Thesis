from redistribution_space.redistribution_fees import redistribute_fees

dir_sorted_blocks = './result/blocks/' # Directory where sorted blocks are saved
dir_results = './result/' # Directory where to store the results
config_file = './data_dungeon/redistribution_space/config.yaml'

def main():
    redistribute_fees(config_file, dir_sorted_blocks, dir_results)

if __name__ == '__main__':
    main()