from configparser import ConfigParser

def load_config(filename='blk_files_parsing/database/bitcoin_database.ini', section='postgresql'):
    parser = ConfigParser()
    with open(filename, 'r') as f:
        parser.read_file(f)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config