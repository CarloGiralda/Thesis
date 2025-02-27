import numpy as np
import pandas as pd
from queue import Queue
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait

# Exchanges, ETF, Custodial Companies addresses in the top 500 holders (and two of Satoshi's addresses)
known_wallets = set(['1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa', '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S',
                     '34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo', 'bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97', 'bc1ql49ydapnjafl5t2cp9zqpjwe6pdgmxy98859v2', 
                     '3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6', 'bc1qazcm763858nkj2dj986etajv6wquslv8uxwczt', '1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF', 
                     'bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfc27a4', '3LYJfcfHPXYJreMsASk2jkn69LWEYKzexb', 'bc1qcv8h9hp5w8c4qpze0a4tdxw6qjtvg8yps23k0g3aymxx7jlesv4q4t6f65', 
                     '3PXBET2GrTwCamkeDzKCx8DeGDyrbuGKoc', 'bc1q4j7fcl8zx5yl56j00nkqez9zf3f6ggqchwzzcs5hjxwqhsgxvavq3qfgpr', '3MgEAFWu1HKSnZ5ZsC8qf61ZW18xrP5pgd', 
                     '3LQUu4v9z6KNch71j7kbj8GPeAGUo1FW6a', 'bc1qk4m9zv5tnxf2pddd565wugsjrkqkfn90aa0wypj2530f4f7tjwrqntpens', 'bc1qx9t2l3pyny2spqpqlye8svce70nppwtaxwdrp4', 
                     '3FHNBLobJnbCTFTVakh5TXmEneyf5PT61B', '1Pzaqw98PeRfyHypfqyEgg5yycJRsENrE7', 'bc1qr4dl5wa7kl8yu792dceg9z5knl2gkn220lk7a9', 
                     '38UmuUqPCrFmQo4khkomQwZ4VbY2nZMJ67', 'bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h', 'bc1qcdqj2smprre85c78d942wx5tauw5n7uw92r7wr', 
                     'bc1qjh0akslml59uuczddqu0y4p3vj64hg5mc94c40', 'bc1qx2x5cqhymfcnjtg902ky6u5t5htmt7fvqztdsm028hkrvxcl4t2sjtpd9l', 'bc1qchctnvmdva5z9vrpxkkxck64v7nmzdtyxsrq64', 
                     'bc1q32lyrhp9zpww22phqjwwmelta0c8a5q990ghs6', '34HpHYiyQwg69gFmCq2BGHjF1DZnZnBeBP', 'bc1q968e7dl5cq938tz87uwqyyd4and4mjhgyelfyq', 
                     'bc1qk7fy6qumtdkjy765ujxqxe0my55ake0zefa2dmt6sjx2sr098d8qf26ufn', '3FM9vDYsN2iuMPKWjAcqgyahdwdrUxhbJ3', 'bc1qtrxc0use4hlm7fl0j6t37z7qlwl5eppj8lywz6', 
                     'bc1qkmk4v2xn29yge68fq6zh7gvfdqrvpq3v3p3y0s','1PJiGp2yDLvUgqeBsuZVCBADArNsk6XEiw', 'bc1q7t9fxfaakmtk8pj7tdxjvwsng6y9x76czuaf5h', 
                     '1Q8QR5k32hexiMQnRgkJ6fmmjn5fMWhdv9', '1LnoZawVFFQihU8d8ntxLMpYheZUfyeVAK', '1DcT5Wij5tfb3oVViF8mA8p4WrG98ahZPT', 
                     '1JQULE6yHr9UaitLr4wahTwJN7DaMX7W1Z', '1CY7fykRLWXeSbKB885Kr4KjQxmDdvW923', '143gLvWYUojXaWZRrxquRKpVNTkhmr415B', 
                     '3FupZp77ySr7jwoLYEJ9mwzJpvoNBXsBnE', '39wVd42giU95ca39sEPkbPTpWygvsBDuA5', 'bc1qdhvtwg0eealy5d2spua2a89sq05ydvtgjy4uau', 
                     '3NpXph8WN1U9hwXjg1bRtzTff1tPR2Gpw4', 'bc1qm6q8tgml3cr9gpx63a5jqtj2dxlsyz4q3ghjlf', 'bc1qe75775tzuvspl59cw77ycc472jl0sgue69x3up', 
                     'bc1qvy0sp8cdj3cv2wwh05scucxw6vxqpdlhfjvqn8', 'bc1qe39l9l84sa44r9j2jjkgdc7p4ltj3sracd932k', 'bc1q5nfww5jn5k4ghg7dpa4gy85x7uu3l4g0m0re76', 
                     'bc1q4srun4yspqem2pqgk47eq9jspcht3fmyrmfdeu', '36NkTqCAApfRJBKicQaqrdKs29g6hyE4LS', '3DVJfEsDTPkGDvqPCLC41X85L1B1DQWDyh', 
                     'bc1qvhxafz8dqk8c25jsx669yd6qrxhl5dx72dyryp', 'bc1q4c8n5t00jmj8temxdgcc3t32nkg2wjwz24lywv', '16rF2zwSJ9goQ9fZfYoti5LsUqqegb5RnA', 
                     '3A1mvU9GSc9cWQxicofHFBAKDGVyA3v5g4', '18DowXoMUQT5EU8zPTDTrq4hrwmi8ddCcc', 'bc1qsm3kjcmp63jsuyrqh58kc6k4ydpactjy6r7a6f', 
                     '3HroDXv8hmzKRtaSfBffRgedKpru8fgy6M', '36ZF5foUhdvma5RrnRr1bu6RtjoUsi6Phg', '162bzZT2hJfv5Gm3ZmWfWfHJjCtMD6rHhw', 
                     '3KbWiPRCgWkQNW7QKE3mkJtBcFD6cSsQE6', '32TiohXoCmHr87xVm3E9A3sLiWBJjYn1gf', '3H5JTt42K7RmZtromfTSefcMEFMMe18pMD', 
                     'bc1q7ramrn7krmgl8ja8vjm9g25a5t98l6kfyqgewe', '3L41yRzWATBFS3TSHGxFAJiTxahB94MpcQ', 'bc1q0dfgg0phamhxyntrenylv98epwn69fq9mwmaz0', 
                     '3E5EPMGRL5PC6YDCLcHLVu9ayC3DysMpau', 'bc1qs5vdqkusz4v7qac8ynx0vt9jrekwuupx2fl5udp9jql3sr03z3gsr2mf0f', 'bc1qmxcagqze2n4hr5rwflyfu35q90y22raxdgcp4p'])

def _process_redistribution_balance_chunk(chunk):
    # save each chunk by selecting only addresses that satisfy the assumptions
    filtered_chunk = chunk[
            (~chunk['address'].isin(known_wallets)) & (chunk['balance'] >= 100000)
        ]
        
    # Extract balances and calculate the total sum in a vectorized manner
    chunk_balances = filtered_chunk['balance'].tolist()
    chunk_total_sum = filtered_chunk['balance'].sum()

    return chunk_balances, chunk_total_sum

def read_redistribution_csv_file(csv_file, percentage ,chunk_size=10000000):
    balances = []
    total_sum = 0

    aggregation_queue = Queue(maxsize=10)

    def aggregate_results():
        nonlocal total_sum

        while True:
            local_variables = aggregation_queue.get()
            if local_variables == None:
                break
                
            chunk_balances, chunk_total_sum = local_variables.result()
            
            balances.extend(chunk_balances)
            total_sum += chunk_total_sum

        return balances, total_sum
    
    with ThreadPoolExecutor(max_workers=1) as aggregator_executor:
        aggregator_future = [aggregator_executor.submit(aggregate_results)]
        
        with ProcessPoolExecutor() as processors:
            for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc=f'Reading accounts_{percentage} file in chunks'):
                aggregation_queue.put(processors.submit(_process_redistribution_balance_chunk, chunk))
            
            aggregation_queue.put(None)
            
            wait(aggregator_future)

    balances_array = np.array(balances, dtype=np.float64)
    balances_array_sorted = np.sort(balances_array)

    return balances_array_sorted, total_sum

def _process_multi_input_redistribution_balance_chunk(chunk, users):
    # save each chunk by selecting only addresses that satisfy the assumptions
    filtered_chunk = chunk[
            (chunk['user'].isin(users)) & (chunk['balance'] >= 100000)
        ]
        
    # Extract balances and calculate the total sum in a vectorized manner
    chunk_balances = filtered_chunk['balance'].tolist()
    chunk_total_sum = filtered_chunk['balance'].sum()

    return chunk_balances, chunk_total_sum

def _process_multi_input_redistribution_account_chunk(chunk):
    # save each chunk by selecting only addresses that satisfy the assumptions
    filtered_chunk = chunk[
            (chunk['address'].isin(known_wallets))
        ]
        
    to_be_deleted = filtered_chunk['user'].tolist()
    all_users = chunk['user'].tolist()

    return to_be_deleted, all_users

def read_multi_input_redistribution_csv_file(csv_file_accounts, csv_file_balances, percentage ,chunk_size=10000000):
    aggregation_queue = Queue(maxsize=10)

    users = set([])
    to_be_deleted_users = set([])

    balances = []
    total_sum = 0

    def aggregate_results():
        nonlocal total_sum

        while True:
            local_variables = aggregation_queue.get()
            if local_variables == None:
                break
                
            chunk_balances, chunk_total_sum = local_variables.result()
            
            balances.extend(chunk_balances)
            total_sum += chunk_total_sum

        return balances, total_sum
    
    with ProcessPoolExecutor() as processors:
        for chunk in tqdm(pd.read_csv(csv_file_accounts, chunksize=chunk_size), desc=f'Reading accounts_{percentage} file in chunks'):
            future = processors.submit(_process_multi_input_redistribution_account_chunk, chunk)
            to_be_deleted, all_users = future.result()

            to_be_deleted_users.update(to_be_deleted)
            users.update(all_users)

    users = users.difference(to_be_deleted_users)

    with ThreadPoolExecutor(max_workers=1) as aggregator_executor:
        aggregator_future = [aggregator_executor.submit(aggregate_results)]

        with ProcessPoolExecutor() as processors:
            for chunk in tqdm(pd.read_csv(csv_file_balances, chunksize=chunk_size), desc=f'Reading balances_{percentage} file in chunks'):
                aggregation_queue.put(processors.submit(_process_multi_input_redistribution_balance_chunk, chunk, users))
            
            aggregation_queue.put(None)
            
            wait(aggregator_future)

    balances_array = np.array(balances, dtype=np.float64)
    balances_array_sorted = np.sort(balances_array)

    return balances_array_sorted, total_sum

def _process_only_redistribution_balance_chunk(chunk):
    # Extract balances and calculate the total sum in a vectorized manner
    local_balances = chunk['redistribution'].tolist()
    local_total_sum = chunk['redistribution'].sum()

    return local_balances, local_total_sum

def read_only_redistribution_csv_file(csv_file, percentage, chunk_size=10000000):
    balances = []
    total_sum = 0

    aggregation_queue = Queue(maxsize=10)
            
    def aggregate_results():
        nonlocal total_sum

        while True:
            local_variables = aggregation_queue.get()
            if local_variables == None:
                break
                
            local_balances, local_total_sum = local_variables.result()
            balances.extend(local_balances)
            total_sum += local_total_sum

        return balances, total_sum
    
    with ThreadPoolExecutor(max_workers=1) as aggregator_executor:
        aggregator_future = [aggregator_executor.submit(aggregate_results)]
        
        with ProcessPoolExecutor() as processors:
            for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc=f'Reading accounts_{percentage} file in chunks'):
                aggregation_queue.put(processors.submit(_process_only_redistribution_balance_chunk, chunk))
            
            aggregation_queue.put(None)
            
            wait(aggregator_future)

    balances_array = np.array(balances, dtype=np.float64)
    balances_array_sorted = np.sort(balances_array)

    return balances_array_sorted, total_sum