import csv
import warnings
import os
import pickle
warnings.filterwarnings("ignore")
import csv
from glob import glob
import numpy as np

# Results will be stored here
if not os.path.exists('temp'):
    os.makedirs('temp')

# Variables to track min and max block_number
min_block_number = float('inf')
max_block_number = float('-inf')


accounts= dict()

with open('sample_data/general_allocated.csv', 'r') as out_allocated:
    reader_allocated = csv.DictReader(out_allocated)
    for line in reader_allocated:
        print(line)
        to_address = line['to_address'].lower()
        amount = int(line['amount'])
        block_number = int(line['block_number'])
        if to_address not in accounts:
            accounts[to_address] = [{}, {}]
        elif block_number not in accounts[to_address][0]:
            accounts[to_address][0][block_number] = amount
        else:
            accounts[to_address][0][block_number] += amount
        
        min_block_number = min(min_block_number, block_number)
        max_block_number = max(max_block_number, block_number)



with open('sample_data/general_transfers.csv', 'r') as out_transfers:
    reader_transfers = csv.DictReader(out_transfers)
    for line in reader_transfers:
        from_address = line['from_address'].lower()
        to_address = line['to_address'].lower()
        amount = int(line['amount'])
        block_number = int(line['block_number'])
        # Assets
        if to_address not in accounts:
            accounts[to_address] = [{}, {}]
        if block_number not in accounts[to_address][0]:
            accounts[to_address][0][block_number] = amount
        else:
            accounts[to_address][0][block_number] += amount
        # Liabilities
        if from_address not in accounts:
            accounts[from_address] = [{}, {}]
        if block_number not in accounts[from_address][1]:
            accounts[from_address][1][block_number] = amount
        else:
            accounts[from_address][1][block_number] += amount
        
        min_block_number = min(min_block_number, block_number)
        max_block_number = max(max_block_number, block_number)

# Limit is the difference between the max and min block numbers in the dataset
# Is the number of blocks we have to process and store, take it into account
LIMIT = max_block_number - min_block_number 
print(f'Min block number: {min_block_number}')
print(f'Max block number: {max_block_number}')
print(f'Number of blocks: {LIMIT}')
velocities = {}
#This fake condition to save indentation
if True:
    for _key in accounts.keys():
        #only the ones with in and out contributes to the velocity of money (the second condition matters, payments you made, liabilities)
        if len(accounts[_key][0])>0 and len(accounts[_key][1])>0:
            arrangedKeys=[list(accounts[_key][0].keys()),list(accounts[_key][1].keys())]
            arrangedKeys[0].sort()
            arrangedKeys[1].sort()
            ind_velocity=np.zeros(LIMIT)
            for _border in arrangedKeys[1]:
                arrangedKeys[0]=list(accounts[_key][0].keys())
                test=np.array(arrangedKeys[0])
                border=_border
                for i in range(0, len(test[test<border])):
                    counter = test[test<border][(len(test[test<border])-1)-i]
                    if (accounts[_key][0][counter] - accounts[_key][1][border]) >= 0:
                        ind_velocity[counter:border]+=(accounts[_key][1][border])/(border-counter)
                        accounts[_key][0][counter] -= accounts[_key][1][border]
                        accounts[_key][1].pop(border)
                        break
                    else:
                        ind_velocity[counter:border]+=(accounts[_key][0][counter])/(border-counter)
                        accounts[_key][1][border] -= accounts[_key][0][counter]
                        accounts[_key][0].pop(counter)
            velocities[_key]=ind_velocity
    final_velocity=np.zeros(LIMIT)

# This file contains the accounts with their balances and the velocities
with open('temp/general_velocities.pickle', 'wb') as f:
    pickle.dump([accounts,velocities], f)

print('Done!')