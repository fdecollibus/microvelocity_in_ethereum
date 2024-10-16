import glob
import csv
import sys
import warnings
import time
import numpy as np
import pickle
from tqdm import tqdm
from concurrent import futures

#warnings.filterwarnings("ignore")
#up to just before London upgrade

with open('sample_data/weekly_blocks_list.pickle','rb') as outf:
    blocks_list = pickle.load(outf)
def processSlicedFile(filename):
    balances = {}
    print(f'processing {filename}')
    with open(filename, 'rb') as f:
        accounts = pickle.load(f)
    counter = 0
    for _key in accounts.keys():
        counter+=1        
        ind_balances=[]
        #only the ones with in and out contributes to the velocity of money (the second condition matters, payments you made, liabilities)
        for _block in blocks_list:
            total=0                                
            if len(accounts[_key][0].keys())>0:
                for _key_in in accounts[_key][0].keys():
                    if (_key_in <= _block):
                        total+=int(accounts[_key][0][_key_in])
            if len(accounts[_key][1].keys())>0:
                for _key_out in accounts[_key][1].keys():
                    if (_key_out <= _block):
                        total-=int(accounts[_key][1][_key_out])
            ind_balances.append(total)
        balances[_key]=ind_balances        
    with open(filename.replace('accounts','balances'), 'wb') as f2:
        pickle.dump(balances, f2)
    print(f'Done filename {filename}')


with futures.ProcessPoolExecutor(max_workers=5) as ex:
    for _filename in  glob.glob('temp/sliced_accounts_*.pickle'):
        ex.submit(processSlicedFile, _filename)
    print('******MAIN******: closing')
