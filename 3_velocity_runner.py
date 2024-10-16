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
LIMIT=12950000

def processSlicedFile(filename):
    velocities = {}
    print(f'processing {filename}')
    with open(filename, 'rb') as f:
        accounts = pickle.load(f)
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
    for _key in velocities.keys():
        final_velocity+=velocities[_key]
    with open(filename.replace('accounts','results').replace('tools','returns'), 'wb') as f2:
        pickle.dump(final_velocity, f2)
    print(f'Done filename {filename}')


with futures.ProcessPoolExecutor(max_workers=14) as ex:
    for _filename in  glob.glob('temp/sliced_accounts_*.pickle'):
        ex.submit(processSlicedFile, _filename)
    print('******MAIN******: closing')


