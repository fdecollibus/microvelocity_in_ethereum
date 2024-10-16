#guardare qui
import glob
import csv
import sys
import warnings
import time
import numpy as np
import pickle
from tqdm import tqdm
from concurrent import futures
#accounts = pickle.load(open('tools/sliced_accounts_2945.pickle','rb'))
#balance = pickle.load(open('ind_balances/sliced_balances_2945.pickle','rb'))



#warnings.filterwarnings("ignore")
#up to just before London upgrade
LIMIT=12950000


with open('weekly_blocks_list.pickle','rb') as outf:
    blocks_list_temp = pickle.load(outf)

blocks_list=[]
for _block in blocks_list_temp[::4]:
    blocks_list.append(_block)


def processFile(filename):
    print('Processing ', filename)
    velocities = {}
    balances = {}
    accounts = pickle.load(open(filename,'rb'))
    for _key in accounts.keys():
        is_good=True
        #only the ones with in and out contributes to the velocity of money (the second condition matters, payments you made, liabilities)
        if len(accounts[_key][0])>0 and len(accounts[_key][1])>0:
            ind_balances=[]
            #only the ones with in and out contributes to the velocity of money (the second condition matters, payments you made, liabilities)
            for _block in blocks_list:
                total=0                                
                if len(accounts[_key][0].keys())>0:
                    #print('mininputs', min(inputs))
                    for _key_in in accounts[_key][0].keys():
                        if (_key_in <= _block):
                            #print('IN:', int(accounts[_key][0][_key_in]))
                            total+=int(accounts[_key][0][_key_in])
                if len(accounts[_key][1].keys())>0:
                    #print('mininputs', min(inputs))
                    for _key_out in accounts[_key][1].keys():
                        if (_key_out <= _block):
                            total-=int(accounts[_key][1][_key_out])
                ind_balances.append(total)
            ind_balances= np.array(ind_balances)
            if not (ind_balances>=0).all():
                #print('Balances somewhere negative quitting for ', _key)
                is_good=False
            if is_good:
                ind_velocity=np.zeros(LIMIT)
                #In LIFO SCHEME same blocks do not contribute to velocity
                listkeys = [key for key in accounts[_key][0]]
                for _teskey in listkeys:
                    if _teskey in accounts[_key][1].keys():
                        if accounts[_key][0][_teskey]==accounts[_key][1][_teskey]:
                            #print(_key, 'pop same at block:', _teskey, ' value: ' , accounts[_key][0][_teskey], accounts[_key][1][_teskey] )
                            accounts[_key][0].pop(_teskey)
                            accounts[_key][1].pop(_teskey)
                        elif accounts[_key][0][_teskey] > accounts[_key][1][_teskey]:
                            #print(_key, 'pop passive at block:', _teskey, ' value: ' , accounts[_key][1][_teskey] )
                            accounts[_key][0][_teskey]-= accounts[_key][1][_teskey]
                            accounts[_key][1].pop(_teskey)
                        elif accounts[_key][0][_teskey] < accounts[_key][1][_teskey]:
                            #print(_key, 'pop active at block:', _teskey, ' value: ' , accounts[_key][0][_teskey] )
                            accounts[_key][1][_teskey]-= accounts[_key][0][_teskey]
                            accounts[_key][0].pop(_teskey)
                #If they have still something
                if len(accounts[_key][0])>0 and len(accounts[_key][1])>0:            
                    arrangedKeys=[list(accounts[_key][0].keys()),list(accounts[_key][1].keys())]
                    arrangedKeys[0].sort()
                    arrangedKeys[1].sort()
                    iterationnum=0

                    for _border in arrangedKeys[1]:
                        iterationnum+=1
                        arrangedKeys[0]=list(accounts[_key][0].keys())
                        test=np.array(arrangedKeys[0])
                        #modifica magica 
                        test = np.sort(test)
                        border=_border
                        for i in range(0, len(test[test<border])):
                            counter = test[test<border][(len(test[test<border])-1)-i]
                            if (accounts[_key][0][counter] - accounts[_key][1][border]) >= 0:
                                ind_velocity[counter:border]+=(accounts[_key][1][border])/(border-counter)
                                for _cou in range(0,len(ind_velocity[blocks_list])):
                                    if ind_velocity[blocks_list][_cou]> ind_balances[_cou]:
                                        #print('1 CROSSED', _key, iterationnum ,counter, border,_cou,ind_velocity[blocks_list][_cou], ind_balances[_cou])
                                        is_good=False
                                        break
                                #print('Popping ingoing itern ', iterationnum, border, accounts[_key][1][border], 'reducing ')
                                accounts[_key][0][counter] -= accounts[_key][1][border]
                                accounts[_key][1].pop(border)
                                break
                            else:
                                ind_velocity[counter:border]+=(accounts[_key][0][counter])/(border-counter)
                                for _cou in range(0,len(ind_velocity[blocks_list])):
                                    if ind_velocity[blocks_list][_cou]> ind_balances[_cou]:
                                        #print('2 CROSSED', _key, iterationnum, counter, border,_cou, ind_velocity[blocks_list][_cou], ind_balances[_cou])
                                        is_good=False
                                        break
                                #print('Popping outgoing itern ', iterationnum ,counter, accounts[_key][0][counter])
                                accounts[_key][1][border] -= accounts[_key][0][counter]
                                accounts[_key][0].pop(counter)
                            #if iterationnum==149:
                                #sample= accounts.copy()
                    #Address contributing to the velocity and correctly inserted here
            if is_good:
                if  (ind_balances>0).any() and (ind_velocity[blocks_list]>0).any():
                    #print('Adding ',_key)
                    velocities[_key]=ind_velocity[blocks_list]
                    balances[_key]=ind_balances  
    pickle.dump([velocities,balances], open(filename.replace('tools','ind_lifo_monthly'),'wb'))
    print('done', filename)


with futures.ProcessPoolExecutor(max_workers=48) as ex:
    for _filename in  glob.glob('tools/sliced_accounts_*.pickle'):
        ex.submit(processFile, _filename)
    print('******MAIN******: closing')
