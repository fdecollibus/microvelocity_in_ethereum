
#This file slices the accounts file into smaller and more manageable files
import pickle 
import itertools
import os
#Create a temp folder if not exists
if not os.path.exists('temp'):
    os.makedirs('temp')
with open('temp/int_raw_accounts.pickle', 'rb') as f:
    accounts = pickle.load(f)
filecounter=1
for _i in range(0,len(accounts.keys()),20000):
    temp  = dict(itertools.islice(accounts.items(),_i,_i+20000))
    print (len(temp.keys()))
    with open(f'temp/sliced_accounts_{filecounter}.pickle', 'wb') as f:
        pickle.dump(temp, f)
    filecounter+=1
print('Done!')

