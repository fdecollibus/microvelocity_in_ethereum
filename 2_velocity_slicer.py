import pickle 
import itertools
with open('raw_accounts.pickle', 'rb') as f:
    accounts = pickle.load(f)
filecounter=1
for _i in range(0,len(accounts.keys()),20000):
    temp  = dict(itertools.islice(accounts.items(),_i,_i+20000))
    #ciao  = dict(itertools.islice(accounts.items(),_i,(_i+1000>len(accounts.keys()))?_i+1000:len(accounts.keys())))
    print (len(temp.keys()))
    with open(f'tmp/sliced_accounts_{filecounter}.pickle', 'wb') as f:
        pickle.dump(temp, f)
    filecounter+=1
print('Done!')

