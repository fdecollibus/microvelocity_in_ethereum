from glob import glob
import pickle
indexes = {}


accumulator=[[],[]]
for _i in range(0,79):
    accumulator[0].append([])
    accumulator[1].append([])
#for _file in glob('/srv/abacus-2/projects/eth_velocity/ind_monthly/sliced_overall_*.pickle'):
for _file in glob('ind_fifo_monthly/sliced_*.pickle'):
    indexes[int(_file.split('/')[-1].split('_')[-1].split('.')[0])]=_file

keys = sorted(list(indexes.keys()))
for _key in keys:
    print(_key)
    print(indexes[_key])
    loader = pickle.load(open(indexes[_key],'rb'))
    for _key in loader[0].keys():
        for _i in range(0,79):
            if loader[0][_key][_i]>0:
                accumulator[0][_i].append(loader[0][_key][_i])
                accumulator[1][_i].append(loader[1][_key][_i])
pickle.dump(accumulator, open('tmp/file_for_correlation_fifo.pickle','wb'))
