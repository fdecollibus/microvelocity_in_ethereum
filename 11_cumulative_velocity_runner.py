import glob
import pickle
list_velocities=[]
for _i in range(0,79):
    list_velocities.append({})

for _file in glob.glob('ind_monthly/sliced_accounts_*.pickle'):
    print('Processing ', _file)
    velocities = pickle.load(open(_file,'rb'))
    for _key in velocities[0].keys():
        for _count in range(0,len(velocities[0][_key])):
            if(velocities[0][_key][_count]>0):
                list_velocities[_count][_key]=velocities[0][_key][_count]
pickle.dump(list_velocities,open('campajola_velocities_new.pickle','wb'))
print('Done')
