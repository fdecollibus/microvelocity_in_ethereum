import csv
import sys
import operator
import pandas as pd
import warnings
import time
import os
import glob
import zarr
import pickle
warnings.filterwarnings("ignore")
import pandas as pd, sys, os, csv
import csv
from glob import glob

# Create a dictionary to store the accounts and the working files in the folder where the script is executed
if not os.path.exists('temp'):
    os.makedirs('temp')

accounts= dict()
out_premined= open('sample_data/premined.csv','r')
reader_premined =csv.reader(out_premined)
for line in reader_premined:
    #print(line[0],int(line[1]))
    if line[0] not in accounts.keys():
        accounts[line[0].lower()]=[{},{}]
        accounts[line[0].lower()][0][0]=int(line[1])
    else :
        accounts[line[0].lower()][0][0]+=int(line[1])
out_premined.close()
miner_block={}
out_block_mine= open('sample_data/block_rewards.csv','r')
reader_block_mine =csv.DictReader(out_block_mine)
for line in reader_block_mine:
    miner_block[int(line['block_number'])]=line['miner'].lower()
    if (int(line['block_number'])%50000 == 0):
        print(line['block_number'].lower(),line['block_reward'])
    if line['miner'].lower() not in accounts.keys():
        accounts[line['miner'].lower()]=[{},{}]
        br = int(line['block_reward'])
        accounts[line['miner'].lower()][0][int(line['block_number'])]=int(br)
    else :
        br = int(line['block_reward'])
        accounts[line['miner'].lower()][0][int(line['block_number'])]=int(br)
out_block_mine.close()
out_uncle_mine= open('sample_data/uncle_rewards.csv','r')
counter_already = 0
reader_uncle_mine =csv.DictReader(out_uncle_mine)
for line in reader_uncle_mine:
    if (int(line['block_mined'])%10000 == 0):
        print(line['block_mined'],line['uncle_reward'])
    if line['miner'].lower() not in accounts.keys():
        accounts[line['miner'].lower()]=[{},{}]
        ur = int(line['uncle_reward'])
        accounts[line['miner'].lower()][0][int(line['block_mined'])]=int(ur)
    else :
        if int(line['block_mined']) not in accounts[line['miner'].lower()][0].keys():
            ur = int(line['uncle_reward'])
            accounts[line['miner'].lower()][0][int(line['block_mined'])]=int(ur)
        else:
            #print("already present ", line['miner'].lower(),line['block_mined'])
            counter_already+=1
            ur = int(line['uncle_reward'])
            accounts[line['miner'].lower()][0][int(line['block_mined'])]+=int(ur)
out_uncle_mine.close()
def calculateBlocks():
    blockTransfers = {}
    blocks = []
    baseLevel = 'sample_data/'
    for firstLevel in os.scandir(baseLevel):
        if firstLevel.name == 'txfees':
            for secondLevel in os.scandir(firstLevel):
                for thirdLevel in os.scandir(secondLevel):
                    from_block= int(secondLevel.name.split("=")[1])
                    to_block= int(thirdLevel.name.split("=")[1])
                    for fileLevel in (os.scandir(thirdLevel)):
                            blockTransfers[from_block]=fileLevel.path
    for key in blockTransfers.keys():
        blocks.append(key)
    blocks.sort()
    return blockTransfers,blocks

blockTransfers, blocks = calculateBlocks()

counter_error=0
counter_good=0

for _file in glob('sample_data/internal_tx/*.csv'):
    print(_file)
    out_fee= open(_file,'r')
    reader_fee =csv.DictReader(out_fee)
    for line in reader_fee:
        if int(line['isError'])<1:
            if len(line['to'])==0:
                if line['type'].lower()=='create':
                    line['to']=line['contractAddress'].lower()
                    counter_good+=1
                else:
                    #print('dont know what to do here')
                    counter_error+=1       
            if line['to'].lower() not in accounts.keys():
                accounts[line['to'].lower()]=[{},{}]
            if int(line['blockNumber']) not in accounts[line['to'].lower()][0].keys():
                accounts[line['to'].lower()][0][int(line['blockNumber'])]=int(line['value'])
            else:
                #print("already present ", line['to_address'].lower(),line['block_number'])
                accounts[line['to'].lower()][0][int(line['blockNumber'])]+=int(line['value'])
            if line['from'].lower() not in accounts.keys():
                accounts[line['from'].lower()]=[{},{}]
            if int(line['blockNumber']) not in accounts[line['from'].lower()][1].keys():
                accounts[line['from'].lower()][1][int(line['blockNumber'])]=int(line['value'])
            else:
                #print("already present ", line['from_address'].lower(),line['block_number'])
                accounts[line['from'].lower()][1][int(line['blockNumber'])]+=int(line['value'])
    out_fee.close()

print('Good:', counter_good , ' bad', counter_error)


###


for _block in blocks:
    if True:
        print(blockTransfers[_block])
        out_fee= open(blockTransfers[_block],'r')
        reader_fee =csv.DictReader(out_fee)
        for line in reader_fee:
            #Assets
            if int(line['status'])>0:
                if line['to_address'].lower() not in accounts.keys():
                    accounts[line['to_address'].lower()]=[{},{}]
                if int(line['block_number']) not in accounts[line['to_address'].lower()][0].keys():
                    accounts[line['to_address'].lower()][0][int(line['block_number'])]=int(line['value'])
                else:
                    #print("already present ", line['to_address'].lower(),line['block_number'])
                    accounts[line['to_address'].lower()][0][int(line['block_number'])]+=int(line['value'])
            #Miner always take its fee, even for reverted transactions
            accounts[miner_block[int(line['block_number'])]][0][int(line['block_number'])]+=int(line['fee'])

            #Liabilities

            if line['from_address'].lower() not in accounts.keys():
                accounts[line['from_address'].lower()]=[{},{}]
            if int(line['block_number']) not in accounts[line['from_address'].lower()][1].keys():
                accounts[line['from_address'].lower()][1][int(line['block_number'])]=int((int(line['status'])*(int(line['value'])))+int(line['fee']))
            else:
                #print("already present ", line['from_address'].lower(),line['block_number'])
                accounts[line['from_address'].lower()][1][int(line['block_number'])]+=int(((int(line['status'])*(int(line['value'])))+int(line['fee'])))
        out_fee.close()


with open('temp/int_raw_accounts.pickle', 'wb') as f:
    pickle.dump(accounts, f)

print('Done!')