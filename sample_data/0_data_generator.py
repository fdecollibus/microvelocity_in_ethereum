import csv
import random

# Leggi il file CSV originale e estrai gli indirizzi
addresses = []
with open('general_allocated.csv', 'r') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        addresses.append(row['to_address'])

# Crea un nuovo file CSV con le colonne richieste
with open('general_transfers.csv', 'w', newline='') as outfile:
    fieldnames = ['block_number', 'from_address', 'to_address', 'amount']
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for block_number in range(30, 501):
        from_address = random.choice(addresses)
        to_address = random.choice(addresses)
        amount = random.randint(1, 9999)
        writer.writerow({
            'block_number': block_number,
            'from_address': from_address,
            'to_address': to_address,
            'amount': amount
        })
