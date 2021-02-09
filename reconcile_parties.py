import csv
import sys
from pathlib import Path
from fuzzywuzzy import fuzz, process
from geopy.geocoders import ArcGIS


def get_parties():
    csvpath = Path(__file__).parent / 'eparties.csv'
    with open(csvpath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        parties = {row['NamFullName']: row['irn'] for row in reader}
        return parties


def reconcile_parties(csvfile):
    rows = []
    parties = get_parties()
    geolocator = ArcGIS()
    with open(csvfile, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row['label'] in ('PERSON', 'ORG'):
                r = process.extractOne(
                    row['text'], parties.keys(), score_cutoff=90,
                    scorer=fuzz.token_sort_ratio)
                if r is not None:
                    row['EMU name'] = r[0]
                    row['EMU IRN'] = parties[r[0]]
                    row['match score'] = r[1]
                    print(f'Matched {row["text"]} with {r[0]}')
            elif row['label'] in ('LOC', 'GPE'):
                r = geolocator.geocode(row['text'])
                if r is not None:
                    row['address'] = r.address
                    row['latitude'] = r.latitude
                    row['longitude'] = r.longitude
                    print(f'Matched {row["text"]} with {r.address}')
            rows.append(row)
    fieldnames.extend([
        'EMU name', 'EMU IRN', 'match score',
        'address', 'latitude', 'longitude'])
    with open(csvfile, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    reconcile_parties(sys.argv[1])
