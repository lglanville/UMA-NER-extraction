import json
import csv
import argparse
from pprint import pprint
import ner_funcs


def write_csv(csvfile, data):
    """writes a csv file of entities, sorted by the frequency from highest to
    lowest"""
    rows = []
    for text, data in data.items():
        records = []
        context = []
        for o in data['occurrences']:
            records.append(o['record'])
            context.append(o['context'])
        row = {
            'text': text,
            'occurrences': len(data['occurrences']),
            'label': data['label'],
            'records': '|'.join(records),
            'context': '|'.join(context)}
        rows.append(row)
        rows = sorted(rows, key=lambda row: row['occurrences'], reverse=True)
        with open(csvfile, 'w', encoding='utf-8-sig', newline='') as f:
            fieldnames = ['label', 'text', 'records', 'context', 'occurrences']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='extract some entities')
    argparser.add_argument(
        'xmlfile', metavar='i', type=str,
        help='the base directory with your files')
    argparser.add_argument(
        '--processor', '-p', type=str,
        default='stanza', choices=('spacy', 'stanza'),
        help='processor to extract entities with')
    argparser.add_argument(
        '--ents', type=str, nargs='+',
        default=['PERSON', 'ORG', 'GPE', 'LOC', 'FAC', 'EVENT', 'LANGUAGE'],
        help='the base directory with your files')
    argparser.add_argument(
        '--dump', type=str,
        help='dump JSON output to file')
    argparser.add_argument(
        '--csv', type=str,
        help='convert to csv')

    args = argparser.parse_args()
    if args.processor == 'stanza':
        ents = ner_funcs.stanza_extract_entities(args.xmlfile, args.ents)
    elif args.processor == 'spacy':
        ents = ner_funcs.spacy_extract_entities(args.xmlfile, args.ents)
    if args.dump is not None:
        with open(args.dump, 'w') as f:
            json.dump(ents, f, indent=1)
    if args.csv is not None:
        write_csv(args.csv, ents)

    pprint(ents)
