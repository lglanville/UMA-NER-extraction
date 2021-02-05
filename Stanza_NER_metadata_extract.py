import json
import csv
import argparse
from pprint import pprint
import stanza
from record_iterator import record_iterator


def return_ents(ident, proc_text):
    ents = {}
    for entity in proc_text.entities:
        if entity.start_char < 10:
            cstart = 0
        else:
            cstart = entity.start_char - 10
        if entity.end_char > len(proc_text.text) + 10:
            cend = len(proc_text.text)
        else:
            cend = entity.end_char + 10
        context = proc_text.text[cstart:cend]
        if entity.text not in ents.keys():
            ents[entity.text] = [
                    {'text': entity.text, 'record': ident, 'label': entity.type,
                        'context': context}]
        else:
            ents[entity.text].append(
                {'text': entity.text, 'record': ident, 'label': entity.type,
                    'context': context})
    return(ents)


def extract_entities(xmlfile, labels=['PERSON', 'ORG', 'NORP', 'WORK OF ART']):
    nlp = stanza.Pipeline('en')
    ents = {}
    for ident, lines in record_iterator(xmlfile):
        for line in lines:
            proc_line = nlp(line)
            line_ents = return_ents(ident, proc_line)
            for ent, instances in line_ents.items():
                if ent in ents.keys():
                    ents[ent].extend(instances)
                else:
                    if instances[0]['label'] in labels:
                        ents.update({ent: instances})
    return(ents)


def write_csv(csvfile, data):
    with open(csvfile, 'w', encoding='utf-8-sig', newline='') as f:
        fieldnames = ['label', 'text', 'records', 'context', 'occurrences']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for text, occurrences in data.items():
            records = []
            context = []
            for o in occurrences:
                records.append(o['record'])
                context.append(o['context'])
            row = {
                'text': text,
                'occurrences': len(occurrences),
                'label': occurrences[0]['label'],
                'records': '|'.join(records),
                'context': '|'.join(context)}
            writer.writerow(row)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='extract some entities')
    argparser.add_argument(
        'xmlfile', metavar='i', type=str,
        help='the base directory with your files')
    argparser.add_argument(
        '--ents', type=str, nargs='+',
        default=['PERSON', 'ORG', 'NORP', 'WORK OF ART'],
        help='the base directory with your files')
    argparser.add_argument(
        '--dump', type=str,
        help='dump JSON output to file')
    argparser.add_argument(
        '--csv', type=str,
        help='convert to csv')

    args = argparser.parse_args()
    ents = extract_entities(args.xmlfile, args.ents)
    if args.dump is not None:
        with open(args.dump, 'w') as f:
            json.dump(ents, f, indent=1)
    if args.csv is not None:
        write_csv(args.csv, ents)

    pprint(ents)
