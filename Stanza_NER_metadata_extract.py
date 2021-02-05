import json
import csv
import argparse
from pprint import pprint
import warnings
from lxml import etree
warnings.filterwarnings("ignore", category=RuntimeWarning)
import en_core_web_sm


def return_ents(ident, proc_text):
    ents = {}
    for entity in proc_text.ents:
        if entity.start < 3:
            cstart = 0
        else:
            cstart = entity.start - 3
        if entity.end > len(proc_text) + 3:
            cend = len(proc_text)
        else:
            cend = entity.end + 3
        context = proc_text[cstart:cend]
        if entity.text not in ents.keys():
            ents[entity.text] = [
                    {'text': entity.text, 'record': ident, 'label': entity.label_,
                        'context': context.text}]
        else:
            ents[entity.text].append(
                {'text': entity.text, 'record': ident, 'label': entity.label_,
                    'context': context.text})
    return(ents)


def extract_entities(xmlfile, labels=['PERSON', 'ORG', 'NORP', 'WORK OF ART']):
    nlp = en_core_web_sm.load()
    ents = {}
    et = etree.parse(xmlfile)
    root = et.getroot()
    for record in root:
        title = record.findtext('atom[@name="EADUnitTitle"]')
        scope = record.findtext('atom[@name="EADScopeAndContent"]')
        ident = record.findtext('atom[@name="EADUnitID"]')
        text = str(title) + '\n' + str(scope)
        for line in text.splitlines():
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
