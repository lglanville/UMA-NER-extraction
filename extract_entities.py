import json
import csv
import argparse
from pprint import pprint
from lxml import etree
from concurrent.futures import ProcessPoolExecutor

LABELS = ['PERSON', 'ORG', 'GPE', 'LOC', 'FAC', 'EVENT', 'LANGUAGE']


def xml_iterator(xmlfile):
    """iterate through an EMu XML export, return an identifier and lines of
    descriptive text from title and scope and content"""
    for event, record in etree.iterparse(xmlfile, tag='tuple'):
        title = record.findtext('atom[@name="EADUnitTitle"]')
        scope = record.findtext('atom[@name="EADScopeAndContent"]')
        ident = record.findtext('atom[@name="EADUnitID"]')
        if ident is not None and record.attrib == {}:
            lines = []
            if title is not None:
                lines.append(title)
            if scope is not None:
                lines.extend(scope.splitlines())
            record.clear()
            yield ident, lines


def csv_iterator(csvfile):
    """iterate through an EMu CSV export, return an identifier and lines of
    descriptive text from title and scope and content"""
    with open(csvfile, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for record in reader:
            lines = []
            title = record.get("EADUnitTitle")
            scope = record.get("EADScopeAndContent")
            ident = record.get("EADUnitID")
            if title is not None:
                lines.append(title)
            if scope is not None:
                lines.extend(scope.splitlines())
            yield ident, lines


def reconcile_entities(futures):
    ents = {}
    for f in futures:
        line_ents = f.result()
        for ent, data in line_ents.items():
            if ent in ents.keys():
                ents[ent]['occurrences'].extend(data['occurrences'])
            else:
                ents[ent] = data
    return(ents)


def stanza_return_ents(ident, proc_text, labels=LABELS):
    "Return a dictionary of entities from a line of text processed with Stanza"
    ents = {}
    for entity in proc_text.entities:
        if entity.type in labels:
            print(ident, entity.type, entity.text)
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
                ents[entity.text] = {'label': entity.type, 'occurrences': [
                        {
                            'text': entity.text, 'record': ident,
                            'label': entity.type, 'context': context
                            }
                        ], 'label': entity.type}
            else:
                ents[entity.text]['occurrences'].append({
                    'text': entity.text, 'record': ident,
                    'label': entity.type, 'context': context})
    return(ents)


def stanza_extract_entities(datafile, labels=LABELS):
    """Extract named entities from an EMu xml file using Stanza,
    returning a dictionary"""
    import stanza
    try:
        nlp = stanza.Pipeline('en')
    except ValueError:
        print('English model has not been downloaded. Downloading now.')
        stanza.download('en')
        nlp = stanza.Pipeline('en')
    futures = []
    if datafile.endswith('.xml'):
        records = xml_iterator(datafile)
    else:
        records = csv_iterator(datafile)
    with ProcessPoolExecutor() as ex:
        for ident, lines in records:
            for line in lines:
                proc_line = nlp(line)
                futures.append(ex.submit(stanza_return_ents, ident, proc_line, labels=labels))
    ents = reconcile_entities(futures)
    return(ents)


def spacy_return_ents(ident, proc_text, labels=LABELS):
    "Return a dictionary of entities from a line of text processed with Spacy"
    ents = {}
    for entity in proc_text.ents:
        if entity.label_ in labels:
            print(ident, entity.label_, entity.text)
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
                ents[entity.text] = {'label': entity.label_, 'occurrences': [
                        {'text': entity.text, 'record': ident, 'label': entity.label_,
                            'context': context.text}]}
            else:
                ents[entity.text]['occurrences'].append(
                    {'text': entity.text, 'record': ident, 'label': entity.label_,
                        'context': context.text})
    return(ents)


def spacy_extract_entities(datafile, labels=LABELS):
    """Extract named entities from an EMu xml file using Spacy,
    returning a dictionary"""
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    # known issue with Spacy
    import en_core_web_sm
    nlp = en_core_web_sm.load()
    ents = {}
    futures = []
    if datafile.endswith('.xml'):
        records = xml_iterator(datafile)
    else:
        records = csv_iterator(datafile)
    with ProcessPoolExecutor() as ex:
        for ident, lines in records:
            for line in lines:
                proc_line = nlp(line)
                futures.append(ex.submit(spacy_return_ents, ident, proc_line, labels=labels))
    ents = reconcile_entities(futures)
    return(ents)


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
        default=LABELS,
        help='the base directory with your files')
    argparser.add_argument(
        '--dump', type=str,
        help='dump JSON output to file')
    argparser.add_argument(
        '--csv', type=str,
        help='convert to csv')

    args = argparser.parse_args()
    if args.processor == 'stanza':
        ents = stanza_extract_entities(args.xmlfile, args.ents)
    elif args.processor == 'spacy':
        ents = spacy_extract_entities(args.xmlfile, args.ents)
    if args.dump is not None:
        with open(args.dump, 'w') as f:
            json.dump(ents, f, indent=1)
    if args.csv is not None:
        write_csv(args.csv, ents)

    pprint(ents)
