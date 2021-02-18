# A bunch of code and experiments for extracting entities from University of Melbourne Archives textual metadata

## Requirements
Python 3 (tested using 3.9, earlier versions may not work).
Spacy and the en_core_web_sm model are required. To install the model,
first `pip install spacy` then `python -m spacy download en_core_web_sm`.
Stanza is also required. `pip install stanza`. When script is first run, it
should download the en model automatically.

## usage
Export an xml or csv file from EMu with the EADUnitTitle and EADScopeAndContent
fields. Use this as in put for the extract_entities.py script.
`python extract_entities.py <EMu data file>`
Additional arguments. You can dump the resulting JSON to a file using the `--json`
parameter, or csv using `--csv <new csv file>`. To restrict or expand the
entities extracted, use the `--ents` parameter. For example, to only extract names
of people and organisations, use `--ents PERSON ORG`.
