from lxml import etree


def record_iterator(xmlfile):
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
