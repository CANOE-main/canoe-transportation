"""Read-only reconciliation of presentation records to the requested SQLite pair."""

from collections import Counter
import hashlib
import json
from math import isclose
from pathlib import Path
import sqlite3
import sys
from xml.etree import ElementTree as ET
from zipfile import ZipFile

BUILD = Path(__file__).resolve().parent
ROOT = BUILD.parents[4]
evidence = json.loads((BUILD / 'chart_evidence.json').read_text(encoding='utf-8'))
manifest = json.loads((BUILD / 'slide_manifest.json').read_text(encoding='utf-8'))
deck = Path(sys.argv[1]).resolve()
hashes = {}
connections = {}
for side in ('candidate', 'legacy'):
    source = evidence['sources'][side]
    source_path = Path(source['path'])
    digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    assert digest == source['sha256'], (side, 'database changed')
    hashes[side] = digest
    connections[side] = sqlite3.connect(source_path.as_uri() + '?mode=ro', uri=True)
    connections[side].execute('PRAGMA query_only=ON')
    connections[side].row_factory = sqlite3.Row

VALUE = {'cost_fixed': 'cost', 'cost_invest': 'cost', 'cost_variable': 'cost',
         'demand': 'demand', 'efficiency': 'efficiency', 'existing_capacity': 'capacity',
         'limit_annual_capacity_factor': 'factor'}
LEGACY = {'cost_fixed': 'CostFixed', 'cost_invest': 'CostInvest',
          'cost_variable': 'CostVariable', 'demand': 'Demand',
          'efficiency': 'Efficiency', 'existing_capacity': 'ExistingCapacity'}
record_count = 0
query_count = 0


def read_value(side, table, keys, field):
    global query_count
    where = ' AND '.join(f'"{k}"=?' for k in keys)
    rows = connections[side].execute(
        f'SELECT "{field}" FROM "{table}" WHERE {where}', tuple(keys.values()),
    ).fetchall()
    query_count += 1
    assert len(rows) == 1, (side, table, keys, len(rows))
    return float(rows[0][0])


def equal(actual, expected, label):
    assert isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12), (label, actual, expected)


for chart in manifest['charts']:
    for r in chart['records']:
        record_count += 1
        param = r['table']
        value = VALUE[param]
        for side in ('candidate', 'legacy'):
            table = param if side == 'candidate' else LEGACY.get(param)
            parts = r.get(side + '_constituent_rows')
            if isinstance(parts, list):
                values = []
                for part in parts:
                    keys = {'region': 'ON', 'tech': part['tech'], 'vintage': part['vintage']}
                    observed = read_value(side, table, keys, value)
                    equal(observed, part[value], (side, keys))
                    values.append(observed)
                equal(sum(values), r[side], ('sum', side, r['label']))
            elif side == 'legacy' and param == 'limit_annual_capacity_factor':
                keys = {'region': 'ON', 'tech': r['tech_or_group'],
                        'period': int(r['comparison_period']), 'output_comm': r['output_comm']}
                equal(read_value(side, 'MinAnnualCapacityFactor', keys, 'factor'),
                      r['legacy_lower'], ('lower', r['label']))
                equal(read_value(side, 'MaxAnnualCapacityFactor', keys, 'factor'),
                      r['legacy_upper'], ('upper', r['label']))
            else:
                keys = r.get('legacy_keys', r['keys']) if side == 'legacy' else r['keys']
                if isinstance(keys, list):
                    for item in keys:
                        equal(read_value(side, table, item, value), r[side], (side, item))
                else:
                    equal(read_value(side, table, keys, value), r[side], (side, keys))
        if 'relative_percent' in r:
            equal(100 * (r['candidate'] - r['legacy']) / abs(r['legacy']),
                  r['relative_percent'], ('percent', r['label']))

ns = {'c': 'http://schemas.openxmlformats.org/drawingml/2006/chart',
      'a': 'http://schemas.openxmlformats.org/drawingml/2006/main'}
chart_series = []
with ZipFile(deck) as archive:
    assert archive.testzip() is None
    names = archive.namelist()
    chart_names = sorted([n for n in names if '/charts/chart' in n and n.endswith('.xml')],
                         key=lambda n: int(Path(n).stem.removeprefix('chart')))
    assert len(chart_names) == len(manifest['charts']) == 13
    for chart_name, expected in zip(chart_names, manifest['charts'], strict=True):
        root = ET.fromstring(archive.read(chart_name))
        series = root.findall('.//c:barChart/c:ser', ns)
        assert len(series) == len(expected['series']), chart_name
        for actual, wanted in zip(series, expected['series'], strict=True):
            values = [float(v.text) for v in actual.findall('./c:val//c:pt/c:v', ns)]
            assert values == wanted['values'], (chart_name, values, wanted)
            chart_series.append({'chart': chart_name, 'name': wanted['name'], 'values': values})
    slide_xmls = [n for n in names if n.startswith('ppt/slides/slide') and n.endswith('.xml')]
    assert len(slide_xmls) == 13
    workbooks = [n for n in names if n.startswith('ppt/embeddings/') and n.endswith('.xlsx')]
    assert len(workbooks) == 13
    text = '\n'.join(' '.join(ET.fromstring(archive.read(n)).itertext()) for n in slide_xmls)
    assert 'Hypothesis:' in text
    assert 'Common 2020 cohort' not in text
    assert '2019' in text and '2023' in text and '2021' in text

counts = Counter(p['parameter'] for p in manifest['pages'] if p['parameter'] != 'cover')
assert set(counts) == set(VALUE)
assert all(1 <= v <= 3 for v in counts.values())
for side, connection in connections.items():
    connection.close()
    assert hashlib.sha256(Path(evidence['sources'][side]['path']).read_bytes()).hexdigest() == hashes[side]

result = {'passed': True, 'plotted_records': record_count, 'read_only_sqlite_queries': query_count,
          'parameter_slide_counts': dict(counts), 'native_charts': 13, 'embedded_workbooks': 13,
          'source_hashes': hashes, 'deck_sha256': hashlib.sha256(deck.read_bytes()).hexdigest(),
          'chart_series_reconciled': len(chart_series), 'chart_precision': '10 significant figures; source values retained in notes'}
(BUILD / 'numeric_validation.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps(result, indent=2))
