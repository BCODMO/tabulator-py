# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
from tabulator import validate


# Tests

def test_validate_test_schemes():
    # Supported
    assert validate('path.csv')
    assert validate('file://path.csv')
    assert validate('http://example.com/path.csv')
    assert validate('https://example.com/path.csv')
    assert validate('ftp://example.com/path.csv')
    assert validate('ftps://example.com/path.csv')
    assert validate('path.csv', scheme='file')
    # Not supported
    assert not validate('ssh://example.com/path.csv')
    assert not validate('bad://example.com/path.csv')


def test_validate_test_formats():
    # Supported
    assert validate('path.csv')
    assert validate('path.json')
    assert validate('path.jsonl')
    assert validate('path.ndjson')
    assert validate('path.tsv')
    assert validate('path.xls')
    assert validate('path.ods')
    assert validate('path.no-format', format='csv')
    # Not supported
    assert not validate('path.txt')
    assert not validate('path.bad')


def test_validate_test_special():
    # Gsheet
    assert validate('https://docs.google.com/spreadsheets/d/id', format='csv')
    # File-like
    assert validate(io.open('data/table.csv', encoding='utf-8'), format='csv')
    # Text
    assert validate('text://name,value\n1,2', format='csv')
    # Inline
    assert validate([{'name': 'value'}])
