# coding: utf-8

import re


ISBN_RE = re.compile(r'^(\d{9}|\d{12})[\dXx]$')
ISBN10_RE = re.compile(r'^\d{9}[\dXx]$')
# accept invalid ISBN-13 ends with X because some scraped-item have wrong ISBN
ISBN13_RE = re.compile(r'^\d{12}[\dXx]$')


def is_isbn(string):
    """
    ISBNでありそうかどうかを判別する。
    チェックディジットが合っているかは考慮しない。

>>> is_isbn("123456789")
False
>>> is_isbn("1234567890")
True
>>> is_isbn("123456789X")
True
>>> is_isbn("123456789X")
True
>>> is_isbn("123456789E")
False
>>> is_isbn("12345678901")
False
>>> is_isbn("1234567890123")
True
>>> is_isbn("123456789012X")
True
>>> is_isbn("123456789012x")
True
>>> is_isbn("123456789012E")
False
>>> is_isbn("12345678901234")
False
    """

    return bool(ISBN_RE.search(string))


def ensure_isbn13(isbn):
    """
    ISBNを13桁にして返す。
    チェックディジットは必ず再計算されるので、
    チェックディジットが間違っていてもOK

    ISBNでない文字列を渡した場合はValueErrorになるが、
    空文字の場合のみ空文字を返す。

>>> ensure_isbn13("1234567890128")
'1234567890128'
>>> ensure_isbn13("1234567890")
'9781234567897'
>>> ensure_isbn13("978140191943X") # wrong check digit
'9781401919436'
>>> ensure_isbn13("123456789")
Traceback (most recent call last):
...
ValueError: Invalid isbn: 123456789
>>> ensure_isbn13("")
''
    """

    if isbn == '':
        return ''

    if ISBN13_RE.search(isbn):
        # re-calculate check digit in case of wrong check digit
        prefix = isbn[:-1]
        check = _check_digit_13(prefix)
        return prefix + check

    if ISBN10_RE.search(isbn):
        return _convert_10_to_13(isbn)

    raise ValueError('Invalid isbn: {0}'.format(isbn))


def ensure_isbn10(isbn):
    """
    ISBNを10桁にして返す。
    チェックディジットは必ず再計算されるので、
    チェックディジットが間違っていてもOK

    ISBNでない文字列を渡した場合はValueErrorになるが、
    空文字の場合のみ空文字を返す。

>>> ensure_isbn10("9784062145909")
'4062145901'
>>> ensure_isbn10("9784915512698")
'491551269X'
>>> ensure_isbn10("491551269X")
'491551269X'
>>> ensure_isbn10("4915512693") # wrong check digit
'491551269X'
>>> ensure_isbn10("123456789")
Traceback (most recent call last):
...
ValueError: Invalid ISBN: 123456789
>>> ensure_isbn10("")
''
    """

    if isbn == '':
        return ''

    if ISBN10_RE.search(isbn):
        # re-calculate check digit in case of wrong check digit
        prefix = isbn[:-1]
        check = _check_digit_10(prefix)
        return prefix + check

    if ISBN13_RE.search(isbn):
        return _convert_13_to_10(isbn)

    raise ValueError('Invalid ISBN: {0}'.format(isbn))


def _check_digit_13(isbn):
    """
    ISBN13の最初の12桁からチェックディジット（13桁目）を取得する
    """
    assert len(isbn) == 12

    checksum = 0
    for i, c in enumerate(isbn):
        if i % 2:
            w = 3
        else:
            w = 1
        checksum += w * int(c)

    r = 10 - (checksum % 10)
    if r == 10:
        return '0'
    else:
        return str(r)


def _check_digit_10(isbn):
    """
    ISBN10の最初の9桁からチェックディジット（10桁目）を取得する
    """
    assert len(isbn) == 9

    checksum = 0
    for i, c in enumerate(isbn):
        w = 10 - i
        checksum += w * int(c)

    r = 11 - (checksum % 11)
    if r == 10:
        return 'X'
    else:
        return str(r)


def _convert_10_to_13(isbn):
    assert len(isbn) == 10
    prefix = '978' + isbn[:-1]
    check = _check_digit_13(prefix)
    return prefix + check


def _convert_13_to_10(isbn):
    assert len(isbn) == 13
    prefix = isbn[3:-1]
    check = _check_digit_10(prefix)
    return prefix + check
