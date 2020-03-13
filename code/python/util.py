#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

from typing import IO
from contextlib import contextmanager


def compress_string(string: str, debugging=False) -> str:
    """
    Compress a UTF-8 string in a way safe for passage as an argument through fork/exec, but not necessarily shells

    :param string: string to be compressed
    :param debugging: whether to print output potentially helpful in debugging
    :return: string of base64-encoded bytes
    """
    from zlib import compress
    from base64 import b64encode

    string_bytes = string.encode('utf-8')
    if debugging:
        print('initial string is {} bytes in size'.format(len(string_bytes)))
    string_compressed = compress(string_bytes)
    if debugging:
        print('string is {} bytes in size after compression with zlib (default level, 6)'
              .format(len(string_compressed)))
        for n in range(1, 10):
            print('string is {} bytes in size after compression with zlib (level {})'
                  .format(n, len(compress(string_bytes, level=n))))
    string_b64 = b64encode(string_compressed)
    if debugging:
        print('string is {} bytes in size after base64-encoding'.format(len(string_b64)))
    return string_b64.decode('utf-8')


def decompress_string(string: str) -> str:
    """
    Decompress a UTF-8 string compressed by compress_string

    :param string: base64-encoded string to be decompressed
    :return: original string
    """
    from zlib import decompress
    from base64 import b64decode

    # b64 string -> b64 byte array -> compressed byte array
    b64_bytes = b64decode(string.encode('utf-8'))
    # compressed byte array -> byte array -> original string
    string_bytes = decompress(b64_bytes)
    string_decompressed = string_bytes.decode('utf-8')
    return string_decompressed


@contextmanager
def stdout_redirected_to(out_stream: IO):
    """
    Return a context in which stdout is redirected to a given File-like object
    Usage example:
        with stdout_redirected_to(open('foo.txt')):
            do_stuff_you_want_to_redirect()
        do_things_normally_again()
    Very slight modification of code suggested in https://stackoverflow.com/a/54058723

    :param out_stream: File-like object to which to redirect stdout
    """
    # TODO: pin down the full scope of what out_stream could be and update type hint and docstring accordingly

    from sys import stdout

    orig_stdout = stdout
    try:
        stdout = out_stream
        yield
    finally:
        stdout = orig_stdout
