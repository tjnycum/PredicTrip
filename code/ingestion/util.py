#  Copyright © 2020 Terry Nycum. All rights reserved except those granted in a LICENSE file.

from logging import debug


def compress_string(string: str) -> str:
    """
    Compress a UTF-8 string in a way safe for passage as an argument through fork/exec, but not necessarily shells

    :param string: string to be compressed
    :return: string of base64-encoded bytes
    """
    from zlib import compress
    from base64 import b64encode

    string_bytes = string.encode('utf-8')
    debug('initial string is {} bytes in size'.format(len(string_bytes)))
    string_compressed = compress(string_bytes)
    debug('string is {} bytes in size after compression with zlib (default level, 6)'
          .format(len(string_compressed)))
    string_b64 = b64encode(string_compressed)
    debug('string is {} bytes in size after base64-encoding'.format(len(string_b64)))
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
