"""Renderizado opcional de QR públicos mediante libqrencode."""
import ctypes as c
from ctypes.util import find_library
import struct
import zlib


class Symbol(c.Structure):
    _fields_ = [("version", c.c_int), ("width", c.c_int), ("modules", c.POINTER(c.c_ubyte))]


def raster(payload):
    library = find_library("qrencode")
    if not library or len(payload) > 512:
        raise ValueError("QR_UNAVAILABLE")
    qr = c.CDLL(library)
    encode = qr.QRcode_encodeString8bit
    encode.argtypes = (c.c_char_p, c.c_int, c.c_int)
    encode.restype = c.POINTER(Symbol)
    free = qr.QRcode_free
    free.argtypes, free.restype = (c.POINTER(Symbol),), None
    code = encode(payload, 0, 1)
    if not code:
        raise ValueError("QR_UNAVAILABLE")
    try:
        n = code.contents.width
        module = code.contents.modules
        scale, margin = 5, 4
        width = (n + 2 * margin) * scale
        pixels = bytearray()
        for y in range(-margin, n + margin):
            row = bytearray()
            for x in range(-margin, n + margin):
                dark = 0 <= x < n and 0 <= y < n and bool(module[y * n + x] & 1)
                row.extend((b"\x00" if dark else b"\xff") * scale)
            pixels.extend(row * scale)
        return width, pixels
    finally:
        free(code)


def png(payload):
    width, pixels = raster(payload)

    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))

    rows = b"".join(b"\0" + pixels[offset:offset + width] for offset in range(0, len(pixels), width))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, width, 8, 0, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))
