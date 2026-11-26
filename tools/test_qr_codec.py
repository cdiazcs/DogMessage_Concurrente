"""Ensayo local de un QR público. Este código no autoriza un dispositivo."""
import argparse
import ctypes as c
from ctypes.util import find_library
import json
from pathlib import Path
import secrets
import struct
import zlib
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway.qr_image import raster


def bind(library, name, result, *args):
    function = getattr(library, name)
    function.argtypes = args
    function.restype = result
    return function


def codec(payload):
    width, pixels = raster(payload)
    if scan_raster(width, pixels) != payload:
        raise RuntimeError("El contenido leído difiere del original")
    return f"P5\n{width} {width}\n255\n".encode("ascii") + pixels


def png_descriptor(raw):
    if not raw.startswith(b"\x89PNG\r\n\x1a\n") or len(raw) > 16000:
        raise ValueError("PNG QR inválido")
    offset, width, encoded = 8, 0, bytearray()
    while offset + 12 <= len(raw):
        size, tag = struct.unpack_from(">I4s", raw, offset)
        data = raw[offset + 8:offset + 8 + size]
        offset += 12 + size
        if tag == b"IHDR":
            width, height, *flags = struct.unpack(">IIBBBBB", data)
            if not 0 < width == height <= 600 or flags != [8, 0, 0, 0, 0]:
                raise ValueError("PNG QR inválido")
        elif tag == b"IDAT":
            encoded.extend(data)
    rows = zlib.decompress(encoded)
    if not width or len(rows) != width * (width + 1) or any(rows[i] for i in range(0, len(rows), width + 1)):
        raise ValueError("PNG QR inválido")
    pixels = bytearray().join(rows[i + 1:i + width + 1] for i in range(0, len(rows), width + 1))
    return scan_raster(width, pixels)


def scan_raster(width, pixels):
    reader_library = find_library("zbar")
    if not reader_library:
        raise RuntimeError("El ensayo local requiere libzbar")
    zbar = c.CDLL(reader_library)

    pointer = c.c_void_p
    create_scanner = bind(zbar, "zbar_image_scanner_create", pointer)
    create_image = bind(zbar, "zbar_image_create", pointer)
    destroy_scanner = bind(zbar, "zbar_image_scanner_destroy", None, pointer)
    destroy_image = bind(zbar, "zbar_image_destroy", None, pointer)
    set_format = bind(zbar, "zbar_image_set_format", None, pointer, c.c_ulong)
    set_size = bind(zbar, "zbar_image_set_size", None, pointer, c.c_uint, c.c_uint)
    set_data = bind(zbar, "zbar_image_set_data", None, pointer, pointer, c.c_ulong, pointer)
    scan = bind(zbar, "zbar_scan_image", c.c_int, pointer, pointer)
    first = bind(zbar, "zbar_image_first_symbol", pointer, pointer)
    get_length = bind(zbar, "zbar_symbol_get_data_length", c.c_uint, pointer)
    get_data = bind(zbar, "zbar_symbol_get_data", pointer, pointer)
    image, scanner = create_image(), create_scanner()
    if not image or not scanner:
        if image:
            destroy_image(image)
        if scanner:
            destroy_scanner(scanner)
        raise RuntimeError("El lector no pudo asignar el ensayo")
    data = (c.c_ubyte * len(pixels)).from_buffer(pixels)
    try:
        set_format(image, int.from_bytes(b"Y800", "little"))
        set_size(image, width, width)
        set_data(image, data, len(pixels), None)
        if scan(scanner, image) != 1:
            raise RuntimeError("El lector no encontró un único símbolo QR")
        symbol = first(image)
        if not symbol:
            raise RuntimeError("Símbolo QR ausente")
        return c.string_at(get_data(symbol), get_length(symbol))
    finally:
        destroy_image(image)
        destroy_scanner(scanner)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", type=Path, required=True)
    args = parser.parse_args()
    sample = {"version": 1, "kind": "DOG-LINK-TEST", "id": secrets.token_hex(16),
              "challenge": secrets.token_hex(16), "fingerprint": secrets.token_hex(32), "ttl_seconds": 120}
    data = json.dumps(sample, sort_keys=True, separators=(",", ":")).encode("ascii")
    raster = codec(data)
    args.export.mkdir(parents=True, exist_ok=False)
    (args.export / "qr-publico-sintetico.pgm").write_bytes(raster)
    (args.export / "results.json").write_text(json.dumps({
        "status": "PASS", "libraries": ["libqrencode", "libzbar"], "payload_bytes": len(data),
        "scope": "codec público de laboratorio"
    }, ensure_ascii=False, indent=2) + "\n")
    print("PASS QR público generado y leído con libqrencode/libzbar")


if __name__ == "__main__":
    main()
