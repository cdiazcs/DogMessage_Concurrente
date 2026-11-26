"""Incorpora una CA de ensayo a un perfil NSS desechable de Chromium en Linux."""
import ctypes as c
from ctypes.util import find_library
import os
from pathlib import Path
import ssl


class Item(c.Structure):
    _fields_ = [("kind", c.c_uint), ("data", c.POINTER(c.c_ubyte)), ("length", c.c_uint)]


class Trust(c.Structure):
    _fields_ = [("tls", c.c_uint), ("email", c.c_uint), ("signing", c.c_uint)]


def configure(home, certificate):
    library = find_library("nss3")
    if not library:
        raise RuntimeError("Falta NSS para la prueba HTTPS de Chromium")
    nss = c.CDLL(library)

    def bind(name, result, *args):
        function = getattr(nss, name)
        function.argtypes, function.restype = args, result
        return function

    path = home / ".local/share/pki/nssdb"
    path.mkdir(parents=True, exist_ok=False)
    legacy = home / ".pki"
    legacy.mkdir()
    (legacy / "nssdb").symlink_to(path, target_is_directory=True)
    pointer = c.c_void_p

    def verified(code, label):
        if code != 0:
            raise RuntimeError(label)

    verified(bind("NSS_InitReadWrite", c.c_int, c.c_char_p)(b"sql:" + os.fsencode(path)), "NSS_InitReadWrite")
    try:
        slot = bind("PK11_GetInternalKeySlot", pointer)()
        if not slot:
            raise RuntimeError("PK11_GetInternalKeySlot")
        try:
            if bind("PK11_NeedUserInit", c.c_int, pointer)(slot):
                verified(bind("PK11_InitPin", c.c_int, pointer, c.c_char_p, c.c_char_p)(slot, None, b""), "PK11_InitPin")
        finally:
            bind("PK11_FreeSlot", None, pointer)(slot)
        db = bind("CERT_GetDefaultCertDB", pointer)()
        raw = ssl.PEM_cert_to_DER_cert(Path(certificate).read_text(encoding="ascii"))
        data = (c.c_ubyte * len(raw)).from_buffer_copy(raw)
        item = Item(0, data, len(raw))
        temp = bind("CERT_NewTempCertificate", pointer, pointer, c.POINTER(Item), c.c_char_p, c.c_int, c.c_int)
        cert = temp(db, c.byref(item), None, 0, 1)
        if not cert:
            raise RuntimeError("CERT_NewTempCertificate")
        try:
            trust = Trust()
            decode = bind("CERT_DecodeTrustString", c.c_int, c.POINTER(Trust), c.c_char_p)
            verified(decode(c.byref(trust), b"C,,"), "CERT_DecodeTrustString")
            add = bind("__CERT_AddTempCertToPerm", c.c_int, pointer, c.c_char_p, c.POINTER(Trust))
            verified(add(cert, b"Dog Test CA", c.byref(trust)), "CERT_AddTempCertToPerm")
        finally:
            bind("CERT_DestroyCertificate", None, pointer)(cert)
    finally:
        verified(bind("NSS_Shutdown", c.c_int)(), "NSS_Shutdown")
