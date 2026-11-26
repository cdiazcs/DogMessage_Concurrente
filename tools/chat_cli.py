"""Consola local de Dog Messenger sobre Raft y 2PC."""
from __future__ import annotations

import argparse
import getpass
import json
import os
from pathlib import Path
import uuid

from chat_model import ADD, DIRECT, GROUP, REMOVE, REVOKE, SEND, decode, identity, require, session_hash
from chat_service import Chat, Consensus
import chat_feed


def private_file(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="ascii") as stream:
        stream.write(data + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--session-file", type=Path)
    parser.add_argument("--outbox", type=Path, help="Archivo privado nuevo para guardar la operación antes de enviarla")
    parser.add_argument("--request-id", type=uuid.UUID)
    parser.add_argument("--replica")
    parser.add_argument("--min-cursor", type=int, default=0)
    parser.add_argument("--strong", action="store_true")
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("resume").add_argument("file", type=Path)
    r = sub.add_parser("register")
    r.add_argument("username")
    r.add_argument("display")
    sub.add_parser("login").add_argument("username")
    for name in ("logout", "users", "groups", "conversations"):
        sub.add_parser(name)
    sub.add_parser("direct").add_argument("peer")
    group = sub.add_parser("group-create")
    group.add_argument("name")
    group.add_argument("members", nargs="*")
    for name in ("group-add", "group-remove"):
        command = sub.add_parser(name)
        command.add_argument("conversation")
        command.add_argument("member")
    for name in ("members", "history", "sync", "send"):
        command = sub.add_parser(name)
        command.add_argument("conversation")
        if name == "send":
            command.add_argument("text")
        if name in ("history", "sync"):
            command.add_argument("--after", type=int, default=0)
            command.add_argument("--limit", type=int, default=50)
    sub.add_parser("status").add_argument("request")
    args = parser.parse_args()
    try:
        secret = chat_feed.key()
        require(args.min_cursor >= 0, "INVALID_CURSOR")
        service = Chat(Consensus(args.config, secret, reader=args.replica))
        mode = args.mode
        if mode == "resume":
            raw = bytes.fromhex(args.file.read_text(encoding="ascii").strip())
            operation = decode(raw, secret)
            return result(service.submit_encoded(raw, operation.request))

        if mode in ("register", "login"):
            if args.request_id is not None:
                parser.error("registro e inicio de sesión deben reintentarse con --outbox y resume")
            if mode == "register":
                operation = service.register_operation(args.username, args.display, getpass.getpass("Contraseña: "))
            else:
                require(args.session_file is not None, "LOGIN_REQUIRES_PRIVATE_SESSION_FILE")
                token, operation = service.login_operation(args.username, getpass.getpass("Contraseña: "),
                                                           minimum=args.min_cursor)
                private_file(args.session_file, token)
        else:
            token = (args.session_file.read_text(encoding="ascii").strip() if args.session_file is not None
                     else getpass.getpass("Token de sesión: "))
            if mode in ("users", "groups", "conversations", "members", "history", "sync", "status"):
                print(json.dumps(service.read(token, mode, args.min_cursor, args.strong,
                                              cid=getattr(args, "conversation", None),
                                              after=getattr(args, "after", 0), limit=getattr(args, "limit", 50),
                                              request=getattr(args, "request", None)), ensure_ascii=False))
                return 0
            session = session_hash(token)
            request = args.request_id or uuid.uuid4()
            if mode == "send":
                operation = service.operation(SEND, session, args.conversation, request.hex, args.text,
                                              token=request.bytes)
            elif mode == "group-create":
                operation = service.operation(GROUP, session, request.hex, args.name, ",".join(args.members),
                                              token=request.bytes)
            elif mode == "direct":
                operation = service.operation(DIRECT, session, args.peer, token=request.bytes)
            elif mode == "logout":
                operation = service.operation(REVOKE, session, token=request.bytes)
            else:
                operation = service.operation(ADD if mode == "group-add" else REMOVE, session,
                                              args.conversation, args.member, token=request.bytes)
        raw = operation.encode(secret)
        if args.outbox is not None:
            private_file(args.outbox, raw.hex())
        return result(service.submit_encoded(raw, operation.request))
    except (OSError, ValueError) as error:
        print("ERROR", str(error))
        return 1


def result(response):
    print(json.dumps(response, ensure_ascii=False))
    return 0 if response.get("application", {}).get("status") == "APPLIED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
