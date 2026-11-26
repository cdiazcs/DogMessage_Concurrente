"""Adaptador de consenso y servicios locales de mensajería."""
from __future__ import annotations

import time
import uuid

from cluster_lib import dm2, dm_node
from raft_cli import send as send_raft
from raft_config import load_config
import chat_feed
import two_pc_wire as twopc
from chat_model import (BARRIER, REGISTER, SESSION, ChatState, Operation, Rejected, identity,
                        new_credential, new_session, require, session_hash)

CLIENT = "chat-app"


class Consensus:
    def __init__(self, config, secret, attempts=12, reader=None, state_timeout=None):
        self.addresses = load_config(config, algorithm=None)
        require(all(node.host == "127.0.0.1" for node in self.addresses.values()), "CHAT_REQUIRES_LOOPBACK")
        self.secret = secret
        self.attempts = attempts
        self.reader = reader
        self.state_timeout = state_timeout
        require(reader is None or reader in self.addresses, "INVALID_REPLICA")
        self.coordinator = next(iter(self.addresses.values())).coordinator
        self.client = dm_node.TcpClient(1)

    def submit_operation(self, raw, token):
        if self.coordinator is None:
            message = dm2.Message(dm2.Header(dm2.RAFT, dm2.CLIENT_COMMAND, token, CLIENT), dm2.ClientCommand(raw))
            _, response, _ = send_raft(self.addresses, message, self.attempts, 1, emit=lambda _: None)
            if response is None:
                return "INDETERMINATE", 0
            if response.status == dm2.REQUEST_ID_CONFLICT:
                return "CONFLICT", 0
            return "COMMITTED", response.log_index

        body = twopc.encode(twopc.SUBMIT, token, CLIENT, twopc.command_bytes(raw))
        address = self.addresses[self.coordinator]
        pending = 0
        for attempt in range(self.attempts):
            try:
                response = self.client.exchange(address.endpoint, body)
                status, seq, coordinator = twopc.correlate(response, address.id, token, twopc.SUBMIT)
                require(coordinator == address.id, "UNEXPECTED_COORDINATOR")
                if status == twopc.COMMITTED:
                    return "COMMITTED", seq
                if status == twopc.ABORTED:
                    return "ABORTED", seq
                if status == twopc.CONFLICT:
                    return "CONFLICT", 0
                if status == twopc.PENDING:
                    pending = max(pending, seq)
            except (OSError, ValueError):
                pass
            if attempt + 1 < self.attempts:
                time.sleep(.07)
        return ("PENDING", pending) if pending else ("INDETERMINATE", 0)

    def get_applied_operations(self, node, after):
        request = uuid.uuid4().bytes
        payload = twopc.position(after, True) + chat_feed.signature(self.secret, request, after)
        raw = twopc.encode(1, request, CLIENT, payload, chat_feed.ALGORITHM)
        body = self.client.exchange(node.endpoint, raw)
        algorithm, kind, found, sender, reader = twopc.header(body)
        require((algorithm, kind, found, sender) == (chat_feed.ALGORITHM, 0x81, request, node.id),
                "INVALID_FEED_REPLY")
        cursor = reader.sequence(True)
        size = reader.number(2)
        require(cursor >= after and size <= 64, "STALE_OR_INVALID_FEED")
        entries = []
        last = after
        for _ in range(size):
            index = reader.sequence()
            require(last < index <= cursor, "INVALID_FEED_ORDER")
            entries.append((index, reader.command()))
            last = index
        require(not reader.read(1), "INVALID_FEED_TRAILER")
        return cursor, entries

    def state(self, minimum=0):
        deadline = None if self.state_timeout is None else time.monotonic() + self.state_timeout
        candidates = ([self.addresses[self.reader]] if self.reader is not None else list(self.addresses.values()))
        for attempt in range(self.attempts):
            for node in candidates:
                try:
                    model = ChatState(self.secret)
                    cursor = 0
                    for _ in range(10_000):
                        if deadline is not None and time.monotonic() >= deadline:
                            break
                        next_cursor, entries = self.get_applied_operations(node, cursor)
                        for position, command in entries:
                            model.apply(position, command)
                        model.cursor = next_cursor
                        if next_cursor == cursor or not entries:
                            break
                        cursor = next_cursor
                    else:
                        raise Rejected("FEED_SCAN_LIMIT")
                    if (deadline is None or time.monotonic() < deadline) and model.cursor >= minimum:
                        return model
                except (OSError, ValueError):
                    continue
            if deadline is not None and time.monotonic() >= deadline:
                break
            if attempt + 1 < self.attempts:
                time.sleep(.04)
        raise Rejected("APPLIED_CURSOR_UNAVAILABLE")


class Chat:
    def __init__(self, consensus):
        self.consensus = consensus

    def operation(self, kind, *fields, token=None):
        token = token or uuid.uuid4().bytes
        return Operation(kind, token, tuple(fields))

    def submit(self, operation):
        return self.submit_encoded(operation.encode(self.consensus.secret), operation.request)

    def submit_encoded(self, raw, request):
        status, position = self.consensus.submit_operation(raw, request)
        result = {"consensus": status, "request_id": request.hex(), "position": position}
        if status == "COMMITTED":
            try:
                model = self.consensus.state(position)
            except Rejected:
                result["application"] = {"status": "NOT_OBSERVED"}
            else:
                entry = model.results.get(request.hex())
                result["application"] = entry["result"] if entry else {"status": "REJECTED"}
        return result

    def register_operation(self, username, display, password):
        return self.operation(REGISTER, identity(), username, display, *new_credential(password))

    def login_operation(self, username, password, minimum=0):
        user = self.consensus.state(minimum).authenticate(username, password)
        token, digest = new_session()
        return token, self.operation(SESSION, user, digest)

    def read(self, token, name, minimum=0, strong=False, **kwargs):
        digest = session_hash(token)
        if strong:
            barrier = self.submit(self.operation(BARRIER, digest))
            require(barrier.get("application", {}).get("status") == "APPLIED", "BARRIER_NOT_CONFIRMED")
            minimum = max(minimum, barrier["position"])
        return self.consensus.state(minimum).query(digest, name, **kwargs)
