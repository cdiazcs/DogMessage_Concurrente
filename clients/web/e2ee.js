"use strict";

// Convenciones E1 compartidas con el cliente JVM.
const DogCrypto = (() => {
    const utf8 = new TextEncoder();
    const curve = {name: "ECDH", namedCurve: "P-256"};
    const signature = {name: "ECDSA", hash: "SHA-256"};
    const raw = value => Uint8Array.from(atob(value), c => c.charCodeAt(0));
    const b64 = value => btoa(String.fromCharCode(...new Uint8Array(value)));
    const hex = value => Array.from(new Uint8Array(value), b => b.toString(16).padStart(2, "0")).join("");
    const unhex = value => Uint8Array.from(value.match(/../g), b => parseInt(b, 16));
    const join = (...arrays) => {
        const data = new Uint8Array(arrays.reduce((n, bytes) => n + bytes.length, 0));
        let start = 0;
        for (const bytes of arrays) { data.set(bytes, start); start += bytes.length; }
        return data;
    };
    const rand = size => crypto.getRandomValues(new Uint8Array(size));
    const digest = bytes => crypto.subtle.digest("SHA-256", bytes);
    const fingerprint = async (exchange, signing) => hex(await digest(join(raw(exchange), raw(signing))));

    async function generate() {
        const dh = await crypto.subtle.generateKey(curve, false, ["deriveBits"]);
        const sign = await crypto.subtle.generateKey({name: "ECDSA", namedCurve: "P-256"}, false, ["sign", "verify"]);
        const exchange = b64(await crypto.subtle.exportKey("spki", dh.publicKey));
        const signing = b64(await crypto.subtle.exportKey("spki", sign.publicKey));
        const id = (await fingerprint(exchange, signing)).slice(0, 32);
        return {id, exchange, signing, dh, sign};
    }

    async function proof(device, request, token) {
        const session = hex(await digest(unhex(token)));
        const statement = utf8.encode(`dog.device.1\n${request}\n${session}\n${device.id}`);
        return b64(await crypto.subtle.sign(signature, device.sign.privateKey, statement));
    }

    function context({conversation, message, device, epoch, purpose = "text"}) {
        if (![conversation, message, device].every(v => /^[0-9a-f]{32}$/.test(v)) || !/^[0-9a-f]{64}$/.test(epoch) || !["text", "file", "grant"].includes(purpose)) {
            throw new Error("Contexto E2EE inválido");
        }
        return `dog.e2ee.1\n${conversation}\n${message}\n${device}\n${epoch}\n${purpose}`;
    }

    async function wrapping(privateKey, exchange, aad, recipient) {
        const publicKey = await crypto.subtle.importKey("spki", raw(exchange), curve, false, []);
        const shared = await crypto.subtle.deriveBits({name: "ECDH", public: publicKey}, privateKey, 256);
        const ikm = await crypto.subtle.importKey("raw", shared, "HKDF", false, ["deriveKey"]);
        return crypto.subtle.deriveKey({name: "HKDF", hash: "SHA-256", salt: await digest(utf8.encode(aad)),
            info: utf8.encode("dog.wrap.1\n" + recipient)}, ikm, {name: "AES-GCM", length: 256},
            false, ["encrypt", "decrypt"]);
    }

    const aes = (key, usage) => crypto.subtle.importKey("raw", key, "AES-GCM", false, [usage]);
    const gcm = (iv, aad) => ({name: "AES-GCM", iv, additionalData: utf8.encode(aad), tagLength: 128});

    async function seal(text, meta, sender, recipients) {
        const plaintext = utf8.encode(text);
        if (!text.trim() || plaintext.length > 4096 || recipients.length < 1 || recipients.length > 16) {
            throw new Error("Texto o destinatarios fuera de límite");
        }
        const aad = context(meta), key = rand(32), nonce = rand(12);
        const content = await crypto.subtle.encrypt(gcm(nonce, aad), await aes(key, "encrypt"), plaintext);
        const wraps = [];
        for (const recipient of [...recipients].sort((a,b) => a.id < b.id ? -1 : a.id === b.id ? 0 : 1)) {
            const iv = rand(12);
            const kek = await wrapping(sender.dh.privateKey, recipient.exchange, aad, recipient.id);
            const data = await crypto.subtle.encrypt(gcm(iv, aad + "\n" + recipient.id), kek, key);
            wraps.push([recipient.id, b64(iv), b64(data)].join(":"));
        }
        const body = ["E1", b64(nonce), b64(content), wraps.join(";")].join(".");
        const signed = await crypto.subtle.sign(signature, sender.sign.privateKey, utf8.encode(aad + "\n" + body));
        return body + "." + b64(signed);
    }

    async function validate(box, meta, sender) {
        const fields = box.split(".");
        if (fields.length !== 5 || fields[0] !== "E1" || box.length > 8192) throw new Error("Sobre E2EE inválido");
        const aad = context(meta);
        if ((await fingerprint(sender.exchange, sender.signing)).slice(0, 32) !== meta.device) {
            throw new Error("Identidad del emisor alterada");
        }
        const publicKey = await crypto.subtle.importKey("spki", raw(sender.signing),
            {name: "ECDSA", namedCurve: "P-256"}, false, ["verify"]);
        const valid = await crypto.subtle.verify(signature, publicKey, raw(fields[4]),
            utf8.encode(aad + "\n" + fields.slice(0, 4).join(".")));
        if (!valid) throw new Error("Firma del mensaje inválida");
        return {fields, aad};
    }
    async function unwrap(fields, aad, sender, recipient) {
        const recipientBox = fields[3].split(";").map(entry => entry.split(":")).find(e => e[0] === recipient.id);
        if (!recipientBox) throw new Error("Este dispositivo no recibió la clave");
        const kek = await wrapping(recipient.dh.privateKey, sender.exchange, aad, recipient.id);
        return crypto.subtle.decrypt(gcm(raw(recipientBox[1]), aad + "\n" + recipient.id), kek, raw(recipientBox[2]));
    }
    async function exportKey(box, meta, sender, recipient) {
        const {fields, aad} = await validate(box, meta, sender);
        const key = await unwrap(fields, aad, sender, recipient);
        await decrypt(fields, aad, key);
        return b64(key);
    }
    async function decrypt(fields, aad, key) {
        if (key.byteLength !== 32) throw new Error("Clave fuera de contrato");
        const plain = await crypto.subtle.decrypt(gcm(raw(fields[1]), aad), await aes(key, "decrypt"), raw(fields[2]));
        return new TextDecoder("utf-8", {fatal: true}).decode(plain);
    }
    async function open(box, meta, sender, recipient, historyKey) {
        const {fields, aad} = await validate(box, meta, sender);
        const key = historyKey === undefined ? await unwrap(fields, aad, sender, recipient) : raw(historyKey);
        return decrypt(fields, aad, key);
    }
    async function signLink(device, purpose, request, token, ...fields) {
        const session = hex(await digest(unhex(token)));
        const statement = ["dog.link." + purpose + ".1", request, session, device.id, ...fields].join("\n");
        return b64(await crypto.subtle.sign(signature, device.sign.privateKey, utf8.encode(statement)));
    }

    function fileAad(conversation, message, size, index) {
        if (![conversation, message].every(value => /^[0-9a-f]{32}$/.test(value))) throw new Error("ID de archivo inválido");
        return `dog.file.1\n${conversation}\n${message}\n${size}\n${index}`;
    }

    function fileIv(prefix, index) {
        const iv = new Uint8Array(12);
        iv.set(prefix);
        new DataView(iv.buffer).setUint32(8, index);
        return iv;
    }

    function fileSize(size) {
        const encrypted = 20 + size + 16 * Math.ceil(size / 8192);
        if (!Number.isSafeInteger(size) || size < 1 || encrypted > 52_428_800) throw new Error("Tamaño fuera de límite");
        return encrypted;
    }

    async function sealFile(file, conversation, message) {
        const expected = fileSize(file.size), fileKey = rand(32), prefix = rand(8), header = new Uint8Array(20);
        header.set(utf8.encode("DGF1"));
        new DataView(header.buffer).setBigUint64(4, BigInt(file.size));
        header.set(prefix, 12);
        const blocks = [header], key = await aes(fileKey, "encrypt");
        for (let offset = 0, index = 0; offset < file.size; offset += 8192, index++) {
            const bytes = await file.slice(offset, offset + 8192).arrayBuffer();
            blocks.push(await crypto.subtle.encrypt(gcm(fileIv(prefix, index),
                fileAad(conversation, message, file.size, index)), key, bytes));
        }
        const blob = new Blob(blocks);
        if (blob.size !== expected) throw new Error("Longitud cifrada inesperada");
        return {blob, key: b64(fileKey), sha256: hex(await digest(await blob.arrayBuffer()))};
    }

    async function openFile(blob, conversation, message, expectedSize, secret) {
        if (blob.size !== fileSize(expectedSize)) throw new Error("Longitud cifrada inválida");
        const h = new Uint8Array(await blob.slice(0, 20).arrayBuffer());
        if (new TextDecoder().decode(h.slice(0, 4)) !== "DGF1" ||
            new DataView(h.buffer).getBigUint64(4) !== BigInt(expectedSize) || raw(secret).length !== 32) {
            throw new Error("Cabecera cifrada inválida");
        }
        const blocks = [], key = await aes(raw(secret), "decrypt");
        for (let offset = 20, plain = 0, index = 0; plain < expectedSize; index++) {
            const count = Math.min(8192, expectedSize - plain);
            const bytes = await blob.slice(offset, offset + count + 16).arrayBuffer();
            blocks.push(await crypto.subtle.decrypt(gcm(fileIv(h.slice(12), index),
                fileAad(conversation, message, expectedSize, index)), key, bytes));
            plain += count;
            offset += count + 16;
        }
        return new Blob(blocks);
    }

    async function storage() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open("dog-e2ee-1", 1);
            request.onupgradeneeded = () => request.result.createObjectStore("state");
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    async function local(key, value = undefined) {
        const db = await storage();
        try {
            return await new Promise((resolve, reject) => {
                const tx = db.transaction("state", value === undefined ? "readonly" : "readwrite");
                const table = tx.objectStore("state");
                const req = value === undefined ? table.get(key) : table.put(value, key);
                tx.oncomplete = () => resolve(req.result);
                tx.onabort = () => reject(tx.error);
                tx.onerror = () => reject(tx.error);
            });
        } finally { db.close(); }
    }
    async function rememberDevice(name, candidate) {
        const db = await storage();
        try {
            return await new Promise((resolve, reject) => {
                const tx = db.transaction("state", "readwrite");
                const table = tx.objectStore("state");
                const existing = table.get(name);
                let selected;
                existing.onsuccess = () => {
                    selected = existing.result ?? candidate;
                    if (!existing.result) table.put(selected, name);
                };
                tx.oncomplete = () => resolve(selected);
                tx.onabort = () => reject(tx.error);
                tx.onerror = () => reject(tx.error);
            });
        } finally { db.close(); }
    }
    return {generate, proof, seal, open, local, fingerprint, sealFile, openFile, exportKey, signLink, rememberDevice};
})();
if (typeof module !== "undefined") module.exports = DogCrypto;
