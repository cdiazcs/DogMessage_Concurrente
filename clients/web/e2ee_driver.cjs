"use strict";
const readline = require("node:readline");
const cryptoApi = require("./e2ee.js");
const keys = new Map();
async function execute(input) {
    const {op, name, token, request, box, meta, sender, text, devices} = input;
    if (op === "generate") {
        const {id, exchange, signing, dh, sign} = await cryptoApi.generate();
        keys.set(name, {id, exchange, signing, dh, sign});
        return {id, exchange, signing};
    }
    if (op === "file-seal" || op === "file-open") {
        const blob = new Blob([Buffer.from(input.bytes, "base64")]);
        if (op === "file-seal") {
            const file = await cryptoApi.sealFile(blob, input.conversation, input.message);
            return {bytes: Buffer.from(await file.blob.arrayBuffer()).toString("base64"),
                key: file.key, sha256: file.sha256};
        }
        const file = await cryptoApi.openFile(blob, input.conversation, input.message, input.size, input.key);
        return Buffer.from(await file.arrayBuffer()).toString("base64");
    }
    const device = keys.get(name);
    if (op === "proof") return cryptoApi.proof(device, request, token);
    if (op === "link-proof") return cryptoApi.signLink(device, input.purpose, request, token, ...input.fields);
    if (op === "seal") return cryptoApi.seal(text, meta, device, devices);
    if (op === "open") return cryptoApi.open(box, meta, sender, device, input.historyKey);
    if (op === "export-key") return cryptoApi.exportKey(box, meta, sender, device);
    throw new Error("Unknown operation");
}
(async () => {
    for await (const line of readline.createInterface({input: process.stdin})) {
        let result;
        try { result = {result: await execute(JSON.parse(line))}; }
        catch (error) { result = {error: error.name}; }
        console.log(JSON.stringify(result));
    }
})();
