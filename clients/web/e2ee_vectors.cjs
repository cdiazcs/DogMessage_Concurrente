"use strict";
const {readFileSync} = require("node:fs");
(async () => {
    const rows = readFileSync("protocol/fixtures/e2ee.tsv", "utf8").trim().split("\n").filter(line => !line.startsWith("#"));
    for (const row of rows) {
        const [kind, a, b, c, expected] = row.split("\t");
        const [key, param, data] = [a,b,c].map(value => Buffer.from(value, "hex"));
        let result;
        if (kind === "gcm") {
            const handle = await crypto.subtle.importKey("raw", key, "AES-GCM", false, ["encrypt"]);
            result = await crypto.subtle.encrypt({name: "AES-GCM", iv: param, tagLength: 128}, handle, data);
        } else {
            const handle = await crypto.subtle.importKey("raw", key, "HKDF", false, ["deriveBits"]);
            result = await crypto.subtle.deriveBits({name: "HKDF", hash: "SHA-256", salt: param, info: data}, handle, 256);
        }
        if (Buffer.from(result).toString("hex") !== expected) throw new Error(kind);
    }
    console.log("PASS Web Crypto E2EE: " + rows.length + " vectores");
})().catch(error => { console.error(error.message); process.exitCode = 1; });
