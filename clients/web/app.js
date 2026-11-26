"use strict";
const $ = id => document.getElementById(id);
const request = (op, data) => ({v: 1, id: crypto.randomUUID().replaceAll("-", ""), op, data});
const auth = $("credentials"), composer = $("composer");
let session, current, own, pending, pendingAuth, localDevice, users = [], conversations = [];
let cursor = "0", minimum = "0", more = false, busy = false, online = false;
const seen = new Set();
function feedback(message) { $("feedback").textContent = message; }
function reset() {
    session = current = own = pending = pendingAuth = localDevice = null;
    users = []; conversations = []; minimum = cursor = "0"; more = false; seen.clear();
    $("messages").replaceChildren();
    $("workspace").hidden = $("composer").hidden = $("members").hidden = $("more").hidden = $("retry").hidden = true;
    $("auth").hidden = false;
    $("title").textContent = "Elige una conversación";
    $("connection").textContent = "Sin sesión";
    auth.reset(); composer.reset();
    clearMedia();
}
async function call(message) {
    const headers = {"Content-Type": "application/json"};
    if (session) headers.Authorization = "Bearer " + session;
    let reply;
    try {
        const response = await fetch("/api/v1", {method: "POST", headers, credentials: "omit",
            signal: AbortSignal.timeout(15000), body: JSON.stringify({...message, min_cursor: minimum})});
        if (!response.ok) throw new Error("HTTP " + response.status);
        reply = await response.json();
        if (reply.v !== 1 || reply.id !== message.id) throw new Error("Respuesta fuera de contrato");
        online = true;
        $("connection").textContent = "Conectado · " + reply.algorithm.toUpperCase();
    } catch (error) {
        online = false;
        $("connection").textContent = "Desconectado";
        throw error;
    }
    if (!reply.ok) {
        if (reply.error === "AUTH_REQUIRED") reset();
        throw new Error(reply.error);
    }
    if (BigInt(reply.cursor) > BigInt(minimum)) minimum = reply.cursor;
    return reply.result;
}
function run(action) {
    if (busy) return;
    busy = true;
    Promise.resolve().then(action).catch(error => feedback(error.message)).finally(() => { busy = false; });
}
function mutationStatus(result) {
    const app = result.application;
    if (app?.reason === "AUTH_REQUIRED") reset();
    if (app?.status === "APPLIED") return "Confirmado";
    if (app?.status === "REJECTED") {
        if (app.reason === "AUTH_REQUIRED") reset();
        return "Rechazado · " + app.reason;
    }
    if (result.consensus === "ABORTED") return "Abortado";
    if (result.consensus === "CONFLICT") return "Rechazado · identidad en conflicto";
    return "Pendiente o indeterminado";
}
const label = id => users.find(user => user.id === id)?.display ?? id;
function button(text, onClick) {
    const element = document.createElement("button");
    element.type = "button"; element.textContent = text; element.onclick = onClick;
    return element;
}
function selected() {
    return [...$("users").querySelectorAll("input:checked")].map(input => input.value);
}
function renderUsers() {
    const chosen = new Set(selected());
    const query = $("search").value.toLowerCase();
    $("users").replaceChildren(...users.filter(user => user.id !== own &&
        (user.display + " " + user.username).toLowerCase().includes(query)).map(user => {
        const row = document.createElement("label"), check = document.createElement("input");
        check.type = "checkbox"; check.value = user.id; check.checked = chosen.has(user.id);
        row.append(check, user.display, button("Chat", () => run(() => change("direct", {peer: user.id}))));
        return row;
    }));
}
async function refresh(directory = false) {
    if (directory) users = await call(request("users", {}));
    const result = await call(request("conversations", {}));
    own = result.user; conversations = result.items;
    $("identity").textContent = label(own);
    if (directory) renderUsers();
    $("conversations").replaceChildren(...conversations.map(item => button(
        item.name ?? label(item.members.find(id => id !== own)), () => run(() => open(item.id)))));
    if (current && !conversations.some(item => item.id === current)) clearThread();
}
function clearThread() {
    current = null; cursor = "0"; more = false; seen.clear();
    $("messages").replaceChildren(); $("members").hidden = $("composer").hidden = $("more").hidden = true;
    $("title").textContent = "Elige una conversación";
    composer.reset();
    $("media").hidden = $("secure").hidden = $("share-history").hidden = true;
}
async function open(id) {
    const item = conversations.find(row => row.id === id);
    if (!item) return;
    clearThread(); current = id;
    $("title").textContent = item.name ?? label(item.members.find(member => member !== own));
    $("members").hidden = item.type !== "GROUP";
    $("add").hidden = $("remove").hidden = item.owner !== own;
    $("composer").hidden = $("secure").hidden = false;
    $("media").hidden = false;
    $("share-history").hidden = !item.e2ee;
    if (item.e2ee) await DogCrypto.local(`locked:${own}:${id}`, true);
    await page("history");
}
async function page(op) {
    const result = await call(request(op, {conversation: current, after: cursor, limit: 40}));
    for (const message of result.messages) {
        if (seen.has(message.id)) continue;
        seen.add(message.id);
        const row = document.createElement("li");
        row.classList.toggle("own", message.sender === own);
        if (message.type === "E2EE" || message.type === "E2EE_FILE") {
            const detail = document.createElement("span");
            row.append(document.createTextNode(label(message.sender) + "\n"), detail);
            try {
                await DogCrypto.local(`locked:${own}:${current}`, true);
                const verified = await openMessage(message, current);
                if (message.type === "E2EE") detail.textContent = verified;
                else {
                    const ticket = JSON.parse(verified);
                    const item = message.attachment;
                    if (ticket.sha256 !== item.sha256 || !validAttachmentName(ticket.filename)) {
                        throw new Error("Metadatos del archivo alterados");
                    }
                    detail.textContent = ticket.filename + " · " + ticket.size + " bytes";
                    const context = {ticket, conversation: current};
                    row.append(button("Descargar", () => mediaWork(() => download(item, false, row, context))));
                    if (["image/png", "image/jpeg"].includes(ticket.type) && ticket.size <= 2_097_152) {
                        row.append(button("Vista previa", () => mediaWork(() => download(item, true, row, context))));
                    }
                }
            } catch (error) { detail.textContent = "Contenido cifrado · " + error.message; }
        } else if (message.attachment) {
            const item = message.attachment;
            row.append(document.createTextNode(label(message.sender) + "\n" + item.filename + " · " + item.size + " bytes"),
                button("Descargar", () => mediaWork(() => download(item, false))));
            if (message.type === "IMAGE" && item.size <= 2_097_152) {
                row.append(button("Vista previa", () => mediaWork(() => download(item, true, row))));
            }
        } else row.textContent = label(message.sender) + "\n" + message.text;
        $("messages").append(row);
    }
    cursor = result.next_cursor; more = result.more; $("more").hidden = !more;
}
async function change(op, data) {
    const result = await call(request(op, data));
    feedback(mutationStatus(result));
    if (result.application?.status === "APPLIED") {
        await refresh(true);
        if (result.application.conversation) await open(result.application.conversation);
    }
}
auth.onsubmit = event => {
    event.preventDefault();
    const op = event.submitter.value, fields = new FormData(auth);
    const data = {username: fields.get("username").trim(), password: fields.get("password")};
    if (op === "register") data.display = fields.get("display").trim();
    run(async () => {
        if (pendingAuth?.op !== op || JSON.stringify(pendingAuth?.data) !== JSON.stringify(data)) {
            pendingAuth = request(op, data);
        }
        try {
            const result = await call(pendingAuth);
            const status = mutationStatus(result);
            feedback(status);
            if (status === "Pendiente o indeterminado") return;
            pendingAuth = null;
            auth.elements.password.value = "";
            if (result.session) {
                session = result.session; $("auth").hidden = true; $("workspace").hidden = false;
                await refresh(true);
            }
        } catch (error) {
            if (online) pendingAuth = null;
            throw error;
        }
    });
};
$("logout").onclick = () => run(async () => {
    const result = await call(request("logout", {}));
    const status = mutationStatus(result);
    if (status === "Confirmado") reset();
    feedback(status);
});
$("search").oninput = renderUsers;
$("group").onsubmit = event => {
    event.preventDefault();
    run(() => change("group-create", {name: $("group").elements.name.value, members: selected()}));
};
$("list-members").onclick = () => run(async () =>
    feedback((await call(request("group-members", {conversation: current}))).map(label).join(", ")));
for (const op of ["add", "remove"]) {
    $(op).onclick = () => run(async () => {
        const members = selected();
        if (members.length !== 1) throw new Error("Marca un usuario de la lista");
        await change("group-" + op, {conversation: current, member: members[0]});
    });
}
async function send() {
    feedback("Enviando…");
    try {
        const result = await call(pending);
        const status = mutationStatus(result);
        feedback(status);
        if (status !== "Pendiente o indeterminado") pending = null;
        if (status === "Confirmado") {
            composer.reset();
            if (current && !more) await page("sync");
        }
    } finally {
        $("retry").hidden = !pending;
    }
}
composer.onsubmit = event => {
    event.preventDefault();
    run(async () => {
        if (pending) throw new Error("Usa Reintentar envío para resolver la solicitud anterior");
        const item = conversations.find(row => row.id === current);
        const locked = await DogCrypto.local(`locked:${own}:${current}`);
        if (item?.e2ee || locked) {
            const device = await attachDevice(false);
            const directory = await call(request("e2ee-directory", {conversation: current}));
            if (!directory.enabled) throw new Error("El servidor cambió el modo de cifrado de esta conversación");
            await checkPins(directory.devices, false);
            pending = request("e2ee-send", {conversation: current, device: device.id, epoch: directory.epoch});
            pending.data.envelope = await DogCrypto.seal(composer.elements.text.value, {
                conversation: current, message: pending.id, device: device.id, epoch: directory.epoch
            }, device, directory.devices).catch(error => { pending = null; throw error; });
        } else pending = request("send", {conversation: current, text: composer.elements.text.value});
        await send();
    });
};
$("retry").onclick = () => run(send);
$("more").onclick = () => run(() => page("history"));
let poll = 0;
setInterval(() => {
    if (!session || document.hidden) return;
    run(async () => {
        if (++poll % 4 === 0 || !online || !current) await refresh(poll % 8 === 0);
        if (current && !more) await page("sync");
    });
}, 3000);

let uploadState, mediaBusy = false;
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
function transferState(text) { $("transfer-state").textContent = text; }
function clearMedia() {
    uploadState = null;
    $("media").hidden = $("resume-upload").hidden = $("cancel-upload").hidden = $("transfer-progress").hidden = true;
    $("attachment").value = "";
    transferState("");
}
function mediaWork(action) {
    if (mediaBusy) return;
    mediaBusy = true;
    Promise.resolve().then(action).catch(error => transferState(error.message)).finally(() => {
        mediaBusy = false;
        $("resume-upload").hidden = !uploadState;
        $("cancel-upload").hidden = !uploadState || !!uploadState.completing;
    });
}
async function sha256(blob) {
    const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", await blob.arrayBuffer()));
    return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}
function encodeBlock(bytes) {
    return btoa(Array.from(bytes, byte => String.fromCharCode(byte)).join(""));
}
function progress(value, total) {
    const indicator = $("transfer-progress");
    indicator.max = total; indicator.value = value; indicator.hidden = false;
}
function requireSession(original) {
    if (session !== original) throw new Error("La sesión de la transferencia terminó");
}
async function upload() {
    const work = uploadState;
    requireSession(work.session);
    const opened = await call(work.init);
    if (opened.status === "CANCELLED") {
        uploadState = null;
        transferState("Carga cancelada");
        return;
    }
    const id = opened.transfer;
    const status = await call(request("upload-status", {transfer: id}));
    for (let offset = status.received; !work.completing && offset < work.file.size;) {
        if (work.cancel) {
            await call(request("upload-cancel", {transfer: id}));
            uploadState = null; transferState("Carga cancelada");
            return;
        }
        requireSession(work.session);
        const data = new Uint8Array(await work.file.slice(offset, offset + status.block_size).arrayBuffer());
        const reply = await call(request("upload-chunk", {transfer: id, offset, block: encodeBlock(data)}));
        if (reply.received <= offset) throw new Error("El servidor no confirmó el bloque");
        offset = reply.received; progress(offset, work.file.size);
    }
    if (work.cancel) {
        await call(request("upload-cancel", {transfer: id}));
        uploadState = null; transferState("Carga cancelada");
        return;
    }
    work.completing = true;
    $("cancel-upload").hidden = true;
    for (let i = 0; i < 24; i++) {
        requireSession(work.session);
        const reply = await call(work.complete);
        transferState(reply.status + (reply.reason ? " · " + reply.reason : ""));
        if (["COMMITTED", "REJECTED", "ABORTED"].includes(reply.status)) {
            uploadState = null;
            if (reply.status === "COMMITTED") {
                transferState("Archivo confirmado");
                if (current === work.init.data.conversation && !busy && !more) run(() => page("sync"));
            }
            return;
        }
        if (reply.status === "FAILED") throw new Error(reply.reason || "Carga interrumpida");
        await delay(750);
    }
    throw new Error("Publicación pendiente. Usa Reanudar");
}
$("upload").onclick = () => mediaWork(async () => {
    if (uploadState) throw new Error("Reanuda o cancela la carga anterior");
    const file = $("attachment").files[0], conversation = current, owner = session;
    if (!file || !conversation) return;
    if (file.size < 1 || file.size > 52_428_800) throw new Error("El archivo debe tener entre 1 byte y 50 MiB");
    transferState("Preparando archivo…");
    const item = conversations.find(row => row.id === conversation);
    const locked = await DogCrypto.local(`locked:${own}:${conversation}`);
    let init, complete, bytes = file;
    if (item?.e2ee || locked) {
        if (!validAttachmentName(file.name)) throw new Error("Nombre de archivo inválido");
        const device = await attachDevice(false);
        const directory = await call(request("e2ee-directory", {conversation}));
        if (!directory.enabled) throw new Error("El servidor cambió el modo cifrado");
        await checkPins(directory.devices, false);
        complete = request("upload-complete", {});
        const sealed = await DogCrypto.sealFile(file, conversation, complete.id);
        bytes = sealed.blob;
        const type = ["image/png", "image/jpeg"].includes(file.type) ? file.type : "application/octet-stream";
        const ticket = JSON.stringify({filename: file.name, type, size: file.size,
            sha256: sealed.sha256, key: sealed.key});
        const envelope = await DogCrypto.seal(ticket, {conversation, message: complete.id, device: device.id,
            epoch: directory.epoch, purpose: "file"}, device, directory.devices);
        init = request("upload-encrypted-init", {conversation, size: bytes.size, sha256: sealed.sha256,
            message: complete.id, device: device.id, epoch: directory.epoch, envelope});
    } else {
        const hash = await sha256(file);
        init = request("upload-init", {conversation, filename: file.name, size: file.size,
            sha256: hash, content_type: "application/octet-stream"});
        complete = request("upload-complete", {});
    }
    complete.data.transfer = init.id;
    requireSession(owner);
    uploadState = {file: bytes, init, session: owner, complete};
    $("cancel-upload").hidden = false;
    await upload();
});
$("resume-upload").onclick = () => mediaWork(async () => { if (uploadState) await upload(); });
$("cancel-upload").onclick = () => {
    if (!uploadState || uploadState.completing) return;
    uploadState.cancel = true;
    if (!mediaBusy) mediaWork(upload);
};
async function download(item, preview, row, secure) {
    const owner = session;
    let ready;
    for (let i = 0; i < 24; i++) {
        requireSession(owner);
        ready = await call(request("download-init", {attachment: item.id}));
        if (ready.status === "READY") break;
        transferState("Verificando una copia del archivo…");
        await delay(500);
    }
    if (ready.status !== "READY") throw new Error("Contenido temporalmente inaccesible");
    const blocks = [];
    for (let offset = 0; offset < item.size; offset += ready.block_size) {
        requireSession(owner);
        const reply = await call(request("download-chunk", {attachment: item.id, offset}));
        const bytes = Uint8Array.from(atob(reply.block), value => value.charCodeAt(0));
        if (reply.offset !== offset || bytes.length !== Math.min(ready.block_size, item.size - offset)) {
            throw new Error("Bloque incompleto");
        }
        blocks.push(bytes);
        progress(offset + bytes.length, item.size);
    }
    const mediaType = secure?.ticket.type ?? item.content_type;
    const type = preview && ["image/png", "image/jpeg"].includes(mediaType)
        ? mediaType : "application/octet-stream";
    let file = new Blob(blocks);
    if (await sha256(file) !== item.sha256) throw new Error("Integridad SHA-256 incorrecta");
    if (secure) {
        file = await DogCrypto.openFile(file, secure.conversation, item.id, secure.ticket.size, secure.ticket.key);
        if (preview) await verifyPreview(file, type);
    }
    file = new Blob([file], {type});
    requireSession(owner);
    const url = URL.createObjectURL(file);
    const filename = secure?.ticket.filename ?? item.filename;
    if (preview) {
        const image = document.createElement("img");
        image.className = "file-preview";
        image.alt = filename;
        image.onload = image.onerror = () => URL.revokeObjectURL(url);
        image.src = url;
        row.append(image);
    } else {
        const anchor = document.createElement("a");
        anchor.href = url; anchor.download = filename; anchor.click();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }
    transferState("Integridad verificada");
}

async function attachDevice(create) {
    if (location.protocol !== "https:" || !globalThis.isSecureContext || !crypto.subtle) throw new Error("Identidad protegida requiere HTTPS");
    if (!localDevice) {
        const name = "device:" + own;
        const saved = await DogCrypto.local(name);
        if (!saved && !create) throw new Error("Configura primero la identidad de dispositivo");
        const device = saved || await DogCrypto.rememberDevice(name, await DogCrypto.generate());
        const operation = request("device-register", {
            device: device.id, exchange: device.exchange, signing: device.signing
        });
        operation.data.proof = await DogCrypto.proof(device, operation.id, session);
        const response = await call(operation);
        if (response.application?.status !== "APPLIED") throw new Error(mutationStatus(response));
        localDevice = device;
    }
    return localDevice;
}
async function checkPins(devices, allowVerify) {
    for (const item of devices) {
        const fingerprint = await DogCrypto.fingerprint(item.exchange, item.signing);
        if (item.id !== fingerprint.slice(0,32)) throw new Error("Identidad de dispositivo alterada");
        if (item.user === own && localDevice?.id === item.id) continue;
        const name = `pin:${own}:${item.user}:${item.id}`;
        const known = await DogCrypto.local(name);
        if (known && known !== fingerprint) throw new Error("Cambió la huella de " + label(item.user));
        if (!known) {
            if (!allowVerify || !confirm(`Compara la huella de ${label(item.user)} por un canal independiente:
${fingerprint}
¿Coincide?`)) {
                throw new Error("Huella sin verificar");
            }
            await DogCrypto.local(name, fingerprint);
        }
    }
}
$("device").onclick = () => run(async () => {
    const device = await attachDevice(true);
    const mark = await DogCrypto.fingerprint(device.exchange, device.signing);
    feedback("Huella de este dispositivo · " + mark);
});
$("device-revoke").onclick = () => run(async () => {
    const devices = (await call(request("devices", {}))).filter(item => item.active);
    const id = prompt("ID del dispositivo que se revocará:\n" + devices.map(item => item.id).join("\n"));
    if (!devices.some(item => item.id === id) || !confirm("¿Revocar " + id + " y sus sesiones?")) return;
    const result = await call(request("device-revoke", {device: id}));
    if (result.application?.status === "APPLIED" && localDevice?.id === id) reset();
    feedback(mutationStatus(result));
});
$("secure").onclick = () => run(async () => {
    await attachDevice(false);
    let directory = await call(request("e2ee-directory", {conversation: current}));
    await checkPins(directory.devices, true);
    if (!directory.enabled) {
        if (!confirm("Activa el cifrado para los mensajes nuevos. El historial previo conserva su formato.")) return;
        const answer = await call(request("e2ee-enable", {conversation: current}));
        if (answer.application?.status !== "APPLIED") throw new Error(mutationStatus(answer));
        await DogCrypto.local(`locked:${own}:${current}`, true);
    }
    const id = current;
    await refresh();
    await open(id);
    feedback("Cifrado activado · huellas verificadas");
});

function validAttachmentName(name) {
    return typeof name === "string" && name !== "." && name !== ".." && !!name.trim() &&
        new TextEncoder().encode(name).length <= 240 && !/[\\/:\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/u.test(name);
}
function messageMeta(message, conversation) {
    return {conversation, message: message.id, device: message.device,
        epoch: message.epoch, purpose: message.type === "E2EE_FILE" ? "file" : "text"};
}
async function verifiedSender(user, ident, keys, key) {
    const fingerprint = await DogCrypto.fingerprint(keys.exchange, keys.signing);
    if (fingerprint.slice(0,32) !== ident) throw new Error("Identidad del emisor alterada");
    if (user === own && key.id === ident) return;
    const pin = await DogCrypto.local(`pin:${own}:${user}:${ident}`);
    if (pin !== fingerprint) throw new Error("Huella del emisor sin verificar");
}
async function openMessage(message, conversation) {
    const keys = message.sender_keys;
    const key = localDevice ?? await DogCrypto.local("device:" + own);
    if (!key) throw new Error("Identidad local no disponible");
    await verifiedSender(message.sender, message.device, keys, key);
    const meta = messageMeta(message, conversation);
    let shared;
    if (message.history_key) {
        const grant = message.history_key;
        await verifiedSender(own, grant.issuer, grant, key);
        shared = await DogCrypto.open(grant.envelope, {...meta, device: grant.issuer, purpose: "grant"}, grant, key);
    }
    return DogCrypto.open(message.envelope, meta, keys, key, shared);
}
async function verifyPreview(file, type) {
    if (file.size > 2_097_152) throw new Error("La imagen excede el límite de vista previa");
    // Se examina la cabecera antes de permitir que el navegador decodifique píxeles.
    const bytes = new Uint8Array(await file.slice(0, 262144).arrayBuffer());
    const view = new DataView(bytes.buffer);
    let width = 0, height = 0;
    if (type === "image/png" && bytes.length >= 24 && view.getUint32(0) === 0x89504e47 &&
        view.getUint32(4) === 0x0d0a1a0a && view.getUint32(12) === 0x49484452) {
        width = view.getUint32(16); height = view.getUint32(20);
    } else if (type === "image/jpeg" && bytes.length > 2 && view.getUint16(0) === 0xffd8) {
        for (let offset = 2; offset + 8 < bytes.length;) {
            if (bytes[offset++] !== 255) break;
            const marker = bytes[offset++];
            if (marker === 255) { offset--; continue; }
            if ([0, 0xd9, 0xda].includes(marker)) break;
            const length = view.getUint16(offset);
            if (length < 2) break;
            if ([0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf].includes(marker)) {
                if (length < 8) break;
                height = view.getUint16(offset + 3); width = view.getUint16(offset + 5); break;
            }
            offset += length;
        }
    }
    if (!width || !height || width > 8192 || height > 8192 || width * height > 8_000_000) {
        throw new Error("Formato o dimensiones fuera del límite");
    }
}

function secureOrigin() {
    if (location.protocol !== "https:" || !crypto.subtle) throw new Error("El vínculo requiere HTTPS");
}
const linkDialog = $("link-dialog");
let qrUrl;
function showLink(data, fingerprint = "") {
    if (qrUrl) URL.revokeObjectURL(qrUrl);
    qrUrl = null;
    $("link-image").hidden = !data?.png;
    if (data?.png) {
        const bytes = Uint8Array.from(atob(data.png), char => char.charCodeAt(0));
        qrUrl = URL.createObjectURL(new Blob([bytes], {type: "image/png"}));
        $("link-image").src = qrUrl;
    }
    $("link-descriptor").value = data?.descriptor || "";
    $("link-fingerprint").textContent = fingerprint ? "Huella SHA-256 · " + fingerprint : "";
    if (!linkDialog.open) linkDialog.showModal();
}
$("link-close").onclick = () => linkDialog.close();
$("link-start").onclick = () => run(async () => {
    secureOrigin();
    const name = "device:" + own;
    const candidate = await DogCrypto.local(name) || await DogCrypto.rememberDevice(name, await DogCrypto.generate());
    const mark = await DogCrypto.fingerprint(candidate.exchange, candidate.signing);
    const old = await DogCrypto.local("link:" + own);
    if (old) {
        const state = await call(request("link-info", {link: old}));
        if (state.status === "PENDING") {
            showLink(await call(request("link-qr", {link: old})), mark);
            return;
        }
    }
    const challenge = Array.from(crypto.getRandomValues(new Uint8Array(32)), b => b.toString(16).padStart(2, "0")).join("");
    const issued = String(Math.floor(Date.now() / 1000));
    const operation = request("link-start", {device: candidate.id, exchange: candidate.exchange,
        signing: candidate.signing, challenge, issued});
    operation.data.proof = await DogCrypto.signLink(candidate, "start", operation.id, session, challenge, issued);
    const reply = await call(operation);
    if (reply.application?.status !== "APPLIED") throw new Error(mutationStatus(reply));
    await DogCrypto.local("link:" + own, operation.id);
    showLink(await call(request("link-qr", {link: operation.id})), mark);
});
$("link-approve").onclick = () => run(async () => { secureOrigin(); showLink(); });
$("link-scan").onchange = () => run(async () => {
    const file = $("link-scan").files[0];
    if (!file || file.size > 1_048_576 || !["image/png", "image/jpeg"].includes(file.type)) throw new Error("Imagen QR inválida");
    if (!globalThis.BarcodeDetector || !(await BarcodeDetector.getSupportedFormats()).includes("qr_code")) {
        throw new Error("Lectura QR indisponible en este navegador. Pega el descriptor público.");
    }
    await verifyPreview(file, file.type);
    const image = await createImageBitmap(file);
    try {
        const found = await new BarcodeDetector({formats: ["qr_code"]}).detect(image);
        if (found.length !== 1) throw new Error("Se necesita un único QR");
        $("link-descriptor").value = found[0].rawValue;
    } finally { image.close(); }
});
$("link-confirm").onclick = () => run(async () => {
    secureOrigin();
    const text = $("link-descriptor").value;
    if (text.length > 512) throw new Error("Descriptor inválido");
    const descriptor = JSON.parse(text);
    if (descriptor.v !== 1 || !/^[0-9a-f]{32}$/.test(descriptor.id)) throw new Error("Descriptor inválido");
    const device = await attachDevice(false);
    const actual = await call(request("link-info", {link: descriptor.id}));
    for (const field of ["id", "challenge", "device", "expires"]) {
        if (descriptor[field] !== actual[field]) throw new Error("El descriptor no coincide con la solicitud");
    }
    const mark = await DogCrypto.fingerprint(actual.exchange, actual.signing);
    if (mark.slice(0,32) !== actual.device || actual.status !== "PENDING") throw new Error("Vínculo no disponible");
    if (!confirm("Compara la huella con el dispositivo nuevo y autoriza explícitamente:\n" + mark)) return;
    const observed = String(Math.floor(Date.now() / 1000));
    const approve = request("link-approve", {link: actual.id, device: device.id, observed});
    approve.data.proof = await DogCrypto.signLink(device, "approve", approve.id, session,
        actual.id, actual.device, actual.challenge, observed);
    const response = await call(approve);
    if (response.application?.status !== "APPLIED") throw new Error(mutationStatus(response));
    await DogCrypto.local(`pin:${own}:${own}:${actual.device}`, mark);
    linkDialog.close();
    feedback("Dispositivo autorizado. El historial requiere compartir sus claves.");
});
$("link-status").onclick = () => run(async () => {
    const id = await DogCrypto.local("link:" + own);
    if (!id) throw new Error("No hay vínculo local");
    const status = await call(request("link-info", {link: id}));
    if (status.status === "LINKED") await attachDevice(false);
    feedback(status.status);
});
$("link-cancel").onclick = () => run(async () => {
    const id = await DogCrypto.local("link:" + own);
    if (!id) throw new Error("No hay vínculo local");
    const result = await call(request("link-cancel", {link: id}));
    if (result.application?.status === "APPLIED") await DogCrypto.local("link:" + own, null);
    feedback(mutationStatus(result));
});
$("share-history").onclick = () => run(async () => {
    const device = await attachDevice(false);
    if (!current) return;
    const candidates = (await call(request("devices", {}))).filter(row => row.active && row.id !== device.id);
    if (!candidates.length) throw new Error("Vincula un segundo dispositivo primero");
    const id = prompt("ID de destino autorizado:\n" + candidates.map(row => row.id).join("\n"));
    const target = candidates.find(row => row.id === id);
    if (!target || !confirm("¿Compartir las claves de esta conversación con " + id + "?")) return;
    await checkPins([target], true);
    let after = "0", sent = 0;
    while (true) {
        const page = await call(request("history", {conversation: current, after, limit: 50}));
        for (const message of page.messages) {
            if (!["E2EE", "E2EE_FILE"].includes(message.type) ||
                !message.envelope.split(".")[3].split(";").some(entry => entry.startsWith(device.id + ":"))) continue;
            await verifiedSender(message.sender, message.device, message.sender_keys, device);
            const meta = messageMeta(message, current);
            const key = await DogCrypto.exportKey(message.envelope, meta, message.sender_keys, device);
            const envelope = await DogCrypto.seal(key, {...meta, device: device.id, purpose: "grant"}, device, [target]);
            const reply = await call(request("history-grant", {message: message.id, device: device.id,
                target: target.id, envelope}));
            if (reply.application?.status === "APPLIED") sent++;
            else if (reply.application?.reason !== "HISTORY_GRANT_EXISTS") throw new Error(mutationStatus(reply));
        }
        after = page.next_cursor;
        if (!page.more) break;
    }
    feedback("Claves compartidas · " + sent);
});
