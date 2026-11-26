// Chrome DevTools por tuberías locales. No se usa ningún transporte de aplicación adicional.
const {spawn} = require("node:child_process");
const {existsSync, mkdtempSync} = require("node:fs");
const {rm} = require("node:fs/promises");
const {tmpdir} = require("node:os");
const {join} = require("node:path");

const profile = mkdtempSync(join(tmpdir(), "dog-web-ui-"));
const browser = spawn(process.argv[2], ["--headless", "--no-first-run", "--disable-background-networking",
    "--disable-dev-shm-usage", "--remote-debugging-pipe", "--user-data-dir=" + profile, "about:blank"],
    {stdio: ["ignore", "ignore", "ignore", "pipe", "pipe"],
        env: process.env.DOG_CHROME_HOME ? {...process.env, HOME: process.env.DOG_CHROME_HOME,
            XDG_DATA_HOME: join(process.env.DOG_CHROME_HOME, ".local/share"),
            XDG_CONFIG_HOME: join(process.env.DOG_CHROME_HOME, ".config"),
            XDG_CACHE_HOME: join(process.env.DOG_CHROME_HOME, ".cache")} : process.env});
let sequence = 0, buffer = "";
const requests = new Map();
browser.stdio[4].setEncoding("utf8");
browser.stdio[4].on("data", chunk => {
    buffer += chunk;
    const packets = buffer.split("\0");
    buffer = packets.pop();
    for (const raw of packets) {
        const message = JSON.parse(raw), pending = requests.get(message.id);
        if (!pending) continue;
        requests.delete(message.id);
        if (message.error) pending.reject(new Error(message.error.message));
        else pending.resolve(message.result);
    }
});
function call(method, params = {}, sessionId) {
    const id = ++sequence;
    return new Promise((resolve, reject) => {
        requests.set(id, {resolve, reject});
        browser.stdio[3].write(JSON.stringify({id, method, params, sessionId}) + "\0");
    });
}
async function run() {
    const {targetId} = await call("Target.createTarget", {url: "about:blank"});
    const {sessionId} = await call("Target.attachToTarget", {targetId, flatten: true});
    const invoke = (method, params) => call(method, params, sessionId);
    await invoke("Page.navigate", {url: process.argv[3]});
    async function evaluate(expression) {
        const result = await invoke("Runtime.evaluate", {expression, returnByValue: true});
        if (result.exceptionDetails) throw new Error("Error al interactuar con la página");
        return result.result.value;
    }
    async function wait(expression) {
        for (let i = 0; i < (process.argv[4] === "--e2ee" ? 250 : 130); i++) {
            if (await evaluate(expression)) return;
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        throw new Error("Estado web no observado: " + expression);
    }
    await wait("typeof document.getElementById('credentials')?.onsubmit === 'function'");
    if (process.argv[4] === "--e2ee") {
        await wait("location.protocol === 'https:' && isSecureContext");
        await evaluate(`(() => {
            const f = document.getElementById('credentials');
            f.elements.username.value = 'browser_secure';
            f.elements.display.value = 'Chrome protegido';
            f.elements.password.value = 'solo-prueba-web-segura-2026';
            f.querySelector('[value=register]').click();
        })()`);
        await wait("document.getElementById('feedback').textContent === 'Confirmado'");
        await evaluate(`(() => {
            const f = document.getElementById('credentials');
            f.elements.password.value = 'solo-prueba-web-segura-2026';
            f.querySelector('[value=login]').click();
        })()`);
        await wait("!document.getElementById('workspace').hidden && !busy");
        await evaluate("document.getElementById('device').click()");
        await wait("localDevice && !busy");
        const mark = await evaluate("document.getElementById('feedback').textContent.split(' · ')[1]");
        if (!/^[0-9a-f]{64}$/.test(mark)) throw new Error("Huella de Chrome ausente");
        console.log("PUBLIC_BROWSER " + mark);
        await evaluate(`window.confirm = text => text.includes(${JSON.stringify(process.argv[5])}) ||
            text.includes("Activa el cifrado")`);
        await evaluate("[...document.querySelectorAll('#users label')].find(row => row.textContent.includes('Swing protegido')).querySelector('button').click()");
        await wait("!document.getElementById('composer').hidden && !busy");
        await evaluate("document.getElementById('secure').click()");
        await wait("document.getElementById('feedback').textContent.startsWith('Cifrado activado') && !busy");
        await evaluate(`(() => {
            const f = document.getElementById('composer');
            f.elements.text.value = 'Texto E2EE desde Chrome 🐕';
            f.querySelector('button').click();
        })()`);
        await wait("document.getElementById('messages').textContent.includes('Texto E2EE desde Chrome')");
        await wait("document.getElementById('messages').textContent.includes('Texto E2EE desde Swing')");
        console.log("PASS Chrome HTTPS cifró y descifró desde Swing");
        if (process.argv[6]) {
            await wait("!busy");
            const document = await invoke("DOM.getDocument", {});
            const {nodeId} = await invoke("DOM.querySelector", {nodeId: document.root.nodeId, selector: "#attachment"});
            await invoke("DOM.setFileInputFiles", {nodeId, files: [process.argv[6]]});
            await evaluate("document.getElementById('upload').click()");
            await wait("document.getElementById('transfer-state').textContent === 'Archivo confirmado'");
            await wait("!mediaBusy && [...document.querySelectorAll('#messages li')].some(row => row.textContent.includes('pixel.png'))");
            await evaluate("[...document.querySelectorAll('#messages li')].find(row => row.textContent.includes('pixel.png')).querySelectorAll('button')[1].click()");
            await wait("document.querySelector('#messages img')?.naturalWidth > 0 && !mediaBusy");
            await call("Browser.setDownloadBehavior", {behavior: "allow", downloadPath: process.argv[7]});
            await wait("[...document.querySelectorAll('#messages li')].some(row => row.textContent.includes('pagina.pdf'))");
            await evaluate("[...document.querySelectorAll('#messages li')].find(row => row.textContent.includes('pagina.pdf')).querySelector('button').click()");
            await wait("document.getElementById('transfer-state').textContent === 'Integridad verificada' && !mediaBusy");
            const destination = join(process.argv[7], "pagina.pdf");
            for (let i = 0; i < 80 && !existsSync(destination); i++) await new Promise(resolve => setTimeout(resolve, 100));
            if (!existsSync(destination)) throw new Error("PDF no guardado");
            console.log("PASS Chrome HTTPS adjuntó y mostró PNG cifrado y guardó PDF de Swing");
        }
        return;
    }
    const receiving = process.argv[4] === "--receive";
    if (!receiving) {
        await evaluate(`(() => {
        const f = document.getElementById('credentials');
        f.elements.username.value = 'web_test';
        f.elements.display.value = 'Cliente web';
        f.elements.password.value = 'solo-prueba-web-2026';
        f.querySelector('[value=register]').click();
    })()`);
        await wait("document.getElementById('feedback').textContent === 'Confirmado'");
    }
    await evaluate(`(() => {
        const f = document.getElementById('credentials');
        f.elements.username.value = 'web_test';
        f.elements.password.value = 'solo-prueba-web-2026';
        f.querySelector('[value=login]').click();
    })()`);
    await wait("document.querySelector('#users button') !== null");
    await evaluate("[...document.querySelectorAll('#users label')].find(row => row.textContent.includes('Cliente Swing')).querySelector('button').click()");
    await wait("!document.getElementById('composer').hidden && !busy");
    if (receiving) {
        await call("Browser.setDownloadBehavior", {behavior: "allow", downloadPath: process.argv[5]});
        await wait("[...document.querySelectorAll('#messages li')].some(row => row.textContent.includes('pagina.pdf'))");
        await evaluate("[...document.querySelectorAll('#messages li')].find(row => row.textContent.includes('pagina.pdf')).querySelector('button').click()");
        await wait("document.getElementById('transfer-state').textContent === 'Integridad verificada'");
        const destination = join(process.argv[5], "pagina.pdf");
        for (let i = 0; i < 50 && !existsSync(destination); i++) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        if (!existsSync(destination)) throw new Error("No se guardó el PDF descargado");
        console.log("PASS Chrome descargó y verificó el PDF enviado por Swing");
        return;
    }
    await evaluate(`(() => {
        const f = document.getElementById('composer');
        f.elements.text.value = '<script>throw 123</script> 🐶';
        f.querySelector('button').click();
    })()`);
    await wait("document.querySelector('#messages li')?.textContent.includes('<script>throw 123</script>')");
    if (await evaluate("document.querySelector('#messages script') !== null")) throw new Error("DOM inseguro");
    if (process.argv[4]) {
        await wait("!busy");
        const document = await invoke("DOM.getDocument", {});
        const {nodeId} = await invoke("DOM.querySelector", {nodeId: document.root.nodeId, selector: "#attachment"});
        await invoke("DOM.setFileInputFiles", {nodeId, files: [process.argv[4]]});
        await evaluate("document.getElementById('upload').click()");
        await wait("document.getElementById('transfer-state').textContent === 'Archivo confirmado'");
        await wait("document.querySelector('#messages button') !== null && !mediaBusy");
        await evaluate("[...document.querySelectorAll('#messages button')].find(button => button.textContent === 'Vista previa').click()");
        await wait("document.querySelector('#messages img')?.naturalWidth > 0");
        console.log("PASS Chrome adjuntó una imagen, verificó SHA-256 y mostró la vista previa");
    }
    await wait("!busy");
    await evaluate(`(() => {
        const form = document.getElementById('group');
        document.querySelector('#users input').checked = true;
        form.elements.name.value = 'Grupo web';
        form.querySelector('button').click();
    })()`);
    await wait("document.getElementById('title').textContent === 'Grupo web' && !busy");
    await evaluate("document.getElementById('list-members').click()");
    await wait("document.getElementById('feedback').textContent.includes('Cliente web')");
    console.log("PASS Chrome headless, registro, login, chat, envío, Unicode y grupo");
}
const deadline = setTimeout(() => {
    console.error("Tiempo de espera Chrome agotado");
    browser.kill();
    process.exitCode = 1;
}, process.argv[4] === "--e2ee" ? 105000 : 35000);
let shutdown;
run().catch(error => {
    console.error(error.message);
    process.exitCode = 1;
}).finally(() => {
    clearTimeout(deadline);
    if (browser.exitCode === null) {
        call("Browser.close").catch(() => {});
        shutdown = setTimeout(() => browser.kill(), 4000);
    }
});
browser.on("close", async () => {
    clearTimeout(deadline);
    clearTimeout(shutdown);
    try {
        await rm(profile, {recursive: true, force: true, maxRetries: 10, retryDelay: 100});
    } catch (error) {
        console.error("No se pudo limpiar el perfil de ensayo: " + error.code);
        process.exitCode = 1;
    }
});
