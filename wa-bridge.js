if (!global.crypto) {
    try {
        global.crypto = require('node:crypto').webcrypto;
    } catch (e) {
        global.crypto = require('crypto');
    }
}

const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    fetchLatestBaileysVersion,
    delay
} = require("@whiskeysockets/baileys");
const fs = require("fs");
const path = require("path");
const pino = require("pino");

// Konfigurasi Folder Sesi
const SESSION_DIR = path.join(__dirname, "wa_session");
if (!fs.existsSync(SESSION_DIR)) fs.mkdirSync(SESSION_DIR);

const logger = pino({ level: "info" });

// Argumen dari Python: node wa-bridge.js --mode [test|batch] --to [number] --msg [message]
const args = process.argv.slice(2);
const mode = getArg("--mode") || "test";
const target = getArg("--to");
const message = getArg("--msg");


function getArg(key) {
    const idx = args.indexOf(key);
    return idx !== -1 ? args[idx + 1] : null;
}

async function startWA() {
    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
        version,
        auth: state,
        printQRInTerminal: true, // Untuk debug di terminal STB
        logger
    });

    // Auto-exit if not connected within 3 minutes (safety for zombie processes)
    const autoExit = setTimeout(() => {
        if (sock.user) return; // Already connected
        console.log("[STATUS] Timeout: Tidak ada aktivitas pairing selama 3 menit. Mematikan service...");
        process.exit(0);
    }, 180000);

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (connection === "open") clearTimeout(autoExit); // Cancel timeout on success

        if (qr) {
            fs.writeFileSync(path.join(__dirname, "wa_qr.txt"), qr);
            console.log("[QR] Silakan scan QR Code yang muncul di terminal atau UI.");
        }

        if (connection === "close") {
            const shouldReconnect = lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log("[STATUS] Koneksi terputus. Kodenya:", lastDisconnect.error?.output?.statusCode);
            if (!shouldReconnect) {
                console.log("[STATUS] Sesi dihentikan (Logged Out).");
                process.exit(1);
            }
            setTimeout(() => startWA(), 5000);
        } else if (connection === "open") {
            console.log("[STATUS] Terhubung ke WhatsApp!");
            try { fs.unlinkSync(path.join(__dirname, "wa_qr.txt")); } catch (e) { }

            if (mode === "test") {
                const img = getArg("--image");
                await sendSingle(sock, target, message, img);
            } else if (mode === "batch") {
                const taskFile = getArg("--file");
                if (taskFile && fs.existsSync(taskFile)) {
                    try {
                        const tasks = JSON.parse(fs.readFileSync(taskFile, "utf-8"));
                        console.log(`[STATUS] Memproses ${tasks.length} antrean pesan...`);
                        for (let i = 0; i < tasks.length; i++) {
                            const t = tasks[i];
                            await sendSingle(sock, t.to, t.msg, t.image);
                            if (i < tasks.length - 1) {
                                console.log("[WAIT] Menunggu 30 detik sebelum pesan berikutnya...");
                                await delay(30000); // Throttling 30 detik sesuai request
                            }
                        }
                        // Hapus file antrean setelah selesai
                        fs.unlinkSync(taskFile);
                    } catch (e) {
                        console.log("[ERROR] Gagal memproses file antrean:", e.message);
                    }
                } else {
                    // Fallback to single if no file
                    await sendSingle(sock, target, message);
                }
            }

            console.log("[STATUS] Semua tugas selesai. Mematikan service...");
            await delay(3000);
            process.exit(0);
        }
    });
}

async function sendSingle(sock, to, msg, imagePath = null) {
    if (!to || (!msg && !imagePath)) {
        console.log("[ERROR] Nomor tujuan atau konten pesan kosong.");
        return;
    }

    // Format nomor: 08xxx -> 628xxx@s.whatsapp.net
    let jid = to.replace(/[^0-9]/g, "");
    if (jid.startsWith("0")) jid = "62" + jid.slice(1);
    jid = jid.includes("@s.whatsapp.net") ? jid : jid + "@s.whatsapp.net";

    console.log(`[SEND] Mengirim ke ${jid}...`);
    try {
        if (imagePath && fs.existsSync(imagePath)) {
            // Kirim Gambar dengan Caption
            await sock.sendMessage(jid, {
                image: fs.readFileSync(imagePath),
                caption: msg
            });
            console.log(`[SUCCESS] Gambar + Pesan terkirim ke ${to}`);
        } else {
            // Kirim Teks Biasa
            await sock.sendMessage(jid, { text: msg });
            console.log(`[SUCCESS] Pesan teks terkirim ke ${to}`);
        }
    } catch (err) {
        console.log(`[FAILED] Gagal kirim ke ${to}: ${err.message}`);
    }
}

// Start
startWA().catch(err => {
    console.error("[CRITICAL ERROR]", err);
    process.exit(1);
});
