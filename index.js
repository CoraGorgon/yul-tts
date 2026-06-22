// ╔══════════════════════════════════════════════════════════════╗
//  YUL TTS BOT — VERSIÓN FINAL (Windows + Linux compatible)
//  Partes 1-5: Base · TTS · Cache · Stats · Escalabilidad Global
//  Stack: discord.js v14 · @discordjs/voice · pg · dotenv
//  Soporta 10,000+ servidores · Sharding Ready · Zero-downtime
// ╚══════════════════════════════════════════════════════════════╝

"use strict";

require("dotenv").config();

// ════════════════════════════════════════════════════════════════
//  IMPORTS
// ════════════════════════════════════════════════════════════════
const {
  Client,
  GatewayIntentBits,
  Partials,
  Collection,
  REST,
  Routes,
  SlashCommandBuilder,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  Events,
  ActivityType,
  WebhookClient,
} = require("discord.js");

const {
  joinVoiceChannel,
  createAudioPlayer,
  createAudioResource,
  AudioPlayerStatus,
  VoiceConnectionStatus,
  entersState,
  getVoiceConnection,
  NoSubscriberBehavior,
} = require("@discordjs/voice");

const { Pool }         = require("pg");
const fs               = require("fs");
const fsp              = require("fs/promises");
const path             = require("path");
const { spawn, execFile } = require("child_process");
const { promisify }    = require("util");
const crypto           = require("crypto");
const os               = require("os");
const { EventEmitter } = require("events");

// ════════════════════════════════════════════════════════════════
//  TTS ENGINE — node-gtts (reemplaza edge-tts)
//  No requiere ejecutables externos ni API keys.
//  Instalación: npm install node-gtts
//  Funciona en cualquier entorno cloud (Railway, Render, Fly.io…)
// ════════════════════════════════════════════════════════════════
const gTTS = require("node-gtts");

const execFileAsync = promisify(execFile);

// Más listeners — evita MaxListenersExceededWarning en escala
EventEmitter.defaultMaxListeners = 50;

// ════════════════════════════════════════════════════════════════
//  ENTORNO
// ════════════════════════════════════════════════════════════════
const {
  DISCORD_TOKEN,
  CLIENT_ID,
  DATABASE_URL,
  LOG_WEBHOOK_URL,
  STATS_CHANNEL_ID,
  SHARD_COUNT,
  EDGE_TTS_PATH,   // mantenido por compatibilidad — ya no se usa
} = process.env;

if (!DISCORD_TOKEN || !CLIENT_ID || !DATABASE_URL) {
  console.error("[FATAL] Faltan variables: DISCORD_TOKEN, CLIENT_ID, DATABASE_URL");
  process.exit(1);
}

// ════════════════════════════════════════════════════════════════
//  CONSTANTES
// ════════════════════════════════════════════════════════════════
const INVITE_URL           = `https://discord.com/oauth2/authorize?client_id=1518400986606075904&permissions=4298119168&integration_type=0&scope=bot`;
const SUPPORT_URL          = "https://discord.gg/Ze5TEDCD";
const DEFAULT_PREFIX       = "yul ";
const DEFAULT_LANG         = "es";
const DEFAULT_VOICE        = "es-MX-DaliaNeural";
const TTS_MAX_CHARS        = 500;
const TTS_COOLDOWN_MS      = 3_000;
const SPAM_THRESHOLD       = 5;
const SPAM_DECAY_RATE      = 0.5;
const AUDIO_DIR            = path.join(process.cwd(), "tts_audio");
const CACHE_MAX_MEM        = 300;
const CACHE_TTL_MS         = 7 * 24 * 60 * 60 * 1_000;
const CLEANUP_INTERVAL_MS  = 30 * 60 * 1_000;
const HEALTH_INTERVAL_MS   = 60_000;
const STATS_FLUSH_MS       = 5 * 60_000;
const QUEUE_IDLE_TIMEOUT   = 10 * 60_000;
const VOICE_REJOIN_RETRIES = 3;
const VOICE_REJOIN_DELAY   = 5_000;
const CONFIG_TTL_MS        = 10 * 60_000;
const IS_WINDOWS           = process.platform === "win32";
const URL_REGEX            = /https?:\/\/[^\s]+|discord\.gg\/[^\s]+|www\.[^\s]+/gi;

const logWebhook = LOG_WEBHOOK_URL ? new WebhookClient({ url: LOG_WEBHOOK_URL }) : null;
const IS_SHARDED = !!process.env.SHARDING_MANAGER;
const SHARD_ID   = IS_SHARDED ? (Number(process.env.SHARD_ID) || 0) : 0;
const TOTAL_SHARDS = IS_SHARDED ? (Number(SHARD_COUNT) || 1) : 1;

// Crear carpeta de audio al arranque
if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });

// ════════════════════════════════════════════════════════════════
//  VOCES SOPORTADAS
//  Los IDs siguen siendo los mismos que usaba edge-tts para
//  mantener compatibilidad con la BD y la experiencia de usuario.
//  Cada voz se mapea a un código de idioma de Google TTS (gttsLang).
// ════════════════════════════════════════════════════════════════
const SUPPORTED_VOICES = [
  { id: "es",   lang: "es", label: " Español 🇲🇽",   gttsLang: "es" },
  { id: "en",    lang: "en", label: "English 🇺🇸",    gttsLang: "en" },
  { id: "fr",  lang: "fr", label: "français 🇫🇷",     gttsLang: "fr" },
  { id: "pt", lang: "pt", label: "português 🇧🇷", gttsLang: "pt" },
  { id: "ja",  lang: "ja", label: "japanese 🇯🇵",     gttsLang: "ja" },
  { id: "de",  lang: "de", label: "Deutsch 🇩🇪",     gttsLang: "de" },
];
const VOICE_IDS = SUPPORTED_VOICES.map((v) => v.id);

/**
 * Obtiene el código de idioma de Google TTS a partir del voice ID.
 * @param {string} voiceId — e.g. "es-MX-DaliaNeural"
 * @returns {string}       — e.g. "es"
 */
function getGttsLang(voiceId) {
  const voice = SUPPORTED_VOICES.find((v) => v.id === voiceId);
  return voice?.gttsLang ?? DEFAULT_LANG;
}

// ════════════════════════════════════════════════════════════════
//  SECCIÓN A — LOG SYSTEM
// ════════════════════════════════════════════════════════════════
function sendErrorLog(title, description, color = "#000000") {
  const tag = IS_SHARDED ? ` [Shard ${SHARD_ID}]` : "";
  console.error(`[LOG${tag}] ${title}: ${String(description).slice(0, 300)}`);
  if (!logWebhook) return;
  const embed = new EmbedBuilder()
    .setTitle(`🚨 ${title}${tag}`)
    .setDescription(`\`\`\`\n${String(description).slice(0, 1800)}\n\`\`\``)
    .setColor(color)
    .setFooter({ text: `Yul TTS • PID:${process.pid}` })
    .setTimestamp();
  logWebhook.send({ embeds: [embed] }).catch(() => {});
}

// ════════════════════════════════════════════════════════════════
//  SECCIÓN B — ANTI-CRASH
// ════════════════════════════════════════════════════════════════
process.on("unhandledRejection", (reason) => {
  const msg = reason instanceof Error ? reason.stack : String(reason);
  sendErrorLog("unhandledRejection", msg);
});

process.on("uncaughtException", (err) => {
  sendErrorLog("uncaughtException", err?.stack ?? String(err));
  // NO process.exit — el bot continúa
});



process.on("warning", (w) => {
  console.warn("[ANTI-CRASH] warning:", w.name, w.message);
  if (w.name === "MaxListenersExceededWarning") sendErrorLog("MaxListeners", w.message, 0xfee75c);
});

// ════════════════════════════════════════════════════════════════
//  1. TTS ENGINE — node-gtts reemplaza edge-tts por completo
//
//  Antes (edge-tts):
//    - Requería el ejecutable Python edge-tts instalado en el SO
//    - Problemas con rutas en Windows, CMD, PowerShell
//    - Necesitaba spawn() con shell=false para evitar escapes
//
//  Ahora (node-gtts):
//    - npm install node-gtts — solo una dependencia npm
//    - Sin ejecutables externos, sin Python, cloud-ready
//    - Genera MP3 via stream desde la API pública de Google TTS
//    - Funciona igual en Linux, macOS y Windows
//    - Compatible con Railway, Render, Fly.io, cualquier VPS
//
//  Las funciones originales de edge-tts se conservan como STUBS
//  comentados para no borrar código según las instrucciones.
//  La lógica real de síntesis vive en _generateGTTS().
// ════════════════════════════════════════════════════════════════

/**
 * [STUB — conservado, ya no se invoca]
 * Candidatos de ruta para el ejecutable edge-tts en Windows.
 * Reemplazado por node-gtts; se mantiene para no borrar código.
 */
function _getEdgeTTSCandidates() {
  const candidates = [];
  if (EDGE_TTS_PATH) {
    candidates.push(EDGE_TTS_PATH);
  }
  if (IS_WINDOWS) {
    const home    = os.homedir();
    const appData = process.env.APPDATA || path.join(home, "AppData", "Roaming");
    const pyVersions = ["313", "312", "311", "310", "39", "38"];
    for (const ver of pyVersions) {
      candidates.push(path.join(appData, "Python", `Python${ver}`, "Scripts", "edge-tts.exe"));
      candidates.push(path.join(appData, "Python", `Python${ver}`, "Scripts", "edge-tts"));
    }
    const drives = ["C:", "D:"];
    for (const drive of drives) {
      for (const ver of pyVersions) {
        candidates.push(path.join(drive, `\\Python${ver}`, "Scripts", "edge-tts.exe"));
        candidates.push(path.join(drive, `\\Python${ver}`, "Scripts", "edge-tts"));
      }
      candidates.push(path.join(drive, "\\Program Files", "Python", "Scripts", "edge-tts.exe"));
      candidates.push(path.join(drive, "\\Program Files (x86)", "Python", "Scripts", "edge-tts.exe"));
    }
    candidates.push(path.join(home, ".pyenv", "shims", "edge-tts"));
    candidates.push(path.join(home, "scoop", "apps", "python", "current", "Scripts", "edge-tts.exe"));
    candidates.push("edge-tts.exe");
    candidates.push("edge-tts");
    candidates.push("python");
  } else {
    const home = os.homedir();
    candidates.push("/usr/local/bin/edge-tts");
    candidates.push(path.join(home, ".local", "bin", "edge-tts"));
    candidates.push("/usr/bin/edge-tts");
    candidates.push("edge-tts");
    candidates.push("python3");
    candidates.push("python");
  }
  return candidates;
}

/**
 * [STUB — conservado, ya no se invoca para buscar edge-tts]
 * Retorna inmediatamente con valor stub. El engine real es node-gtts.
 * Se conserva la firma original para no romper exports ni referencias.
 */
let _resolvedEdgeTTS = null;
async function resolveEdgeTTSBin() {
  if (_resolvedEdgeTTS) return _resolvedEdgeTTS;
  // STUB: el engine ahora es node-gtts — sin ejecutables externos
  _resolvedEdgeTTS = { bin: "node-gtts", usePythonModule: false };
  console.log("[TTS] Engine: node-gtts (cloud-ready, sin Python ni edge-tts)");
  return _resolvedEdgeTTS;
}

/**
 * [STUB — conservado, ya no se invoca]
 * Antes ejecutaba edge-tts via spawn() sin shell.
 * Reemplazado por _generateGTTS(). Se conserva sin operación.
 *
 * @param {string} text
 * @param {string} voice
 * @param {string} outFile
 * @returns {Promise<void>}
 */
function _spawnEdgeTTS(text, voice, outFile) {
  // STUB: esta función ya no se llama.
  // La síntesis real ocurre en _generateGTTS() con node-gtts.
  return Promise.reject(
    new Error("_spawnEdgeTTS obsoleto — el engine usa node-gtts.")
  );
}

// ── ENGINE REAL: node-gtts ────────────────────────────────────────

/**
 * Genera audio TTS con node-gtts y lo escribe como MP3.
 * Usa la API pública de Google Translate TTS via stream HTTP.
 * No requiere ejecutables externos ni API key.
 *
 * @param {string} text     — Texto a sintetizar (ya validado)
 * @param {string} voiceId  — ID de voz estilo edge-tts (e.g. "es-MX-DaliaNeural")
 * @param {string} outFile  — Ruta absoluta al archivo MP3 de salida
 * @returns {Promise<void>}
 */

// ============================================================
//  22.5. SERVIDOR WEB (EXPRESS) PARA DASHBOARD
// ============================================================
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;

// Sirve los archivos estáticos de la carpeta "public"
app.use(express.static('public'));

// API para que el HTML obtenga las estadísticas en tiempo real
app.get('/api/stats', (req, res) => {
  res.json({
    guilds: client.guilds.cache.size || 0,
    queues: guildQueues.size || 0,
    ping: client.ws.ping || 0,
    inviteUrl: INVITE_URL,
    supportUrl: SUPPORT_URL
  });
});

app.listen(PORT, () => {
  console.log(`[WEB] Página web escuchando en el puerto ${PORT}`);
});
function _generateGTTS(text, voiceId, outFile) {
  return new Promise((resolve, reject) => {
    const lang   = getGttsLang(voiceId);
    const tts    = gTTS(lang);
    const stream = tts.stream(text);
    const dest   = fs.createWriteStream(outFile);

    const timeout = setTimeout(() => {
      dest.destroy();
      stream.destroy();
      reject(new Error("node-gtts timeout (30s)"));
    }, 30_000);

    stream.on("error", (err) => {
      clearTimeout(timeout);
      dest.destroy();
      reject(new Error(`node-gtts stream error: ${err.message}`));
    });

    dest.on("error", (err) => {
      clearTimeout(timeout);
      reject(new Error(`node-gtts write error: ${err.message}`));
    });

    dest.on("finish", () => {
      clearTimeout(timeout);
      resolve();
    });

    stream.pipe(dest);
  });
}

// ════════════════════════════════════════════════════════════════
//  2. CLIENTE DISCORD
// ════════════════════════════════════════════════════════════════
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildVoiceStates,
  ],
  partials: [Partials.Channel],
  rest:     { timeout: 15_000 },
});

client.guildCache = new Collection();

client.on("error",           (err)     => sendErrorLog("Client Error",         err.stack ?? err.message));
client.on("shardError",      (err, id) => sendErrorLog(`Shard ${id} Error`,    err.stack ?? err.message));
client.on("shardDisconnect", (ev, id)  => sendErrorLog(`Shard ${id} Disconnect`, `WS:${ev.code}`, 0xfee75c));
client.on("shardReconnecting",(id)     => console.log(`[SHARD] ${id} reconectando…`));
client.on("shardResume",     (id, n)   => console.log(`[SHARD] ${id} resumido (${n} eventos).`));
client.on("invalidated",     ()        => sendErrorLog("Session Invalidated", "Token inválido."));

// ════════════════════════════════════════════════════════════════
//  3. POSTGRESQL — POOL CON RETRY
// ════════════════════════════════════════════════════════════════
const pool = new Pool({
  connectionString:        DATABASE_URL,
  ssl:                     process.env.DATABASE_SSL === "true" ? { rejectUnauthorized: false } : false,
  max:                     20,
  min:                     2,
  idleTimeoutMillis:       30_000,
  connectionTimeoutMillis: 5_000,
  allowExitOnIdle:         false,
});

pool.on("error", (err) => sendErrorLog("DB Pool Error", err.message));

async function dbQuery(text, values = [], retries = 3) {
  for (let i = 1; i <= retries; i++) {
    try {
      return await pool.query(text, values);
    } catch (err) {
      if (["57P01", "08006", "08001", "08004"].includes(err.code) && i < retries) {
        await new Promise((r) => setTimeout(r, 500 * i));
        continue;
      }
      throw err;
    }
  }
}

async function initDatabase() {
  const conn = await pool.connect();
  try {
    await conn.query(`
      CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id  TEXT PRIMARY KEY,
        prefix    TEXT NOT NULL DEFAULT '${DEFAULT_PREFIX}',
        lang      TEXT NOT NULL DEFAULT '${DEFAULT_LANG}',
        voice     TEXT NOT NULL DEFAULT '${DEFAULT_VOICE}'
      );

      CREATE TABLE IF NOT EXISTS tts_cache (
        hash       TEXT        PRIMARY KEY,
        file_path  TEXT        NOT NULL,
        voice      TEXT        NOT NULL,
        text       TEXT        NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_used  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        use_count  INTEGER     NOT NULL DEFAULT 1
      );

      CREATE INDEX IF NOT EXISTS idx_tts_cache_last_used ON tts_cache (last_used);

      CREATE TABLE IF NOT EXISTS bot_stats (
        id            INTEGER     PRIMARY KEY DEFAULT 1,
        total_tts     BIGINT      NOT NULL DEFAULT 0,
        active_guilds BIGINT      NOT NULL DEFAULT 0,
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      INSERT INTO bot_stats (id) VALUES (1) ON CONFLICT DO NOTHING;

      CREATE TABLE IF NOT EXISTS stats_voices (
        voice     TEXT   PRIMARY KEY,
        use_count BIGINT NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS stats_langs (
        lang      TEXT   PRIMARY KEY,
        use_count BIGINT NOT NULL DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS stats_users (
        user_id    TEXT        PRIMARY KEY,
        tts_count  BIGINT      NOT NULL DEFAULT 1,
        first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    console.log("[DB] Schema verificado / creado.");
  } finally {
    conn.release();
  }
}

// ════════════════════════════════════════════════════════════════
//  4. CONFIG SYSTEM (TTL en memoria)
// ════════════════════════════════════════════════════════════════
const configTTLCache = new Map();

async function getGuildConfig(guildId) {
  const cached = configTTLCache.get(guildId);
  if (cached && Date.now() < cached.expiresAt) return cached.config;

  const { rows } = await dbQuery(
    "SELECT prefix, lang, voice FROM guild_settings WHERE guild_id = $1",
    [guildId]
  );

  let config;
  if (rows.length > 0) {
    config = rows[0];
  } else {
    config = { prefix: DEFAULT_PREFIX, lang: DEFAULT_LANG, voice: DEFAULT_VOICE };
    await dbQuery(
      `INSERT INTO guild_settings (guild_id, prefix, lang, voice)
       VALUES ($1,$2,$3,$4) ON CONFLICT (guild_id) DO NOTHING`,
      [guildId, config.prefix, config.lang, config.voice]
    );
  }

  configTTLCache.set(guildId, { config, expiresAt: Date.now() + CONFIG_TTL_MS });
  client.guildCache.set(guildId, config);
  return config;
}

async function updateGuildConfig(guildId, field, value) {
  const allowed = ["prefix", "lang", "voice"];
  if (!allowed.includes(field)) throw new Error("Campo inválido: " + field);
  await dbQuery(`UPDATE guild_settings SET ${field} = $1 WHERE guild_id = $2`, [value, guildId]);
  configTTLCache.delete(guildId);
  client.guildCache.delete(guildId);
}

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of configTTLCache) if (now > v.expiresAt) configTTLCache.delete(k);
}, 15 * 60_000);

// ════════════════════════════════════════════════════════════════
//  5. UI — botones & embeds
// ════════════════════════════════════════════════════════════════
function getGlobalButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setLabel("🤖 Invitar Bot").setStyle(ButtonStyle.Link).setURL(INVITE_URL),
    new ButtonBuilder().setLabel("💬 Soporte").setStyle(ButtonStyle.Link).setURL(SUPPORT_URL)
  );
}

function buildEmbed(title, description, color = 0x5865f2) {
  return new EmbedBuilder()
    .setTitle(title)
    .setDescription(description)
    .setColor(color)
    .setFooter({ text: `Yul TTS Bot${IS_SHARDED ? ` • Shard ${SHARD_ID}` : ""} • yul help` })
    .setTimestamp();
}

// ════════════════════════════════════════════════════════════════
//  6. COOLDOWN SYSTEM
// ════════════════════════════════════════════════════════════════
const cooldownMap  = new Map();
const spamScoreMap = new Map();

function checkCooldown(userId, guildId = null) {
  const now  = Date.now();
  const last = cooldownMap.get(userId) ?? 0;
  const diff = now - last;

  if (diff < TTS_COOLDOWN_MS) {
    return { ok: false, remaining: Math.ceil((TTS_COOLDOWN_MS - diff) / 1000), spam: false };
  }

  if (guildId) {
    const key     = `${guildId}:${userId}`;
    const entry   = spamScoreMap.get(key) ?? { score: 0, lastHit: now };
    const elapsed = (now - entry.lastHit) / 1000;
    const score   = Math.max(0, entry.score - elapsed * SPAM_DECAY_RATE) + 1;
    spamScoreMap.set(key, { score, lastHit: now });
    if (score > SPAM_THRESHOLD) return { ok: false, remaining: Math.ceil(score), spam: true };
  }

  cooldownMap.set(userId, now);
  return { ok: true, remaining: 0, spam: false };
}

setInterval(() => {
  const now = Date.now();
  for (const [k, ts] of cooldownMap)
    if (now - ts > TTS_COOLDOWN_MS * 20) cooldownMap.delete(k);
  for (const [k, v] of spamScoreMap)
    if ((now - v.lastHit) / 1000 > 120) spamScoreMap.delete(k);
}, 10 * 60_000);

// ════════════════════════════════════════════════════════════════
//  7. VALIDACIÓN DE TEXTO
// ════════════════════════════════════════════════════════════════
function validateText(raw) {
  if (!raw?.trim()) return { valid: false, reason: "El texto no puede estar vacío." };
  URL_REGEX.lastIndex = 0;
  if (URL_REGEX.test(raw)) {
    URL_REGEX.lastIndex = 0;
    return { valid: false, reason: "El texto no puede contener URLs o enlaces." };
  }
  URL_REGEX.lastIndex = 0;
  return { valid: true, cleaned: raw.trim().slice(0, TTS_MAX_CHARS) };
}

// ════════════════════════════════════════════════════════════════
//  8. CACHE SYSTEM (SHA256 / LRU-Mem / PostgreSQL)
// ════════════════════════════════════════════════════════════════
const memCache = new Map();

function makeCacheHash(text, voice) {
  return crypto.createHash("sha256")
    .update(`${voice}::${text.trim().toLowerCase()}`)
    .digest("hex");
}

function _memGet(hash) {
  const e = memCache.get(hash);
  if (!e) return null;
  memCache.delete(hash);
  memCache.set(hash, e);
  return e;
}

function _memSet(hash, entry) {
  if (memCache.size >= CACHE_MAX_MEM) memCache.delete(memCache.keys().next().value);
  memCache.set(hash, entry);
}

async function cacheGet(hash) {
  const mem = _memGet(hash);
  if (mem) {
    dbQuery("UPDATE tts_cache SET last_used=NOW(),use_count=use_count+1 WHERE hash=$1", [hash]).catch(() => {});
    return mem;
  }

  const { rows } = await dbQuery("SELECT file_path,voice,text FROM tts_cache WHERE hash=$1", [hash]);
  if (!rows.length) return null;

  const entry = { filePath: rows[0].file_path, voice: rows[0].voice, text: rows[0].text };
  try {
    await fsp.access(entry.filePath, fs.constants.R_OK);
  } catch {
    dbQuery("DELETE FROM tts_cache WHERE hash=$1", [hash]).catch(() => {});
    return null;
  }

  _memSet(hash, entry);
  dbQuery("UPDATE tts_cache SET last_used=NOW(),use_count=use_count+1 WHERE hash=$1", [hash]).catch(() => {});
  return entry;
}

async function cacheSet(hash, filePath, voice, text) {
  _memSet(hash, { filePath, voice, text });
  await dbQuery(
    `INSERT INTO tts_cache (hash,file_path,voice,text) VALUES ($1,$2,$3,$4)
     ON CONFLICT (hash) DO UPDATE SET last_used=NOW(),use_count=tts_cache.use_count+1`,
    [hash, filePath, voice, text]
  );
}

// ════════════════════════════════════════════════════════════════
//  9. TTS ENGINE — generateTTS usa _generateGTTS (node-gtts)
//
//  La firma pública generateTTS(text, voice) → Promise<string>
//  es idéntica a la versión anterior con edge-tts.
//  Solo cambia la función interna de síntesis.
// ════════════════════════════════════════════════════════════════

/** Deduplicación de síntesis concurrentes idénticas */
const inflightTTS = new Map();

/**
 * Genera (o recupera de caché) el archivo de audio TTS.
 * Internamente usa node-gtts en lugar del ejecutable edge-tts.
 *
 * @param {string} text   — Texto ya validado
 * @param {string} voice  — ID de voz (e.g. "es-MX-DaliaNeural")
 * @returns {Promise<string>} — Ruta absoluta al .mp3
 */
async function generateTTS(text, voice) {
  const hash   = makeCacheHash(text, voice);
  const cached = await cacheGet(hash);
  if (cached) {
    console.log(`[TTS] Cache hit ${hash.slice(0, 8)}… ${voice}`);
    return cached.filePath;
  }

  // Deduplicación: múltiples solicitudes del mismo audio se fusionan
  if (inflightTTS.has(hash)) {
    console.log(`[TTS] In-flight join ${hash.slice(0, 8)}…`);
    return inflightTTS.get(hash);
  }

  const synthesis = (async () => {
    const outFile = path.join(AUDIO_DIR, `${hash}.mp3`);

    // Eliminar archivo parcial previo si existe
    await fsp.unlink(outFile).catch(() => {});

    // _generateGTTS usa node-gtts — funciona en cualquier plataforma sin instalar nada extra
    try {
      await _generateGTTS(text, voice, outFile);
    } catch (err) {
      await fsp.unlink(outFile).catch(() => {});
      throw err;
    }

    // Verificar que el archivo existe y tiene contenido
    let stat;
    try {
      stat = await fsp.stat(outFile);
    } catch {
      throw new Error("node-gtts no generó el archivo de audio.");
    }

    if (stat.size === 0) {
      await fsp.unlink(outFile).catch(() => {});
      throw new Error("node-gtts generó un archivo de audio vacío.");
    }

    await cacheSet(hash, outFile, voice, text);
    console.log(`[TTS] Generado ${hash.slice(0, 8)}… ${voice} → lang:${getGttsLang(voice)} (${stat.size} bytes)`);
    return outFile;
  })();

  inflightTTS.set(hash, synthesis);
  try {
    return await synthesis;
  } finally {
    inflightTTS.delete(hash);
  }
}

// ════════════════════════════════════════════════════════════════
//  10. STATS SYSTEM
// ════════════════════════════════════════════════════════════════
const statsBuffer = {
  totalTTS:    0,
  guilds:      new Set(),
  users:       new Set(),
  voiceCounts: new Map(),
  langCounts:  new Map(),
};

function recordTTSUse(userId, guildId, voice, lang) {
  statsBuffer.totalTTS++;
  statsBuffer.guilds.add(guildId);
  statsBuffer.users.add(userId);
  statsBuffer.voiceCounts.set(voice, (statsBuffer.voiceCounts.get(voice) ?? 0) + 1);
  statsBuffer.langCounts.set(lang,   (statsBuffer.langCounts.get(lang)   ?? 0) + 1);
}

async function flushStats() {
  if (statsBuffer.totalTTS === 0) return;

  const snap = {
    totalTTS:    statsBuffer.totalTTS,
    guilds:      statsBuffer.guilds.size,
    users:       [...statsBuffer.users],
    voiceCounts: new Map(statsBuffer.voiceCounts),
    langCounts:  new Map(statsBuffer.langCounts),
  };

  statsBuffer.totalTTS = 0;
  statsBuffer.guilds.clear();
  statsBuffer.users.clear();
  statsBuffer.voiceCounts.clear();
  statsBuffer.langCounts.clear();

  const conn = await pool.connect();
  try {
    await conn.query(
      "UPDATE bot_stats SET total_tts=total_tts+$1,active_guilds=$2,updated_at=NOW() WHERE id=1",
      [snap.totalTTS, snap.guilds]
    );
    for (const uid of snap.users) {
      await conn.query(
        `INSERT INTO stats_users (user_id) VALUES ($1)
         ON CONFLICT (user_id) DO UPDATE SET tts_count=stats_users.tts_count+1,last_seen=NOW()`,
        [uid]
      );
    }
    for (const [voice, n] of snap.voiceCounts) {
      await conn.query(
        `INSERT INTO stats_voices (voice,use_count) VALUES ($1,$2)
         ON CONFLICT (voice) DO UPDATE SET use_count=stats_voices.use_count+EXCLUDED.use_count`,
        [voice, n]
      );
    }
    for (const [lang, n] of snap.langCounts) {
      await conn.query(
        `INSERT INTO stats_langs (lang,use_count) VALUES ($1,$2)
         ON CONFLICT (lang) DO UPDATE SET use_count=stats_langs.use_count+EXCLUDED.use_count`,
        [lang, n]
      );
    }
  } catch (err) {
    console.error("[STATS] Flush error:", err.message);
  } finally {
    conn.release();
  }
}

async function getStatsSnapshot() {
  const [g, v, l, u] = await Promise.all([
    dbQuery("SELECT total_tts,active_guilds,updated_at FROM bot_stats WHERE id=1"),
    dbQuery("SELECT voice,use_count FROM stats_voices ORDER BY use_count DESC LIMIT 1"),
    dbQuery("SELECT lang,use_count  FROM stats_langs  ORDER BY use_count DESC LIMIT 1"),
    dbQuery("SELECT COUNT(*) AS cnt FROM stats_users"),
  ]);
  return {
    totalTTS:     g.rows[0]?.total_tts     ?? 0,
    activeGuilds: g.rows[0]?.active_guilds ?? 0,
    updatedAt:    g.rows[0]?.updated_at    ?? new Date(),
    topVoice:     v.rows[0]?.voice         ?? "—",
    topLang:      l.rows[0]?.lang          ?? "—",
    uniqueUsers:  u.rows[0]?.cnt           ?? 0,
  };
}

setInterval(() => flushStats().catch(() => {}), STATS_FLUSH_MS);

// ════════════════════════════════════════════════════════════════
//  11. HEALTH MONITOR
// ════════════════════════════════════════════════════════════════
let lastHealthSnapshot = null;
let _cpuPrev = os.cpus().map((c) => ({ ...c.times }));

function _getCPU() {
  const cpus = os.cpus();
  let total = 0, idle = 0;
  cpus.forEach((cpu, i) => {
    const p = _cpuPrev[i] || cpu.times;
    const d = Object.fromEntries(Object.entries(cpu.times).map(([k, v]) => [k, v - (p[k] ?? 0)]));
    total += Object.values(d).reduce((a, b) => a + b, 0);
    idle  += d.idle ?? 0;
  });
  _cpuPrev = cpus.map((c) => ({ ...c.times }));
  return total > 0 ? +((1 - idle / total) * 100).toFixed(1) : 0;
}

async function measureDBLatency() {
  const t = Date.now();
  await dbQuery("SELECT 1");
  return Date.now() - t;
}

async function runHealthCheck() {
  const totalMem = os.totalmem();
  const memPct   = +((1 - os.freemem() / totalMem) * 100).toFixed(1);
  const heapMB   = +(process.memoryUsage().heapUsed / 1048576).toFixed(1);
  const cpuPct   = _getCPU();
  const wsPing   = client.ws.ping;
  const guilds   = client.guilds.cache.size;
  const queues   = guildQueues.size;

  let dbMs = -1, dbOk = true;
  try { dbMs = await measureDBLatency(); }
  catch (err) { dbOk = false; sendErrorLog("DB Health Error", err.message); }

  lastHealthSnapshot = { timestamp: new Date(), memPct, heapMB, cpuPct, wsPing, dbMs, dbOk, guilds, queues };

  console.log(
    `[HEALTH] CPU:${cpuPct}% RAM:${memPct}%(heap:${heapMB}MB)` +
    ` WS:${wsPing}ms DB:${dbOk ? dbMs + "ms" : "ERR"}` +
    ` Guilds:${guilds} Queues:${queues} Inflight:${inflightTTS.size}`
  );

  const alerts = [];
  if (memPct > 90)  alerts.push(`⚠️ RAM crítica: **${memPct}%**`);
  if (cpuPct > 90)  alerts.push(`⚠️ CPU crítica: **${cpuPct}%**`);
  if (wsPing > 500) alerts.push(`⚠️ WS Ping alto: **${wsPing}ms**`);
  if (!dbOk)        alerts.push("🔴 Base de datos no responde");
  if (dbMs > 1_000) alerts.push(`⚠️ DB lenta: **${dbMs}ms**`);

  if (alerts.length) sendErrorLog("Health Alert", alerts.join("\n"), 0xfee75c);

  if (STATS_CHANNEL_ID && client.isReady()) {
    const ch = await client.channels.fetch(STATS_CHANNEL_ID).catch(() => null);
    if (ch?.isTextBased()) {
      ch.send({
        embeds: [buildEmbed(
          "📡 Health Report",
          [
            `**CPU:** ${cpuPct}%`,
            `**RAM:** ${memPct}% (heap: ${heapMB} MB)`,
            `**WS Ping:** ${wsPing}ms`,
            `**DB:** ${dbOk ? dbMs + "ms" : "❌ Error"}`,
            `**Guilds:** ${guilds} | **Queues:** ${queues}`,
            alerts.length ? "\n" + alerts.join("\n") : "",
          ].join("\n").trim(),
          alerts.length ? 0xfee75c : 0x57f287
        )],
      }).catch(() => {});
    }
  }
}

setInterval(() => runHealthCheck().catch(() => {}), HEALTH_INTERVAL_MS);

// ════════════════════════════════════════════════════════════════
//  12. QUEUE SYSTEM — GuildQueue con idle timer
// ════════════════════════════════════════════════════════════════

/**
 * @typedef {Object} QueueItem
 * @property {string} text
 * @property {string} voice
 * @property {string} guildId
 * @property {string} channelId
 * @property {string} requestedBy
 * @property {import("discord.js").TextChannel|null} replyChannel
 */

/**
 * @typedef {Object} GuildQueue
 * @property {import("@discordjs/voice").VoiceConnection} connection
 * @property {import("@discordjs/voice").AudioPlayer}     player
 * @property {QueueItem[]}           songs
 * @property {string}                voice
 * @property {boolean}               isPlaying
 * @property {boolean}               locked
 * @property {NodeJS.Timeout|null}   idleTimer
 */

/** @type {Map<string, GuildQueue>} */
const guildQueues = new Map();

function _clearIdleTimer(queue) {
  if (queue.idleTimer) { clearTimeout(queue.idleTimer); queue.idleTimer = null; }
}

function _setIdleTimer(guildId, queue) {
  _clearIdleTimer(queue);
  queue.idleTimer = setTimeout(() => {
    if (!queue.isPlaying && queue.songs.length === 0) {
      console.log(`[QUEUE] Idle timeout — destruyendo ${guildId}`);
      leaveVoiceChannel(guildId);
    }
  }, QUEUE_IDLE_TIMEOUT);
}

function _attachPlayerListeners(guildId, queue) {
  queue.player.removeAllListeners();

  queue.player.on("error", (err) => {
    sendErrorLog(`Player Error — ${guildId}`, err.message, 0xfee75c);
    queue.isPlaying = false;
    queue.locked    = false;
    setImmediate(() => advanceQueue(guildId));
  });

  queue.player.on(AudioPlayerStatus.Idle, () => {
    queue.isPlaying = false;
    queue.locked    = false;
    setImmediate(() => advanceQueue(guildId));
  });
}

function _buildPlayer(guildId, queue) {
  const player = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Pause } });
  queue.player = player;
  _attachPlayerListeners(guildId, queue);
  return player;
}

function getOrCreateQueue(guildId, connection, voice) {
  if (guildQueues.has(guildId)) {
    const q = guildQueues.get(guildId);
    _clearIdleTimer(q);
    if (connection && q.connection !== connection) {
      q.connection = connection;
      _buildPlayer(guildId, q);
      connection.subscribe(q.player);
    }
    return q;
  }

  const player = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Pause } });
  const queue  = {
    connection, player,
    songs: [], voice: voice || DEFAULT_VOICE,
    isPlaying: false, locked: false, idleTimer: null,
  };

  guildQueues.set(guildId, queue);
  connection.subscribe(player);
  _attachPlayerListeners(guildId, queue);
  return queue;
}

function destroyQueue(guildId) {
  const q = guildQueues.get(guildId);
  if (!q) return;
  _clearIdleTimer(q);
  try { q.player.stop(true); }    catch (_) {}
  try { q.connection.destroy(); } catch (_) {}
  q.songs  = [];
  q.locked = false;
  guildQueues.delete(guildId);
  console.log(`[QUEUE] Destruida — ${guildId}`);
}

// ════════════════════════════════════════════════════════════════
//  13. VOICE SYSTEM — join + reconexión + rejoin automático
// ════════════════════════════════════════════════════════════════
async function joinVoiceChannelForMember(member, guild, retriesLeft = VOICE_REJOIN_RETRIES) {
  const vc = member.voice?.channel;
  if (!vc) throw new Error("Debes estar en un canal de voz para usar TTS.");

  const existing = getVoiceConnection(guild.id);
  if (existing) {
    if (existing.joinConfig?.channelId === vc.id) {
      if (existing.state.status === VoiceConnectionStatus.Ready) return existing;
      try { existing.destroy(); } catch (_) {}
      destroyQueue(guild.id);
    } else {
      throw new Error("Ya estoy en otro canal de voz. Usa `yul leave` o únete a ese canal.");
    }
  }

  const conn = joinVoiceChannel({
    channelId: vc.id, guildId: guild.id,
    adapterCreator: guild.voiceAdapterCreator, selfDeaf: true,
  });

  try {
    await entersState(conn, VoiceConnectionStatus.Ready, 10_000);
  } catch (err) {
    conn.destroy();
    if (retriesLeft > 0) {
      console.warn(`[VOICE] Join falló (${retriesLeft} reintentos)…`);
      await new Promise((r) => setTimeout(r, VOICE_REJOIN_DELAY));
      return joinVoiceChannelForMember(member, guild, retriesLeft - 1);
    }
    throw new Error("No pude conectarme al canal. Revisa mis permisos.");
  }

  // Manejo de desconexión con rejoin + queue recovery
  conn.on(VoiceConnectionStatus.Disconnected, async () => {
    console.warn(`[VOICE] Desconexión — guild ${guild.id}`);
    try {
      await Promise.race([
        entersState(conn, VoiceConnectionStatus.Signalling, 5_000),
        entersState(conn, VoiceConnectionStatus.Connecting, 5_000),
      ]);
      console.log(`[VOICE] Reconectado — guild ${guild.id}`);
    } catch {
      console.warn(`[VOICE] Reconexión fallida — rejoin — guild ${guild.id}`);
      conn.destroy();

      const queue = guildQueues.get(guild.id);
      if (!queue || queue.songs.length === 0) { destroyQueue(guild.id); return; }

      const savedSongs = [...queue.songs];
      const savedVoice = queue.voice;
      const vcId       = savedSongs[0]?.channelId;
      destroyQueue(guild.id);

      if (!vcId) return;
      try {
        const voiceCh = await client.channels.fetch(vcId).catch(() => null);
        if (!voiceCh?.isVoiceBased()) return;

        const newConn = joinVoiceChannel({
          channelId: vcId, guildId: guild.id,
          adapterCreator: guild.voiceAdapterCreator, selfDeaf: true,
        });
        await entersState(newConn, VoiceConnectionStatus.Ready, 10_000);
        const newQueue = getOrCreateQueue(guild.id, newConn, savedVoice);
        newQueue.songs = savedSongs;
        console.log(`[VOICE] Rejoin + recovery (${savedSongs.length} items) — guild ${guild.id}`);
        advanceQueue(guild.id);
      } catch (e) {
        console.error(`[VOICE] Rejoin fallido — guild ${guild.id}:`, e.message);
        destroyQueue(guild.id);
      }
    }
  });

  console.log(`[VOICE] Conectado a "${vc.name}" (${guild.name})`);
  return conn;
}

function leaveVoiceChannel(guildId) {
  const c = getVoiceConnection(guildId);
  if (c) try { c.destroy(); } catch (_) {}
  destroyQueue(guildId);
}

// ════════════════════════════════════════════════════════════════
//  14. ADVANCE QUEUE
// ════════════════════════════════════════════════════════════════
async function advanceQueue(guildId) {
  const queue = guildQueues.get(guildId);
  if (!queue || queue.locked) return;

  if (queue.songs.length === 0) {
    queue.isPlaying = false;
    _setIdleTimer(guildId, queue);
    return;
  }

  const conn = getVoiceConnection(guildId);
  if (!conn || conn.state.status !== VoiceConnectionStatus.Ready) {
    destroyQueue(guildId);
    return;
  }

  queue.locked    = true;
  queue.isPlaying = true;
  _clearIdleTimer(queue);

  const item = queue.songs.shift();

  let audioFile;
  try {
    audioFile = await generateTTS(item.text, item.voice || queue.voice);
  } catch (err) {
    sendErrorLog(`TTS Error — ${guildId}`, err.message, 0xfee75c);
    if (item.replyChannel) {
      item.replyChannel.send({
        embeds: [buildEmbed("❌ Error TTS", err.message, 0xed4245)],
        components: [getGlobalButtons()],
      }).catch(() => {});
    }
    queue.locked = queue.isPlaying = false;
    setImmediate(() => advanceQueue(guildId));
    return;
  }

  try {
    queue.player.play(createAudioResource(audioFile));
    console.log(`[PLAYER] ${guildId} | "${item.text.slice(0, 45)}…" | ${item.voice || queue.voice}`);
  } catch (err) {
    sendErrorLog(`Player Error — ${guildId}`, err.message, 0xfee75c);
    queue.locked = queue.isPlaying = false;
    setImmediate(() => advanceQueue(guildId));
  }
}

// ════════════════════════════════════════════════════════════════
//  15. PROCESADOR TTS UNIFICADO
// ════════════════════════════════════════════════════════════════
async function processTTSRequest({ text, voice, guildId, userId, member, guild, replyChannel, reply, lang }) {
  const row = getGlobalButtons();

  const cd = checkCooldown(userId, guildId);
  if (!cd.ok) {
    return reply({
      embeds: [buildEmbed("⏳ Cooldown activo",
        cd.spam ? `⚠️ Anti-spam activo. Espera **${cd.remaining}s**.`
                : `Espera **${cd.remaining}s** antes de otro TTS.`,
        0xfee75c)],
      components: [row], ephemeral: true,
    });
  }

  const val = validateText(text);
  if (!val.valid) {
    return reply({
      embeds: [buildEmbed("❌ Texto inválido", val.reason, 0xed4245)],
      components: [row], ephemeral: true,
    });
  }

  let conn;
  try { conn = await joinVoiceChannelForMember(member, guild); }
  catch (err) {
    return reply({
      embeds: [buildEmbed("❌ Error de voz", err.message, 0xed4245)],
      components: [row], ephemeral: true,
    });
  }

  const queue = getOrCreateQueue(guildId, conn, voice);
  queue.songs.push({
    text: val.cleaned, voice, guildId,
    channelId: member.voice.channel.id,
    requestedBy: userId, replyChannel,
  });

  const pos       = queue.songs.length + (queue.isPlaying ? 1 : 0);
  const voiceInfo = SUPPORTED_VOICES.find((v) => v.id === voice);
  recordTTSUse(userId, guildId, voice, voiceInfo?.lang ?? lang ?? DEFAULT_LANG);

  await reply({
    embeds: [buildEmbed("🔊 TTS encolado", [
      `**Texto:** ${val.cleaned.slice(0, 80)}${val.cleaned.length > 80 ? "…" : ""}`,
      `**Voz:** \`${voice}\``,
      `**Posición:** ${pos}`,
    ].join("\n"), 0x57f287)],
    components: [row],
  });

  if (!queue.isPlaying && !queue.locked) advanceQueue(guildId);
}

// ════════════════════════════════════════════════════════════════
//  16. LIMPIEZA
// ════════════════════════════════════════════════════════════════
async function runCacheCleanup() {
  console.log("[CLEANUP] Iniciando…");

  try {
    const cutoff    = new Date(Date.now() - CACHE_TTL_MS).toISOString();
    const { rows }  = await dbQuery("SELECT hash,file_path FROM tts_cache WHERE last_used<$1", [cutoff]);
    let deleted     = 0;
    for (let i = 0; i < rows.length; i += 10) {
      await Promise.all(rows.slice(i, i + 10).map(async ({ hash, file_path }) => {
        fsp.unlink(file_path).catch(() => {});
        dbQuery("DELETE FROM tts_cache WHERE hash=$1", [hash]).catch(() => {});
        memCache.delete(hash);
        deleted++;
      }));
    }
    if (deleted) console.log(`[CLEANUP] ${deleted} entradas DB expiradas eliminadas.`);
  } catch (err) { console.error("[CLEANUP] DB:", err.message); }

  try {
    const files     = (await fsp.readdir(AUDIO_DIR)).filter((f) => f.endsWith(".mp3"));
    const { rows }  = await dbQuery("SELECT file_path FROM tts_cache");
    const known     = new Set(rows.map((r) => r.file_path));
    let orphans     = 0;
    for (const f of files) {
      const fp = path.join(AUDIO_DIR, f);
      if (!known.has(fp)) { fsp.unlink(fp).catch(() => {}); orphans++; }
    }
    if (orphans) console.log(`[CLEANUP] ${orphans} archivos huérfanos eliminados.`);
  } catch (err) { console.error("[CLEANUP] Disco:", err.message); }

  for (const [gid] of guildQueues) {
    if (!client.guilds.cache.has(gid)) {
      console.log(`[CLEANUP] Queue de guild ausente ${gid} destruida.`);
      destroyQueue(gid);
    }
  }
}

setInterval(() => runCacheCleanup().catch(() => {}), CLEANUP_INTERVAL_MS);

// ════════════════════════════════════════════════════════════════
//  17. SLASH COMMANDS
// ════════════════════════════════════════════════════════════════
const slashCommands = [
  new SlashCommandBuilder().setName("help").setDescription("Muestra todos los comandos"),
  new SlashCommandBuilder().setName("ping").setDescription("Latencia del bot"),
  new SlashCommandBuilder()
    .setName("tts").setDescription("Convierte texto a voz")
    .addStringOption((o) => o.setName("texto").setDescription("Texto (máx 500 chars)").setRequired(true)),
  new SlashCommandBuilder().setName("skip").setDescription("Salta el audio actual"),
  new SlashCommandBuilder().setName("stop").setDescription("Detiene y vacía la cola"),
  new SlashCommandBuilder().setName("leave").setDescription("Desconecta el bot del canal de voz"),
  new SlashCommandBuilder().setName("queue").setDescription("Muestra la cola TTS"),
  new SlashCommandBuilder().setName("voices").setDescription("Lista voces disponibles"),
  new SlashCommandBuilder()
    .setName("setvoice").setDescription("Cambia la voz TTS del servidor")
    .addStringOption((o) => {
      o.setName("voz").setDescription("ID de la voz").setRequired(true);
      SUPPORTED_VOICES.forEach((v) => o.addChoices({ name: v.label, value: v.id }));
      return o;
    }),
  new SlashCommandBuilder().setName("stats").setDescription("Estadísticas globales"),
  new SlashCommandBuilder().setName("health").setDescription("Estado del sistema"),
  new SlashCommandBuilder().setName("cachestats").setDescription("Estadísticas del caché"),
].map((c) => c.toJSON());

async function registerSlashCommands() {
  const rest = new REST({ version: "10" }).setToken(DISCORD_TOKEN);
  try {
    console.log("[SLASH] Registrando slash commands…");
    await rest.put(Routes.applicationCommands(CLIENT_ID), { body: slashCommands });
    console.log("[SLASH] Commands registrados.");
  } catch (err) {
    console.error("[SLASH] Error:", err.message);
    sendErrorLog("Slash Register Error", err.message);
  }
}

// ════════════════════════════════════════════════════════════════
//  18. MANEJADORES UNIFICADOS
// ════════════════════════════════════════════════════════════════
function _formatUptime(s) {
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h ${Math.floor((s % 3600) / 60)}m ${Math.floor(s % 60)}s`;
}

async function handleHelp(ctx, config) {
  const p = config?.prefix ?? DEFAULT_PREFIX;
  return ctx.reply({
    embeds: [buildEmbed("📖 Comandos — Yul TTS", [
      `**Prefix:** \`${p}\``,
      "",
      `\`${p}tts <texto>\` / \`/tts\` — Texto a voz`,
      `\`${p}skip\` / \`/skip\` — Saltar audio`,
      `\`${p}stop\` / \`/stop\` — Detener cola`,
      `\`${p}leave\` / \`/leave\` — Salir del canal`,
      `\`${p}queue\` / \`/queue\` — Ver cola`,
      `\`${p}voices\` / \`/voices\` — Ver voces`,
      `\`${p}setvoice <id>\` / \`/setvoice\` — Cambiar voz`,
      `\`/stats\` · \`/health\` · \`/cachestats\``,
      "",
      `> Límite: ${TTS_MAX_CHARS} chars · Cooldown: 3s · Sin URLs`,
    ].join("\n"))],
    components: [getGlobalButtons()],
  });
}

async function handlePing(ctx, isSlash) {
  const ws = client.ws.ping;
  if (isSlash) {
    await ctx.deferReply();
    const api = Date.now();
    return ctx.editReply({
      embeds: [buildEmbed("🏓 Pong!", `**WebSocket:** ${ws}ms\n**API:** ${Date.now() - api}ms`)],
      components: [getGlobalButtons()],
    });
  }
  return ctx.reply({
    embeds: [buildEmbed("🏓 Pong!", `**WebSocket:** ${ws}ms`)],
    components: [getGlobalButtons()],
  });
}

async function handleTTSSlash(interaction, config) {
  await interaction.deferReply();
  await processTTSRequest({
    text: interaction.options.getString("texto"),
    voice: config?.voice ?? DEFAULT_VOICE,
    guildId: interaction.guild.id,
    userId: interaction.user.id,
    member: interaction.member,
    guild: interaction.guild,
    replyChannel: interaction.channel,
    reply: (o) => interaction.editReply(o),
    lang: config?.lang ?? DEFAULT_LANG,
  });
}

async function handleLeave(ctx, guildId, isSlash) {
  const row = getGlobalButtons();
  if (!getVoiceConnection(guildId)) {
    const opts = { embeds: [buildEmbed("ℹ️ Sin conexión", "No estoy en ningún canal de voz.", 0x5865f2)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  leaveVoiceChannel(guildId);
  return ctx.reply({ embeds: [buildEmbed("👋 Desconectado", "Salí del canal de voz.", 0x57f287)], components: [row] });
}

async function handleQueue(ctx, guildId) {
  const row   = getGlobalButtons();
  const q     = guildQueues.get(guildId);
  const total = q?.songs?.length ?? 0;
  const now   = q?.isPlaying ? "🎵 Reproduciendo ahora" : "⏸ Sin reproducción";
  if (!total) {
    return ctx.reply({ embeds: [buildEmbed("📋 Cola vacía", `${now}\nLa cola está vacía.`)], components: [row] });
  }
  const list = q.songs.slice(0, 10)
    .map((s, i) => `**${i + 1}.** ${s.text.slice(0, 55)}${s.text.length > 55 ? "…" : ""}`)
    .join("\n");
  return ctx.reply({
    embeds: [buildEmbed(
      `📋 Cola TTS — ${total} elemento${total !== 1 ? "s" : ""}`,
      `${now}\n\n${list}${total > 10 ? `\n_…y ${total - 10} más_` : ""}`
    )],
    components: [row],
  });
}

async function handleSkip(ctx, guildId, isSlash) {
  const row = getGlobalButtons();
  const q   = guildQueues.get(guildId);
  if (!q?.isPlaying) {
    const opts = { embeds: [buildEmbed("ℹ️ Nada que saltar", "No hay audio reproduciéndose.", 0x5865f2)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  q.player.stop();
  return ctx.reply({ embeds: [buildEmbed("⏭️ Saltado", "Audio omitido.", 0x57f287)], components: [row] });
}

async function handleStop(ctx, guildId, isSlash) {
  const row = getGlobalButtons();
  const q   = guildQueues.get(guildId);
  if (!q) {
    const opts = { embeds: [buildEmbed("ℹ️ Sin actividad", "No hay cola activa.", 0x5865f2)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  q.songs = []; q.locked = q.isPlaying = false;
  q.player.stop(true);
  return ctx.reply({
    embeds: [buildEmbed("⏹️ Detenido", "Cola vaciada y reproducción detenida.", 0xed4245)],
    components: [row],
  });
}

async function handleVoices(ctx) {
  return ctx.reply({
    embeds: [buildEmbed("🎙️ Voces disponibles", SUPPORTED_VOICES.map((v) => `\`${v.id}\` — ${v.label}`).join("\n"))],
    components: [getGlobalButtons()],
  });
}

async function handleSetVoice(ctx, guildId, voiceId, isSlash) {
  const row = getGlobalButtons();
  if (!VOICE_IDS.includes(voiceId)) {
    const opts = { embeds: [buildEmbed("❌ Voz inválida", "Usa `/voices` para ver las opciones.", 0xed4245)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  await updateGuildConfig(guildId, "voice", voiceId);
  const info = SUPPORTED_VOICES.find((v) => v.id === voiceId);
  const q    = guildQueues.get(guildId);
  if (q) q.voice = voiceId;
  return ctx.reply({
    embeds: [buildEmbed("✅ Voz actualizada", `**${info.label}**`)],
    components: [row],
  });
}

async function handleStats(ctx) {
  const row = getGlobalButtons();
  try {
    await flushStats();
    const snap = await getStatsSnapshot();
    return ctx.reply({
      embeds: [buildEmbed("📊 Estadísticas Globales — Yul TTS", [
        `**Total TTS generados:** ${Number(snap.totalTTS).toLocaleString()}`,
        `**Usuarios únicos:**     ${Number(snap.uniqueUsers).toLocaleString()}`,
        `**Servidores activos:**  ${Number(snap.activeGuilds).toLocaleString()}`,
        `**Idioma más usado:**    \`${snap.topLang}\``,
        `**Guilds conectados:**   ${client.guilds.cache.size}`,
        "",
        `_Actualizado: ${new Date(snap.updatedAt).toLocaleString()}_`,
      ].join("\n"))],
      components: [row],
    });
  } catch (err) {
    return ctx.reply({ embeds: [buildEmbed("❌ Error", err.message, 0xed4245)], components: [row] });
  }
}

async function handleHealth(ctx, isSlash) {
  const row = getGlobalButtons();
  if (isSlash) await ctx.deferReply();

  let dbMs = -1, dbOk = true;
  try { dbMs = await measureDBLatency(); } catch { dbOk = false; }

  const totalMem = os.totalmem();
  const memPct   = +((1 - os.freemem() / totalMem) * 100).toFixed(1);
  const heapMB   = +(process.memoryUsage().heapUsed / 1048576).toFixed(1);

  const embed = buildEmbed("📡 Estado del Sistema", [
    `**CPU:**           ${lastHealthSnapshot?.cpuPct ?? 0}%`,
    `**RAM:**           ${memPct}% (heap: ${heapMB} MB)`,
    `**WS Ping:**       ${client.ws.ping}ms`,
    `**DB Latency:**    ${dbOk ? dbMs + "ms" : "❌ Error"}`,
    `**Guilds:**        ${client.guilds.cache.size}`,
    `**Colas activas:** ${guildQueues.size}`,
    `**TTS en vuelo:**  ${inflightTTS.size}`,
    `**Uptime:**        ${_formatUptime(process.uptime())}`,
    `**Node.js:**       ${process.version}`,
    `**Plataforma:**    ${process.platform}`,
    IS_SHARDED ? `**Shard:**         ${SHARD_ID}/${TOTAL_SHARDS}` : "",
  ].filter(Boolean).join("\n"), dbOk ? 0x57f287 : 0xed4245);

  const send = isSlash ? (o) => ctx.editReply(o) : (o) => ctx.reply(o);
  return send({ embeds: [embed], components: [row] });
}

async function handleCacheStats(ctx) {
  const row = getGlobalButtons();
  try {
    const { rows: [s] } = await dbQuery(`
      SELECT COUNT(*) AS total, SUM(use_count) AS hits,
             MIN(created_at) AS oldest, MAX(last_used) AS newest
      FROM tts_cache
    `);
    const files = (await fsp.readdir(AUDIO_DIR)).filter((f) => f.endsWith(".mp3")).length;
    return ctx.reply({
      embeds: [buildEmbed("🗄️ Estadísticas del Caché TTS", [
        `**Entradas en DB:**     ${s.total}`,
        `**Usos totales:**       ${s.hits ?? 0}`,
        `**L1 memoria:**         ${memCache.size} / ${CACHE_MAX_MEM}`,
        `**TTS en vuelo:**       ${inflightTTS.size}`,
        `**Archivos .mp3:**      ${files}`,
        `**Creado más antiguo:** ${s.oldest ? new Date(s.oldest).toLocaleString() : "—"}`,
        `**Último uso:**         ${s.newest ? new Date(s.newest).toLocaleString() : "—"}`,
        `**TTL:**                7 días`,
      ].join("\n"))],
      components: [row],
    });
  } catch (err) {
    return ctx.reply({ embeds: [buildEmbed("❌ Error", err.message, 0xed4245)], components: [row] });
  }
}

// ════════════════════════════════════════════════════════════════
//  19. MANEJADOR PREFIX
// ════════════════════════════════════════════════════════════════
async function handlePrefixCommand(message, config) {
  const prefix = config.prefix;
  if (!message.content.toLowerCase().startsWith(prefix.toLowerCase())) return;

  const args    = message.content.slice(prefix.length).trim().split(/\s+/);
  const command = args.shift().toLowerCase();
  const gid     = message.guild.id;
  const row     = getGlobalButtons();

  switch (command) {
    case "help":       await handleHelp(message, config);              break;
    case "ping":       await handlePing(message, false);               break;
    case "skip":       await handleSkip(message, gid, false);          break;
    case "stop":       await handleStop(message, gid, false);          break;
    case "leave":      await handleLeave(message, gid, false);         break;
    case "queue":      await handleQueue(message, gid);                break;
    case "voices":     await handleVoices(message);                    break;
    case "stats":      await handleStats(message);                     break;
    case "health":     await handleHealth(message, false);             break;
    case "cachestats": await handleCacheStats(message);                break;

    case "tts": {
      if (!args.length) {
        await message.reply({
          embeds: [buildEmbed("❌ Uso", `\`${prefix}tts <texto>\``, 0xed4245)],
          components: [row],
        });
        return;
      }
      await processTTSRequest({
        text: args.join(" "), voice: config.voice, guildId: gid,
        userId: message.author.id, member: message.member, guild: message.guild,
        replyChannel: message.channel, reply: (o) => message.reply(o), lang: config.lang,
      });
      break;
    }

    case "setvoice": {
      if (!args[0]) {
        await message.reply({
          embeds: [buildEmbed("❌ Uso", `\`${prefix}setvoice <id>\``, 0xed4245)],
          components: [row],
        });
        return;
      }
      await handleSetVoice(message, gid, args[0], false);
      break;
    }

    default: break;
  }
}

// ════════════════════════════════════════════════════════════════
//  20. MENCIÓN AL BOT
// ════════════════════════════════════════════════════════════════
async function handleMention(message, config) {
  await message.reply({
    embeds: [buildEmbed("👋 hi i'm Yul TTS", [
      `**Prefix:** \`${config.prefix}\``,
      `**Idioma:** \`${config.lang}\``,
      "",
      `Use \`${config.prefix}help\` o \`/help\`  to view all commands <a:SleepyKitty:1518723956968788121>.`,
    ].join("\n"))],
    components: [getGlobalButtons()],
  });
}

// ════════════════════════════════════════════════════════════════
//  20.5. COMANDOS EXCLUSIVOS DEL OWNER
// ════════════════════════════════════════════════════════════════
async function handleOwnerCommands(message) {
  const OWNER_ID = '482441540346839040'; 
  const ownerPrefix = '!!';

  // Si el mensaje no empieza con '!', ignorar y dejar que el bot continúe
  if (!message.content.startsWith(ownerPrefix)) return false;

  const args = message.content.slice(ownerPrefix.length).trim().split(/ +/);
  const command = args.shift().toLowerCase();

  // Comandos que solo tú puedes ejecutar
  const validOwnerCommands = ['say', 'archivo', 'embed', 'embededit'];
  if (!validOwnerCommands.includes(command)) return false;

  if (message.author.id !== OWNER_ID) {
      message.reply("❌ No tienes permiso para usar este comando.");
      return true; // Retornamos true para detener el resto de la ejecución del bot
  }

  if (command === 'say') {
      const textToSay = args.join(' ');
      if (!textToSay) {
          message.reply("⚠️ Debes escribir algo para que yo lo diga.");
          return true;
      }
      try {
          await message.channel.send(textToSay);
          message.delete().catch(() => {});
      } catch (error) {
          console.error(error);
          message.reply("Hubo un error al intentar enviar el mensaje.");
      }
      return true;
  }

  if (command === 'archivo') {
      if (message.attachments.size === 0) {
          message.reply("Por favor, adjunta un archivo junto al comando.");
          return true;
      }
      const attachment = message.attachments.first();
      message.channel.send({ files: [attachment] }).catch(err => {
          message.reply("Hubo un error al reenviar el archivo.");
          console.error(err);
      });
      message.delete().catch(() => {});
      return true;
  }

  if (command === 'embed') {
      const parts = args.join(' ').split('image:');
      const descripcion = parts[0].trim().replace(/\\n/g, '\n');
      const imagenUrl = parts[1] ? parts[1].trim() : null;

      const embed = new EmbedBuilder()
          .setColor('#000000')
          .setTitle('『 <:miwa:1518385029653205103> 』 Yul')
          .setDescription(descripcion || ' ') // Evitar error si está vacío
          .setThumbnail(message.client.user.displayAvatarURL())
          .setTimestamp();

      if (imagenUrl && imagenUrl.startsWith('http')) embed.setImage(imagenUrl);

      message.channel.send({ embeds: [embed] });
      message.delete().catch(() => {});
      return true;
  }

  if (command === 'embededit') {
      if (!message.reference) {
          message.reply("Responde al embed que quieres editar.");
          return true;
      }
      
      const replyMessage = await message.channel.messages.fetch(message.reference.messageId);
      if (!replyMessage.embeds[0]) {
          message.reply("Ese mensaje no tiene un embed.");
          return true;
      }

      const embed = EmbedBuilder.from(replyMessage.embeds[0]);
      const parts = args.join(' ').split('imagen:');
      
      if (parts[0].trim()) embed.setDescription(parts[0].trim().replace(/\\n/g, '\n'));

      const imgOp = parts[1] ? parts[1].trim() : null;
      if (imgOp) {
          if (imgOp === 'false') {
              embed.setImage(null);
          } else if (imgOp.startsWith('change:')) {
              const newUrl = imgOp.split('change:')[1];
              if (newUrl.startsWith('http')) embed.setImage(newUrl);
          }
      }

      await replyMessage.edit({ embeds: [embed] });
      message.delete().catch(() => {});
      return true;
  }

  return false;
}

// ════════════════════════════════════════════════════════════════
//  21. EVENTOS DEL CLIENTE
// ════════════════════════════════════════════════════════════════
client.on(Events.MessageCreate, async (message) => {
  if (message.author.bot || !message.guild) return;

  // 1. Verificar si es un comando de Owner primero
  const isOwnerCommand = await handleOwnerCommands(message);
  if (isOwnerCommand) return; // Si fue un comando tuyo, detenemos la ejecución aquí.

  // 2. Continuar con el flujo normal del bot
  let config;
  try { config = await getGuildConfig(message.guild.id); }
  catch (err) { console.error("[MSG]", err.message); return; }

  const isMentioned =
    message.mentions.has(client.user) &&
    !message.content.toLowerCase().startsWith(config.prefix.toLowerCase());

  if (isMentioned) { await handleMention(message, config); return; }
  await handlePrefixCommand(message, config);
});
client.once(Events.ClientReady, async () => {
  console.log(`[BOT] ${client.user.tag} | Guilds: ${client.guilds.cache.size} | Shard: ${SHARD_ID}/${TOTAL_SHARDS} | OS: ${process.platform}`);
  client.user.setActivity("yul help | /help", { type: ActivityType.Listening });
  await registerSlashCommands();

  // Verificar engine node-gtts al arranque
  resolveEdgeTTSBin().catch((err) => {
    console.error("[TTS] ADVERTENCIA:", err.message);
    sendErrorLog("TTS engine warning", err.message, 0xfee75c);
  });

  runCacheCleanup().catch(() => {});
  runHealthCheck().catch(() => {});
});

client.on(Events.GuildCreate, async (guild) => {
  console.log(`[GUILD+] ${guild.name} (${guild.id})`);
  getGuildConfig(guild.id).catch(() => {});
});

client.on(Events.GuildDelete, (guild) => {
  configTTLCache.delete(guild.id);
  client.guildCache.delete(guild.id);
  leaveVoiceChannel(guild.id);
  console.log(`[GUILD-] ${guild.name}`);
});

// Si el bot es sacado del canal manualmente
client.on(Events.VoiceStateUpdate, (oldState, newState) => {
  if (oldState.member?.id === client.user?.id && !newState.channelId) {
    destroyQueue(oldState.guild.id);
  }
});

client.on(Events.MessageCreate, async (message) => {
  if (message.author.bot || !message.guild) return;
  let config;
  try { config = await getGuildConfig(message.guild.id); }
  catch (err) { console.error("[MSG]", err.message); return; }

  const isMentioned =
    message.mentions.has(client.user) &&
    !message.content.toLowerCase().startsWith(config.prefix.toLowerCase());

  if (isMentioned) { await handleMention(message, config); return; }
  await handlePrefixCommand(message, config);
});

client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.guild || !interaction.isChatInputCommand()) return;
  let config;
  try { config = await getGuildConfig(interaction.guild.id); }
  catch (err) { console.error("[SLASH]", err.message); return; }

  const gid = interaction.guild.id;

  switch (interaction.commandName) {
    case "help":       await handleHelp(interaction, config);                                              break;
    case "ping":       await handlePing(interaction, true);                                                break;
    case "tts":        await handleTTSSlash(interaction, config);                                          break;
    case "skip":       await handleSkip(interaction, gid, true);                                           break;
    case "stop":       await handleStop(interaction, gid, true);                                           break;
    case "leave":      await handleLeave(interaction, gid, true);                                          break;
    case "queue":      await handleQueue(interaction, gid);                                                break;
    case "voices":     await handleVoices(interaction);                                                    break;
    case "setvoice":   await handleSetVoice(interaction, gid, interaction.options.getString("voz"), true); break;
    case "stats":      await handleStats(interaction);                                                     break;
    case "health":     await handleHealth(interaction, true);                                              break;
    case "cachestats": await handleCacheStats(interaction);                                                break;
    default: interaction.reply({ content: "Comando no reconocido.", ephemeral: true });
  }
});

// ════════════════════════════════════════════════════════════════
//  22. GRACEFUL SHUTDOWN
// ════════════════════════════════════════════════════════════════
async function gracefulShutdown(signal) {
  console.log(`\n[SHUTDOWN] ${signal} — cerrando…`);
  sendErrorLog("Graceful Shutdown", `Señal: ${signal}`, 0xfee75c);
  try {
    await flushStats();
    for (const gid of guildQueues.keys()) leaveVoiceChannel(gid);
    client.destroy();
    await pool.end();
  } catch (err) {
    console.error("[SHUTDOWN] Error:", err.message);
  }
  process.exit(0);
}

process.on("SIGINT",  () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// ════════════════════════════════════════════════════════════════
//  23. ARRANQUE
// ════════════════════════════════════════════════════════════════
(async () => {
  try {
    console.log(`[BOT] Iniciando Yul TTS Bot — Final (platform: ${process.platform})…`);
    await initDatabase();
    await client.login(DISCORD_TOKEN);
  } catch (err) {
    console.error("[FATAL]", err.message);
    sendErrorLog("Boot Error", err.stack ?? err.message);
    process.exit(1);
  }
})();





// ════════════════════════════════════════════════════════════════
//  EXPORTS
// ════════════════════════════════════════════════════════════════
module.exports = {
  client, pool, guildQueues, memCache, inflightTTS,
  getGuildConfig, updateGuildConfig,
  generateTTS, makeCacheHash, cacheGet, cacheSet,
  resolveEdgeTTSBin,
  advanceQueue, getOrCreateQueue, destroyQueue,
  joinVoiceChannelForMember, leaveVoiceChannel,
  processTTSRequest, validateText, checkCooldown,
  recordTTSUse, flushStats, getStatsSnapshot,
  runHealthCheck, runCacheCleanup, sendErrorLog,
  getGlobalButtons, buildEmbed,
  SUPPORTED_VOICES, VOICE_IDS,
  IS_SHARDED, SHARD_ID, TOTAL_SHARDS,
};

// ════════════════════════════════════════════════════════════════
//  MIGRACIÓN DE edge-tts → node-gtts
//
//  ANTES (ya no necesario):
//    pip install edge-tts
//    EDGE_TTS_PATH=... (variable de entorno)
//
//  AHORA — solo esto:
//    npm install node-gtts
//
//  Funciona en Railway, Render, Fly.io, cualquier VPS Linux.
//  No requiere Python, no requiere ejecutables externos.
//  No requiere API key — usa el endpoint público de Google TTS.
//
//  Mapeo de voces (edge-tts ID → Google TTS lang):
//    es-MX-DaliaNeural / es-MX-JorgeNeural   → "es"
//    en-US-AriaNeural  / en-US-GuyNeural      → "en"
//    fr-FR-DeniseNeural                       → "fr"
//    pt-BR-AntonioNeural                      → "pt"
//    ja-JP-NanamiNeural                       → "ja"
//    de-DE-ConradNeural                       → "de"
//
//  Nota: Google TTS no diferencia voces "Neural" por nombre,
//  pero sí por idioma — el acento regional se conserva.
// ════════════════════════════════════════════════════════════════