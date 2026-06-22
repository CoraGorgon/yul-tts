// ============================================================
//  YUL TTS BOT — PARTE 4: SISTEMA GLOBAL PRO
//  Anti-crash · Stats · Health Monitor · Cooldown Pro
//  Stack: discord.js v14 · @discordjs/voice · pg · dotenv
//  Extiende Partes 1-3 — versión producción
// ============================================================

"use strict";

require("dotenv").config();

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

const { Pool }      = require("pg");
const fs            = require("fs");
const fsp           = require("fs/promises");
const path          = require("path");
const { exec }      = require("child_process");
const { promisify } = require("util");
const crypto        = require("crypto");
const os            = require("os");

const execAsync = promisify(exec);

// ============================================================
//  ENTORNO Y VALIDACIÓN INICIAL
// ============================================================
const {
  DISCORD_TOKEN,
  CLIENT_ID,
  DATABASE_URL,
  LOG_WEBHOOK_URL,   // opcional — webhook para logs de errores
  STATS_CHANNEL_ID,  // opcional — canal donde postear health reports
} = process.env;

if (!DISCORD_TOKEN || !CLIENT_ID || !DATABASE_URL) {
  console.error("[FATAL] Faltan variables: DISCORD_TOKEN, CLIENT_ID, DATABASE_URL");
  process.exit(1);
}

// ============================================================
//  CONSTANTES GLOBALES
// ============================================================
const INVITE_URL          = `https://discord.com/api/oauth2/authorize?client_id=${CLIENT_ID}&permissions=8&scope=bot%20applications.commands`;
const SUPPORT_URL         = "https://discord.gg/Ze5TEDCD";
const DEFAULT_PREFIX      = "yul ";
const DEFAULT_LANG        = "es";
const DEFAULT_VOICE       = "es-MX-DaliaNeural";
const TTS_MAX_CHARS       = 500;
const TTS_COOLDOWN_MS     = 3_000;
const AUDIO_DIR           = path.join(process.cwd(), "tts_audio");
const CACHE_MAX_MEM       = 200;
const CACHE_TTL_MS        = 7 * 24 * 60 * 60 * 1_000;
const CLEANUP_INTERVAL_MS = 30 * 60 * 1_000;
const HEALTH_INTERVAL_MS  = 60_000;
const STATS_SAVE_MS       = 5 * 60_000;
const URL_REGEX           = /https?:\/\/[^\s]+|discord\.gg\/[^\s]+|www\.[^\s]+/gi;

// Webhook opcional para logs de errores en Discord
const logWebhook = LOG_WEBHOOK_URL ? new WebhookClient({ url: LOG_WEBHOOK_URL }) : null;

if (!fs.existsSync(AUDIO_DIR)) fs.mkdirSync(AUDIO_DIR, { recursive: true });

// ============================================================
//  VOCES SOPORTADAS
// ============================================================
const SUPPORTED_VOICES = [
  { id: "es-MX-DaliaNeural",   lang: "es", label: "Dalia (ES-MX) 🇲🇽"   },
  { id: "es-MX-JorgeNeural",   lang: "es", label: "Jorge (ES-MX) 🇲🇽"   },
  { id: "en-US-AriaNeural",    lang: "en", label: "Aria (EN-US) 🇺🇸"    },
  { id: "en-US-GuyNeural",     lang: "en", label: "Guy (EN-US) 🇺🇸"     },
  { id: "fr-FR-DeniseNeural",  lang: "fr", label: "Denise (FR) 🇫🇷"     },
  { id: "pt-BR-AntonioNeural", lang: "pt", label: "Antonio (PT-BR) 🇧🇷" },
  { id: "ja-JP-NanamiNeural",  lang: "ja", label: "Nanami (JA) 🇯🇵"     },
  { id: "de-DE-ConradNeural",  lang: "de", label: "Conrad (DE) 🇩🇪"     },
];
const VOICE_IDS = SUPPORTED_VOICES.map((v) => v.id);

// ============================================================
//  1. CLIENTE DISCORD
// ============================================================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildVoiceStates,
  ],
  partials: [Partials.Channel],
});

client.guildCache = new Collection();

// ============================================================
//  2. POSTGRESQL — POOL + INIT
// ============================================================
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DATABASE_SSL === "true" ? { rejectUnauthorized: false } : false,
  max: 20,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

pool.on("error", (err) => {
  console.error("[DB] Pool error:", err.message);
  sendErrorLog("DB Pool Error", err.message);
});

async function initDatabase() {
  const conn = await pool.connect();
  try {
    // guild_settings
    await conn.query(`
      CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id  TEXT PRIMARY KEY,
        prefix    TEXT NOT NULL DEFAULT '${DEFAULT_PREFIX}',
        lang      TEXT NOT NULL DEFAULT '${DEFAULT_LANG}',
        voice     TEXT NOT NULL DEFAULT '${DEFAULT_VOICE}'
      );
    `);

    // tts_cache (SHA256)
    await conn.query(`
      CREATE TABLE IF NOT EXISTS tts_cache (
        hash        TEXT PRIMARY KEY,
        file_path   TEXT        NOT NULL,
        voice       TEXT        NOT NULL,
        text        TEXT        NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_used   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        use_count   INTEGER     NOT NULL DEFAULT 1
      );
      CREATE INDEX IF NOT EXISTS idx_tts_cache_last_used ON tts_cache (last_used);
    `);

    // bot_stats — una fila global
    await conn.query(`
      CREATE TABLE IF NOT EXISTS bot_stats (
        id            INTEGER PRIMARY KEY DEFAULT 1,
        total_tts     BIGINT  NOT NULL DEFAULT 0,
        unique_users  BIGINT  NOT NULL DEFAULT 0,
        active_guilds BIGINT  NOT NULL DEFAULT 0,
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      INSERT INTO bot_stats (id) VALUES (1) ON CONFLICT DO NOTHING;
    `);

    // stats_voices — uso por voz
    await conn.query(`
      CREATE TABLE IF NOT EXISTS stats_voices (
        voice     TEXT PRIMARY KEY,
        use_count BIGINT NOT NULL DEFAULT 0
      );
    `);

    // stats_langs — uso por idioma
    await conn.query(`
      CREATE TABLE IF NOT EXISTS stats_langs (
        lang      TEXT PRIMARY KEY,
        use_count BIGINT NOT NULL DEFAULT 0
      );
    `);

    // stats_users — usuarios únicos
    await conn.query(`
      CREATE TABLE IF NOT EXISTS stats_users (
        user_id    TEXT PRIMARY KEY,
        tts_count  BIGINT NOT NULL DEFAULT 1,
        first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    console.log("[DB] Tablas verificadas / creadas.");
  } finally {
    conn.release();
  }
}

// ============================================================
//  3. CONFIG SYSTEM
// ============================================================
async function getGuildConfig(guildId) {
  if (client.guildCache.has(guildId)) return client.guildCache.get(guildId);

  const { rows } = await pool.query(
    "SELECT prefix, lang, voice FROM guild_settings WHERE guild_id = $1",
    [guildId]
  );

  if (rows.length > 0) {
    client.guildCache.set(guildId, rows[0]);
    return rows[0];
  }

  const def = { prefix: DEFAULT_PREFIX, lang: DEFAULT_LANG, voice: DEFAULT_VOICE };
  await pool.query(
    `INSERT INTO guild_settings (guild_id, prefix, lang, voice)
     VALUES ($1,$2,$3,$4) ON CONFLICT (guild_id) DO NOTHING`,
    [guildId, def.prefix, def.lang, def.voice]
  );
  client.guildCache.set(guildId, def);
  return def;
}

async function updateGuildConfig(guildId, field, value) {
  const allowed = ["prefix", "lang", "voice"];
  if (!allowed.includes(field)) throw new Error("Campo inválido: " + field);
  await pool.query(
    `UPDATE guild_settings SET ${field} = $1 WHERE guild_id = $2`,
    [value, guildId]
  );
  const cached = client.guildCache.get(guildId) || {};
  cached[field] = value;
  client.guildCache.set(guildId, cached);
}

// ============================================================
//  4. UI — botones & embeds
// ============================================================
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
    .setFooter({ text: "Yul TTS Bot • yul help" })
    .setTimestamp();
}

// ============================================================
//  5. STATS SYSTEM
// ============================================================

/**
 * Contador en memoria — se persiste en DB cada STATS_SAVE_MS.
 * @type {{ totalTTS: number, activeGuilds: Set<string>, activeUsers: Set<string>,
 *          voiceCounts: Map<string,number>, langCounts: Map<string,number> }}
 */
const statsBuffer = {
  totalTTS:    0,
  activeGuilds: new Set(),
  activeUsers:  new Set(),
  voiceCounts:  new Map(),
  langCounts:   new Map(),
};

/**
 * Registra un uso TTS en el buffer en memoria.
 * @param {string} userId
 * @param {string} guildId
 * @param {string} voice
 * @param {string} lang
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
function recordTTSUse(userId, guildId, voice, lang) {
  statsBuffer.totalTTS++;
  statsBuffer.activeGuilds.add(guildId);
  statsBuffer.activeUsers.add(userId);
  statsBuffer.voiceCounts.set(voice, (statsBuffer.voiceCounts.get(voice) ?? 0) + 1);
  statsBuffer.langCounts.set(lang,   (statsBuffer.langCounts.get(lang)   ?? 0) + 1);
}

/**
 * Persiste el buffer de stats a PostgreSQL.
 * Se llama periódicamente (no bloqueante en flujo principal).
 */
async function flushStats() {
  if (statsBuffer.totalTTS === 0) return;

  const snapshot = {
    totalTTS:    statsBuffer.totalTTS,
    guilds:      statsBuffer.activeGuilds.size,
    users:       [...statsBuffer.activeUsers],
    voiceCounts: new Map(statsBuffer.voiceCounts),
    langCounts:  new Map(statsBuffer.langCounts),
  };

  // Reset buffer
  statsBuffer.totalTTS = 0;
  statsBuffer.activeGuilds.clear();
  statsBuffer.activeUsers.clear();
  statsBuffer.voiceCounts.clear();
  statsBuffer.langCounts.clear();

  const conn = await pool.connect();
  try {
    // Actualizar tabla global
    await conn.query(`
      UPDATE bot_stats
         SET total_tts     = total_tts + $1,
             active_guilds = $2,
             updated_at    = NOW()
       WHERE id = 1
    `, [snapshot.totalTTS, snapshot.guilds]);

    // Usuarios únicos (upsert)
    for (const uid of snapshot.users) {
      await conn.query(`
        INSERT INTO stats_users (user_id)
        VALUES ($1)
        ON CONFLICT (user_id) DO UPDATE
          SET tts_count = stats_users.tts_count + 1,
              last_seen = NOW()
      `, [uid]);
    }

    // Conteos por voz
    for (const [voice, count] of snapshot.voiceCounts) {
      await conn.query(`
        INSERT INTO stats_voices (voice, use_count)
        VALUES ($1, $2)
        ON CONFLICT (voice) DO UPDATE
          SET use_count = stats_voices.use_count + EXCLUDED.use_count
      `, [voice, count]);
    }

    // Conteos por idioma
    for (const [lang, count] of snapshot.langCounts) {
      await conn.query(`
        INSERT INTO stats_langs (lang, use_count)
        VALUES ($1, $2)
        ON CONFLICT (lang) DO UPDATE
          SET use_count = stats_langs.use_count + EXCLUDED.use_count
      `, [lang, count]);
    }
  } catch (err) {
    console.error("[STATS] Error al persistir stats:", err.message);
  } finally {
    conn.release();
  }
}

/**
 * Obtiene un resumen de stats desde DB.
 * @returns {Promise<object>}
 */
async function getStatsSnapshot() {
  const [globalRow, topVoice, topLang, uniqueUsers] = await Promise.all([
    pool.query("SELECT total_tts, active_guilds, updated_at FROM bot_stats WHERE id = 1"),
    pool.query("SELECT voice, use_count FROM stats_voices ORDER BY use_count DESC LIMIT 1"),
    pool.query("SELECT lang,  use_count FROM stats_langs  ORDER BY use_count DESC LIMIT 1"),
    pool.query("SELECT COUNT(*) AS cnt FROM stats_users"),
  ]);

  return {
    totalTTS:     globalRow.rows[0]?.total_tts     ?? 0,
    activeGuilds: globalRow.rows[0]?.active_guilds ?? 0,
    updatedAt:    globalRow.rows[0]?.updated_at    ?? new Date(),
    topVoice:     topVoice.rows[0]?.voice          ?? "—",
    topLang:      topLang.rows[0]?.lang            ?? "—",
    uniqueUsers:  uniqueUsers.rows[0]?.cnt         ?? 0,
  };
}

// Flush periódico de stats
setInterval(() => flushStats().catch(() => {}), STATS_SAVE_MS);

// ============================================================
//  6. HEALTH MONITOR
// ============================================================

/** Almacena el último snapshot de salud para referencia */
let lastHealthSnapshot = null;

/** Acumula métricas de CPU entre muestras */
let _cpuPrev = os.cpus().map((c) => ({ ...c.times }));

function _getCpuPercent() {
  const cpus    = os.cpus();
  let totalDiff = 0, idleDiff = 0;

  cpus.forEach((cpu, i) => {
    const prev = _cpuPrev[i] || cpu.times;
    const diff = Object.keys(cpu.times).reduce((acc, k) => {
      acc[k] = cpu.times[k] - (prev[k] ?? 0);
      return acc;
    }, {});
    totalDiff += Object.values(diff).reduce((a, b) => a + b, 0);
    idleDiff  += diff.idle ?? 0;
  });

  _cpuPrev = cpus.map((c) => ({ ...c.times }));
  return totalDiff > 0 ? +((1 - idleDiff / totalDiff) * 100).toFixed(1) : 0;
}

/**
 * Mide la latencia de la base de datos.
 * @returns {Promise<number>} ms
 */
async function measureDBLatency() {
  const start = Date.now();
  await pool.query("SELECT 1");
  return Date.now() - start;
}

/**
 * Ejecuta el ciclo de monitoreo y loguea si hay anomalías.
 */
async function runHealthCheck() {
  const totalMem  = os.totalmem();
  const freeMem   = os.freemem();
  const usedMem   = totalMem - freeMem;
  const memPct    = +((usedMem / totalMem) * 100).toFixed(1);
  const heapUsed  = +(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
  const cpuPct    = _getCpuPercent();
  const wsPing    = client.ws.ping;
  const guilds    = client.guilds.cache.size;
  const queues    = guildQueues.size;

  let dbMs = -1;
  let dbOk = true;
  try {
    dbMs = await measureDBLatency();
  } catch (err) {
    dbOk = false;
    console.error("[HEALTH] DB no responde:", err.message);
    sendErrorLog("Health Monitor — DB Error", err.message);
  }

  lastHealthSnapshot = {
    timestamp: new Date(),
    memPct, heapUsed, cpuPct, wsPing, dbMs, dbOk,
    guilds, queues,
  };

  // Log de consola
  console.log(
    `[HEALTH] CPU:${cpuPct}% | RAM:${memPct}% (heap:${heapUsed}MB)` +
    ` | WS:${wsPing}ms | DB:${dbOk ? dbMs + "ms" : "ERR"}` +
    ` | Guilds:${guilds} | Queues:${queues}`
  );

  // Alertas por umbral
  const alerts = [];
  if (memPct    > 90)    alerts.push(`⚠️ RAM crítica: **${memPct}%**`);
  if (cpuPct    > 90)    alerts.push(`⚠️ CPU crítica: **${cpuPct}%**`);
  if (wsPing    > 500)   alerts.push(`⚠️ WS Ping alto: **${wsPing}ms**`);
  if (!dbOk)             alerts.push("🔴 Base de datos no responde");
  if (dbMs      > 1_000) alerts.push(`⚠️ DB lenta: **${dbMs}ms**`);

  if (alerts.length > 0) {
    sendErrorLog("⚠️ Health Alert", alerts.join("\n"), 0xfee75c);
  }

  // Postear en canal de stats si configurado
  if (STATS_CHANNEL_ID && client.isReady()) {
    try {
      const ch = await client.channels.fetch(STATS_CHANNEL_ID).catch(() => null);
      if (ch?.isTextBased()) {
        const embed = buildEmbed(
          "📡 Health Report",
          [
            `**CPU:** ${cpuPct}%`,
            `**RAM:** ${memPct}% (heap: ${heapUsed} MB)`,
            `**WS Ping:** ${wsPing}ms`,
            `**DB Latency:** ${dbOk ? dbMs + "ms" : "❌ Error"}`,
            `**Guilds:** ${guilds}`,
            `**Colas activas:** ${queues}`,
            alerts.length ? "\n" + alerts.join("\n") : "",
          ].join("\n").trim(),
          alerts.length ? 0xfee75c : 0x57f287
        );
        ch.send({ embeds: [embed] }).catch(() => {});
      }
    } catch (_) {}
  }
}

setInterval(() => runHealthCheck().catch(() => {}), HEALTH_INTERVAL_MS);

// ============================================================
//  7. LOG SYSTEM — errores a Discord webhook
// ============================================================

/**
 * Envía un embed de error al webhook de logs de Discord.
 * No lanza excepciones — completamente no-bloqueante.
 * @param {string} title
 * @param {string} description
 * @param {number} [color]
 */
function sendErrorLog(title, description, color = 0xed4245) {
  if (!logWebhook) return;

  const embed = new EmbedBuilder()
    .setTitle(`🚨 ${title}`)
    .setDescription(`\`\`\`\n${String(description).slice(0, 1800)}\n\`\`\``)
    .setColor(color)
    .setFooter({ text: `Yul TTS Bot • PID ${process.pid}` })
    .setTimestamp();

  logWebhook.send({ embeds: [embed] }).catch(() => {});
}

// ============================================================
//  8. ANTI-CRASH SYSTEM
//     Nunca mata el proceso — loguea y continúa
// ============================================================

// Manejo de promesas rechazadas sin catch
process.on("unhandledRejection", (reason, promise) => {
  const msg = reason instanceof Error ? reason.stack : String(reason);
  console.error("[ANTI-CRASH] unhandledRejection:", msg);
  sendErrorLog("unhandledRejection", msg);
});

// Excepción síncrona no capturada
process.on("uncaughtException", (err) => {
  const msg = err?.stack ?? String(err);
  console.error("[ANTI-CRASH] uncaughtException:", msg);
  sendErrorLog("uncaughtException", msg);
  // NO llamar process.exit() — el bot continúa
});

// Promesa resuelta múltiples veces
process.on("multipleResolves", (type, promise, reason) => {
  console.warn("[ANTI-CRASH] multipleResolves:", type, String(reason).slice(0, 200));
});

// Advertencias de Node.js
process.on("warning", (warning) => {
  console.warn("[ANTI-CRASH] warning:", warning.name, warning.message);
  if (warning.name === "MaxListenersExceededWarning") {
    sendErrorLog("MaxListeners Warning", warning.message, 0xfee75c);
  }
});

// Error interno del cliente Discord (shard/websocket)
client.on("error", (err) => {
  console.error("[ANTI-CRASH] client.error:", err.message);
  sendErrorLog("Discord Client Error", err.stack ?? err.message);
});

client.on("shardError", (err, shardId) => {
  console.error(`[ANTI-CRASH] shardError (shard ${shardId}):`, err.message);
  sendErrorLog(`Shard Error (${shardId})`, err.stack ?? err.message);
});

// Reconexión automática si el cliente se desconecta
client.on("shardDisconnect", (event, shardId) => {
  console.warn(`[ANTI-CRASH] Shard ${shardId} desconectado. Código: ${event.code}`);
  sendErrorLog(`Shard ${shardId} Disconnect`, `Código WS: ${event.code}`, 0xfee75c);
});

client.on("shardReconnecting", (shardId) => {
  console.log(`[ANTI-CRASH] Shard ${shardId} reconectándose…`);
});

client.on("shardResume", (shardId, replayedEvents) => {
  console.log(`[ANTI-CRASH] Shard ${shardId} resumido (${replayedEvents} eventos).`);
});

// ── Señales del sistema ────────────────────────────────────
// SIGINT / SIGTERM → shutdown limpio sin crashear
async function gracefulShutdown(signal) {
  console.log(`\n[SHUTDOWN] ${signal} recibida. Cerrando limpiamente…`);
  sendErrorLog("Graceful Shutdown", `Señal: ${signal}`, 0xfee75c);

  try {
    await flushStats();
    for (const guildId of guildQueues.keys()) leaveVoiceChannel(guildId);
    for (const guild of client.guilds.cache.values()) {
      const c = getVoiceConnection(guild.id);
      if (c) try { c.destroy(); } catch (_) {}
    }
    client.destroy();
    await pool.end();
  } catch (err) {
    console.error("[SHUTDOWN] Error durante cierre:", err.message);
  }

  console.log("[SHUTDOWN] Apagado correcto.");
  process.exit(0);
}

process.on("SIGINT",  () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// ============================================================
//  9. COOLDOWN SYSTEM (Pro — con bucket por usuario + guild)
// ============================================================

/**
 * @type {Map<string, number>}
 * Clave: `${userId}` — timestamp del último TTS
 */
const cooldownMap = new Map();

/**
 * @type {Map<string, number>}
 * Clave: `${guildId}:${userId}` — spam score (incrementa rápido, decae con tiempo)
 */
const spamScoreMap = new Map();

const SPAM_THRESHOLD  = 5;   // score para considerar spam
const SPAM_DECAY_RATE = 0.5; // puntos que se pierden por segundo

/**
 * Verifica cooldown estándar (3s) + anti-spam por guild.
 * @param {string} userId
 * @param {string} guildId
 * @returns {{ ok: boolean, remaining: number, spam: boolean }}
 */
function checkCooldown(userId, guildId = null) {
  const now     = Date.now();
  const lastKey = userId;
  const last    = cooldownMap.get(lastKey) ?? 0;
  const diff    = now - last;

  // Cooldown estándar
  if (diff < TTS_COOLDOWN_MS) {
    return {
      ok:        false,
      remaining: Math.ceil((TTS_COOLDOWN_MS - diff) / 1000),
      spam:      false,
    };
  }

  // Anti-spam por guild (si se proporciona guildId)
  if (guildId) {
    const spamKey    = `${guildId}:${userId}`;
    const entry      = spamScoreMap.get(spamKey) ?? { score: 0, lastHit: now };
    const elapsed    = (now - entry.lastHit) / 1000;
    const decayed    = Math.max(0, entry.score - elapsed * SPAM_DECAY_RATE);
    const newScore   = decayed + 1;

    spamScoreMap.set(spamKey, { score: newScore, lastHit: now });

    if (newScore > SPAM_THRESHOLD) {
      return { ok: false, remaining: Math.ceil(newScore), spam: true };
    }
  }

  cooldownMap.set(lastKey, now);
  return { ok: true, remaining: 0, spam: false };
}

// Limpieza periódica de cooldowns obsoletos (cada 10 min)
setInterval(() => {
  const now     = Date.now();
  let   removed = 0;

  for (const [k, ts] of cooldownMap) {
    if (now - ts > TTS_COOLDOWN_MS * 20) { cooldownMap.delete(k); removed++; }
  }
  for (const [k, v] of spamScoreMap) {
    const elapsed = (now - v.lastHit) / 1000;
    if (elapsed > 120) { spamScoreMap.delete(k); }
  }

  if (removed > 0) console.log(`[COOLDOWN] ${removed} entradas obsoletas eliminadas.`);
}, 10 * 60_000);

// ============================================================
//  10. VALIDACIÓN DE TEXTO
// ============================================================
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

// ============================================================
//  11. CACHE SYSTEM (SHA256 — L1 mem + L2 DB)
// ============================================================
const memCache = new Map();

function makeCacheHash(text, voice) {
  return crypto
    .createHash("sha256")
    .update(`${voice}::${text.trim().toLowerCase()}`)
    .digest("hex");
}

function memCacheGet(hash) {
  const e = memCache.get(hash);
  if (!e) return null;
  memCache.delete(hash);
  memCache.set(hash, e);
  return e;
}

function memCacheSet(hash, entry) {
  if (memCache.size >= CACHE_MAX_MEM) memCache.delete(memCache.keys().next().value);
  memCache.set(hash, entry);
}

async function cacheGet(hash) {
  const mem = memCacheGet(hash);
  if (mem) {
    pool.query(
      "UPDATE tts_cache SET last_used = NOW(), use_count = use_count + 1 WHERE hash = $1",
      [hash]
    ).catch(() => {});
    return mem;
  }

  const { rows } = await pool.query(
    "SELECT file_path, voice, text FROM tts_cache WHERE hash = $1",
    [hash]
  );
  if (!rows.length) return null;

  const entry = { filePath: rows[0].file_path, voice: rows[0].voice, text: rows[0].text };

  try {
    await fsp.access(entry.filePath, fs.constants.R_OK);
  } catch {
    pool.query("DELETE FROM tts_cache WHERE hash = $1", [hash]).catch(() => {});
    return null;
  }

  memCacheSet(hash, entry);
  pool.query(
    "UPDATE tts_cache SET last_used = NOW(), use_count = use_count + 1 WHERE hash = $1",
    [hash]
  ).catch(() => {});
  return entry;
}

async function cacheSet(hash, filePath, voice, text) {
  memCacheSet(hash, { filePath, voice, text });
  await pool.query(
    `INSERT INTO tts_cache (hash, file_path, voice, text)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (hash) DO UPDATE
       SET last_used = NOW(), use_count = tts_cache.use_count + 1`,
    [hash, filePath, voice, text]
  );
}

// ============================================================
//  12. TTS ENGINE — edge-tts CLI
// ============================================================
async function generateTTS(text, voice) {
  const hash   = makeCacheHash(text, voice);
  const cached = await cacheGet(hash);
  if (cached) {
    console.log(`[TTS] Cache hit ${hash.slice(0, 8)}… ${voice}`);
    return cached.filePath;
  }

  const outFile  = path.join(AUDIO_DIR, `${hash}.mp3`);
  const safeText = text.replace(/'/g, "'\\''");
  const cmd      = `edge-tts --voice "${voice}" --text '${safeText}' --write-media "${outFile}"`;

  try {
    await execAsync(cmd, { timeout: 30_000 });
  } catch (err) {
    fsp.unlink(outFile).catch(() => {});
    throw new Error(`edge-tts error: ${err.message}`);
  }

  try {
    await fsp.access(outFile, fs.constants.R_OK);
  } catch {
    throw new Error("edge-tts no generó el archivo de audio.");
  }

  await cacheSet(hash, outFile, voice, text);
  console.log(`[TTS] Nuevo audio ${hash.slice(0, 8)}… ${voice}`);
  return outFile;
}

// ============================================================
//  13. QUEUE SYSTEM — GuildQueue
// ============================================================

/**
 * @typedef {Object} QueueItem
 * @property {string}  text
 * @property {string}  voice
 * @property {string}  guildId
 * @property {string}  channelId
 * @property {string}  requestedBy
 * @property {import("discord.js").TextChannel|null} replyChannel
 */

/**
 * @typedef {Object} GuildQueue
 * @property {import("@discordjs/voice").VoiceConnection} connection
 * @property {import("@discordjs/voice").AudioPlayer}     player
 * @property {QueueItem[]}  songs
 * @property {string}       voice
 * @property {boolean}      isPlaying
 * @property {boolean}      locked
 */

/** @type {Map<string, GuildQueue>} */
const guildQueues = new Map();

function _attachPlayerListeners(guildId, queue) {
  queue.player.removeAllListeners();

  queue.player.on("error", (err) => {
    console.error(`[PLAYER] Guild ${guildId}:`, err.message);
    sendErrorLog(`Player Error — Guild ${guildId}`, err.message, 0xfee75c);
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
    if (connection && q.connection !== connection) {
      q.connection = connection;
      _buildPlayer(guildId, q);
      connection.subscribe(q.player);
    }
    return q;
  }

  const player = createAudioPlayer({ behaviors: { noSubscriber: NoSubscriberBehavior.Pause } });
  const queue  = { connection, player, songs: [], voice: voice || DEFAULT_VOICE, isPlaying: false, locked: false };

  guildQueues.set(guildId, queue);
  connection.subscribe(player);
  _attachPlayerListeners(guildId, queue);
  return queue;
}

function destroyQueue(guildId) {
  const q = guildQueues.get(guildId);
  if (!q) return;
  try { q.player.stop(true); }    catch (_) {}
  try { q.connection.destroy(); } catch (_) {}
  q.songs  = [];
  q.locked = false;
  guildQueues.delete(guildId);
  console.log(`[QUEUE] Destruida para guild ${guildId}`);
}

// ============================================================
//  14. VOICE SYSTEM
// ============================================================
async function joinVoiceChannelForMember(member, guild) {
  const vc = member.voice?.channel;
  if (!vc) throw new Error("Debes estar en un canal de voz para usar TTS.");

  const existing = getVoiceConnection(guild.id);
  if (existing) {
    if (existing.joinConfig?.channelId === vc.id) return existing;
    throw new Error("Ya estoy en otro canal. Usa `yul leave` o únete a ese canal.");
  }

  const conn = joinVoiceChannel({
    channelId: vc.id, guildId: guild.id,
    adapterCreator: guild.voiceAdapterCreator, selfDeaf: true,
  });

  try {
    await entersState(conn, VoiceConnectionStatus.Ready, 10_000);
  } catch {
    conn.destroy();
    throw new Error("No pude conectarme al canal. Revisa mis permisos.");
  }

  conn.on(VoiceConnectionStatus.Disconnected, async () => {
    try {
      await Promise.race([
        entersState(conn, VoiceConnectionStatus.Signalling, 5_000),
        entersState(conn, VoiceConnectionStatus.Connecting, 5_000),
      ]);
    } catch {
      destroyQueue(guild.id);
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

// ============================================================
//  15. ADVANCE QUEUE
// ============================================================
async function advanceQueue(guildId) {
  const queue = guildQueues.get(guildId);
  if (!queue || queue.locked || queue.songs.length === 0) {
    if (queue) queue.isPlaying = false;
    return;
  }

  const conn = getVoiceConnection(guildId);
  if (!conn) { destroyQueue(guildId); return; }

  queue.locked    = true;
  queue.isPlaying = true;
  const item      = queue.songs.shift();

  let audioFile;
  try {
    audioFile = await generateTTS(item.text, item.voice || queue.voice);
  } catch (err) {
    console.error(`[TTS] Guild ${guildId}:`, err.message);
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

  queue.player.play(createAudioResource(audioFile));
  console.log(`[PLAYER] Guild ${guildId} | "${item.text.slice(0, 45)}…" | ${item.voice || queue.voice}`);
}

// ============================================================
//  16. PROCESADOR TTS UNIFICADO
// ============================================================
async function processTTSRequest({ text, voice, guildId, userId, member, guild, replyChannel, reply, lang }) {
  const row = getGlobalButtons();

  // Cooldown Pro
  const cd = checkCooldown(userId, guildId);
  if (!cd.ok) {
    const desc = cd.spam
      ? `⚠️ Anti-spam activado. Espera **${cd.remaining}s**.`
      : `Espera **${cd.remaining}s** antes de enviar otro TTS.`;
    return reply({
      embeds: [buildEmbed("⏳ Cooldown activo", desc, 0xfee75c)],
      components: [row], ephemeral: true,
    });
  }

  // Validar texto
  const val = validateText(text);
  if (!val.valid) {
    return reply({
      embeds: [buildEmbed("❌ Texto inválido", val.reason, 0xed4245)],
      components: [row], ephemeral: true,
    });
  }
  const cleanText = val.cleaned;

  // Unirse al canal
  let conn;
  try { conn = await joinVoiceChannelForMember(member, guild); }
  catch (err) {
    return reply({
      embeds: [buildEmbed("❌ Error de voz", err.message, 0xed4245)],
      components: [row], ephemeral: true,
    });
  }

  // Obtener / crear GuildQueue
  const queue = getOrCreateQueue(guildId, conn, voice);

  // Encolar
  queue.songs.push({ text: cleanText, voice, guildId, channelId: member.voice.channel.id, requestedBy: userId, replyChannel });
  const pos = queue.songs.length + (queue.isPlaying ? 1 : 0);

  // Registrar en stats
  const voiceInfo = SUPPORTED_VOICES.find((v) => v.id === voice);
  recordTTSUse(userId, guildId, voice, voiceInfo?.lang ?? lang ?? DEFAULT_LANG);

  // Responder
  await reply({
    embeds: [buildEmbed(
      "🔊 TTS encolado",
      [
        `**Texto:** ${cleanText.slice(0, 80)}${cleanText.length > 80 ? "…" : ""}`,
        `**Voz:** \`${voice}\``,
        `**Posición:** ${pos}`,
      ].join("\n"),
      0x57f287
    )],
    components: [row],
  });

  if (!queue.isPlaying && !queue.locked) advanceQueue(guildId);
}

// ============================================================
//  17. LIMPIEZA DE CACHÉ
// ============================================================
async function runCacheCleanup() {
  console.log("[CLEANUP] Iniciando limpieza…");

  try {
    const cutoff  = new Date(Date.now() - CACHE_TTL_MS).toISOString();
    const { rows } = await pool.query(
      "SELECT hash, file_path FROM tts_cache WHERE last_used < $1",
      [cutoff]
    );

    let deleted = 0;
    const BATCH = 10;
    for (let i = 0; i < rows.length; i += BATCH) {
      await Promise.all(rows.slice(i, i + BATCH).map(async ({ hash, file_path }) => {
        fsp.unlink(file_path).catch(() => {});
        await pool.query("DELETE FROM tts_cache WHERE hash = $1", [hash]).catch(() => {});
        memCache.delete(hash);
        deleted++;
      }));
    }
    if (deleted) console.log(`[CLEANUP] ${deleted} entradas expiradas eliminadas.`);
  } catch (err) {
    console.error("[CLEANUP] Error DB:", err.message);
  }

  // Huérfanos en disco
  try {
    const files      = (await fsp.readdir(AUDIO_DIR)).filter((f) => f.endsWith(".mp3"));
    const { rows }   = await pool.query("SELECT file_path FROM tts_cache");
    const knownPaths = new Set(rows.map((r) => r.file_path));
    let orphans = 0;
    for (const f of files) {
      const fp = path.join(AUDIO_DIR, f);
      if (!knownPaths.has(fp)) { fsp.unlink(fp).catch(() => {}); orphans++; }
    }
    if (orphans) console.log(`[CLEANUP] ${orphans} archivos huérfanos eliminados.`);
  } catch (err) {
    console.error("[CLEANUP] Error disco:", err.message);
  }
}

setInterval(() => runCacheCleanup().catch(() => {}), CLEANUP_INTERVAL_MS);

// ============================================================
//  18. SLASH COMMANDS — definición y registro
// ============================================================
const slashCommands = [
  new SlashCommandBuilder().setName("help").setDescription("Muestra todos los comandos"),
  new SlashCommandBuilder().setName("ping").setDescription("Latencia del bot"),
  new SlashCommandBuilder()
    .setName("tts")
    .setDescription("Convierte texto a voz")
    .addStringOption((o) => o.setName("texto").setDescription("Texto (máx 500 chars)").setRequired(true)),
  new SlashCommandBuilder().setName("skip").setDescription("Salta el audio actual"),
  new SlashCommandBuilder().setName("stop").setDescription("Detiene y vacía la cola"),
  new SlashCommandBuilder().setName("leave").setDescription("Desconecta el bot del canal de voz"),
  new SlashCommandBuilder().setName("queue").setDescription("Muestra la cola TTS actual"),
  new SlashCommandBuilder().setName("voices").setDescription("Lista voces disponibles"),
  new SlashCommandBuilder()
    .setName("setvoice")
    .setDescription("Cambia la voz TTS del servidor")
    .addStringOption((o) => {
      o.setName("voz").setDescription("ID de la voz").setRequired(true);
      SUPPORTED_VOICES.forEach((v) => o.addChoices({ name: v.label, value: v.id }));
      return o;
    }),
  new SlashCommandBuilder().setName("stats").setDescription("Estadísticas globales del bot"),
  new SlashCommandBuilder().setName("health").setDescription("Estado del sistema en tiempo real"),
  new SlashCommandBuilder().setName("cachestats").setDescription("Estadísticas del caché TTS"),
].map((c) => c.toJSON());

async function registerSlashCommands() {
  const rest = new REST({ version: "10" }).setToken(DISCORD_TOKEN);
  try {
    console.log("[SLASH] Registrando slash commands globales…");
    await rest.put(Routes.applicationCommands(CLIENT_ID), { body: slashCommands });
    console.log("[SLASH] Commands registrados.");
  } catch (err) {
    console.error("[SLASH] Error:", err.message);
    sendErrorLog("Slash Command Register Error", err.message);
  }
}

// ============================================================
//  19. MANEJADORES — slash + prefix compartidos
// ============================================================

async function handleHelp(ctx, config, isSlash) {
  const p = config?.prefix ?? DEFAULT_PREFIX;
  const embed = buildEmbed(
    "📖 Comandos — Yul TTS",
    [
      `**Prefix:** \`${p}\``,
      "",
      `\`${p}help\` / \`/help\` — Este menú`,
      `\`${p}ping\` / \`/ping\` — Latencia`,
      `\`${p}tts <texto>\` / \`/tts\` — Texto a voz`,
      `\`${p}skip\` / \`/skip\` — Saltar audio`,
      `\`${p}stop\` / \`/stop\` — Detener y vaciar cola`,
      `\`${p}leave\` / \`/leave\` — Salir del canal`,
      `\`${p}queue\` / \`/queue\` — Ver cola`,
      `\`${p}voices\` / \`/voices\` — Ver voces`,
      `\`${p}setvoice <id>\` / \`/setvoice\` — Cambiar voz`,
      `\`/stats\` — Estadísticas globales`,
      `\`/health\` — Estado del sistema`,
      `\`/cachestats\` — Estadísticas de caché`,
      "",
      `> Límite: ${TTS_MAX_CHARS} chars · Cooldown: 3s · Sin URLs`,
    ].join("\n")
  );

  const opts = { embeds: [embed], components: [getGlobalButtons()] };
  return isSlash ? ctx.reply(opts) : ctx.reply(opts);
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
    text:         interaction.options.getString("texto"),
    voice:        config?.voice ?? DEFAULT_VOICE,
    guildId:      interaction.guild.id,
    userId:       interaction.user.id,
    member:       interaction.member,
    guild:        interaction.guild,
    replyChannel: interaction.channel,
    reply:        (o) => interaction.editReply(o),
    lang:         config?.lang ?? DEFAULT_LANG,
  });
}

async function handleLeave(ctx, guildId, isSlash) {
  const row  = getGlobalButtons();
  const conn = getVoiceConnection(guildId);
  if (!conn) {
    const opts = { embeds: [buildEmbed("ℹ️ Sin conexión", "No estoy en ningún canal de voz.", 0x5865f2)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  leaveVoiceChannel(guildId);
  return ctx.reply({ embeds: [buildEmbed("👋 Desconectado", "Salí del canal de voz.", 0x57f287)], components: [row] });
}

async function handleQueue(ctx, guildId, isSlash) {
  const row   = getGlobalButtons();
  const queue = guildQueues.get(guildId);
  const total = queue?.songs?.length ?? 0;
  const now   = queue?.isPlaying ? "🎵 Reproduciendo ahora" : "⏸ Sin reproducción activa";

  if (!total) {
    return ctx.reply({ embeds: [buildEmbed("📋 Cola vacía", `${now}\nLa cola está vacía.`)], components: [row] });
  }

  const list = queue.songs.slice(0, 10)
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
  const row   = getGlobalButtons();
  const queue = guildQueues.get(guildId);
  if (!queue?.isPlaying) {
    const opts = { embeds: [buildEmbed("ℹ️ Nada que saltar", "No hay audio reproduciéndose.", 0x5865f2)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  queue.player.stop();
  return ctx.reply({ embeds: [buildEmbed("⏭️ Saltado", "Audio omitido.", 0x57f287)], components: [row] });
}

async function handleStop(ctx, guildId, isSlash) {
  const row   = getGlobalButtons();
  const queue = guildQueues.get(guildId);
  if (!queue) {
    const opts = { embeds: [buildEmbed("ℹ️ Sin actividad", "No hay cola activa.", 0x5865f2)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  queue.songs = []; queue.locked = queue.isPlaying = false;
  queue.player.stop(true);
  return ctx.reply({ embeds: [buildEmbed("⏹️ Detenido", "Cola vaciada y reproducción detenida.", 0xed4245)], components: [row] });
}

async function handleVoices(ctx) {
  const list = SUPPORTED_VOICES.map((v) => `\`${v.id}\` — ${v.label}`).join("\n");
  return ctx.reply({ embeds: [buildEmbed("🎙️ Voces disponibles", list)], components: [getGlobalButtons()] });
}

async function handleSetVoice(ctx, guildId, voiceId, isSlash) {
  const row = getGlobalButtons();
  if (!VOICE_IDS.includes(voiceId)) {
    const opts = { embeds: [buildEmbed("❌ Voz inválida", "Usa `/voices` para ver las opciones.", 0xed4245)], components: [row] };
    return isSlash ? ctx.reply({ ...opts, ephemeral: true }) : ctx.reply(opts);
  }
  await updateGuildConfig(guildId, "voice", voiceId);
  const info  = SUPPORTED_VOICES.find((v) => v.id === voiceId);
  const queue = guildQueues.get(guildId);
  if (queue) queue.voice = voiceId;
  return ctx.reply({ embeds: [buildEmbed("✅ Voz actualizada", `**${info.label}**\n\`${voiceId}\``)], components: [row] });
}

async function handleStats(ctx) {
  const row = getGlobalButtons();
  try {
    await flushStats();
    const snap  = await getStatsSnapshot();
    const embed = buildEmbed(
      "📊 Estadísticas Globales — Yul TTS",
      [
        `**Total TTS generados:** ${Number(snap.totalTTS).toLocaleString()}`,
        `**Usuarios únicos:**     ${Number(snap.uniqueUsers).toLocaleString()}`,
        `**Servidores activos:**  ${Number(snap.activeGuilds).toLocaleString()}`,
        `**Voz más usada:**       \`${snap.topVoice}\``,
        `**Idioma más usado:**    \`${snap.topLang}\``,
        `**Guilds conectados:**   ${client.guilds.cache.size}`,
        "",
        `_Actualizado: ${new Date(snap.updatedAt).toLocaleString()}_`,
      ].join("\n")
    );
    return ctx.reply({ embeds: [embed], components: [row] });
  } catch (err) {
    return ctx.reply({ embeds: [buildEmbed("❌ Error", err.message, 0xed4245)], components: [row] });
  }
}

async function handleHealth(ctx, isSlash) {
  const row = getGlobalButtons();

  if (isSlash) await ctx.deferReply();

  // Tomar snapshot en tiempo real
  let dbMs = -1, dbOk = true;
  try { dbMs = await measureDBLatency(); }
  catch { dbOk = false; }

  const totalMem = os.totalmem();
  const freeMem  = os.freemem();
  const memPct   = +((1 - freeMem / totalMem) * 100).toFixed(1);
  const heapUsed = +(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
  const cpuPct   = lastHealthSnapshot?.cpuPct ?? 0;

  const embed = buildEmbed(
    "📡 Estado del Sistema",
    [
      `**CPU:**          ${cpuPct}%`,
      `**RAM:**          ${memPct}% (heap: ${heapUsed} MB)`,
      `**WS Ping:**      ${client.ws.ping}ms`,
      `**DB Latency:**   ${dbOk ? dbMs + "ms" : "❌ Error"}`,
      `**Guilds:**       ${client.guilds.cache.size}`,
      `**Colas activas:** ${guildQueues.size}`,
      `**Uptime:**       ${_formatUptime(process.uptime())}`,
      `**Node.js:**      ${process.version}`,
    ].join("\n"),
    dbOk ? 0x57f287 : 0xed4245
  );

  const reply = isSlash ? (o) => ctx.editReply(o) : (o) => ctx.reply(o);
  return reply({ embeds: [embed], components: [row] });
}

async function handleCacheStats(ctx) {
  const row = getGlobalButtons();
  try {
    const { rows: [s] } = await pool.query(`
      SELECT COUNT(*) AS total, SUM(use_count) AS hits,
             MIN(created_at) AS oldest, MAX(last_used) AS newest
      FROM tts_cache
    `);
    const files = (await fsp.readdir(AUDIO_DIR)).filter((f) => f.endsWith(".mp3")).length;
    const embed = buildEmbed(
      "🗄️ Estadísticas de Caché TTS",
      [
        `**Entradas en DB:**     ${s.total}`,
        `**Usos totales:**       ${s.hits ?? 0}`,
        `**L1 (memoria):**       ${memCache.size} / ${CACHE_MAX_MEM}`,
        `**Archivos .mp3:**      ${files}`,
        `**Entrada más antigua:** ${s.oldest ? new Date(s.oldest).toLocaleString() : "—"}`,
        `**Último uso:**         ${s.newest ? new Date(s.newest).toLocaleString() : "—"}`,
        `**TTL:**                7 días`,
      ].join("\n")
    );
    return ctx.reply({ embeds: [embed], components: [row] });
  } catch (err) {
    return ctx.reply({ embeds: [buildEmbed("❌ Error", err.message, 0xed4245)], components: [row] });
  }
}

function _formatUptime(seconds) {
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return `${d}d ${h}h ${m}m ${s}s`;
}

// ============================================================
//  20. MANEJADOR PREFIX
// ============================================================
async function handlePrefixCommand(message, config) {
  const prefix = config.prefix;
  if (!message.content.toLowerCase().startsWith(prefix.toLowerCase())) return;

  const args    = message.content.slice(prefix.length).trim().split(/\s+/);
  const command = args.shift().toLowerCase();
  const gid     = message.guild.id;
  const row     = getGlobalButtons();

  switch (command) {
    case "help":    await handleHelp(message, config, false); break;
    case "ping":    await handlePing(message, false);         break;

    case "tts": {
      if (!args.length) {
        await message.reply({ embeds: [buildEmbed("❌ Uso", `\`${prefix}tts <texto>\``, 0xed4245)], components: [row] });
        return;
      }
      await processTTSRequest({
        text: args.join(" "), voice: config.voice, guildId: gid,
        userId: message.author.id, member: message.member, guild: message.guild,
        replyChannel: message.channel, reply: (o) => message.reply(o),
        lang: config.lang,
      });
      break;
    }

    case "skip":     await handleSkip(message, gid, false);            break;
    case "stop":     await handleStop(message, gid, false);            break;
    case "leave":    await handleLeave(message, gid, false);           break;
    case "queue":    await handleQueue(message, gid, false);           break;
    case "voices":   await handleVoices(message);                      break;
    case "stats":    await handleStats(message);                       break;
    case "health":   await handleHealth(message, false);               break;
    case "cachestats": await handleCacheStats(message);                break;

    case "setvoice": {
      const voiceId = args[0];
      if (!voiceId) {
        await message.reply({ embeds: [buildEmbed("❌ Uso", `\`${prefix}setvoice <id>\``, 0xed4245)], components: [row] });
        return;
      }
      await handleSetVoice(message, gid, voiceId, false);
      break;
    }

    default: break;
  }
}

// ============================================================
//  21. MENCIÓN AL BOT
// ============================================================
async function handleMention(message, config) {
  await message.reply({
    embeds: [buildEmbed(
      "👋 ¡Hola! Soy Yul TTS",
      [
        `**Prefix:** \`${config.prefix}\``,
        `**Idioma:** \`${config.lang}\``,
        `**Voz:** \`${config.voice}\``,
        "",
        `Escribe \`${config.prefix}help\` o \`/help\` para ver todos los comandos.`,
      ].join("\n")
    )],
    components: [getGlobalButtons()],
  });
}

// ============================================================
//  22. EVENTOS DEL CLIENTE
// ============================================================

client.once(Events.ClientReady, async () => {
  console.log(`[BOT] Conectado como ${client.user.tag} | Guilds: ${client.guilds.cache.size}`);
  client.user.setActivity("AL RATO JALO CHIDO", { type: ActivityType.Listening });
  await registerSlashCommands();
  runCacheCleanup().catch(() => {});
  runHealthCheck().catch(() => {});
});

client.on(Events.GuildCreate, async (guild) => {
  console.log(`[GUILD] Nuevo: ${guild.name} (${guild.id})`);
  getGuildConfig(guild.id).catch((e) => console.error("[GUILD]", e.message));
});

client.on(Events.GuildDelete, (guild) => {
  client.guildCache.delete(guild.id);
  leaveVoiceChannel(guild.id);
  console.log(`[GUILD] Removido de: ${guild.name}`);
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
  if (!interaction.guild) return;
  let config;
  try { config = await getGuildConfig(interaction.guild.id); }
  catch (err) { console.error("[SLASH]", err.message); return; }

  if (!interaction.isChatInputCommand()) return;

  const gid = interaction.guild.id;

  switch (interaction.commandName) {
    case "help":       await handleHelp(interaction, config, true);                               break;
    case "ping":       await handlePing(interaction, true);                                       break;
    case "tts":        await handleTTSSlash(interaction, config);                                 break;
    case "skip":       await handleSkip(interaction, gid, true);                                  break;
    case "stop":       await handleStop(interaction, gid, true);                                  break;
    case "leave":      await handleLeave(interaction, gid, true);                                 break;
    case "queue":      await handleQueue(interaction, gid, true);                                 break;
    case "voices":     await handleVoices(interaction);                                           break;
    case "setvoice":   await handleSetVoice(interaction, gid, interaction.options.getString("voz"), true); break;
    case "stats":      await handleStats(interaction);                                            break;
    case "health":     await handleHealth(interaction, true);                                     break;
    case "cachestats": await handleCacheStats(interaction);                                       break;
    default:
      interaction.reply({ content: "Comando no reconocido.", ephemeral: true });
  }
});

// ============================================================
//  23. ARRANQUE
// ============================================================
(async () => {
  try {
    console.log("[BOT] Iniciando Yul TTS Bot (Parte 4 — Global Pro)…");
    await initDatabase();
    await client.login(DISCORD_TOKEN);
  } catch (err) {
    console.error("[FATAL]", err.message);
    sendErrorLog("Boot Error", err.stack ?? err.message);
    process.exit(1);
  }
})();

// ============================================================
//  EXPORTS — para extensiones futuras
// ============================================================
module.exports = {
  client, pool,
  guildQueues, memCache,
  getGuildConfig, updateGuildConfig,
  generateTTS, makeCacheHash, cacheGet, cacheSet,
  advanceQueue, getOrCreateQueue, destroyQueue,
  joinVoiceChannelForMember, leaveVoiceChannel,
  processTTSRequest,
  validateText, checkCooldown,
  recordTTSUse, flushStats, getStatsSnapshot,
  runHealthCheck, runCacheCleanup,
  sendErrorLog,
  getGlobalButtons, buildEmbed,
  SUPPORTED_VOICES, VOICE_IDS,
};