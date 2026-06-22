const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Crear tabla si no existe al iniciar
pool.query(`
  CREATE TABLE IF NOT EXISTS guild_settings (
    guild_id TEXT PRIMARY KEY,
    prefix TEXT DEFAULT 'yul ',
    lang TEXT DEFAULT 'es'
  )
`);
module.exports = pool;