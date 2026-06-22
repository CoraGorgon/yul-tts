  shard.js
  const { ShardingManager } = require("discord.js");
  require("dotenv").config();

  const manager = new ShardingManager("./index.js", {
    totalShards: "auto",
    token: process.env.DISCORD_TOKEN,
    respawn: true,
  });

  manager.on("shardCreate", (shard) => {
    console.log(`[MANAGER] Shard ${shard.id} lanzado.`);
    shard.on("death", () => console.warn(`[MANAGER] Shard ${shard.id} murió.`));
  });

 manager.spawn({ timeout: 60_000 });