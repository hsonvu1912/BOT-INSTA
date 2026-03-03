const { REST, Routes, SlashCommandBuilder } = require("discord.js");
const { mustEnv } = require("./utils");

const token = mustEnv("DISCORD_TOKEN");
const clientId = mustEnv("DISCORD_CLIENT_ID");
const guildId = mustEnv("DISCORD_GUILD_ID");

const commands = [
  new SlashCommandBuilder()
    .setName("ig_schedule")
    .setDescription("Lên lịch đăng bài Instagram từ folder Drive + caption trong Sheet")
    .addStringOption(o =>
      o.setName("shop")
        .setDescription("Chọn shop")
        .setRequired(true)
        .addChoices(
          { name: "Màu mè", value: "MAUME" },
          { name: "Burger", value: "BURGER" }
        )
    )
    .addStringOption(o =>
      o.setName("time")
        .setDescription("Giờ đăng (YYYY-MM-DD HH:mm) giờ VN")
        .setRequired(true)
    )
    .addStringOption(o =>
      o.setName("folder")
        .setDescription("Link folder Google Drive")
        .setRequired(true)
    )
].map(c => c.toJSON());

(async () => {
  const rest = new REST({ version: "10" }).setToken(token);
  await rest.put(Routes.applicationGuildCommands(clientId, guildId), { body: commands });
  console.log("✅ Deployed slash commands to guild.");
})().catch(e => {
  console.error(e);
  process.exit(1);
});