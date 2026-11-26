/**
 * Cloudflare Worker: Telegram Webhook -> GitHub Actions YouTube Downloader
 *
 * Required Secrets / Env Vars:
 *   TELEGRAM_BOT_TOKEN      - Bot token from @BotFather
 *   TELEGRAM_WEBHOOK_SECRET - Random secret string for webhook verification
 *   GITHUB_TOKEN            - GitHub Fine-Grained PAT (Actions: Read and write)
 *   GITHUB_OWNER            - GitHub username or org
 *   GITHUB_REPO             - GitHub repository name
 *   GITHUB_BRANCH           - Branch name (default: "main")
 *   ALLOWED_USER_IDS        - Comma-separated Telegram numeric user IDs
 */

const YT_URL_REGEX =
  /https?:\/\/(?:www\.|m\.|music\.)?(?:youtube\.com\/[^\s<>"]+|youtu\.be\/[^\s<>"]+)/i;

const VALID_QUALITIES = new Set(["1080", "720", "480", "360", "mp3"]);

const QUALITY_LABELS = {
  "1080": "1080p",
  "720": "720p",
  "480": "480p",
  "360": "360p",
  mp3: "MP3 (صوت)",
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Health check endpoint
    if (request.method === "GET" && url.pathname === "/") {
      return new Response("yt-tg-bot worker is running.", { status: 200 });
    }

    // One-click webhook registration helper:
    // Visit: https://<your-worker>.workers.dev/setup?secret=<TELEGRAM_WEBHOOK_SECRET>
    if (request.method === "GET" && url.pathname === "/setup") {
      const secret = url.searchParams.get("secret") || "";
      if (!env.TELEGRAM_WEBHOOK_SECRET || !timingSafeEqual(secret, env.TELEGRAM_WEBHOOK_SECRET)) {
        return new Response("Forbidden: invalid secret", { status: 403 });
      }
      const webhookUrl = `${url.origin}/webhook`;
      const result = await tgApi(env.TELEGRAM_BOT_TOKEN, "setWebhook", {
        url: webhookUrl,
        secret_token: env.TELEGRAM_WEBHOOK_SECRET,
        allowed_updates: ["message", "callback_query"],
        drop_pending_updates: true,
      });
      return new Response(JSON.stringify({ webhookUrl, telegram: result }, null, 2), {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
      });
    }

    // Telegram Webhook receiver
    if (request.method === "POST" && url.pathname === "/webhook") {
      const incomingSecret = request.headers.get("X-Telegram-Bot-Api-Secret-Token") || "";
      if (!env.TELEGRAM_WEBHOOK_SECRET || !timingSafeEqual(incomingSecret, env.TELEGRAM_WEBHOOK_SECRET)) {
        return new Response("Forbidden", { status: 403 });
      }

      let update;
      try {
        update = await request.json();
      } catch {
        return new Response("Bad Request", { status: 400 });
      }

      try {
        if (update.message) {
          await handleMessage(update.message, env);
        } else if (update.callback_query) {
          await handleCallbackQuery(update.callback_query, env);
        }
      } catch (err) {
        console.error("Update handling error:", err);
      }

      // Always return 200 so Telegram does not retry indefinitely
      return new Response("OK", { status: 200 });
    }

    return new Response("Not Found", { status: 404 });
  },
};

function timingSafeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return diff === 0;
}

function getAllowedUsers(env) {
  const raw = String(env.ALLOWED_USER_IDS || "").trim();
  if (!raw) return new Set();
  return new Set(
    raw
      .split(/[,\s]+/)
      .map((s) => s.trim())
      .filter(Boolean)
  );
}

function isAuthorized(userId, env) {
  if (!userId) return false;
  const allowed = getAllowedUsers(env);
  return allowed.has(String(userId));
}

async function tgApi(token, method, payload) {
  const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

function parseYouTubeInput(text) {
  const match = text.match(YT_URL_REGEX);
  if (!match) return null;

  const rawUrl = match[0].replace(/[.,)!?\]]+$/, "");
  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    return null;
  }

  const listParam = parsed.searchParams.get("list") || "";
  const isInfiniteMix = listParam.startsWith("RD");
  const hasRealList = Boolean(listParam) && !isInfiniteMix;
  const isPurePlaylist = parsed.pathname.startsWith("/playlist") && hasRealList;
  const isVideoInPlaylist = !isPurePlaylist && hasRealList;

  let playlistItems = "";
  if (hasRealList) {
    const remainder = text.replace(match[0], " ").trim();
    const rangeMatch = remainder.match(/\b(\d+(?:[-:]\d+)?(?:,\d+(?:[-:]\d+)?)*)\b/);
    playlistItems = rangeMatch ? rangeMatch[1] : "";
  }

  return {
    url: parsed.toString(),
    playlistItems,
    isPurePlaylist,
    isVideoInPlaylist,
  };
}

function buildKeyboard(info) {
  if (info.isPurePlaylist) {
    return {
      inline_keyboard: [
        [
          { text: "📋 1080p", callback_data: "dl:p:1080" },
          { text: "📋 720p", callback_data: "dl:p:720" },
        ],
        [
          { text: "📋 480p", callback_data: "dl:p:480" },
          { text: "📋 360p", callback_data: "dl:p:360" },
        ],
        [
          { text: "🎵 صوت MP3", callback_data: "dl:p:mp3" },
          { text: "❌ إلغاء", callback_data: "cancel" },
        ],
      ],
    };
  }

  if (info.isVideoInPlaylist) {
    return {
      inline_keyboard: [
        [
          { text: "🎬 هذا الفيديو 1080p", callback_data: "dl:v:1080" },
          { text: "🎬 هذا الفيديو 720p", callback_data: "dl:v:720" },
        ],
        [
          { text: "🎬 هذا الفيديو 360p", callback_data: "dl:v:360" },
          { text: "🎵 هذا الفيديو MP3", callback_data: "dl:v:mp3" },
        ],
        [
          { text: "📋 البلايليست كاملة 720p", callback_data: "dl:p:720" },
          { text: "📋 البلايليست كاملة 480p", callback_data: "dl:p:480" },
        ],
        [
          { text: "📋 البلايليست كاملة 360p", callback_data: "dl:p:360" },
          { text: "🎵 البلايليست MP3", callback_data: "dl:p:mp3" },
        ],
        [{ text: "❌ إلغاء", callback_data: "cancel" }],
      ],
    };
  }

  return {
    inline_keyboard: [
      [
        { text: "🎬 1080p", callback_data: "dl:v:1080" },
        { text: "🎬 720p", callback_data: "dl:v:720" },
      ],
      [
        { text: "🎬 480p", callback_data: "dl:v:480" },
        { text: "🎬 360p", callback_data: "dl:v:360" },
      ],
      [
        { text: "🎵 صوت MP3", callback_data: "dl:v:mp3" },
        { text: "❌ إلغاء", callback_data: "cancel" },
      ],
    ],
  };
}

async function handleMessage(msg, env) {
  const chatId = msg.chat?.id;
  const userId = msg.from?.id;
  const text = (msg.text || msg.caption || "").trim();

  if (!chatId || !text) return;

  if (text === "/myid") {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "sendMessage", {
      chat_id: chatId,
      text: `🆔 معرّفك (User ID): ${userId}`,
    });
    return;
  }

  if (!isAuthorized(userId, env)) {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "sendMessage", {
      chat_id: chatId,
      text: `⛔ هذا البوت خاص.\n🆔 معرّفك: ${userId}\nأضفه في المتغير ALLOWED_USER_IDS في Cloudflare لتفعيل الوصول.`,
    });
    return;
  }

  if (text === "/start" || text === "/help") {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "sendMessage", {
      chat_id: chatId,
      text: [
        "👋 أهلاً بك في بوت تحميل يوتيوب!",
        "",
        "📌 الاستخدام:",
        "• أرسل رابط فيديو يوتيوب أو Shorts لتحميله.",
        "• أرسل رابط بلايليست لتحميلها بالكامل.",
        "• لتحميل جزء محدد من بلايليست، أضف النطاق بعد الرابط:",
        "  https://youtube.com/playlist?list=... 1-10",
        "",
        "📦 يدعم رفع الملفات حتى 2 GB عبر Telethon.",
      ].join("\n"),
      link_preview_options: { is_disabled: true },
    });
    return;
  }

  const info = parseYouTubeInput(text);
  if (!info) {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "sendMessage", {
      chat_id: chatId,
      reply_to_message_id: msg.message_id,
      text: "⚠️ لم يتم العثور على رابط يوتيوب صالح في رسالتك.",
    });
    return;
  }

  const lines = ["🎯 اختر الجودة المطلوبة:", `🔗 ${info.url}`];
  if (info.playlistItems) {
    lines.push(`📌 النطاق: ${info.playlistItems}`);
  }

  await tgApi(env.TELEGRAM_BOT_TOKEN, "sendMessage", {
    chat_id: chatId,
    reply_to_message_id: msg.message_id,
    text: lines.join("\n"),
    reply_markup: buildKeyboard(info),
    link_preview_options: { is_disabled: true },
  });
}

async function handleCallbackQuery(cq, env) {
  const userId = cq.from?.id;
  const chatId = cq.message?.chat?.id;
  const messageId = cq.message?.message_id;
  const data = cq.data || "";

  if (!isAuthorized(userId, env)) {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: cq.id,
      text: "⛔ غير مصرح لك باستخدام هذا البوت.",
      show_alert: true,
    });
    return;
  }

  if (data === "cancel") {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: cq.id,
      text: "تم الإلغاء",
    });
    await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
      chat_id: chatId,
      message_id: messageId,
      text: "❌ تم إلغاء الطلب.",
    });
    return;
  }

  const parts = data.split(":");
  if (parts.length !== 3 || parts[0] !== "dl") {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: cq.id,
      text: "⚠️ خيار غير صالح",
    });
    return;
  }

  const mode = parts[1] === "p" ? "playlist" : "video";
  const quality = parts[2];
  if (!VALID_QUALITIES.has(quality)) {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: cq.id,
      text: "⚠️ جودة غير صالحة",
    });
    return;
  }

  const msgText = cq.message?.text || "";
  const urlLine = msgText.split("\n").find((l) => l.startsWith("🔗 "));
  const rangeLine = msgText.split("\n").find((l) => l.startsWith("📌 النطاق: "));
  const ytUrl = urlLine ? urlLine.replace(/^🔗\s*/, "").trim() : "";
  const playlistItems = rangeLine ? rangeLine.replace(/^📌 النطاق:\s*/, "").trim() : "";

  if (!ytUrl) {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
      callback_query_id: cq.id,
      text: "⚠️ تعذر استخراج الرابط، أرسله مجدداً.",
      show_alert: true,
    });
    return;
  }

  await tgApi(env.TELEGRAM_BOT_TOKEN, "answerCallbackQuery", {
    callback_query_id: cq.id,
    text: "⏳ جاري بدء التحميل...",
  });

  const modeLabel = mode === "playlist" ? "بلايليست 📋" : "فيديو 🎬";
  const qualityLabel = QUALITY_LABELS[quality] || quality;

  const statusLines = [
    "⏳ تم إرسال الطلب إلى GitHub Actions...",
    `📦 النوع: ${modeLabel} | الجودة: ${qualityLabel}`,
    `🔗 ${ytUrl}`,
  ];
  if (playlistItems && mode === "playlist") {
    statusLines.push(`📌 النطاق: ${playlistItems}`);
  }
  statusLines.push("⏱️ سيبدأ التنفيذ خلال ~20–30 ثانية.");

  await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text: statusLines.join("\n"),
    link_preview_options: { is_disabled: true },
  });

  if (!env.GITHUB_TOKEN || !env.GITHUB_OWNER || !env.GITHUB_REPO) {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
      chat_id: chatId,
      message_id: messageId,
      text: "❌ خطأ في الإعدادات: تأكد من ضبط GITHUB_TOKEN و GITHUB_OWNER و GITHUB_REPO في Cloudflare.",
    });
    return;
  }

  const branch = env.GITHUB_BRANCH || "main";
  const dispatchUrl = `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/actions/workflows/download.yml/dispatches`;

  const ghRes = await fetch(dispatchUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "yt-tg-bot-cloudflare-worker",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      ref: branch,
      inputs: {
        url: ytUrl,
        chat_id: String(chatId),
        status_msg_id: String(messageId),
        mode,
        quality,
        playlist_items: mode === "playlist" ? playlistItems : "",
      },
    }),
  });

  if (!ghRes.ok) {
    const errBody = await ghRes.text();
    await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
      chat_id: chatId,
      message_id: messageId,
      text: `❌ فشل تشغيل GitHub Actions (HTTP ${ghRes.status}):\n${errBody.slice(0, 400)}`,
    });
  }
}
