/**
 * Cloudflare Worker: Telegram Webhook + D1 Warm-Session Queue -> GitHub Actions Downloader
 *
 * Bindings / Env Vars:
 *   DB                      - Cloudflare D1 database (jobs + runner_state)
 *   TELEGRAM_BOT_TOKEN      - Bot token from @BotFather
 *   TELEGRAM_WEBHOOK_SECRET - Random secret string for Telegram webhook verification
 *   RUNNER_SECRET           - Random secret string authenticating GitHub Actions runner API
 *   GITHUB_TOKEN            - GitHub PAT (Actions: Read and write)
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

const HEARTBEAT_ALIVE_SEC = 28;
const DISPATCH_BOOT_SEC = 40;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Health check endpoint
    if (request.method === "GET" && url.pathname === "/") {
      return new Response("yt-tg-bot worker is running.", { status: 200 });
    }

    // One-click webhook registration helper
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
      return jsonResponse({ webhookUrl, telegram: result }, 200);
    }

    // Runner Queue API (used by warm GitHub Actions runner)
    if (request.method === "POST" && url.pathname.startsWith("/runner/")) {
      return handleRunnerApi(request, url.pathname, env);
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

      return new Response("OK", { status: 200 });
    }

    return new Response("Not Found", { status: 404 });
  },
};

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

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
  return getAllowedUsers(env).has(String(userId));
}

async function tgApi(token, method, payload) {
  const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

async function handleRunnerApi(request, pathname, env) {
  const expectedSecret = env.RUNNER_SECRET || env.TELEGRAM_WEBHOOK_SECRET || "";
  const incomingSecret = request.headers.get("X-Runner-Secret") || "";
  if (!expectedSecret || !timingSafeEqual(incomingSecret, expectedSecret)) {
    return jsonResponse({ error: "Forbidden" }, 403);
  }
  if (!env.DB) {
    return jsonResponse({ error: "D1 DB binding missing" }, 500);
  }

  let body = {};
  try {
    const raw = await request.text();
    if (raw) body = JSON.parse(raw);
  } catch {
    body = {};
  }

  const ts = nowSec();
  const runnerId = String(body.runner_id || "");

  if (pathname === "/runner/heartbeat") {
    await env.DB.prepare(
      "UPDATE runner_state SET last_heartbeat = ?, runner_id = CASE WHEN ? != '' THEN ? ELSE runner_id END WHERE id = 1"
    )
      .bind(ts, runnerId, runnerId)
      .run();
    return jsonResponse({ ok: true, ts });
  }

  if (pathname === "/runner/claim") {
    // Refresh heartbeat
    await env.DB.prepare(
      "UPDATE runner_state SET last_heartbeat = ?, runner_id = CASE WHEN ? != '' THEN ? ELSE runner_id END WHERE id = 1"
    )
      .bind(ts, runnerId, runnerId)
      .run();

    // Clean up very old completed/abandoned rows
    await env.DB.prepare("DELETE FROM jobs WHERE status = 'done' AND created_at < ?")
      .bind(ts - 86400)
      .run();
    await env.DB.prepare("UPDATE jobs SET status = 'done' WHERE status = 'running' AND created_at < ?")
      .bind(ts - 21600)
      .run();

    // Atomically claim the oldest pending job
    const claimed = await env.DB.prepare(
      "UPDATE jobs SET status = 'running' WHERE id = (SELECT id FROM jobs WHERE status = 'pending' ORDER BY id ASC LIMIT 1) RETURNING *"
    ).first();

    if (claimed) {
      return jsonResponse({ job: claimed, stop: false });
    }

    const state = await env.DB.prepare("SELECT stop_requested FROM runner_state WHERE id = 1").first();
    const shouldStop = Boolean(state && state.stop_requested);
    return jsonResponse({ job: null, stop: shouldStop });
  }

  if (pathname === "/runner/complete") {
    const jobId = Number(body.job_id || 0);
    if (jobId > 0) {
      await env.DB.prepare("UPDATE jobs SET status = 'done' WHERE id = ?").bind(jobId).run();
    }
    await env.DB.prepare("UPDATE runner_state SET last_heartbeat = ? WHERE id = 1").bind(ts).run();
    return jsonResponse({ ok: true });
  }

  if (pathname === "/runner/bye") {
    // Prevent shutdown race if a new pending job arrived right now
    const pendingRow = await env.DB.prepare(
      "SELECT COUNT(*) AS cnt FROM jobs WHERE status = 'pending'"
    ).first();
    const pending = Number(pendingRow?.cnt || 0);
    if (pending > 0 && !body.force) {
      return jsonResponse({ ok: false, pending });
    }
    await env.DB.prepare(
      "UPDATE runner_state SET last_heartbeat = 0, last_dispatch = 0, stop_requested = 0, runner_id = '' WHERE id = 1"
    ).run();
    return jsonResponse({ ok: true, pending: 0 });
  }

  return jsonResponse({ error: "Not Found" }, 404);
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

  if (text === "/status") {
    let statusText = "⚪ قاعدة الطابور غير مفعّلة.";
    if (env.DB) {
      const ts = nowSec();
      const st = await env.DB.prepare(
        "SELECT last_heartbeat, last_dispatch FROM runner_state WHERE id = 1"
      ).first();
      const counts = await env.DB.prepare(
        "SELECT SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running FROM jobs"
      ).first();
      const hb = Number(st?.last_heartbeat || 0);
      const disp = Number(st?.last_dispatch || 0);
      const isAlive = ts - hb < HEARTBEAT_ALIVE_SEC;
      const isBooting = !isAlive && ts - disp < DISPATCH_BOOT_SEC;
      const pending = Number(counts?.pending || 0);
      const running = Number(counts?.running || 0);

      if (isAlive) {
        statusText = `🟢 السيرفر جاهز الآن (Warm)\n⚡ أي طلب جديد سيبدأ خلال ~1 ثانية.\n📦 قيد التنفيذ: ${running} | بالانتظار: ${pending}`;
      } else if (isBooting) {
        statusText = `🟡 السيرفر قيد التشغيل الآن...\n📦 بالانتظار: ${pending}`;
      } else {
        statusText = `⚪ السيرفر في وضع السكون (يوفّر دقائقك)\n⏱️ أول طلب سيستغرق ~10 ثوانٍ، ثم يبقى جاهزاً لـ 4 دقائق.`;
      }
    }
    await tgApi(env.TELEGRAM_BOT_TOKEN, "sendMessage", {
      chat_id: chatId,
      text: statusText,
    });
    return;
  }

  if (text === "/sleep") {
    if (env.DB) {
      await env.DB.prepare("UPDATE runner_state SET stop_requested = 1 WHERE id = 1").run();
    }
    await tgApi(env.TELEGRAM_BOT_TOKEN, "sendMessage", {
      chat_id: chatId,
      text: "😴 تم إرسال أمر إيقاف السيرفر الساخن (سيتوقف تلقائياً فور انتهاء أي تحميل حالي).",
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
        "• أرسل رابط بلايليست لتحميلها بالتوازي وبنفس الترتيب.",
        "• لتحميل جزء محدد من بلايليست، أضف النطاق بعد الرابط:",
        "  https://youtube.com/playlist?list=... 1-10",
        "",
        "⚡ وضع الجلسة الساخنة مفعّل: أول طلب ~10 ثوانٍ، وأي طلب خلال 4 دقائق يبدأ خلال ~1 ثانية!",
        "• /status — عرض حالة السيرفر",
        "• /sleep — إيقاف السيرفر الساخن فوراً لتوفير الدقائق",
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

async function dispatchGitHubWorkflow(env, inputs) {
  const branch = env.GITHUB_BRANCH || "main";
  const dispatchUrl = `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/actions/workflows/download.yml/dispatches`;
  return fetch(dispatchUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GITHUB_TOKEN}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "yt-tg-bot-cloudflare-worker",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ref: branch, inputs }),
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
    text: "⚡ جاري بدء التحميل...",
  });

  const modeLabel = mode === "playlist" ? "بلايليست 📋" : "فيديو 🎬";
  const qualityLabel = QUALITY_LABELS[quality] || quality;
  const effectiveItems = mode === "playlist" ? playlistItems : "";

  if (!env.GITHUB_TOKEN || !env.GITHUB_OWNER || !env.GITHUB_REPO) {
    await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
      chat_id: chatId,
      message_id: messageId,
      text: "❌ خطأ في الإعدادات: تأكد من ضبط GITHUB_TOKEN و GITHUB_OWNER و GITHUB_REPO في Cloudflare.",
    });
    return;
  }

  // Warm-session D1 queue path
  if (env.DB) {
    const ts = nowSec();
    const inserted = await env.DB.prepare(
      "INSERT INTO jobs (url, chat_id, status_msg_id, mode, quality, playlist_items, status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?) RETURNING id"
    )
      .bind(ytUrl, String(chatId), String(messageId), mode, quality, effectiveItems, ts)
      .first();

    // Cancel any pending /sleep request since user sent a new job
    await env.DB.prepare("UPDATE runner_state SET stop_requested = 0 WHERE id = 1").run();

    const state = await env.DB.prepare(
      "SELECT last_heartbeat, last_dispatch FROM runner_state WHERE id = 1"
    ).first();
    const lastHb = Number(state?.last_heartbeat || 0);
    const lastDisp = Number(state?.last_dispatch || 0);
    const isAlive = ts - lastHb < HEARTBEAT_ALIVE_SEC;
    const isBooting = !isAlive && ts - lastDisp < DISPATCH_BOOT_SEC;

    const statusLines = [];
    if (isAlive) {
      statusLines.push("⚡ السيرفر جاهز — سيبدأ التحميل فوراً!");
    } else if (isBooting) {
      statusLines.push("⏳ السيرفر قيد التشغيل حالياً — تمت إضافة طلبك للطابور وسيبدأ تلقائياً.");
    } else {
      statusLines.push("⏳ جاري تشغيل السيرفر (~10 ثوانٍ)...");
    }
    statusLines.push(`📦 النوع: ${modeLabel} | الجودة: ${qualityLabel}`);
    statusLines.push(`🔗 ${ytUrl}`);
    if (effectiveItems) {
      statusLines.push(`📌 النطاق: ${effectiveItems}`);
    }
    if (!isAlive && !isBooting) {
      statusLines.push("⚡ سيبقى السيرفر ساخناً لـ 4 دقائق لتنفيذ طلباتك التالية خلال ~1 ثانية.");
    }

    await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
      chat_id: chatId,
      message_id: messageId,
      text: statusLines.join("\n"),
      link_preview_options: { is_disabled: true },
    });

    if (!isAlive && !isBooting) {
      await env.DB.prepare("UPDATE runner_state SET last_dispatch = ? WHERE id = 1").bind(ts).run();
      const ghRes = await dispatchGitHubWorkflow(env, {
        mode: "queue",
        chat_id: String(chatId),
        status_msg_id: String(messageId),
        url: "",
        quality,
        playlist_items: "",
      });
      if (!ghRes.ok) {
        const errBody = await ghRes.text();
        if (inserted?.id) {
          await env.DB.prepare("UPDATE jobs SET status = 'done' WHERE id = ?").bind(inserted.id).run();
        }
        await env.DB.prepare("UPDATE runner_state SET last_dispatch = 0 WHERE id = 1").run();
        await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
          chat_id: chatId,
          message_id: messageId,
          text: `❌ فشل تشغيل GitHub Actions (HTTP ${ghRes.status}):\n${errBody.slice(0, 400)}`,
        });
      }
    }
    return;
  }

  // Fallback direct dispatch (if DB binding not configured)
  const statusLines = [
    "⏳ جاري تشغيل السيرفر (~10 ثوانٍ)...",
    `📦 النوع: ${modeLabel} | الجودة: ${qualityLabel}`,
    `🔗 ${ytUrl}`,
  ];
  if (effectiveItems) statusLines.push(`📌 النطاق: ${effectiveItems}`);

  await tgApi(env.TELEGRAM_BOT_TOKEN, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text: statusLines.join("\n"),
    link_preview_options: { is_disabled: true },
  });

  const ghRes = await dispatchGitHubWorkflow(env, {
    url: ytUrl,
    chat_id: String(chatId),
    status_msg_id: String(messageId),
    mode,
    quality,
    playlist_items: effectiveItems,
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
