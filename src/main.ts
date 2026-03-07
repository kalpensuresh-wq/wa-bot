import { Telegraf, Markup } from 'telegraf';
import { Client, LocalAuth } from 'whatsapp-web.js';
import * as QRCode from 'qrcode';
import * as fs from 'fs';
import 'dotenv/config';

const bot = new Telegraf(process.env.TELEGRAM_BOT_TOKEN!);

const ADMIN_IDS = (process.env.ADMIN_TELEGRAM_IDS || '')
  .split(',')
  .map(id => parseInt(id.trim()))
  .filter(id => !isNaN(id));

// Хранилище WhatsApp клиентов (в памяти)
const waClients = new Map<string, { client: Client; status: string; name: string }>();
const userStates = new Map<number, { action: string }>();
const activeBroadcasts = new Map<string, { stop: boolean; sent: number; failed: number; total: number }>();

const isAdmin = (userId: number) => ADMIN_IDS.includes(userId);

const mainMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('📱 Аккаунты', 'accounts')],
  [Markup.button.callback('📋 Мои группы', 'groups')],
  [Markup.button.callback('📨 Рассылка', 'broadcast')],
]);

// Middleware
bot.use(async (ctx, next) => {
  if (ctx.from && !isAdmin(ctx.from.id)) {
    return ctx.reply('⛔ Нет доступа').catch(() => {});
  }
  await next();
});

// Команда /start
bot.start(async (ctx) => {
  await ctx.reply('🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
    parse_mode: 'Markdown',
    ...mainMenu()
  });
});

// Главное меню
bot.action('main', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  userStates.delete(ctx.from!.id);
  await ctx.editMessageText('🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
    parse_mode: 'Markdown',
    ...mainMenu()
  }).catch(() => {});
});

// Список аккаунтов
bot.action('accounts', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  let text = '📱 *Аккаунты WhatsApp*\n\n';

  if (waClients.size === 0) {
    text += 'Нет подключенных аккаунтов\n';
  } else {
    waClients.forEach((wa, id) => {
      const emoji = wa.status === 'connected' ? '🟢' : wa.status === 'qr' ? '📱' : '🔴';
      text += `${emoji} *${wa.name}* (${wa.status})\n\n`;
    });
  }

  const buttons: any[][] = [];
  buttons.push([Markup.button.callback('➕ Добавить аккаунт', 'add_account')]);
  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard(buttons)
  }).catch(() => {});
});

// Добавить аккаунт
bot.action('add_account', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  userStates.set(ctx.from!.id, { action: 'waiting_account_name' });
  await ctx.editMessageText('📱 Введите название для нового аккаунта:', {
    ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'accounts')]])
  }).catch(() => {});
});

// Подключить аккаунт (создать новый клиент)
bot.action(/^connect_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery('Подключение...').catch(() => {});
  try {
    const accountId = ctx.match![1];
    await ctx.editMessageText('⏳ Запуск WhatsApp клиента...\nЭто может занять 10-30 секунд').catch(() => {});

    const sessionsPath = process.env.WA_SESSIONS_PATH || './wa-sessions';
    if (!fs.existsSync(sessionsPath)) {
      fs.mkdirSync(sessionsPath, { recursive: true });
    }

    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: accountId,
        dataPath: sessionsPath,
      }),
      puppeteer: {
        headless: true,
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
        args: [
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-accelerated-2d-canvas',
          '--no-first-run',
          '--no-zygote',
          '--single-process',
          '--disable-gpu',
        ],
      },
    });

    const accountName = waClients.get(accountId)?.name || accountId;
    waClients.set(accountId, { client, status: 'connecting', name: accountName });

    client.on('qr', async (qr) => {
      console.log('QR received for', accountId);
      const waClient = waClients.get(accountId);
      if (waClient) {
        waClient.status = 'qr';
      }
      try {
        const qrDataUrl = await QRCode.toDataURL(qr);
        const base64Data = qrDataUrl.replace(/^data:image\/png;base64,/, '');
        const buffer = Buffer.from(base64Data, 'base64');
        await ctx.replyWithPhoto({ source: buffer }, {
          caption: '📱 *Отсканируйте QR-код*\n\nWhatsApp → Настройки → Связанные устройства → Привязать устройство',
          parse_mode: 'Markdown'
        });
      } catch (e) {
        console.error('QR send error:', e);
      }
    });

    client.on('ready', async () => {
      console.log('Client ready for', accountId);
      const waClient = waClients.get(accountId);
      if (waClient) {
        waClient.status = 'connected';
      }
      await ctx.reply(`✅ *${accountName}* подключен!\nТелефон: ${client.info?.wid?.user}`).catch(() => {});
    });

    client.on('auth_failure', async () => {
      console.log('Auth failure for', accountId);
      const waClient = waClients.get(accountId);
      if (waClient) waClient.status = 'disconnected';
      await ctx.reply('❌ Ошибка авторизации').catch(() => {});
    });

    client.on('disconnected', async () => {
      console.log('Disconnected:', accountId);
      const waClient = waClients.get(accountId);
      if (waClient) waClient.status = 'disconnected';
      await ctx.reply(`⚠️ Аккаунт *${accountName}* отключен`).catch(() => {});
    });

    await client.initialize();
  } catch (error) {
    console.error('Connect error:', error);
    await ctx.editMessageText(`❌ Ошибка: ${(error as Error).message}`).catch(() => {});
  }
});

// Отключить аккаунт
bot.action(/^disconnect_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  try {
    const accountId = ctx.match![1];
    const waClient = waClients.get(accountId);
    if (waClient) {
      try { await waClient.client.destroy(); } catch (e) {}
      waClients.delete(accountId);
    }
    await ctx.reply('✅ Аккаунт отключен').catch(() => {});
  } catch (error) {
    console.error('Error in disconnect:', error);
    await ctx.reply('❌ Ошибка').catch(() => {});
  }
});

// Удалить аккаунт
bot.action(/^delete_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];
  await ctx.editMessageText('⚠️ Удалить этот аккаунт?', {
    ...Markup.inlineKeyboard([
      [Markup.button.callback('✅ Да', `confirm_del_${accountId}`)],
      [Markup.button.callback('❌ Нет', 'accounts')]
    ])
  }).catch(() => {});
});

bot.action(/^confirm_del_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  try {
    const accountId = ctx.match![1];
    const waClient = waClients.get(accountId);
    if (waClient) {
      try { await waClient.client.destroy(); } catch (e) {}
      waClients.delete(accountId);
    }
    await ctx.reply('✅ Аккаунт удален').catch(() => {});
  } catch (error) {
    console.error('Error in confirm_del:', error);
    await ctx.reply('❌ Ошибка').catch(() => {});
  }
});

// Мои группы (из памяти)
bot.action('groups', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  let text = '📋 *Архивные группы*\n\n';
  let totalGroups = 0;

  for (const [id, wa] of waClients) {
    if (wa.status !== 'connected') continue;

    try {
      const chats = await wa.client.getChats();
      const archivedGroups = chats.filter((chat: any) => chat.isGroup && chat.archived);
      totalGroups += archivedGroups.length;

      if (archivedGroups.length > 0) {
        text += `📱 *${wa.name}:* ${archivedGroups.length} групп\n`;
      }
    } catch (error) {
      console.error('Error getting chats:', error);
    }
  }

  if (totalGroups === 0) {
    text += 'Нет архивных групп.\n';
    text += 'Подключите аккаунт и убедитесь, что есть архивные группы в WhatsApp.';
  } else {
    text += `\n📊 Всего: ${totalGroups} групп`;
  }

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard([[Markup.button.callback('◀️ Назад', 'main')]])
  }).catch(() => {});
});

// Рассылка
bot.action('broadcast', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  // Проверяем есть ли подключенные аккаунты
  let connectedCount = 0;
  for (const [id, wa] of waClients) {
    if (wa.status === 'connected') connectedCount++;
  }

  if (connectedCount === 0) {
    await ctx.editMessageText('❌ Нет подключенных аккаунтов.\nСначала добавьте аккаунт в разделе "Аккаунты".', {
      ...Markup.inlineKeyboard([[Markup.button.callback('◀️ Назад', 'main')]])
    }).catch(() => {});
    return;
  }

  userStates.set(ctx.from!.id, { action: 'waiting_broadcast_text' });
  await ctx.editMessageText('📨 *Рассылка*\n\nОтправьте текст или видео для рассылки во ВСЕ архивные группы:', {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'main')]])
  }).catch(() => {});
});

// Остановка рассылки
bot.action(/^stop_broadcast_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const broadcastId = ctx.match![1];
  const broadcast = activeBroadcasts.get(broadcastId);
  if (broadcast) {
    broadcast.stop = true;
    await ctx.editMessageText('⏹ Рассылка остановлена').catch(() => {});
  }
});

// Обработка текстовых сообщений
bot.on('text', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;
  const state = userStates.get(ctx.from!.id);
  if (!state) return;

  try {
    if (state.action === 'waiting_account_name') {
      const name = ctx.message.text;
      const accountId = `wa_${Date.now()}`;
      waClients.set(accountId, { client: null as any, status: 'pending', name });
      userStates.delete(ctx.from!.id);

      // Сразу предлагаем подключить
      await ctx.reply(`✅ Аккаунт "${name}" создан.\nНажмите кнопку ниже для подключения:`, {
        reply_markup: {
          inline_keyboard: [[Markup.button.callback('🔗 Подключить WhatsApp', `connect_${accountId}`)]]
        }
      });
      return;
    }

    if (state.action === 'waiting_broadcast_text') {
      const text = ctx.message.text;
      await startArchivedBroadcast(ctx, text);
      userStates.delete(ctx.from!.id);
      return;
    }
  } catch (error) {
    console.error('Error in text handler:', error);
    await ctx.reply('❌ Ошибка: ' + (error as Error).message).catch(() => {});
    userStates.delete(ctx.from!.id);
  }
});

// Обработка видео
bot.on('video', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;
  const state = userStates.get(ctx.from!.id);

  try {
    if (state?.action === 'waiting_broadcast_text') {
      const caption = ctx.message.caption || '';
      await startArchivedBroadcast(ctx, caption);
      userStates.delete(ctx.from!.id);
    }
  } catch (error) {
    console.error('Error in video handler:', error);
    await ctx.reply('❌ Ошибка').catch(() => {});
    userStates.delete(ctx.from!.id);
  }
});

// Функция рассылки во ВСЕ архивные чаты с задержкой 10 минут
async function startArchivedBroadcast(ctx: any, text: string) {
  const connectedAccounts: { accountId: string; client: Client; name: string }[] = [];

  waClients.forEach((wa, accountId) => {
    if (wa.status === 'connected') {
      connectedAccounts.push({ accountId, client: wa.client, name: wa.name });
    }
  });

  if (connectedAccounts.length === 0) {
    return ctx.reply('❌ Нет подключенных аккаунтов').catch(() => {});
  }

  let allArchivedChats: any[] = [];
  await ctx.reply('🔍 Поиск архивных чатов...').catch(() => {});

  for (const acc of connectedAccounts) {
    try {
      const chats = await acc.client.getChats();
      const archivedChats = chats.filter((chat: any) => chat.isGroup && chat.archived);
      allArchivedChats = [...allArchivedChats, ...archivedChats.map((chat: any) => ({
        ...chat,
        accountId: acc.accountId,
        accountName: acc.name
      }))];
      await ctx.reply(`📱 *${acc.name}:* найдено ${archivedChats.length} архивных групп`, { parse_mode: 'Markdown' }).catch(() => {});
    } catch (error) {
      console.error('Error getting chats:', error);
    }
  }

  if (allArchivedChats.length === 0) {
    return ctx.reply('❌ Архивные чаты не найдены').catch(() => {});
  }

  const broadcastId = Date.now().toString();
  activeBroadcasts.set(broadcastId, { stop: false, sent: 0, failed: 0, total: allArchivedChats.length });

  const estimatedMin = allArchivedChats.length * 10;
  const hours = Math.floor(estimatedMin / 60);
  const mins = estimatedMin % 60;

  await ctx.reply(
    `📦 *Рассылка в архивные чаты*\n\n` +
    `Найдено: ${allArchivedChats.length} чатов\n` +
    `Время: ~${hours}ч ${mins}мин\n` +
    `Задержка: 10 минут между сообщениями\n\n` +
    `Нажмите /start для остановки`,
    { parse_mode: 'Markdown' }
  );

  let sent = 0, failed = 0;

  for (let i = 0; i < allArchivedChats.length; i++) {
    const broadcast = activeBroadcasts.get(broadcastId);
    if (broadcast?.stop) {
      await ctx.reply(`⏹ Остановлено\n✅ ${sent} | ❌ ${failed}`).catch(() => {});
      activeBroadcasts.delete(broadcastId);
      return;
    }

    const chat = allArchivedChats[i];
    const wa = waClients.get(chat.accountId);

    if (!wa || wa.status !== 'connected') {
      failed++;
      continue;
    }

    try {
      await wa.client.sendMessage(chat.id._serialized, text);
      sent++;

      console.log(`Sent to ${chat.name} (${i+1}/${allArchivedChats.length})`);

      // Обновляем каждые 5 сообщений
      if (i % 5 === 0 || i === allArchivedChats.length - 1) {
        const remaining = allArchivedChats.length - (i + 1);
        const remainingTime = remaining * 10;
        await ctx.reply(
          `📊 Прогресс: ${i + 1}/${allArchivedChats.length}\n` +
          `✅ Успешно: ${sent} | ❌ Ошибок: ${failed}\n` +
          `⏳ Осталось: ~${Math.ceil(remainingTime / 60)} мин`,
        ).catch(() => {});
      }

      // Задержка 10 минут между сообщениями
      if (i < allArchivedChats.length - 1) {
        await new Promise(r => setTimeout(r, 600000)); // 10 минут
      }
    } catch (error) {
      console.error('Send error:', error);
      failed++;
    }
  }

  await ctx.reply(
    `🏁 *Рассылка завершена*\n\n` +
    `✅ Отправлено: ${sent}\n` +
    `❌ Ошибок: ${failed}\n` +
    `📊 Всего: ${allArchivedChats.length}`,
    { parse_mode: 'Markdown' }
  ).catch(() => {});

  activeBroadcasts.delete(broadcastId);
}

// Глобальный обработчик ошибок
bot.catch((err, ctx) => {
  console.error('Bot error:', err);
  try {
    ctx.reply('⚠️ Ошибка. Попробуйте еще раз.').catch(() => {});
  } catch (e) {
    console.error('Error in catch:', e);
  }
});

// Запуск
async function main() {
  console.log('🚀 Starting bot...');
  console.log('Admin IDs:', ADMIN_IDS);

  await bot.launch();
  console.log('✅ Bot started');
}

main().catch(console.error);

process.once('SIGINT', () => {
  console.log('SIGINT, stopping...');
  bot.stop('SIGINT');
  process.exit(0);
});

process.once('SIGTERM', () => {
  console.log('SIGTERM, stopping...');
  bot.stop('SIGTERM');
  process.exit(0);
});
