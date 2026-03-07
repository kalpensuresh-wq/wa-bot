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

// Хранилище аккаунтов
const waAccounts = new Map<string, {
  client: Client;
  status: 'disconnected' | 'connecting' | 'connected';
  phone?: string;
  name: string;
  customMessage: string;
}>();

const userStates = new Map<number, { action: string; accountId?: string }>();
const activeBroadcasts = new Map<string, { stop: boolean; sent: number; failed: number; total: number }>();

const isAdmin = (userId: number) => ADMIN_IDS.includes(userId);

// Главное меню
const mainMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('📱 Аккаунты', 'accounts')],
  [Markup.button.callback('📨 Рассылка', 'broadcast_menu')],
]);

// Меню аккаунтов
const accountsMenu = (accountId?: string) => {
  const buttons: any[][] = [];

  if (accountId) {
    const acc = waAccounts.get(accountId);
    if (acc) {
      if (acc.status === 'connected') {
        buttons.push([Markup.button.callback('📝 Изменить текст', `edit_msg_${accountId}`)]);
      }
      buttons.push([Markup.button.callback('❌ Отвязать номер', `unbind_${accountId}`)]);
    }
    buttons.push([Markup.button.callback('◀️ Назад к списку', 'accounts')]);
  } else {
    if (waAccounts.size > 0) {
      waAccounts.forEach((acc, id) => {
        const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
        buttons.push([Markup.button.callback(`${emoji} ${acc.name || acc.phone || id}`, `acc_${id}`)]);
      });
    }
    buttons.push([Markup.button.callback('➕ Добавить номер', 'add_account')]);
    buttons.push([Markup.button.callback('◀️ Назад', 'main')]);
  }

  return Markup.inlineKeyboard(buttons);
};

// Меню выбора аккаунта для рассылки
const broadcastSelectMenu = () => {
  const buttons: any[][] = [];

  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected') {
      buttons.push([
        Markup.button.callback(`📱 ${acc.name || acc.phone}`, `broadcast_acc_${id}`)
      ]);
    }
  });

  if (buttons.length === 0) {
    buttons.push([Markup.button.callback('❌ Нет подключенных аккаунтов', 'main')]);
  }

  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);
  return Markup.inlineKeyboard(buttons);
};

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

// === УПРАВЛЕНИЕ АККАУНТАМИ ===

// Список аккаунтов
bot.action('accounts', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  let text = '📱 *Мои аккаунты*\n\n';

  if (waAccounts.size === 0) {
    text += 'Нет добавленных аккаунтов\n';
  } else {
    waAccounts.forEach((acc, id) => {
      const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
      text += `${emoji} *${acc.name || acc.phone || id}*\n`;
      if (acc.phone) text += `   📞 ${acc.phone}\n`;
      if (acc.customMessage) {
        text += `   📝 ${acc.customMessage.substring(0, 40)}...\n`;
      }
      text += '\n';
    });
  }

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...accountsMenu()
  }).catch(() => {});
});

// Просмотр аккаунта
bot.action(/^acc_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];
  const acc = waAccounts.get(accountId);

  if (!acc) {
    await ctx.editMessageText('❌ Аккаунт не найден', {
      ...accountsMenu()
    }).catch(() => {});
    return;
  }

  let text = `📱 *${acc.name || acc.phone || accountId}*\n\n`;
  text += `📊 Статус: ${acc.status}\n`;
  if (acc.phone) text += `📞 Телефон: ${acc.phone}\n`;
  if (acc.customMessage) {
    text += `\n📝 *Текст рассылки:*\n${acc.customMessage}\n`;
  } else {
    text += `\n📝 Текст не задан\n`;
  }

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...accountsMenu(accountId)
  }).catch(() => {});
});

// Добавить аккаунт - выбор способа
bot.action('add_account', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  userStates.set(ctx.from!.id, { action: 'waiting_phone' });

  await ctx.editMessageText(
    '📱 *Добавление номера*\n\n' +
    'Введите номер телефона в формате:\n' +
    '+79991234567\n\n' +
    'После ввода номера бот отправит QR-код для сканирования.',
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'accounts')]])
    }
  ).catch(() => {});
});

// Отвязать аккаунт
bot.action(/^unbind_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];

  await ctx.editMessageText(
    `⚠️ *Отвязать номер?*\n\n` +
    `Аккаунт будет удален и отключен от WhatsApp.`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('✅ Да, отвязать', `confirm_unbind_${accountId}`)],
          [Markup.button.callback('❌ Отмена', `acc_${accountId}`)]
        ]
      }
    }
  ).catch(() => {});
});

bot.action(/^confirm_unbind_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];
  const acc = waAccounts.get(accountId);

  if (acc && acc.client) {
    try {
      await acc.client.logout();
      await acc.client.destroy();
    } catch (e) {
      console.error('Error destroying client:', e);
    }
  }

  // Удаляем сессию
  const sessionsPath = process.env.WA_SESSIONS_PATH || './wa-sessions';
  const sessionDir = `${sessionsPath}/${accountId}`;
  if (fs.existsSync(sessionDir)) {
    fs.rmSync(sessionDir, { recursive: true, force: true });
  }

  waAccounts.delete(accountId);

  await ctx.reply('✅ Аккаунт отвязан').catch(() => {});
  await ctx.editMessageText('✅ Аккаунт удален', {
    ...accountsMenu()
  }).catch(() => {});
});

// Изменить текст рассылки
bot.action(/^edit_msg_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];
  userStates.set(ctx.from!.id, { action: 'waiting_custom_message', accountId });

  const acc = waAccounts.get(accountId);
  const currentMsg = acc?.customMessage || '';

  await ctx.editMessageText(
    `📝 *Текст для аккаунта ${acc?.name || acc?.phone || accountId}*\n\n` +
    `Текущий текст: ${currentMsg || 'не задан'}\n\n` +
    `Введите новый текст для рассылки:`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', `acc_${accountId}`)]])
    }
  ).catch(() => {});
});

// === РАССЫЛКА ===

bot.action('broadcast_menu', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  let text = '📨 *Рассылка*\n\n';
  text += 'Выберите аккаунт для рассылки:\n\n';

  let hasConnected = false;
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected') {
      hasConnected = true;
      const msg = acc.customMessage ? `"${acc.customMessage.substring(0, 25)}..."` : '❌ текст не задан';
      text += `📱 ${acc.name || acc.phone}\n`;
      text += `   ${msg}\n\n`;
    }
  });

  if (!hasConnected) {
    text += '❌ Нет подключенных аккаунтов';
  }

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...broadcastSelectMenu()
  }).catch(() => {});
});

// Выбрали аккаунт для рассылки
bot.action(/^broadcast_acc_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];
  const acc = waAccounts.get(accountId);

  if (!acc || acc.status !== 'connected') {
    await ctx.editMessageText('❌ Аккаунт не подключен', {
      ...broadcastSelectMenu()
    }).catch(() => {});
    return;
  }

  if (!acc.customMessage) {
    userStates.set(ctx.from!.id, { action: 'waiting_broadcast_text', accountId });
    await ctx.editMessageText(
      `📨 *Рассылка с ${acc.name || acc.phone}*\n\n` +
      `⚠️ Текст не настроен!\n\n` +
      `Введите текст для рассылки:`,
      {
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'broadcast_menu')]])
      }
    ).catch(() => {});
    return;
  }

  // Показываем подтверждение
  await ctx.editMessageText(
    `📨 *Подтверждение рассылки*\n\n` +
    `📱 Аккаунт: ${acc.name || acc.phone}\n\n` +
    `📝 *Текст:*\n${acc.customMessage}\n\n` +
    `⚠️ Отправить во все архивные группы?\nЗадержка: 10 минут между сообщениями.`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('✅ Да, начать', `confirm_broadcast_${accountId}`)],
          [Markup.button.callback('❌ Отмена', 'broadcast_menu')]
        ]
      }
    }
  ).catch(() => {});
});

// Подтверждение рассылки
bot.action(/^confirm_broadcast_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];
  const acc = waAccounts.get(accountId);

  if (!acc || acc.status !== 'connected' || !acc.customMessage) {
    await ctx.reply('❌ Аккаунт не готов к рассылке').catch(() => {});
    return;
  }

  await startBroadcast(ctx, accountId, acc.customMessage);
});

// === ОБРАБОТЧИКИ СООБЩЕНИЙ ===

bot.on('text', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;
  const state = userStates.get(ctx.from!.id);
  if (!state) return;

  try {
    // Ввод номера телефона
    if (state.action === 'waiting_phone') {
      const phone = ctx.message.text.trim();
      const cleanPhone = phone.replace(/[^0-9+]/g, '');
      if (cleanPhone.length < 10) {
        await ctx.reply('❌ Неверный формат номера. Введите в формате +79991234567');
        return;
      }

      await addNewAccount(ctx, cleanPhone);
      userStates.delete(ctx.from!.id);
      return;
    }

    // Ввод текста для аккаунта
    if (state.action === 'waiting_custom_message') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        acc.customMessage = ctx.message.text;
        await ctx.reply(`✅ Текст сохранен для аккаунта ${acc.name || acc.phone}`);
        await ctx.editMessageText(
          `✅ Текст сохранен!\n\n📝 ${ctx.message.text}`,
          { parse_mode: 'Markdown' }
        ).catch(() => {});
      }
      userStates.delete(ctx.from!.id);
      return;
    }

    // Ввод текста для рассылки
    if (state.action === 'waiting_broadcast_text') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        acc.customMessage = ctx.message.text;
        await ctx.reply('✅ Текст сохранен. Нажмите "Да, начать" для рассылки.');
      }
      userStates.delete(ctx.from!.id);
      return;
    }
  } catch (error) {
    console.error('Error in text handler:', error);
    await ctx.reply('❌ Ошибка: ' + (error as Error).message).catch(() => {});
    userStates.delete(ctx.from!.id);
  }
});

// === ФУНКЦИИ ===

// Добавить новый аккаунт
async function addNewAccount(ctx: any, phone: string) {
  const accountId = `wa_${phone.replace(/\+/g, '')}`;

  // Проверяем не добавлен ли уже
  if (waAccounts.has(accountId)) {
    const existing = waAccounts.get(accountId);
    if (existing?.status === 'connected') {
      await ctx.reply('✅ Этот номер уже подключен!').catch(() => {});
      return;
    }
    // Если был - удаляем старую сессию
    if (existing?.client) {
      try { await existing.client.destroy(); } catch (e) {}
    }
  }

  await ctx.reply('⏳ Создание сессии...\nЭто может занять 10-30 секунд').catch(() => {});

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

  // Сохраняем в память
  waAccounts.set(accountId, {
    client,
    status: 'connecting',
    phone,
    name: phone,
    customMessage: '',
  });

  let qrSent = false;

  client.on('qr', async (qr) => {
    console.log('QR received for', accountId);

    // Если уже подключен - не показываем QR
    const acc = waAccounts.get(accountId);
    if (acc?.status === 'connected') return;

    if (!qrSent) {
      qrSent = true;
      try {
        const qrDataUrl = await QRCode.toDataURL(qr);
        const base64Data = qrDataUrl.replace(/^data:image\/png;base64,/, '');
        const buffer = Buffer.from(base64Data, 'base64');
        await ctx.replyWithPhoto({ source: buffer }, {
          caption: '📱 *Отсканируйте QR-код*\n\n' +
            'WhatsApp → Настройки → Связанные устройства → Привязать устройство\n\n' +
            '⚠️ Если не сканируется, проверьте:\n' +
            '1. Интернет на телефоне\n' +
            '2. Закройте другие сессии WhatsApp Web',
          parse_mode: 'Markdown'
        });
      } catch (e) {
        console.error('QR send error:', e);
      }
    }
  });

  client.on('ready', async () => {
    console.log('Client ready for', accountId);
    const acc = waAccounts.get(accountId);
    if (acc) {
      acc.status = 'connected';
    }
    await ctx.reply(
      `✅ *Подключено!*\n\n` +
      `📞 Телефон: ${phone}\n\n` +
      `Теперь настройте текст для рассылки в разделе "Аккаунты"`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
  });

  client.on('auth_failure', async (msg) => {
    console.log('Auth failure for', accountId, msg);
    const acc = waAccounts.get(accountId);
    if (acc) acc.status = 'disconnected';
    await ctx.reply(
      `❌ Ошибка авторизации\n\n` +
      `Попробуйте ещё раз: удалите аккаунт и добавьте заново`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
  });

  client.on('disconnected', async () => {
    console.log('Disconnected:', accountId);
    const acc = waAccounts.get(accountId);
    if (acc) acc.status = 'disconnected';
    await ctx.reply(`⚠️ Аккаунт отключился`).catch(() => {});
  });

  try {
    await client.initialize();
  } catch (error) {
    console.error('Initialize error:', error);
    const acc = waAccounts.get(accountId);
    if (acc) acc.status = 'disconnected';
    await ctx.reply(`❌ Ошибка: ${(error as Error).message}`).catch(() => {});
  }
}

// Рассылка
async function startBroadcast(ctx: any, accountId: string, text: string) {
  const acc = waAccounts.get(accountId);

  if (!acc || acc.status !== 'connected') {
    return ctx.reply('❌ Аккаунт не подключен').catch(() => {});
  }

  await ctx.reply('🔍 Поиск архивных чатов...').catch(() => {});

  try {
    const chats = await acc.client.getChats();
    const archivedChats = chats.filter((chat: any) => chat.isGroup && chat.archived);

    if (archivedChats.length === 0) {
      return ctx.reply('❌ Архивные группы не найдены').catch(() => {});
    }

    const broadcastId = Date.now().toString();
    activeBroadcasts.set(broadcastId, { stop: false, sent: 0, failed: 0, total: archivedChats.length });

    const estimatedMin = archivedChats.length * 10;
    const hours = Math.floor(estimatedMin / 60);
    const mins = estimatedMin % 60;

    await ctx.reply(
      `📦 *Рассылка*\n\n` +
      `📱 Аккаунт: ${acc.name || acc.phone}\n` +
      `📊 Найдено: ${archivedChats.length} чатов\n` +
      `⏱ Время: ~${hours}ч ${mins}мин\n\n` +
      `Нажмите /start для остановки`,
      { parse_mode: 'Markdown' }
    );

    let sent = 0, failed = 0;

    for (let i = 0; i < archivedChats.length; i++) {
      const broadcast = activeBroadcasts.get(broadcastId);
      if (broadcast?.stop) {
        await ctx.reply(`⏹ Остановлено\n✅ ${sent} | ❌ ${failed}`).catch(() => {});
        activeBroadcasts.delete(broadcastId);
        return;
      }

      const chat = archivedChats[i];

      try {
        await acc.client.sendMessage(chat.id._serialized, text);
        sent++;
        console.log(`Sent to ${chat.name} (${i+1}/${archivedChats.length})`);

        if (i % 5 === 0 || i === archivedChats.length - 1) {
          const remaining = archivedChats.length - (i + 1);
          await ctx.reply(
            `📊 ${i + 1}/${archivedChats.length}\n` +
            `✅ ${sent} | ❌ ${failed}\n` +
            `⏳ ~${Math.ceil(remaining * 10 / 60)} мин`,
          ).catch(() => {});
        }

        if (i < archivedChats.length - 1) {
          await new Promise(r => setTimeout(r, 600000));
        }
      } catch (error) {
        console.error('Send error:', error);
        failed++;
      }
    }

    await ctx.reply(
      `🏁 *Завершено*\n\n` +
      `✅ Отправлено: ${sent}\n` +
      `❌ Ошибок: ${failed}`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});

    activeBroadcasts.delete(broadcastId);

  } catch (error) {
    console.error('Broadcast error:', error);
    await ctx.reply(`❌ Ошибка: ${(error as Error).message}`).catch(() => {});
  }
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
