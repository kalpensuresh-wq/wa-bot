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

// Хранилище аккаунтов с расширенными настройками
const waAccounts = new Map<string, {
  client: Client;
  status: 'disconnected' | 'connecting' | 'connected';
  phone?: string;
  name: string;
  customMessage: string;
  enabled: boolean; // Включен/выключен для рассылки
  sentToday: number;
  failedToday: number;
}>();

const userStates = new Map<number, { action: string; accountId?: string }>();
const activeBroadcasts = new Map<string, {
  stop: boolean;
  sent: number;
  failed: number;
  total: number;
  accountId: string;
}>();

const isAdmin = (userId: number) => ADMIN_IDS.includes(userId);

// === КОНФИГУРАЦИЯ ===
const BROADCAST_DELAY = 10 * 60 * 1000; // 10 минут между сообщениями
const MIN_DELAY = 5 * 60 * 1000; // Минимум 5 минут

// Главное меню
const mainMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('📱 Аккаунты', 'accounts')],
  [Markup.button.callback('📨 Рассылка', 'broadcast_menu')],
  [Markup.button.callback('⚙️ Настройки', 'settings')],
]);

// Меню аккаунтов
const accountsMenu = (accountId?: string) => {
  const buttons: any[][] = [];

  if (accountId) {
    const acc = waAccounts.get(accountId);
    if (acc) {
      const statusEmoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
      const enabledEmoji = acc.enabled ? '✅' : '❌';

      buttons.push([
        Markup.button.callback(
          `${enabledEmoji} Рассылка: ${acc.enabled ? 'ВКЛ' : 'ВЫКЛ'}`,
          `toggle_${accountId}`
        )
      ]);

      if (acc.status === 'connected') {
        buttons.push([Markup.button.callback('📝 Изменить текст', `edit_msg_${accountId}`)]);
        buttons.push([Markup.button.callback('🔍 Просмотр чатов', `view_chats_${accountId}`)]);
      }
      buttons.push([Markup.button.callback('❌ Отвязать номер', `unbind_${accountId}`)]);
    }
    buttons.push([Markup.button.callback('◀️ Назад к списку', 'accounts')]);
  } else {
    if (waAccounts.size > 0) {
      waAccounts.forEach((acc, id) => {
        const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
        const enabledEmoji = acc.enabled ? '✅' : '❌';
        buttons.push([
          Markup.button.callback(
            `${emoji} ${enabledEmoji} ${acc.name || acc.phone || id}`,
            `acc_${id}`
          )
        ]);
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

  let hasEnabled = false;
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected' && acc.enabled) {
      hasEnabled = true;
      buttons.push([
        Markup.button.callback(`📱 ${acc.name || acc.phone}`, `broadcast_acc_${id}`)
      ]);
    }
  });

  if (!hasEnabled) {
    buttons.push([Markup.button.callback('❌ Нет включенных аккаунтов', 'main')]);
  }

  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);
  return Markup.inlineKeyboard(buttons);
};

// Меню настроек
const settingsMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('🔄 Перезапустить все сессии', 'restart_all')],
  [Markup.button.callback('📊 Статистика', 'stats')],
  [Markup.button.callback('◀️ Назад', 'main')],
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

// === УПРАВЛЕНИЕ АККАУНТАМИ ===

bot.action('accounts', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  let text = '📱 *Мои аккаунты*\n\n';
  text += `Всего: ${waAccounts.size}\n`;
  text += `Подключено: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}\n`;
  text += `Включено для рассылки: ${[...waAccounts.values()].filter(a => a.enabled).length}\n\n`;

  if (waAccounts.size === 0) {
    text += 'Нет добавленных аккаунтов\n';
  } else {
    waAccounts.forEach((acc, id) => {
      const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
      const enabledEmoji = acc.enabled ? '✅' : '❌';
      text += `${emoji}${enabledEmoji} *${acc.name || acc.phone || id}*\n`;
      if (acc.phone) text += `   📞 ${acc.phone}\n`;
      if (acc.sentToday > 0) text += `   📤 Отправлено сегодня: ${acc.sentToday}\n`;
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

  const statusText = acc.status === 'connected' ? 'Подключен' : acc.status === 'connecting' ? 'Подключается' : 'Отключен';
  const enabledText = acc.enabled ? 'Включен' : 'Выключен';

  let text = `📱 *${acc.name || acc.phone || accountId}*\n\n`;
  text += `📊 Статус: ${statusText}\n`;
  text += `🔐 Рассылка: ${enabledText}\n`;
  if (acc.phone) text += `📞 Телефон: ${acc.phone}\n`;
  text += `\n📈 За сессию:\n`;
  text += `   ✅ Отправлено: ${acc.sentToday}\n`;
  text += `   ❌ Ошибок: ${acc.failedToday}\n`;
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

// Включить/выключить аккаунт для рассылки
bot.action(/^toggle_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  const accountId = ctx.match![1];
  const acc = waAccounts.get(accountId);

  if (acc) {
    acc.enabled = !acc.enabled;
    const status = acc.enabled ? 'включен' : 'выключен';
    await ctx.reply(`✅ Рассылка для этого номера ${status}`).catch(() => {});
  }

  // Обновляем меню
  await ctx.editMessageText('📱 *Управление аккаунтом*\n\nНажмите для обновления:', {
    parse_mode: 'Markdown',
    ...accountsMenu(accountId)
  }).catch(() => {});
});

// Добавить аккаунт
bot.action('add_account', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});
  userStates.set(ctx.from!.id, { action: 'waiting_phone' });

  await ctx.editMessageText(
    '📱 *Добавление номера*\n\n' +
    'Введите номер телефона в формате:\n' +
    '+79991234567\n\n' +
    'После ввода номера бот отправит QR-код.',
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

// Просмотр чатов аккаунта
bot.action(/^view_chats_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery('Загрузка чатов...').catch(() => {});
  const accountId = ctx.match![1];
  const acc = waAccounts.get(accountId);

  if (!acc || acc.status !== 'connected') {
    await ctx.editMessageText('❌ Аккаунт не подключен', {
      ...accountsMenu(accountId)
    }).catch(() => {});
    return;
  }

  try {
    const chats = await acc.client.getChats();
    const archived = chats.filter((c: any) => c.archived);
    const groups = archived.filter((c: any) => c.isGroup);
    const individuals = archived.filter((c: any) => !c.isGroup);

    let text = `📋 *Архивные чаты*\n\n`;
    text += `📊 Всего: ${archived.length}\n`;
    text += `👥 Групп: ${groups.length}\n`;
    text += `👤 Диалогов: ${individuals.length}\n\n`;

    // Показываем первые 10
    const showChats = archived.slice(0, 10);
    showChats.forEach((chat: any) => {
      const emoji = chat.isGroup ? '👥' : '👤';
      text += `${emoji} ${chat.name || chat.id._serialized}\n`;
    });

    if (archived.length > 10) {
      text += `\n... и ещё ${archived.length - 10}`;
    }

    await ctx.editMessageText(text, {
      parse_mode: 'Markdown',
      ...accountsMenu(accountId)
    }).catch(() => {});

  } catch (error) {
    console.error('Error getting chats:', error);
    await ctx.reply('❌ Ошибка при получении чатов').catch(() => {});
  }
});

// === РАССЫЛКА ===

bot.action('broadcast_menu', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  const enabledAccounts = [...waAccounts.values()].filter(a => a.status === 'connected' && a.enabled);

  let text = '📨 *Рассылка*\n\n';

  if (enabledAccounts.length === 0) {
    text += '❌ Нет включенных аккаунтов для рассылки\n\n';
    text += 'Включите аккаунты в разделе "Аккаунты"';
  } else {
    text += `Готовы к рассылке: ${enabledAccounts.length}\n\n`;

    enabledAccounts.forEach(acc => {
      const msg = acc.customMessage ? `"${acc.customMessage.substring(0, 20)}..."` : '❌ текст не задан';
      text += `📱 ${acc.name || acc.phone}\n`;
      text += `   ${msg}\n\n`;
    });

    text += 'Выберите аккаунт для рассылки:';
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

  if (!acc.enabled) {
    await ctx.editMessageText('❌ Этот аккаунт выключен для рассылки\n\nВключите его в разделе "Аккаунты"', {
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
    `⚠️ Отправить во все архивные чаты?\n` +
    `⏱ Задержка: 10 минут между сообщениями\n` +
    `🔄 После каждого сообщения - ожидание`,
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

  if (!acc || acc.status !== 'connected' || !acc.customMessage || !acc.enabled) {
    await ctx.reply('❌ Аккаунт не готов к рассылке').catch(() => {});
    return;
  }

  await startBroadcast(ctx, accountId, acc.customMessage);
});

// === НАСТРОЙКИ ===

bot.action('settings', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  const text = `⚙️ *Настройки*\n\n` +
    `📊 Всего аккаунтов: ${waAccounts.size}\n` +
    `🔄 Подключено: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}\n` +
    `📨 Включено для рассылки: ${[...waAccounts.values()].filter(a => a.enabled).length}\n\n` +
    `⏱ Задержка между сообщениями: 10 минут`;

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...settingsMenu()
  }).catch(() => {});
});

bot.action('stats', async (ctx) => {
  await ctx.answerCbQuery().catch(() => {});

  let text = '📊 *Статистика*\n\n';

  let totalSent = 0;
  let totalFailed = 0;

  waAccounts.forEach((acc, id) => {
    totalSent += acc.sentToday;
    totalFailed += acc.failedToday;
    text += `📱 ${acc.name || acc.phone || id}:\n`;
    text += `   ✅ Отправлено: ${acc.sentToday}\n`;
    text += `   ❌ Ошибок: ${acc.failedToday}\n\n`;
  });

  text += `📈 *Всего:*\n`;
  text += `   ✅ Отправлено: ${totalSent}\n`;
  text += `   ❌ Ошибок: ${totalFailed}`;

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...settingsMenu()
  }).catch(() => {});
});

// === ОБРАБОТЧИКИ СООБЩЕНИЙ ===

bot.on('text', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;
  const state = userStates.get(ctx.from!.id);
  if (!state) return;

  try {
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

async function addNewAccount(ctx: any, phone: string) {
  const accountId = `wa_${phone.replace(/\+/g, '')}`;

  if (waAccounts.has(accountId)) {
    const existing = waAccounts.get(accountId);
    if (existing?.status === 'connected') {
      await ctx.reply('✅ Этот номер уже подключен!').catch(() => {});
      return;
    }
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

  waAccounts.set(accountId, {
    client,
    status: 'connecting',
    phone,
    name: phone,
    customMessage: '',
    enabled: true, // По умолчанию включен
    sentToday: 0,
    failedToday: 0,
  });

  let qrSent = false;

  client.on('qr', async (qr) => {
    console.log('QR received for', accountId);
    const acc = waAccounts.get(accountId);
    if (acc?.status === 'connected') return;

    if (!qrSent) {
      qrSent = true;
      try {
        const qrDataUrl = await QRCode.toDataURL(qr);
        const base64Data = qrDataUrl.replace(/^data:image\/png;base64,/, '');
        const buffer = Buffer.from(base64Data, 'base64');
        await ctx.replyWithPhoto({ source: buffer }, {
          caption: '📱 *Отсканируйте QR-код*\n\nWhatsApp → Настройки → Связанные устройства',
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
      `✅ *Подключено!*\n\n📞 Телефон: ${phone}\n\n` +
      `Теперь настройте текст для рассылки в разделе "Аккаунты"`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
  });

  client.on('auth_failure', async (msg) => {
    console.log('Auth failure for', accountId, msg);
    const acc = waAccounts.get(accountId);
    if (acc) acc.status = 'disconnected';
    await ctx.reply(`❌ Ошибка авторизации`).catch(() => {});
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

// Рассылка с улучшенной обработкой
async function startBroadcast(ctx: any, accountId: string, text: string) {
  const acc = waAccounts.get(accountId);

  if (!acc || acc.status !== 'connected') {
    return ctx.reply('❌ Аккаунт не подключен').catch(() => {});
  }

  if (!acc.enabled) {
    return ctx.reply('❌ Этот аккаунт выключен для рассылки').catch(() => {});
  }

  await ctx.reply('🔍 Поиск архивных чатов...').catch(() => {});

  try {
    const chats = await acc.client.getChats();
    const archivedChats = chats.filter((chat: any) => chat.archived);

    console.log(`Found ${archivedChats.length} archived chats for ${accountId}`);

    if (archivedChats.length === 0) {
      return ctx.reply('❌ Архивные чаты не найдены').catch(() => {});
    }

    const groups = archivedChats.filter((c: any) => c.isGroup);
    const individuals = archivedChats.filter((c: any) => !c.isGroup);

    const broadcastId = Date.now().toString();
    activeBroadcasts.set(broadcastId, {
      stop: false,
      sent: 0,
      failed: 0,
      total: archivedChats.length,
      accountId
    });

    const estimatedMin = archivedChats.length * 10;
    const hours = Math.floor(estimatedMin / 60);
    const mins = estimatedMin % 60;

    await ctx.reply(
      `📦 *Рассылка*\n\n` +
      `📱 Аккаунт: ${acc.name || acc.phone}\n` +
      `📊 Найдено: ${archivedChats.length} чатов\n` +
      `   👥 Групп: ${groups.length}\n` +
      `   👤 Диалогов: ${individuals.length}\n` +
      `⏱ Время: ~${hours}ч ${mins}мин\n\n` +
      `⏳ Начинаем рассылку...`,
      { parse_mode: 'Markdown' }
    );

    let sent = 0;
    let failed = 0;

    for (let i = 0; i < archivedChats.length; i++) {
      const broadcast = activeBroadcasts.get(broadcastId);
      if (broadcast?.stop) {
        await ctx.reply(`⏹ Остановлено\n✅ ${sent} | ❌ ${failed}`).catch(() => {});
        activeBroadcasts.delete(broadcastId);
        return;
      }

      const chat = archivedChats[i];

      try {
        console.log(`Sending to ${chat.name || chat.id._serialized} (${i+1}/${archivedChats.length})`);

        await acc.client.sendMessage(chat.id._serialized, text);
        sent++;
        acc.sentToday++;

        // Обновляем прогресс
        if (i % 3 === 0 || i === archivedChats.length - 1) {
          const remaining = archivedChats.length - (i + 1);
          const remainingTime = remaining * 10;
          await ctx.reply(
            `📊 ${i + 1}/${archivedChats.length}\n` +
            `✅ ${sent} | ❌ ${failed}\n` +
            `⏳ Осталось ~${Math.ceil(remainingTime / 60)} мин`,
          ).catch(() => {});
        }

        // Задержка между сообщениями
        if (i < archivedChats.length - 1) {
          console.log(`Waiting ${BROADCAST_DELAY / 60000} minutes before next message...`);
          await new Promise(r => setTimeout(r, BROADCAST_DELAY));
        }
      } catch (error) {
        console.error('Send error:', error);
        failed++;
        acc.failedToday++;

        // При ошибке - подождём немного дольше
        await new Promise(r => setTimeout(r, 60000));
      }
    }

    await ctx.reply(
      `🏁 *Рассылка завершена*\n\n` +
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
  console.log('Accounts loaded:', waAccounts.size);

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
