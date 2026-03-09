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

// === КОНФИГУРАЦИЯ АНТИ-БАНА ===
const MIN_DELAY = 8 * 60 * 1000;  // Минимум 8 минут
const MAX_DELAY = 15 * 60 * 1000;  // Максимум 15 минут
const PROGRESSIVE_START_DELAY = 12 * 60 * 1000; // Начальная задержка (медленно)
const PROGRESSIVE_END_DELAY = 8 * 60 * 1000;    // Конечная задержка (быстрее)

// Лимиты защиты
const MAX_MESSAGES_PER_HOUR = 25;   // Максимум в час
const MAX_MESSAGES_PER_DAY = 150;   // Максимум в день
const NIGHT_PAUSE_START = 0;        // Ночная пауза с 00:00
const NIGHT_PAUSE_END = 8;          // до 08:00

// Конфигурация авто-присоединения
const JOIN_DELAY_MIN = 30 * 60 * 1000;  // Минимум 30 минут между присоединениями
const JOIN_DELAY_MAX = 60 * 60 * 1000;  // Максимум 60 минут
const MAX_JOINS_PER_DAY = 30;           // Максимум присоединений в день

// Хранилище для авто-присоединения
const activeJoins = new Map<string, {
  stop: boolean;
  joined: number;
  failed: number;
  total: number;
  accountId: string;
}>();

// Список ссылок для присоединения (в памяти)
const pendingGroupLinks = new Map<string, string[]>();

// === ФУНКЦИИ АНТИ-БАНА ===

// Рандомная задержка в диапазоне
function getRandomDelay(): number {
  return Math.floor(Math.random() * (MAX_DELAY - MIN_DELAY + 1)) + MIN_DELAY;
}

// Прогрессивная задержка (начинаем медленно, ускоряемся)
function getProgressiveDelay(sentCount: number, totalCount: number): number {
  const progress = sentCount / totalCount; // 0.0 - 1.0

  if (progress < 0.2) {
    // Первые 20% - медленно
    return PROGRESSIVE_START_DELAY + Math.floor(Math.random() * 120000);
  } else if (progress < 0.5) {
    // 20-50% - средне
    return (PROGRESSIVE_START_DELAY + PROGRESSIVE_END_DELAY) / 2 + Math.floor(Math.random() * 90000);
  } else {
    // 50-100% - быстрее
    return PROGRESSIVE_END_DELAY + Math.floor(Math.random() * 60000);
  }
}

// Имитация человека - симуляция набора текста
async function simulateHumanTyping(client: Client, chatId: string, duration: number = 2000) {
  try {
    await client.sendPresenceAvailable();
    await client.startTyping(chatId);
    // Рандомное время набора 1-3 секунды
    const typingTime = duration + Math.floor(Math.random() * 2000);
    await new Promise(r => setTimeout(r, typingTime));
    await client.stopTyping(chatId);
  } catch (e) {
    // Игнорируем ошибки typing
  }
}

// Замена русских букв на английские (анти-бан вариация текста)
function varyRussianText(text: string): string {
  const replacements: { [key: string]: string } = {
    'а': 'a', 'А': 'A',
    'е': 'e', 'Е': 'E',
    'о': 'o', 'О': 'O',
    'р': 'p', 'Р': 'P',
    'с': 'c', 'С': 'C',
    'у': 'y', 'У': 'Y',
    'х': 'x', 'Х': 'X',
    'і': 'i', 'І': 'I',
    'ј': 'j', 'Ј': 'J',
    'ѕ': 's', 'Ѕ': 'S',
    'ԁ': 'd', 'ԁ': 'D',
    'ԛ': 'q', 'Ԛ': 'Q',
    'ɡ': 'g', 'Ԍ': 'G',
    'һ': 'h', 'Һ': 'H',
    'ԛ': 'q',
  };

  let result = text;
  let changesCount = 0;
  const maxChanges = Math.ceil(text.length * 0.3); // Максимум 30% символов

  // Собираем все позиции русских букв
  const russianPositions: number[] = [];
  for (let i = 0; i < text.length; i++) {
    if (replacements[text[i]]) {
      russianPositions.push(i);
    }
  }

  // Рандомно меняем некоторые буквы
  const positionsToChange = russianPositions
    .sort(() => Math.random() - 0.5)
    .slice(0, Math.min(maxChanges, russianPositions.length));

  for (const pos of positionsToChange) {
    const char = text[pos];
    if (replacements[char] && Math.random() > 0.5) {
      result = result.substring(0, pos) + replacements[char] + result.substring(pos + 1);
      changesCount++;
    }
  }

  // Добавляем случайные пробелы/символы в конец (имитация человека)
  if (Math.random() > 0.5) {
    const extraChars = [' ', '  ', '.', '..', '...'];
    result += extraChars[Math.floor(Math.random() * extraChars.length)];
  }

  return result;
}

// Проверка ночной паузы
function isNightPause(): boolean {
  const hour = new Date().getHours();
  return hour >= NIGHT_PAUSE_START && hour < NIGHT_PAUSE_END;
}

// Получить время до конца ночной паузы (в миллисекундах)
function getTimeUntilMorning(): number {
  const now = new Date();
  const tomorrow = new Date(now);
  tomorrow.setDate(tomorrow.getDate() + 1);
  tomorrow.setHours(NIGHT_PAUSE_END, 0, 0, 0);
  return tomorrow.getTime() - now.getTime();
}

// Рандомная задержка для присоединения к чатам
function getRandomJoinDelay(): number {
  return Math.floor(Math.random() * (JOIN_DELAY_MAX - JOIN_DELAY_MIN + 1)) + JOIN_DELAY_MIN;
}

// Извлечь код приглашения из ссылки
function extractInviteCode(link: string): string | null {
  // Формат: https://chat.whatsapp.com/LwhLlYTokDdIPKqsroAeBo
  // или просто код: LwhLlYTokDdIPKqsroAeBo

  const patterns = [
    /chat\.whatsapp\.com\/([a-zA-Z0-9]{20,})/i,
    /invite\.whatsapp\.com\/([a-zA-Z0-9]{20,})/i,
  ];

  for (const pattern of patterns) {
    const match = link.match(pattern);
    if (match) {
      return match[1];
    }
  }

  // Если это просто код без ссылки (20+ символов)
  if (/^[a-zA-Z0-9]{20,}$/.test(link.trim())) {
    return link.trim();
  }

  return null;
}

// Попытка присоединиться к группе по коду приглашения
async function joinGroupByInvite(client: Client, inviteCode: string): Promise<{ success: boolean; error?: string; needsApproval?: boolean }> {
  try {
    console.log(`  Joining group with invite code: ${inviteCode}`);

    // Метод acceptInvite принимает код приглашения
    const result = await client.acceptInvite(inviteCode);

    console.log(`  ✓ Successfully joined group`);
    return { success: true };

  } catch (error: any) {
    const errorMessage = error.message || String(error);
    console.error(`  ✗ Join failed:`, errorMessage);

    // Разные типы ошибок
    if (errorMessage.includes('invalid') || errorMessage.includes('expired')) {
      return { success: false, error: 'Ссылка недействительна или истекла' };
    }

    if (errorMessage.includes('approval') || errorMessage.includes('join') || errorMessage.includes('require')) {
      return { success: false, error: 'Требуется одобрение администратора', needsApproval: true };
    }

    if (errorMessage.includes('not found') || errorMessage.includes('404')) {
      return { success: false, error: 'Группа не найдена' };
    }

    return { success: false, error: errorMessage };
  }
}

// Главное меню
const mainMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('📱 Аккаунты', 'accounts')],
  [Markup.button.callback('📨 Рассылка', 'broadcast_menu')],
  [Markup.button.callback('🔗 Присоединение к чатам', 'join_menu')],
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

  if (!acc || acc.status !== 