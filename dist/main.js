"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const telegraf_1 = require("telegraf");
const whatsapp_web_js_1 = require("whatsapp-web.js");
const wwebjs_mongo_1 = require("wwebjs-mongo");
const mongoose_1 = __importDefault(require("mongoose"));
const QRCode = __importStar(require("qrcode"));
const fs = __importStar(require("fs"));
const express_1 = __importDefault(require("express"));
require("dotenv/config");
// === ПОДКЛЮЧЕНИЕ К MONGODB ===
const mongoUri = process.env.MONGO_URI;
let mongoConnected = false;
if (mongoUri) {
    mongoose_1.default.connect(mongoUri)
        .then(() => {
        mongoConnected = true;
        console.log('✅ MongoDB подключена');
    })
        .catch((err) => {
        console.error('❌ Ошибка подключения к MongoDB:', err);
        mongoConnected = false;
    });
}
// === EXPRESS СЕРВЕР ДЛЯ HEALTH CHECK ===
const app = (0, express_1.default)();
const PORT = process.env.PORT || 3000;
app.get('/health', (req, res) => {
    const connectedAccounts = [...waAccounts.values()].filter(a => a.status === 'connected').length;
    res.json({
        status: 'ok',
        uptime: process.uptime(),
        accounts: {
            total: waAccounts.size,
            connected: connectedAccounts
        },
        timestamp: new Date().toISOString()
    });
});
app.get('/', (req, res) => {
    res.send('WhatsApp Bot Running');
});
// Запуск Express сервера
app.listen(PORT, () => {
    console.log(`🌐 Health check server running on port ${PORT}`);
});
const bot = new telegraf_1.Telegraf(process.env.TELEGRAM_BOT_TOKEN);
const ADMIN_IDS = (process.env.ADMIN_TELEGRAM_IDS || '')
    .split(',')
    .map(id => parseInt(id.trim()))
    .filter(id => !isNaN(id));
// Хранилище аккаунтов с расширенными настройками
const waAccounts = new Map();
const userStates = new Map();
const activeBroadcasts = new Map();
const isAdmin = (userId) => ADMIN_IDS.includes(userId);
// === КОНФИГУРАЦИЯ АНТИ-БАНА ===
const MIN_DELAY = 8 * 60 * 1000; // Минимум 8 минут
const MAX_DELAY = 15 * 60 * 1000; // Максимум 15 минут
const PROGRESSIVE_START_DELAY = 12 * 60 * 1000; // Начальная задержка (медленно)
const PROGRESSIVE_END_DELAY = 8 * 60 * 1000; // Конечная задержка (быстрее)
// Лимиты защиты
const MAX_MESSAGES_PER_HOUR = 25; // Максимум в час
const MAX_MESSAGES_PER_DAY = 150; // Максимум в день
const NIGHT_PAUSE_START = 24; // Ночная пауза ОТКЛЮЧЕНА
const NIGHT_PAUSE_END = 24; // (24 = никогда не срабатывает)
// Конфигурация авто-присоединения
const JOIN_DELAY_MIN = 8 * 60 * 1000; // Минимум 8 минут между присоединениями
const JOIN_DELAY_MAX = 12 * 60 * 1000; // Максимум 12 минут
const MAX_JOINS_BEFORE_BREAK = 30; // После 30 чатов - перерыв
const JOIN_BREAK_DURATION = 4 * 60 * 60 * 1000; // 4 часа перерыв
const MAX_JOINS_PER_DAY = MAX_JOINS_BEFORE_BREAK * 3; // ~90 в день (с перерывами)
// Хранилище для авто-присоединения
const activeJoins = new Map();
// Список ссылок для присоединения (в памяти)
const pendingGroupLinks = new Map();
// === ФУНКЦИИ АНТИ-БАНА ===
// Рандомная задержка в диапазоне
function getRandomDelay() {
    return Math.floor(Math.random() * (MAX_DELAY - MIN_DELAY + 1)) + MIN_DELAY;
}
// Прогрессивная задержка (начинаем медленно, ускоряемся)
function getProgressiveDelay(sentCount, totalCount) {
    const progress = sentCount / totalCount; // 0.0 - 1.0
    if (progress < 0.2) {
        // Первые 20% - медленно
        return PROGRESSIVE_START_DELAY + Math.floor(Math.random() * 120000);
    }
    else if (progress < 0.5) {
        // 20-50% - средне
        return (PROGRESSIVE_START_DELAY + PROGRESSIVE_END_DELAY) / 2 + Math.floor(Math.random() * 90000);
    }
    else {
        // 50-100% - быстрее
        return PROGRESSIVE_END_DELAY + Math.floor(Math.random() * 60000);
    }
}
// Имитация человека - симуляция набора текста
async function simulateHumanTyping(client, chatId, duration = 2000) {
    try {
        await client.sendPresenceAvailable();
        // Рандомное время набора 1-3 секунды
        const typingTime = duration + Math.floor(Math.random() * 2000);
        await new Promise(r => setTimeout(r, typingTime));
    }
    catch (e) {
        // Игнорируем ошибки typing
    }
}
// Замена русских букв на английские (анти-бан вариация текста)
function varyRussianText(text) {
    const replacements = {
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
        'ԁ': 'd',
        'ԛ': 'q', 'Ԛ': 'Q',
        'ɡ': 'g', 'Ԍ': 'G',
        'һ': 'h', 'Һ': 'H',
    };
    let result = text;
    let changesCount = 0;
    const maxChanges = Math.ceil(text.length * 0.3); // Максимум 30% символов
    // Собираем все позиции русских букв
    const russianPositions = [];
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
function isNightPause() {
    const hour = new Date().getHours();
    return hour >= NIGHT_PAUSE_START && hour < NIGHT_PAUSE_END;
}
// Получить время до конца ночной паузы (в миллисекундах)
function getTimeUntilMorning() {
    const now = new Date();
    const tomorrow = new Date(now);
    tomorrow.setDate(tomorrow.getDate() + 1);
    tomorrow.setHours(NIGHT_PAUSE_END, 0, 0, 0);
    return tomorrow.getTime() - now.getTime();
}
// Рандомная задержка для присоединения к чатам
function getRandomJoinDelay() {
    return Math.floor(Math.random() * (JOIN_DELAY_MAX - JOIN_DELAY_MIN + 1)) + JOIN_DELAY_MIN;
}
// Извлечь код приглашения из ссылки
function extractInviteCode(link) {
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
async function joinGroupByInvite(client, inviteCode) {
    try {
        console.log(`  Checking/joining group with invite code: ${inviteCode}`);
        // Сначала проверяем - получаем все чаты и ищем эту группу
        const allChats = await client.getChats();
        // Пробуем найти группу по коду приглашения
        const existingChat = allChats.find((chat) => chat.isGroup && chat.link && chat.link.includes(inviteCode));
        if (existingChat) {
            console.log(`  ⚠️ Already a member of this group: ${existingChat.name || inviteCode}`);
            return { success: true, alreadyJoined: true };
        }
        // Если не состоит - пробуем присоединиться
        await client.acceptInvite(inviteCode);
        console.log(`  ✓ Successfully joined group`);
        return { success: true };
    }
    catch (error) {
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
        // Проверяем ошибку "already a member"
        if (errorMessage.includes('already') || errorMessage.includes('member')) {
            return { success: true, alreadyJoined: true };
        }
        return { success: false, error: errorMessage };
    }
}
// Главное меню
const mainMenu = () => telegraf_1.Markup.inlineKeyboard([
    [telegraf_1.Markup.button.callback('📱 Аккаунты', 'accounts')],
    [telegraf_1.Markup.button.callback('📨 Рассылка', 'broadcast_menu')],
    [telegraf_1.Markup.button.callback('🔗 Присоединение к чатам', 'join_menu')],
    [telegraf_1.Markup.button.callback('⚙️ Настройки', 'settings')],
]);
// Меню аккаунтов
const accountsMenu = (accountId) => {
    const buttons = [];
    if (accountId) {
        const acc = waAccounts.get(accountId);
        if (acc) {
            const statusEmoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
            const enabledEmoji = acc.enabled ? '✅' : '❌';
            buttons.push([
                telegraf_1.Markup.button.callback(`${enabledEmoji} Рассылка: ${acc.enabled ? 'ВКЛ' : 'ВЫКЛ'}`, `toggle_${accountId}`)
            ]);
            if (acc.status === 'connected') {
                buttons.push([telegraf_1.Markup.button.callback('📝 Изменить текст', `edit_msg_${accountId}`)]);
                buttons.push([telegraf_1.Markup.button.callback('🔍 Просмотр чатов', `view_chats_${accountId}`)]);
            }
            buttons.push([telegraf_1.Markup.button.callback('❌ Отвязать номер', `unbind_${accountId}`)]);
        }
        buttons.push([telegraf_1.Markup.button.callback('◀️ Назад к списку', 'accounts')]);
    }
    else {
        if (waAccounts.size > 0) {
            waAccounts.forEach((acc, id) => {
                const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
                const enabledEmoji = acc.enabled ? '✅' : '❌';
                buttons.push([
                    telegraf_1.Markup.button.callback(`${emoji} ${enabledEmoji} ${acc.name || acc.phone || id}`, `acc_${id}`)
                ]);
            });
        }
        buttons.push([telegraf_1.Markup.button.callback('➕ Добавить номер', 'add_account')]);
        buttons.push([telegraf_1.Markup.button.callback('◀️ Назад', 'main')]);
    }
    return telegraf_1.Markup.inlineKeyboard(buttons);
};
// Меню выбора аккаунта для рассылки
const broadcastSelectMenu = () => {
    const buttons = [];
    let hasEnabled = false;
    waAccounts.forEach((acc, id) => {
        if (acc.status === 'connected' && acc.enabled) {
            hasEnabled = true;
            buttons.push([
                telegraf_1.Markup.button.callback(`📱 ${acc.name || acc.phone}`, `broadcast_acc_${id}`)
            ]);
        }
    });
    if (!hasEnabled) {
        buttons.push([telegraf_1.Markup.button.callback('❌ Нет включенных аккаунтов', 'main')]);
    }
    buttons.push([telegraf_1.Markup.button.callback('◀️ Назад', 'main')]);
    return telegraf_1.Markup.inlineKeyboard(buttons);
};
// Меню настроек
const settingsMenu = () => telegraf_1.Markup.inlineKeyboard([
    [telegraf_1.Markup.button.callback('🔄 Перезапустить все сессии', 'restart_all')],
    [telegraf_1.Markup.button.callback('📊 Статистика', 'stats')],
    [telegraf_1.Markup.button.callback('◀️ Назад', 'main')],
]);
// Admin check middleware - FIRST to block non-admins
bot.use(async (ctx, next) => {
    if (ctx.from && !isAdmin(ctx.from.id)) {
        return ctx.reply('⛔ Нет доступа').catch(() => { });
    }
    await next();
});
// Middleware for debugging
bot.use(async (ctx, next) => {
    // Log all callback queries
    if (ctx.callbackQuery && 'data' in ctx.callbackQuery) {
        console.log('[DEBUG] CallbackQuery:', JSON.stringify({
            data: ctx.callbackQuery.data,
            from: ctx.from?.id,
            chat: ctx.chat?.id
        }));
    }
    await next().catch(err => {
        console.error('[DEBUG] Error in handler:', err);
        ctx.reply('❌ Произошла ошибка: ' + err.message).catch(() => { });
    });
});
// Команда /start
bot.start(async (ctx) => {
    await ctx.reply('🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
        parse_mode: 'Markdown',
        ...mainMenu()
    });
});
// Команда /status - показать статус с эмодзи
bot.command('status', async (ctx) => {
    const connected = [...waAccounts.values()].filter(a => a.status === 'connected').length;
    const enabled = [...waAccounts.values()].filter(a => a.enabled).length;
    const totalSent = [...waAccounts.values()].reduce((sum, a) => sum + a.sentToday, 0);
    // Определяем статус
    let statusEmoji = '🟢';
    let statusText = 'Работает';
    if (connected === 0) {
        statusEmoji = '🔴';
        statusText = 'Нет подключений';
    }
    else if (isBroadcasting) {
        statusEmoji = '🟡';
        statusText = 'Рассылка активна';
    }
    const uptime = Math.floor(process.uptime() / 60); // минуты
    await ctx.reply(`${statusEmoji} *Статус бота*\n\n` +
        `📊 Статус: ${statusText}\n` +
        `🖥 Аккаунтов: ${connected}/${waAccounts.size} подключено\n` +
        `✅ Включено для рассылки: ${enabled}\n` +
        `📤 Отправлено сегодня: ${totalSent}\n` +
        `⏱ Uptime: ${uptime} минут\n\n` +
        `🌐 Health: /health (в браузере)`, { parse_mode: 'Markdown' });
});
// Команда /stop для остановки рассылки
bot.command('stop', async (ctx) => {
    const args = ctx.message.text.split(' ');
    if (args.length < 2) {
        await ctx.reply('❌ Использование: /stop <broadcast_id>\nПолучить ID можно из уведомлений о рассылке').catch(() => { });
        return;
    }
    const broadcastId = args[1];
    const broadcast = activeBroadcasts.get(broadcastId);
    if (broadcast) {
        broadcast.stop = true;
        await ctx.reply(`✅ Команда остановки отправлена для рассылки ${broadcastId}`).catch(() => { });
    }
    else {
        await ctx.reply('❌ Рассылка не найдена').catch(() => { });
    }
});
// Главное меню
bot.action('main', async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    userStates.delete(ctx.from.id);
    await ctx.editMessageText('🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
        parse_mode: 'Markdown',
        ...mainMenu()
    }).catch(() => { });
});
// === УПРАВЛЕНИЕ АККАУНТАМИ ===
bot.action('accounts', async (ctx) => {
    console.log('[DEBUG] accounts callback triggered');
    console.log('[DEBUG] Total accounts:', waAccounts.size);
    await ctx.answerCbQuery().catch(() => { });
    let text = '📱 *Мои аккаунты*\n\n';
    text += `Всего: ${waAccounts.size}\n`;
    text += `Подключено: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}\n`;
    text += `Включено для рассылки: ${[...waAccounts.values()].filter(a => a.enabled).length}\n\n`;
    if (waAccounts.size === 0) {
        text += 'Нет добавленных аккаунтов\n';
    }
    else {
        waAccounts.forEach((acc, id) => {
            const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
            const enabledEmoji = acc.enabled ? '✅' : '❌';
            text += `${emoji}${enabledEmoji} *${acc.name || acc.phone || id}*\n`;
            if (acc.phone)
                text += `   📞 ${acc.phone}\n`;
            if (acc.sentToday > 0)
                text += `   📤 Отправлено сегодня: ${acc.sentToday}\n`;
            text += '\n';
        });
    }
    await ctx.editMessageText(text, {
        parse_mode: 'Markdown',
        ...accountsMenu()
    }).catch(() => { });
});
// Просмотр аккаунта
bot.action(/^acc_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    const acc = waAccounts.get(accountId);
    if (!acc) {
        await ctx.editMessageText('❌ Аккаунт не найден', {
            ...accountsMenu()
        }).catch(() => { });
        return;
    }
    const statusText = acc.status === 'connected' ? 'Подключен' : acc.status === 'connecting' ? 'Подключается' : 'Отключен';
    const enabledText = acc.enabled ? 'Включен' : 'Выключен';
    let text = `📱 *${acc.name || acc.phone || accountId}*\n\n`;
    text += `📊 Статус: ${statusText}\n`;
    text += `🔐 Рассылка: ${enabledText}\n`;
    if (acc.phone)
        text += `📞 Телефон: ${acc.phone}\n`;
    text += `\n📈 За сессию:\n`;
    text += `   ✅ Отправлено: ${acc.sentToday}\n`;
    text += `   ❌ Ошибок: ${acc.failedToday}\n`;
    if (acc.customMessage) {
        text += `\n📝 *Текст рассылки:*\n${acc.customMessage}\n`;
    }
    else {
        text += `\n📝 Текст не задан\n`;
    }
    await ctx.editMessageText(text, {
        parse_mode: 'Markdown',
        ...accountsMenu(accountId)
    }).catch(() => { });
});
// Включить/выключить аккаунт для рассылки
bot.action(/^toggle_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    const acc = waAccounts.get(accountId);
    if (acc) {
        acc.enabled = !acc.enabled;
        const status = acc.enabled ? 'включен' : 'выключен';
        await ctx.reply(`✅ Рассылка для этого номера ${status}`).catch(() => { });
    }
    // Обновляем меню
    await ctx.editMessageText('📱 *Управление аккаунтом*\n\nНажмите для обновления:', {
        parse_mode: 'Markdown',
        ...accountsMenu(accountId)
    }).catch(() => { });
});
// Добавить аккаунт - выбор способа авторизации
bot.action('add_account', async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    await ctx.editMessageText('📱 *Добавление аккаунта*\n\n' +
        'Выберите способ авторизации:\n\n' +
        '📷 *QR-код* - сканировать код в WhatsApp\n' +
        '   ✅ Работает стабильно\n\n' +
        '🔢 *По номеру* - ввод телефона\n' +
        '   ⚠️ Может не работать', {
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [
                [telegraf_1.Markup.button.callback('📷 QR-код', 'add_qr')],
                [telegraf_1.Markup.button.callback('🔢 По номеру', 'add_phone')],
                [telegraf_1.Markup.button.callback('❌ Отмена', 'accounts')]
            ]
        }
    }).catch(() => { });
});
// Добавить через QR-код
bot.action('add_qr', async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    userStates.set(ctx.from.id, { action: 'waiting_phone_qr' });
    await ctx.editMessageText('📷 *Подключение через QR-код*\n\n' +
        'Введите номер телефона в формате:\n' +
        '+79991234567\n\n' +
        'После ввода бот отправит QR-код для сканирования.', {
        parse_mode: 'Markdown',
        ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', 'add_account')]])
    }).catch(() => { });
});
// Добавить через номер (pairing code)
bot.action('add_phone', async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    userStates.set(ctx.from.id, { action: 'waiting_phone_pairing' });
    await ctx.editMessageText('🔢 *Подключение по номеру*\n\n' +
        '📱 *Казахстан (по умолчанию)*\n' +
        'Введите номер в формате:\n' +
        '+7 777 1234567\n' +
        'или просто: 7771234567\n\n' +
        '⚠️ *Важно:*\n' +
        '- WhatsApp должен быть открыт на телефоне\n' +
        '- Иногда требуется подтверждение\n' +
        '- Может не работать если WhatsApp заблокировал', {
        parse_mode: 'Markdown',
        ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', 'add_account')]])
    }).catch(() => { });
});
// Отвязать аккаунт
bot.action(/^unbind_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    await ctx.editMessageText(`⚠️ *Отвязать номер?*\n\n` +
        `Аккаунт будет удален и отключен от WhatsApp.`, {
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [
                [telegraf_1.Markup.button.callback('✅ Да, отвязать', `confirm_unbind_${accountId}`)],
                [telegraf_1.Markup.button.callback('❌ Отмена', `acc_${accountId}`)]
            ]
        }
    }).catch(() => { });
});
bot.action(/^confirm_unbind_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    const acc = waAccounts.get(accountId);
    if (acc && acc.client) {
        try {
            await acc.client.logout();
            await acc.client.destroy();
        }
        catch (e) {
            console.error('Error destroying client:', e);
        }
    }
    const sessionsPath = process.env.WA_SESSIONS_PATH || './wa-sessions';
    const sessionDir = `${sessionsPath}/${accountId}`;
    if (fs.existsSync(sessionDir)) {
        fs.rmSync(sessionDir, { recursive: true, force: true });
    }
    waAccounts.delete(accountId);
    await ctx.reply('✅ Аккаунт отвязан').catch(() => { });
    await ctx.editMessageText('✅ Аккаунт удален', {
        ...accountsMenu()
    }).catch(() => { });
});
// Изменить текст рассылки
bot.action(/^edit_msg_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    userStates.set(ctx.from.id, { action: 'waiting_custom_message', accountId });
    const acc = waAccounts.get(accountId);
    const currentMsg = acc?.customMessage || '';
    await ctx.editMessageText(`📝 *Текст для аккаунта ${acc?.name || acc?.phone || accountId}*\n\n` +
        `Текущий текст: ${currentMsg || 'не задан'}\n\n` +
        `Введите новый текст для рассылки:`, {
        parse_mode: 'Markdown',
        ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', `acc_${accountId}`)]])
    }).catch(() => { });
});
// Просмотр чатов аккаунта
bot.action(/^view_chats_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery('Загрузка чатов...').catch(() => { });
    const accountId = ctx.match[1];
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await ctx.editMessageText('❌ Аккаунт не подключен', {
            ...accountsMenu(accountId)
        }).catch(() => { });
        return;
    }
    try {
        const chats = await acc.client.getChats();
        const archived = chats.filter((c) => c.archived);
        const groups = archived.filter((c) => c.isGroup);
        const individuals = archived.filter((c) => !c.isGroup);
        let text = `📋 *Архивные чаты*\n\n`;
        text += `📊 Всего: ${archived.length}\n`;
        text += `👥 Групп: ${groups.length}\n`;
        text += `👤 Диалогов: ${individuals.length}\n\n`;
        // Показываем первые 10
        const showChats = archived.slice(0, 10);
        showChats.forEach((chat) => {
            const emoji = chat.isGroup ? '👥' : '👤';
            text += `${emoji} ${chat.name || chat.id._serialized}\n`;
        });
        if (archived.length > 10) {
            text += `\n... и ещё ${archived.length - 10}`;
        }
        await ctx.editMessageText(text, {
            parse_mode: 'Markdown',
            ...accountsMenu(accountId)
        }).catch(() => { });
    }
    catch (error) {
        console.error('Error getting chats:', error);
        await ctx.reply('❌ Ошибка при получении чатов').catch(() => { });
    }
});
// === РАССЫЛКА ===
bot.action('broadcast_menu', async (ctx) => {
    console.log('[DEBUG] broadcast_menu callback triggered');
    await ctx.answerCbQuery().catch(() => { });
    const enabledAccounts = [...waAccounts.values()].filter(a => a.status === 'connected' && a.enabled);
    let text = '📨 *Рассылка*\n\n';
    if (enabledAccounts.length === 0) {
        text += '❌ Нет включенных аккаунтов для рассылки\n\n';
        text += 'Включите аккаунты в разделе "Аккаунты"';
    }
    else {
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
    }).catch(() => { });
});
// Выбрали аккаунт для рассылки
bot.action(/^broadcast_acc_(.+)$/, async (ctx) => {
    console.log('[DEBUG] broadcast_acc_ callback triggered');
    console.log('[DEBUG] match:', ctx.match);
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    console.log('[DEBUG] accountId:', accountId);
    console.log('[DEBUG] Available accounts:', [...waAccounts.keys()]);
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await ctx.editMessageText('❌ Аккаунт не подключен', {
            ...broadcastSelectMenu()
        }).catch(() => { });
        return;
    }
    if (!acc.enabled) {
        await ctx.editMessageText('❌ Этот аккаунт выключен для рассылки\n\nВключите его в разделе "Аккаунты"', {
            ...broadcastSelectMenu()
        }).catch(() => { });
        return;
    }
    if (!acc.customMessage) {
        userStates.set(ctx.from.id, { action: 'waiting_broadcast_text', accountId });
        await ctx.editMessageText(`📨 *Рассылка с ${acc.name || acc.phone}*\n\n` +
            `⚠️ Текст не настроен!\n\n` +
            `Введите текст для рассылки:`, {
            parse_mode: 'Markdown',
            ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', 'broadcast_menu')]])
        }).catch(() => { });
        return;
    }
    // Показываем подтверждение
    await ctx.editMessageText(`📨 *Подтверждение рассылки*\n\n` +
        `📱 Аккаунт: ${acc.name || acc.phone}\n\n` +
        `📝 *Текст:*\n${acc.customMessage}\n\n` +
        `⚠️ Отправить во все архивные чаты?\n` +
        `⏱ Задержка: 10 минут между сообщениями\n` +
        `🔄 После каждого сообщения - ожидание`, {
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [
                [telegraf_1.Markup.button.callback('✅ Да, начать', `confirm_broadcast_${accountId}`)],
                [telegraf_1.Markup.button.callback('❌ Отмена', 'broadcast_menu')]
            ]
        }
    }).catch(() => { });
});
// Подтверждение рассылки
bot.action(/^confirm_broadcast_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected' || !acc.customMessage || !acc.enabled) {
        await ctx.reply('❌ Аккаунт не готов к рассылке').catch(() => { });
        return;
    }
    await startBroadcast(ctx, accountId, acc.customMessage);
});
// === ПРИСОЕДИНЕНИЕ К ЧАТАМ ===
// Меню выбора аккаунта для присоединения
const joinSelectMenu = () => {
    const buttons = [];
    waAccounts.forEach((acc, id) => {
        if (acc.status === 'connected') {
            buttons.push([
                telegraf_1.Markup.button.callback(`📱 ${acc.name || acc.phone}`, `join_acc_${id}`)
            ]);
        }
    });
    if (buttons.length === 0) {
        buttons.push([telegraf_1.Markup.button.callback('❌ Нет подключенных аккаунтов', 'main')]);
    }
    buttons.push([telegraf_1.Markup.button.callback('◀️ Назад', 'main')]);
    return telegraf_1.Markup.inlineKeyboard(buttons);
};
// Меню присоединения
bot.action('join_menu', async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    let text = '🔗 *Присоединение к чатам*\n\n';
    text += `📋 Отправьте ссылки на группы (одну или несколько через пробел)\n`;
    text += `Формат: https://chat.whatsapp.com/Код\n`;
    text += `или просто код приглашения\n\n`;
    text += `🛡️ *Защита:*\n`;
    text += `   ⏱ Задержка: ${Math.round(JOIN_DELAY_MIN / 60000)}-${Math.round(JOIN_DELAY_MAX / 60000)} мин между присоединениями\n`;
    text += `   📊 Перерыв после ${MAX_JOINS_BEFORE_BREAK} чатов: ${JOIN_BREAK_DURATION / 3600000} часа\n\n`;
    text += `Выберите аккаунт для присоединения:`;
    await ctx.editMessageText(text, {
        parse_mode: 'Markdown',
        ...joinSelectMenu()
    }).catch(() => { });
});
// Выбрали аккаунт для присоединения
bot.action(/^join_acc_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await ctx.editMessageText('❌ Аккаунт не подключен', {
            ...joinSelectMenu()
        }).catch(() => { });
        return;
    }
    userStates.set(ctx.from.id, { action: 'waiting_group_links', accountId });
    await ctx.editMessageText(`🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc.name || acc.phone}\n\n` +
        `Введите ссылки на группы через пробел:\n\n` +
        `Пример:\n` +
        `https://chat.whatsapp.com/DiNo0pMTcZ28k2jf6rNpex https://chat.whatsapp.com/DkUIXcma2Lx29eUH3eX17P https://chat.whatsapp.com/L7MY2T19YCtKN2bEXxeASa`, {
        parse_mode: 'Markdown',
        ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', 'join_menu')]])
    }).catch(() => { });
});
// Подтверждение присоединения
bot.action(/^confirm_join_(.+)$/, async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const accountId = ctx.match[1];
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await ctx.reply('❌ Аккаунт не подключен').catch(() => { });
        return;
    }
    const links = pendingGroupLinks.get(accountId);
    if (!links || links.length === 0) {
        await ctx.reply('❌ Нет ссылок для присоединения').catch(() => { });
        return;
    }
    await startJoinProcess(ctx, accountId, links);
});
// Запуск процесса присоединения
async function startJoinProcess(ctx, accountId, links) {
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        return ctx.reply('❌ Аккаунт не подключен').catch(() => { });
    }
    // Фильтруем только валидные ссылки
    const validLinks = [];
    for (const link of links) {
        const code = extractInviteCode(link);
        if (code) {
            validLinks.push(code);
        }
    }
    if (validLinks.length === 0) {
        return ctx.reply('❌ Не найдено валидных ссылок').catch(() => { });
    }
    const joinId = Date.now().toString();
    activeJoins.set(joinId, {
        stop: false,
        joined: 0,
        failed: 0,
        total: validLinks.length,
        accountId
    });
    await ctx.reply(`🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc.name || acc.phone}\n` +
        `📊 Найдено ссылок: ${validLinks.length}\n\n` +
        `🛡️ *Защита:*\n` +
        `   ⏱ Задержка: ${Math.round(JOIN_DELAY_MIN / 60000)}-${Math.round(JOIN_DELAY_MAX / 60000)} мин\n` +
        `   📊 Перерыв после ${MAX_JOINS_BEFORE_BREAK} чатов: ${JOIN_BREAK_DURATION / 3600000} часа\n\n` +
        `⏳ Начинаем...`, { parse_mode: 'Markdown' });
    let joined = 0;
    let alreadyJoined = 0;
    let failed = 0;
    let needsApproval = 0;
    let joinsSinceBreak = 0;
    for (let i = 0; i < validLinks.length; i++) {
        const join = activeJoins.get(joinId);
        if (join?.stop) {
            await ctx.reply(`⏹ Остановлено\n✅ Присоединено: ${joined} | ❌ Ошибок: ${failed} | ⏳ Ожидает одобрения: ${needsApproval}`).catch(() => { });
            activeJoins.delete(joinId);
            return;
        }
        // Проверка перерыва после MAX_JOINS_BEFORE_BREAK (30) чатов
        if (joinsSinceBreak >= MAX_JOINS_BEFORE_BREAK) {
            const breakHours = JOIN_BREAK_DURATION / 3600000;
            await ctx.reply(`⏸ *Перерыв ${breakHours} часа*\n\n` +
                `После ${MAX_JOINS_BEFORE_BREAK} присоединений - перерыв\n` +
                `Ожидаем ${breakHours} часов...`, { parse_mode: 'Markdown' }).catch(() => { });
            await new Promise(r => setTimeout(r, JOIN_BREAK_DURATION));
            joinsSinceBreak = 0;
            await ctx.reply(`✅ Перерыв завершён, продолжаем...`).catch(() => { });
        }
        // Проверка ночной паузы
        if (isNightPause()) {
            const waitTime = getTimeUntilMorning();
            const hoursLeft = Math.floor(waitTime / 3600000);
            await ctx.reply(`🌙 *Ночная пауза*\n\nБот приостановлен до 08:00\nОсталось: ~${hoursLeft} ч`, { parse_mode: 'Markdown' }).catch(() => { });
            await new Promise(r => setTimeout(r, waitTime));
        }
        const inviteCode = validLinks[i];
        console.log(`[${i + 1}/${validLinks.length}] Joining: ${inviteCode}`);
        try {
            const result = await joinGroupByInvite(acc.client, inviteCode);
            if (result.success) {
                if (result.alreadyJoined) {
                    alreadyJoined++;
                    console.log(`  ⚠️ Already joined, skipping`);
                }
                else {
                    joined++;
                    joinsSinceBreak++;
                    console.log(`  ✓ Joined successfully`);
                }
            }
            else if (result.needsApproval) {
                needsApproval++;
                console.log(`  ⏳ Needs approval: ${result.error}`);
            }
            else {
                failed++;
                console.log(`  ✗ Failed: ${result.error}`);
            }
            // Прогресс каждые 3
            if (i % 3 === 0 || i === validLinks.length - 1) {
                await ctx.reply(`📊 ${i + 1}/${validLinks.length}\n` +
                    `✅ Присоединено: ${joined}\n` +
                    `⏭️ Уже в группе: ${alreadyJoined}\n` +
                    `❌ Ошибок: ${failed}\n` +
                    `⏳ Ожидает одобрения: ${needsApproval}`).catch(() => { });
            }
        }
        catch (error) {
            console.error('Join error:', error);
            failed++;
        }
        // Задержка между присоединениями
        if (i < validLinks.length - 1) {
            const delay = getRandomJoinDelay();
            console.log(`Waiting ${Math.round(delay / 60000)} minutes before next join...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
    pendingGroupLinks.delete(accountId);
    await ctx.reply(`🏁 *Присоединение завершено*\n\n` +
        `✅ Присоединено: ${joined}\n` +
        `⏭️ Уже в группе: ${alreadyJoined}\n` +
        `❌ Ошибок: ${failed}\n` +
        `⏳ Требует одобрения: ${needsApproval}`, { parse_mode: 'Markdown' }).catch(() => { });
    activeJoins.delete(joinId);
}
// === НАСТРОЙКИ ===
bot.action('settings', async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
    const text = `⚙️ *Настройки*\n\n` +
        `📊 Всего аккаунтов: ${waAccounts.size}\n` +
        `🔄 Подключено: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}\n` +
        `📨 Включено для рассылки: ${[...waAccounts.values()].filter(a => a.enabled).length}\n\n` +
        `🛡️ *Анти-бан система:*\n` +
        `   ⏱ Задержка: ${Math.round(MIN_DELAY / 60000)}-${Math.round(MAX_DELAY / 60000)} мин\n` +
        `   📊 Лимит/день: ${MAX_MESSAGES_PER_DAY}\n` +
        `   🌙 Ночная пауза: ОТКЛЮЧЕНА`;
    await ctx.editMessageText(text, {
        parse_mode: 'Markdown',
        ...settingsMenu()
    }).catch(() => { });
});
bot.action('stats', async (ctx) => {
    await ctx.answerCbQuery().catch(() => { });
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
    }).catch(() => { });
});
// === ОБРАБОТЧИКИ СООБЩЕНИЙ ===
bot.on('text', async (ctx) => {
    if (!isAdmin(ctx.from.id))
        return;
    const state = userStates.get(ctx.from.id);
    if (!state)
        return;
    try {
        // QR-код авторизация
        if (state.action === 'waiting_phone_qr') {
            const phone = ctx.message.text.trim();
            const cleanPhone = phone.replace(/[^0-9+]/g, '');
            if (cleanPhone.length < 10) {
                await ctx.reply('❌ Неверный формат номера. Введите в формате +79991234567');
                return;
            }
            await addNewAccount(ctx, cleanPhone);
            userStates.delete(ctx.from.id);
            return;
        }
        // Авторизация по номеру телефона
        if (state.action === 'waiting_phone_pairing') {
            let phone = ctx.message.text.trim();
            // Убираем все кроме цифр
            let cleanPhone = phone.replace(/[^0-9]/g, '');
            // Если 10 цифр (без кода страны) - добавляем +7 (Казахстан)
            if (cleanPhone.length === 10 && /^[0-9]/.test(cleanPhone)) {
                cleanPhone = '+7' + cleanPhone;
            }
            else if (cleanPhone.length === 11 && cleanPhone.startsWith('8')) {
                // Если начинается с 8, заменяем на +7
                cleanPhone = '+7' + cleanPhone.substring(1);
            }
            else if (!cleanPhone.startsWith('+')) {
                cleanPhone = '+' + cleanPhone;
            }
            if (cleanPhone.length < 11) {
                await ctx.reply('❌ Неверный формат номера.\nВведите 10 цифр: 7771234567');
                return;
            }
            await addNewAccount(ctx, cleanPhone);
            userStates.delete(ctx.from.id);
            return;
        }
        if (state.action === 'waiting_custom_message') {
            const accountId = state.accountId;
            const acc = waAccounts.get(accountId);
            if (acc) {
                acc.customMessage = ctx.message.text;
                await ctx.reply(`✅ Текст сохранен для аккаунта ${acc.name || acc.phone}`);
                await ctx.editMessageText(`✅ Текст сохранен!\n\n📝 ${ctx.message.text}`, { parse_mode: 'Markdown' }).catch(() => { });
            }
            userStates.delete(ctx.from.id);
            return;
        }
        if (state.action === 'waiting_broadcast_text') {
            const accountId = state.accountId;
            const acc = waAccounts.get(accountId);
            if (acc) {
                acc.customMessage = ctx.message.text;
                await ctx.reply('✅ Текст сохранен. Нажмите "Да, начать" для рассылки.');
            }
            userStates.delete(ctx.from.id);
            return;
        }
        if (state.action === 'waiting_group_links') {
            const accountId = state.accountId;
            const text = ctx.message.text.trim();
            // Разделяем текст на ссылки (по пробелам или новым строкам)
            const links = text.split(/[\s\n]+/).filter(l => l.length > 0);
            if (links.length === 0) {
                await ctx.reply('❌ Не найдены ссылки').catch(() => { });
                return;
            }
            // Сохраняем ссылки
            pendingGroupLinks.set(accountId, links);
            // Показываем подтверждение
            await ctx.reply(`✅ Найдено ссылок: ${links.length}\n\n` +
                `Нажмите "Да, начать" для присоединения к чатам.`, {
                reply_markup: {
                    inline_keyboard: [
                        [telegraf_1.Markup.button.callback('✅ Да, начать', `confirm_join_${accountId}`)],
                        [telegraf_1.Markup.button.callback('❌ Отмена', 'join_menu')]
                    ]
                }
            }).catch(() => { });
            userStates.delete(ctx.from.id);
            return;
        }
    }
    catch (error) {
        console.error('Error in text handler:', error);
        await ctx.reply('❌ Ошибка: ' + error.message).catch(() => { });
        userStates.delete(ctx.from.id);
    }
});
// === ФУНКЦИИ ===
// Создание аккаунта - используем только QR-код (надежнее)
async function addNewAccount(ctx, phone) {
    const accountId = `wa_${phone.replace(/\+/g, '')}`;
    if (waAccounts.has(accountId)) {
        const existing = waAccounts.get(accountId);
        if (existing?.status === 'connected') {
            await ctx.reply('✅ Этот номер уже подключен!').catch(() => { });
            return;
        }
        if (existing?.client) {
            try {
                await existing.client.destroy();
            }
            catch (e) { }
        }
    }
    await ctx.reply('⏳ Создание сессии...\nЭто может занять 10-30 секунд').catch(() => { });
    let authStrategy;
    // Используем RemoteAuth только если MongoDB подключена
    // ВАЖНО: Для wwebjs-mongo нужно передать mongoose экземпляр, а не client
    if (mongoUri && mongoConnected && mongoose_1.default.connection.readyState === 1) {
        console.log('Using RemoteAuth with MongoDB');
        try {
            // Создаем MongoStore с передачей mongoose экземпляра
            const store = new wwebjs_mongo_1.MongoStore({ mongoose: mongoose_1.default });
            authStrategy = new whatsapp_web_js_1.RemoteAuth({
                store: store,
                backupSyncIntervalMs: 300000, // 5 минут
                clientId: accountId,
            });
            console.log('RemoteAuth created successfully with MongoStore');
        }
        catch (e) {
            console.error('Failed to create RemoteAuth:', e);
            // Fallback to LocalAuth
            authStrategy = createLocalAuth(accountId);
        }
    }
    else {
        // Fallback на LocalAuth если нет MongoDB
        console.log('MongoDB not available, using LocalAuth');
        authStrategy = createLocalAuth(accountId);
    }
    // Функция создания LocalAuth
    function createLocalAuth(accountId) {
        const sessionsPath = process.env.WA_SESSIONS_PATH || './wa-sessions';
        if (!fs.existsSync(sessionsPath)) {
            fs.mkdirSync(sessionsPath, { recursive: true });
        }
        return new whatsapp_web_js_1.LocalAuth({
            clientId: accountId,
            dataPath: sessionsPath,
        });
    }
    // Настройки клиента
    const clientOptions = {
        authStrategy,
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
    };
    const client = new whatsapp_web_js_1.Client(clientOptions);
    waAccounts.set(accountId, {
        client,
        status: 'connecting',
        phone,
        name: phone,
        customMessage: '',
        enabled: true,
        sentToday: 0,
        failedToday: 0,
    });
    let qrSent = false;
    // Обработчик QR кода
    client.on('qr', async (qr) => {
        console.log('QR received for', accountId);
        const acc = waAccounts.get(accountId);
        if (acc?.status === 'connected')
            return;
        if (!qrSent) {
            qrSent = true;
            try {
                const qrDataUrl = await QRCode.toDataURL(qr);
                const base64Data = qrDataUrl.replace(/^data:image\/png;base64,/, '');
                const buffer = Buffer.from(base64Data, 'base64');
                // Отправляем QR код с четкими инструкциями
                await ctx.reply('📱 *Подключение WhatsApp*\n\n' +
                    '1. Откройте WhatsApp на телефоне\n' +
                    '2. Нажмите Настройки → Связанные устройства\n' +
                    '3. Нажмите "Подключить устройство"\n' +
                    '4. Отсканируйте QR-код ниже\n\n' +
                    '⏰ QR-код действителен ~60 секунд', { parse_mode: 'Markdown' }).catch(() => { });
                await ctx.replyWithPhoto({ source: buffer }, {
                    caption: '📱 *Отсканируйте этот QR-код*',
                    parse_mode: 'Markdown'
                });
            }
            catch (e) {
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
        await ctx.reply(`✅ *WhatsApp подключен!*\n\n📞 Номер: ${phone}\n\nТеперь вы можете:\n` +
            `• Настроить текст рассылки в "Аккаунты"\n` +
            `• Запустить рассылку в архивные чаты`, { parse_mode: 'Markdown' }).catch(() => { });
    });
    client.on('auth_failure', async (msg) => {
        console.log('Auth failure for', accountId, msg);
        const acc = waAccounts.get(accountId);
        if (acc)
            acc.status = 'disconnected';
        await ctx.reply(`❌ Ошибка авторизации: ${msg}`).catch(() => { });
    });
    client.on('disconnected', async () => {
        console.log('Disconnected:', accountId);
        const acc = waAccounts.get(accountId);
        if (acc)
            acc.status = 'disconnected';
        await ctx.reply(`⚠️ Аккаунт отключился`).catch(() => { });
    });
    try {
        await client.initialize();
    }
    catch (error) {
        console.error('Initialize error:', error);
        const acc = waAccounts.get(accountId);
        if (acc)
            acc.status = 'disconnected';
        await ctx.reply(`❌ Ошибка: ${error.message}`).catch(() => { });
    }
}
// Улучшенная функция отправки сообщения с повторными попытками и анти-бан функциями
async function sendMessageWithRetry(client, chatId, baseText, messageNumber, totalMessages, useTypingSimulation = true, useTextVariation = true, maxRetries = 3) {
    let lastError = null;
    // Convert chatId to string in case it's a ChatId object
    const chatIdStr = String(chatId);
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            console.log(`  Attempt ${attempt}/${maxRetries} to send to ${chatIdStr}`);
            // Проверяем что клиент подключен
            if (!client.info?.wid) {
                throw new Error('Client not ready');
            }
            // Вариация текста (каждое сообщение немного отличается)
            let messageText = baseText;
            if (useTextVariation && messageNumber > 0) {
                messageText = varyRussianText(baseText);
            }
            // Имитация набора текста (для первых попыток)
            if (useTypingSimulation && attempt === 1) {
                await simulateHumanTyping(client, chatIdStr);
            }
            const result = await client.sendMessage(chatIdStr, messageText);
            console.log(`  Successfully sent to ${chatIdStr}, message ID: ${result.id}`);
            return true;
        }
        catch (error) {
            lastError = error;
            console.error(`  Attempt ${attempt} failed:`, error);
            // Разные задержки для разных попыток
            if (attempt < maxRetries) {
                const delay = attempt * 10000; // 10s, 20s, 30s
                console.log(`  Waiting ${delay}ms before retry...`);
                await new Promise(r => setTimeout(r, delay));
            }
        }
    }
    console.error(`  All ${maxRetries} attempts failed for ${chatIdStr}:`, lastError);
    return false;
}
// Рассылка с анти-бан системой
async function startBroadcast(ctx, accountId, text) {
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        return ctx.reply('❌ Аккаунт не подключен').catch(() => { });
    }
    // Устанавливаем флаг что идёт рассылка
    isBroadcasting = true;
    if (!acc.enabled) {
        return ctx.reply('❌ Этот аккаунт выключен для рассылки').catch(() => { });
    }
    // Проверка дневного лимита
    if (acc.sentToday >= MAX_MESSAGES_PER_DAY) {
        return ctx.reply(`❌ Достигнут дневной лимит (${MAX_MESSAGES_PER_DAY} сообщений)`).catch(() => { });
    }
    await ctx.reply('🔍 Поиск архивных чатов...').catch(() => { });
    try {
        const chats = await acc.client.getChats();
        const archivedChats = chats.filter((chat) => chat.archived);
        console.log(`Found ${archivedChats.length} archived chats for ${accountId}`);
        if (archivedChats.length === 0) {
            return ctx.reply('❌ Архивные чаты не найдены').catch(() => { });
        }
        const groups = archivedChats.filter((c) => c.isGroup);
        const individuals = archivedChats.filter((c) => !c.isGroup);
        const broadcastId = Date.now().toString();
        activeBroadcasts.set(broadcastId, {
            stop: false,
            sent: 0,
            failed: 0,
            total: archivedChats.length,
            accountId
        });
        // Расчёт времени с учётом прогрессивной задержки
        const avgDelay = (MIN_DELAY + MAX_DELAY) / 2;
        const estimatedMin = Math.ceil(archivedChats.length * (avgDelay / 60000));
        const hours = Math.floor(estimatedMin / 60);
        const mins = estimatedMin % 60;
        // Уведомление о начале рассылки
        await ctx.reply(`🔔 *Рассылка началась!*\n\n` +
            `📱 Аккаунт: ${acc.name || acc.phone}\n` +
            `📊 Найдено: ${archivedChats.length} чатов\n` +
            `   👥 Групп: ${groups.length}\n` +
            `   👤 Диалогов: ${individuals.length}\n\n` +
            `🛡️ *Защита:*\n` +
            `   ⏱ Задержка: 8-15 мин (прогрессивная)\n` +
            `   🤖 Имитация человека: ВКЛ\n` +
            `   🔤 Вариация текста: ВКЛ\n` +
            `   📊 Лимит/день: ${MAX_MESSAGES_PER_DAY}\n\n` +
            `⏱ Примерное время: ~${hours}ч ${mins}мин\n\n` +
            `⏳ Рассылка по кругу пока не остановите...\n` +
            `🛑 Для остановки нажмите "⏹ Стоп"`, { parse_mode: 'Markdown' });
        let sent = 0;
        let failed = 0;
        let loopCount = 0;
        let chatIndex = 0;
        // Бесконечный цикл пока не остановят
        while (true) {
            const broadcast = activeBroadcasts.get(broadcastId);
            if (broadcast?.stop) {
                isBroadcasting = false;
                await ctx.reply(`⏹ *Рассылка остановлена*\n\n✅ Отправлено: ${sent}\n❌ Ошибок: ${failed}\n📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`, { parse_mode: 'Markdown' }).catch(() => { });
                activeBroadcasts.delete(broadcastId);
                return;
            }
            // Проверка дневного лимита - ждём до полуночи
            if (acc.sentToday >= MAX_MESSAGES_PER_DAY) {
                await ctx.reply(`⚠️ *Достигнут дневной лимит*\n\n` +
                    `Отправлено сегодня: ${acc.sentToday}\n` +
                    `Максимум: ${MAX_MESSAGES_PER_DAY}\n\n` +
                    `⏳ Ожидаем сброса лимита...`, { parse_mode: 'Markdown' }).catch(() => { });
                // Ждём 1 час и проверяем снова
                await new Promise(r => setTimeout(r, 60 * 60 * 1000));
                continue;
            }
            // Если прошли все чаты - начинаем сначала (по кругу)
            if (chatIndex >= archivedChats.length) {
                chatIndex = 0;
                loopCount++;
                await ctx.reply(`🔄 *Новый круг рассылки #${loopCount + 1}*\n\n` +
                    `✅ Отправлено: ${sent}\n` +
                    `❌ Ошибок: ${failed}\n` +
                    `📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`, { parse_mode: 'Markdown' }).catch(() => { });
            }
            const chat = archivedChats[chatIndex];
            // Проверяем что есть ID чата
            const chatId = String(chat.id?._serialized || chat.id);
            if (!chatId) {
                console.error(`Chat ${chatIndex} has no ID, skipping`);
                chatIndex++;
                continue;
            }
            const currentMsgNum = loopCount * archivedChats.length + chatIndex + 1;
            console.log(`[Круг ${loopCount + 1}, ${chatIndex + 1}/${archivedChats.length}] Sending to: ${chat.name || String(chatId)}`);
            try {
                // Используем прогрессивную задержку
                const delay = getProgressiveDelay(sent, archivedChats.length);
                console.log(`  Using progressive delay: ${Math.round(delay / 60000)} minutes`);
                const success = await sendMessageWithRetry(acc.client, chatId, text, currentMsgNum, archivedChats.length, true, true, 3);
                if (success) {
                    sent++;
                    acc.sentToday++;
                    console.log(`  ✓ Success! Total sent: ${sent}`);
                }
                else {
                    failed++;
                    acc.failedToday++;
                    console.log(`  ✗ Failed! Total failed: ${failed}`);
                    // Пропускаем чат с ошибкой - не пауза
                }
                // Обновляем прогресс каждые 3 сообщения
                if (currentMsgNum % 3 === 0) {
                    await ctx.reply(`📊 Круг ${loopCount + 1} | Сообщение ${chatIndex + 1}/${archivedChats.length}\n` +
                        `✅ ${sent} | ❌ ${failed}\n` +
                        `📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}\n` +
                        `🛑 Стоп: /stop_${broadcastId}`).catch(() => { });
                }
            }
            catch (error) {
                console.error('Send error:', error);
                failed++;
                acc.failedToday++;
                // Пропускаем чат с ошибкой
            }
            chatIndex++;
            // Задержка между сообщениями (прогрессивная)
            const delay = getProgressiveDelay(sent, archivedChats.length);
            console.log(`Waiting ${Math.round(delay / 60000)} minutes before next message...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
    catch (error) {
        isBroadcasting = false;
        console.error('Broadcast error:', error);
        await ctx.reply(`❌ Ошибка: ${error.message}`).catch(() => { });
    }
}
// Глобальный обработчик ошибок
bot.catch((err, ctx) => {
    console.error('Bot error:', err);
    try {
        ctx.reply('⚠️ Ошибка. Попробуйте еще раз.').catch(() => { });
    }
    catch (e) {
        console.error('Error in catch:', e);
    }
});
// === НАСТРОЙКА МЕНЮ БОТА ===
async function setupBotMenu() {
    try {
        await bot.telegram.setMyCommands([
            { command: 'start', description: 'Главное меню' },
            { command: 'status', description: 'Статус бота' },
            { command: 'broadcast', description: 'Начать рассылку' },
            { command: 'stop', description: 'Остановить рассылку' },
            { command: 'accounts', description: 'Управление аккаунтами' }
        ]);
        console.log('✅ Menu commands set');
    }
    catch (e) {
        console.error('Error setting menu:', e);
    }
}
// === АВТО-ПЕРЕЗАПУСК CHROME (каждые 6 часов) ===
const CHROME_RESTART_INTERVAL = 6 * 60 * 60 * 1000; // 6 часов
let isBroadcasting = false;
function startChromeAutoRestart() {
    setInterval(async () => {
        if (isBroadcasting) {
            console.log('⚠️ Broadcast in progress, postponing Chrome restart');
            return;
        }
        console.log('🔄 Scheduled Chrome restart...');
        // Перезапускаем все подключенные аккаунты
        for (const [id, acc] of waAccounts) {
            if (acc.status === 'connected') {
                try {
                    console.log(`  Restarting Chrome for ${id}...`);
                    await acc.client.destroy();
                    // Новая инициализация
                    acc.client.initialize();
                    console.log(`  ✅ Chrome restarted for ${id}`);
                }
                catch (e) {
                    console.error(`  ❌ Failed to restart Chrome for ${id}:`, e);
                }
            }
        }
    }, CHROME_RESTART_INTERVAL);
}
// Подключение к MongoDB для RemoteAuth
async function connectMongoDB() {
    const mongoUri = process.env.MONGO_URI;
    if (mongoUri) {
        try {
            await mongoose_1.default.connect(mongoUri);
            console.log('✅ MongoDB connected for session persistence');
            return true;
        }
        catch (e) {
            console.error('❌ MongoDB connection failed:', e);
            return false;
        }
    }
    else {
        console.log('⚠️ MONGO_URI not set, sessions will not persist after restart');
        return false;
    }
}
// Запуск
async function main() {
    console.log('🚀 Starting bot...');
    console.log('Admin IDs:', ADMIN_IDS);
    console.log('Accounts loaded:', waAccounts.size);
    // Подключаем MongoDB для сохранения сессий
    await connectMongoDB();
    // Настраиваем меню
    await setupBotMenu();
    // Запускаем авто-перезапуск Chrome
    startChromeAutoRestart();
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
//# sourceMappingURL=main.js.map