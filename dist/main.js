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
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const telegraf_1 = require("telegraf");
const whatsapp_web_js_1 = require("whatsapp-web.js");
const QRCode = __importStar(require("qrcode"));
const fs = __importStar(require("fs"));
const express_1 = __importDefault(require("express"));
require("dotenv/config");
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
// Хранилище аккаунтов
const waAccounts = new Map();
const userStates = new Map();
const activeBroadcasts = new Map();
const pendingGroupLinks = new Map();
const isAdmin = (userId) => ADMIN_IDS.includes(userId);
// === КОНФИГУРАЦИЯ АНТИ-БАНА ===
const FIXED_DELAY = 10 * 60 * 1000; // ФИКСИРОВАННЫЕ 10 минут
const MAX_MESSAGES_PER_DAY = 150;
const NIGHT_PAUSE_START = 24; // Отключена
const NIGHT_PAUSE_END = 24;
// Конфигурация авто-присоединения
const JOIN_DELAY_MIN = 10 * 60 * 1000;
const JOIN_DELAY_MAX = 10 * 60 * 1000;
const MAX_JOINS_BEFORE_BREAK = 30;
const JOIN_BREAK_DURATION = 4 * 60 * 60 * 1000;
const MAX_PENDING_JOIN_REQUESTS = 5; // Максимум 5 ожидающих заявок
const activeJoins = new Map();
// === БЕЗОПАСНЫЕ ОБЕРТКИ ===
const safeReply = async (ctx, text, extra) => {
    try {
        await ctx.reply(text, extra).catch(() => { });
    }
    catch (e) {
        console.error('Reply error:', e);
    }
};
const safeEdit = async (ctx, text, extra) => {
    try {
        await ctx.editMessageText(text, extra).catch(() => { });
    }
    catch (e) {
        console.error('Edit error:', e);
    }
};
const safeAnswerCb = async (ctx, text) => {
    try {
        await ctx.answerCbQuery(text).catch(() => { });
    }
    catch (e) {
        // Ignore
    }
};
// === ФУНКЦИИ АНТИ-БАНА ===
function getFixedDelay() {
    return FIXED_DELAY;
}
function isNightPause() {
    const hour = new Date().getHours();
    return hour >= NIGHT_PAUSE_START && hour < NIGHT_PAUSE_END;
}
function getTimeUntilMorning() {
    const now = new Date();
    const tomorrow = new Date(now);
    tomorrow.setDate(tomorrow.getDate() + 1);
    tomorrow.setHours(NIGHT_PAUSE_END, 0, 0, 0);
    return tomorrow.getTime() - now.getTime();
}
function varyRussianText(text) {
    const replacements = {
        'а': 'a', 'А': 'A', 'е': 'e', 'Е': 'E', 'о': 'o', 'О': 'O',
        'р': 'p', 'Р': 'P', 'с': 'c', 'С': 'C', 'у': 'y', 'У': 'Y',
        'х': 'x', 'Х': 'X', 'і': 'i', 'І': 'I', 'ј': 'j', 'Ј': 'J',
    };
    let result = text;
    for (let i = 0; i < text.length; i++) {
        if (replacements[text[i]] && Math.random() > 0.7) {
            result = result.substring(0, i) + replacements[text[i]] + result.substring(i + 1);
        }
    }
    if (Math.random() > 0.5) {
        const extra = [' ', ' ', '.', '..'];
        result += extra[Math.floor(Math.random() * extra.length)];
    }
    return result;
}
async function simulateHumanTyping(client, chatId, duration = 2000) {
    try {
        await client.sendPresenceAvailable();
        const typingTime = duration + Math.floor(Math.random() * 2000);
        await new Promise(r => setTimeout(r, typingTime));
    }
    catch (e) { }
}
function getRandomJoinDelay() {
    return Math.floor(Math.random() * (JOIN_DELAY_MAX - JOIN_DELAY_MIN + 1)) + JOIN_DELAY_MIN;
}
function extractInviteCode(link) {
    const patterns = [
        /chat\.whatsapp\.com\/([a-zA-Z0-9]{20,})/i,
        /invite\.whatsapp\.com\/([a-zA-Z0-9]{20,})/i,
    ];
    for (const pattern of patterns) {
        const match = link.match(pattern);
        if (match)
            return match[1];
    }
    if (/^[a-zA-Z0-9]{20,}$/.test(link.trim())) {
        return link.trim();
    }
    return null;
}
async function joinGroupByInvite(client, inviteCode) {
    try {
        console.log(`  Checking/joining group with invite code: ${inviteCode}`);
        const allChats = await client.getChats();
        const existingChat = allChats.find((chat) => chat.isGroup && chat.link && chat.link.includes(inviteCode));
        if (existingChat) {
            console.log(`  ⚠️ Already a member of this group: ${existingChat.name || inviteCode}`);
            return { success: true, alreadyJoined: true };
        }
        // Проверяем требование одобрения ДО попытки присоединения
        // Если сразу видим что нужно одобрение - пропускаем
        try {
            await client.acceptInvite(inviteCode);
            console.log(`  ✓ Successfully joined group`);
            return { success: true };
        }
        catch (joinError) {
            const joinErrorMsg = joinError.message || String(joinError);
            // Если требуется одобрение - пропускаем чат (не подаём заявку)
            if (joinErrorMsg.includes('approval') || joinErrorMsg.includes('join') || joinErrorMsg.includes('require') || joinErrorMsg.includes('Admin')) {
                console.log(`  ⚠️ Group requires approval - skipping`);
                return { success: false, error: 'Требуется одобрение', needsApproval: true, skipped: true };
            }
            // Для других ошибок - пробросим их выше
            throw joinError;
        }
    }
    catch (error) {
        const errorMessage = error.message || String(error);
        console.error(`  ✗ Join failed:`, errorMessage);
        if (errorMessage.includes('invalid') || errorMessage.includes('expired')) {
            return { success: false, error: 'Ссылка недействительна или истекла' };
        }
        if (errorMessage.includes('approval') || errorMessage.includes('join') || errorMessage.includes('require') || errorMessage.includes('Admin')) {
            return { success: false, error: 'Требуется одобрение администратора', needsApproval: true };
        }
        if (errorMessage.includes('not found') || errorMessage.includes('404')) {
            return { success: false, error: 'Группа не найдена' };
        }
        if (errorMessage.includes('already') || errorMessage.includes('member')) {
            return { success: true, alreadyJoined: true };
        }
        return { success: false, error: errorMessage };
    }
}
// === МЕНЮ ===
const mainMenu = () => telegraf_1.Markup.inlineKeyboard([
    [telegraf_1.Markup.button.callback('📱 Аккаунты', 'accounts')],
    [telegraf_1.Markup.button.callback('📨 Рассылка', 'broadcast_menu')],
    [telegraf_1.Markup.button.callback('🔗 Присоединение к чатам', 'join_menu')],
    [telegraf_1.Markup.button.callback('⚙️ Настройки', 'settings')],
]);
const accountsMenu = (accountId) => {
    const buttons = [];
    if (accountId) {
        const acc = waAccounts.get(accountId);
        if (acc) {
            const statusEmoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
            const enabledEmoji = acc.enabled ? '✅' : '❌';
            buttons.push([telegraf_1.Markup.button.callback(`${enabledEmoji} Рассылка: ${acc.enabled ? 'ВКЛ' : 'ВЫКЛ'}`, `toggle_${accountId}`)]);
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
                buttons.push([telegraf_1.Markup.button.callback(`${emoji} ${enabledEmoji} ${acc.name || acc.phone || id}`, `acc_${id}`)]);
            });
        }
        buttons.push([telegraf_1.Markup.button.callback('📷 QR-код (рекомендуется)', 'add_qr')]);
        buttons.push([telegraf_1.Markup.button.callback('◀️ Назад', 'main')]);
    }
    return telegraf_1.Markup.inlineKeyboard(buttons);
};
const broadcastSelectMenu = () => {
    const buttons = [];
    let hasEnabled = false;
    waAccounts.forEach((acc, id) => {
        if (acc.status === 'connected' && acc.enabled) {
            hasEnabled = true;
            buttons.push([telegraf_1.Markup.button.callback(`📱 ${acc.name || acc.phone}`, `broadcast_acc_${id}`)]);
        }
    });
    if (!hasEnabled) {
        buttons.push([telegraf_1.Markup.button.callback('❌ Нет включенных аккаунтов', 'main')]);
    }
    buttons.push([telegraf_1.Markup.button.callback('◀️ Назад', 'main')]);
    return telegraf_1.Markup.inlineKeyboard(buttons);
};
const settingsMenu = () => telegraf_1.Markup.inlineKeyboard([
    [telegraf_1.Markup.button.callback('🔄 Перезапустить все сессии', 'restart_all')],
    [telegraf_1.Markup.button.callback('📊 Статистика', 'stats')],
    [telegraf_1.Markup.button.callback('◀️ Назад', 'main')],
]);
const joinSelectMenu = () => {
    const buttons = [];
    waAccounts.forEach((acc, id) => {
        if (acc.status === 'connected') {
            buttons.push([telegraf_1.Markup.button.callback(`📱 ${acc.name || acc.phone}`, `join_acc_${id}`)]);
        }
    });
    if (buttons.length === 0) {
        buttons.push([telegraf_1.Markup.button.callback('❌ Нет подключенных аккаунтов', 'main')]);
    }
    buttons.push([telegraf_1.Markup.button.callback('◀️ Назад', 'main')]);
    return telegraf_1.Markup.inlineKeyboard(buttons);
};
// Admin middleware
bot.use(async (ctx, next) => {
    if (ctx.from && !isAdmin(ctx.from.id)) {
        return safeReply(ctx, '⛔ Нет доступа');
    }
    await next();
});
// Error middleware
bot.use(async (ctx, next) => {
    if (ctx.callbackQuery && 'data' in ctx.callbackQuery) {
        console.log('[DEBUG] CallbackQuery:', JSON.stringify({
            data: ctx.callbackQuery.data,
            from: ctx.from?.id,
        }));
    }
    await next().catch(err => {
        console.error('[DEBUG] Error in handler:', err);
        safeReply(ctx, '❌ Произошла ошибка: ' + err.message);
    });
});
// Commands
bot.start(async (ctx) => {
    await ctx.reply('🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
        parse_mode: 'Markdown',
        ...mainMenu()
    });
});
bot.command('status', async (ctx) => {
    const connected = [...waAccounts.values()].filter(a => a.status === 'connected').length;
    const enabled = [...waAccounts.values()].filter(a => a.enabled).length;
    const totalSent = [...waAccounts.values()].reduce((sum, a) => sum + a.sentToday, 0);
    const uptime = Math.floor(process.uptime() / 60);
    await safeReply(ctx, `${isBroadcasting ? '🟡' : '🟢'} *Статус бота*\n\n` +
        `📊 Статус: ${isBroadcasting ? 'Рассылка активна' : 'Работает'}\n` +
        `🖥 Аккаунтов: ${connected}/${waAccounts.size} подключено\n` +
        `✅ Включено для рассылки: ${enabled}\n` +
        `📤 Отправлено сегодня: ${totalSent}\n` +
        `⏱ Uptime: ${uptime} минут`, { parse_mode: 'Markdown' });
});
bot.command('stop', async (ctx) => {
    const args = ctx.message.text.split(' ');
    if (args.length < 2) {
        await safeReply(ctx, '❌ Использование: /stop <broadcast_id>');
        return;
    }
    const broadcastId = args[1];
    const broadcast = activeBroadcasts.get(broadcastId);
    if (broadcast) {
        broadcast.stop = true;
        await safeReply(ctx, `✅ Команда остановки отправлена для рассылки ${broadcastId}`);
    }
    else {
        await safeReply(ctx, '❌ Рассылка не найдена');
    }
});
// Main menu
bot.action('main', async (ctx) => {
    await safeAnswerCb(ctx);
    userStates.delete(ctx.from.id);
    await safeEdit(ctx, '🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
        parse_mode: 'Markdown',
        ...mainMenu()
    });
});
// === ACCOUNT MANAGEMENT ===
bot.action('accounts', async (ctx) => {
    await safeAnswerCb(ctx);
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
    await safeEdit(ctx, text, { parse_mode: 'Markdown', ...accountsMenu() });
});
bot.action(/^acc_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (!acc) {
        await safeEdit(ctx, '❌ Аккаунт не найден', { ...accountsMenu() });
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
    await safeEdit(ctx, text, { parse_mode: 'Markdown', ...accountsMenu(accountId) });
});
bot.action(/^toggle_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (acc) {
        acc.enabled = !acc.enabled;
        await safeReply(ctx, `✅ Рассылка для этого номера ${acc.enabled ? 'включен' : 'выключен'}`);
    }
    await safeEdit(ctx, '📱 *Управление аккаунтом*\n\nНажмите для обновления:', {
        parse_mode: 'Markdown',
        ...accountsMenu(accountId)
    });
});
bot.action('add_account', async (ctx) => {
    await safeAnswerCb(ctx);
    await safeEdit(ctx, '📱 *Добавление аккаунта*\n\n📷 *Подключение через QR-код*\n   ✅ Работает стабильно\n\n⚠️ *Код подтверждения временно недоступен*\n   WhatsApp изменил интерфейс', {
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [
                [telegraf_1.Markup.button.callback('📷 QR-код', 'add_qr')],
                [telegraf_1.Markup.button.callback('❌ Отмена', 'accounts')]
            ]
        }
    });
});
bot.action('add_qr', async (ctx) => {
    await safeAnswerCb(ctx);
    userStates.set(ctx.from.id, { action: 'waiting_phone_qr' });
    await safeEdit(ctx, '📷 *Подключение через QR-код*\n\n' +
        'Введите номер телефона в формате:\n' +
        '+79991234567\n\n' +
        'После ввода бот отправит QR-код для сканирования.', {
        parse_mode: 'Markdown',
        ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', 'add_account')]])
    });
});
bot.action(/^unbind_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    await safeEdit(ctx, `⚠️ *Отвязать номер?*\n\nАккаунт будет удален и отключен от WhatsApp.`, {
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [
                [telegraf_1.Markup.button.callback('✅ Да, отвязать', `confirm_unbind_${accountId}`)],
                [telegraf_1.Markup.button.callback('❌ Отмена', `acc_${accountId}`)]
            ]
        }
    });
});
bot.action(/^confirm_unbind_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (acc?.client) {
        try {
            await acc.client.logout();
            await acc.client.destroy();
        }
        catch (e) { }
    }
    const sessionsPath = process.env.WA_SESSIONS_PATH || './wa-sessions';
    const sessionDir = `${sessionsPath}/${accountId}`;
    if (fs.existsSync(sessionDir)) {
        fs.rmSync(sessionDir, { recursive: true, force: true });
    }
    waAccounts.delete(accountId);
    await safeReply(ctx, '✅ Аккаунт отвязан');
    await safeEdit(ctx, '✅ Аккаунт удален', { ...accountsMenu() });
});
bot.action(/^edit_msg_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    userStates.set(ctx.from.id, { action: 'waiting_custom_message', accountId });
    const acc = waAccounts.get(accountId);
    const currentMsg = acc?.customMessage || '';
    await safeEdit(ctx, `📝 *Текст для аккаунта ${acc?.name || acc?.phone || accountId}*\n\n` +
        `Текущий текст: ${currentMsg || 'не задан'}\n\n` +
        `Введите новый текст для рассылки:`, {
        parse_mode: 'Markdown',
        ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', `acc_${accountId}`)]])
    });
});
bot.action(/^view_chats_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx, 'Загрузка чатов...');
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await safeEdit(ctx, '❌ Аккаунт не подключен', { ...accountsMenu(accountId) });
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
        const showChats = archived.slice(0, 10);
        showChats.forEach((chat) => {
            const emoji = chat.isGroup ? '👥' : '👤';
            text += `${emoji} ${chat.name || chat.id._serialized}\n`;
        });
        if (archived.length > 10)
            text += `\n... и ещё ${archived.length - 10}`;
        await safeEdit(ctx, text, { parse_mode: 'Markdown', ...accountsMenu(accountId) });
    }
    catch (error) {
        console.error('Error getting chats:', error);
        await safeReply(ctx, '❌ Ошибка при получении чатов');
    }
});
// === BROADCAST ===
bot.action('broadcast_menu', async (ctx) => {
    await safeAnswerCb(ctx);
    const enabledAccounts = [...waAccounts.values()].filter(a => a.status === 'connected' && a.enabled);
    let text = '📨 *Рассылка*\n\n';
    if (enabledAccounts.length === 0) {
        text += '❌ Нет включенных аккаунтов для рассылки\n\nВключите аккаунты в разделе "Аккаунты"';
    }
    else {
        text += `Готовы к рассылке: ${enabledAccounts.length}\n\n`;
        enabledAccounts.forEach(acc => {
            const msg = acc.customMessage ? `"${acc.customMessage.substring(0, 20)}..."` : '❌ текст не задан';
            text += `📱 ${acc.name || acc.phone}\n   ${msg}\n\n`;
        });
        text += 'Выберите аккаунт для рассылки:';
    }
    await safeEdit(ctx, text, { parse_mode: 'Markdown', ...broadcastSelectMenu() });
});
bot.action(/^broadcast_acc_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await safeEdit(ctx, '❌ Аккаунт не подключен', { ...broadcastSelectMenu() });
        return;
    }
    if (!acc.enabled) {
        await safeEdit(ctx, '❌ Этот аккаунт выключен для рассылки\n\nВключите его в разделе "Аккаунты"', { ...broadcastSelectMenu() });
        return;
    }
    if (!acc.customMessage) {
        userStates.set(ctx.from.id, { action: 'waiting_broadcast_text', accountId });
        await safeEdit(ctx, `📨 *Рассылка с ${acc.name || acc.phone}*\n\n⚠️ Текст не настроен!\n\nВведите текст для рассылки:`, {
            parse_mode: 'Markdown',
            ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', 'broadcast_menu')]])
        });
        return;
    }
    await safeEdit(ctx, `📨 *Подтверждение рассылки*\n\n` +
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
    });
});
bot.action(/^confirm_broadcast_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected' || !acc.customMessage || !acc.enabled) {
        await safeReply(ctx, '❌ Аккаунт не готов к рассылке');
        return;
    }
    await startBroadcast(ctx, accountId, acc.customMessage);
});
// === JOIN CHATS ===
bot.action('join_menu', async (ctx) => {
    await safeAnswerCb(ctx);
    let text = '🔗 *Присоединение к чатам*\n\n';
    text += `📋 Отправьте ссылки на группы (одну или несколько через пробел)\n`;
    text += `Формат: https://chat.whatsapp.com/Код\n`;
    text += `🛡️ *Защита:*\n`;
    text += `   ⏱ Задержка: ${Math.round(JOIN_DELAY_MIN / 60000)} мин между присоединениями\n\n`;
    text += `Выберите аккаунт для присоединения:`;
    await safeEdit(ctx, text, { parse_mode: 'Markdown', ...joinSelectMenu() });
});
bot.action(/^join_acc_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await safeEdit(ctx, '❌ Аккаунт не подключен', { ...joinSelectMenu() });
        return;
    }
    userStates.set(ctx.from.id, { action: 'waiting_group_links', accountId });
    await safeEdit(ctx, `🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc.name || acc.phone}\n\n` +
        `Введите ссылки на группы через пробел:`, {
        parse_mode: 'Markdown',
        ...telegraf_1.Markup.inlineKeyboard([[telegraf_1.Markup.button.callback('❌ Отмена', 'join_menu')]])
    });
});
bot.action(/^confirm_join_(.+)$/, async (ctx) => {
    await safeAnswerCb(ctx);
    const accountId = ctx.match?.[1];
    if (!accountId)
        return;
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        await safeReply(ctx, '❌ Аккаунт не подключен');
        return;
    }
    const links = pendingGroupLinks.get(accountId);
    if (!links || links.length === 0) {
        await safeReply(ctx, '❌ Нет ссылок для присоединения');
        return;
    }
    await startJoinProcess(ctx, accountId, links);
});
// === SETTINGS ===
bot.action('settings', async (ctx) => {
    await safeAnswerCb(ctx);
    const text = `⚙️ *Настройки*\n\n` +
        `📊 Всего аккаунтов: ${waAccounts.size}\n` +
        `🔄 Подключено: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}\n` +
        `📨 Включено для рассылки: ${[...waAccounts.values()].filter(a => a.enabled).length}\n\n` +
        `🛡️ *Анти-бан система:*\n` +
        `   ⏱ Задержка: 10 мин (фиксированная)\n` +
        `   📊 Лимит/день: ${MAX_MESSAGES_PER_DAY}`;
    await safeEdit(ctx, text, { parse_mode: 'Markdown', ...settingsMenu() });
});
bot.action('stats', async (ctx) => {
    await safeAnswerCb(ctx);
    let text = '📊 *Статистика*\n\n';
    let totalSent = 0;
    let totalFailed = 0;
    waAccounts.forEach((acc, id) => {
        totalSent += acc.sentToday;
        totalFailed += acc.failedToday;
        text += `📱 ${acc.name || acc.phone || id}:\n`;
        text += `   ✅ Отправлено: ${acc.sentToday}\n   ❌ Ошибок: ${acc.failedToday}\n\n`;
    });
    text += `📈 *Всего:*\n   ✅ Отправлено: ${totalSent}\n   ❌ Ошибок: ${totalFailed}`;
    await safeEdit(ctx, text, { parse_mode: 'Markdown', ...settingsMenu() });
});
bot.action('restart_all', async (ctx) => {
    await safeAnswerCb(ctx);
    for (const [id, acc] of waAccounts) {
        if (acc.status === 'connected') {
            try {
                await acc.client.destroy();
                acc.status = 'disconnected';
            }
            catch (e) { }
        }
    }
    await safeReply(ctx, '✅ Все сессии перезапущены');
    await safeEdit(ctx, '✅ Сессии перезапущены', { ...settingsMenu() });
});
// === TEXT HANDLER ===
bot.on('text', async (ctx) => {
    if (!isAdmin(ctx.from.id))
        return;
    const state = userStates.get(ctx.from.id);
    if (!state)
        return;
    try {
        if (state.action === 'waiting_phone_qr') {
            const phone = ctx.message.text.trim();
            const cleanPhone = phone.replace(/[^0-9+]/g, '');
            if (cleanPhone.length < 10) {
                await safeReply(ctx, '❌ Неверный формат номера. Введите в формате +79991234567');
                return;
            }
            await addNewAccount(ctx, cleanPhone, 'qr');
            userStates.delete(ctx.from.id);
            return;
        }
        if (state.action === 'waiting_custom_message') {
            const accountId = state.accountId;
            const acc = waAccounts.get(accountId);
            if (acc) {
                acc.customMessage = ctx.message.text;
                await safeReply(ctx, `✅ Текст сохранен для аккаунта ${acc.name || acc.phone}`);
                await safeEdit(ctx, `✅ Текст сохранен!\n\n📝 ${ctx.message.text}`, { parse_mode: 'Markdown' });
            }
            userStates.delete(ctx.from.id);
            return;
        }
        if (state.action === 'waiting_broadcast_text') {
            const accountId = state.accountId;
            const acc = waAccounts.get(accountId);
            if (acc) {
                acc.customMessage = ctx.message.text;
                await safeReply(ctx, '✅ Текст сохранен. Нажмите "Да, начать" для рассылки.');
            }
            userStates.delete(ctx.from.id);
            return;
        }
        if (state.action === 'waiting_group_links') {
            const accountId = state.accountId;
            const text = ctx.message.text.trim();
            const links = text.split(/[\s\n]+/).filter(l => l.length > 0);
            if (links.length === 0) {
                await safeReply(ctx, '❌ Не найдены ссылки');
                return;
            }
            pendingGroupLinks.set(accountId, links);
            await safeReply(ctx, `✅ Найдено ссылок: ${links.length}\n\nНажмите "Да, начать" для присоединения к чатам.`, {
                reply_markup: {
                    inline_keyboard: [
                        [telegraf_1.Markup.button.callback('✅ Да, начать', `confirm_join_${accountId}`)],
                        [telegraf_1.Markup.button.callback('❌ Отмена', 'join_menu')]
                    ]
                }
            });
            userStates.delete(ctx.from.id);
            return;
        }
    }
    catch (error) {
        console.error('Error in text handler:', error);
        await safeReply(ctx, '❌ Ошибка: ' + error.message);
        userStates.delete(ctx.from.id);
    }
});
// === FUNCTIONS ===
async function addNewAccount(ctx, phone, authMethod = 'qr') {
    const accountId = `wa_${phone.replace(/\+/g, '')}`;
    if (waAccounts.has(accountId)) {
        const existing = waAccounts.get(accountId);
        if (existing?.status === 'connected') {
            await safeReply(ctx, '✅ Этот номер уже подключен!');
            return;
        }
        if (existing?.client) {
            try {
                await existing.client.destroy();
            }
            catch (e) { }
        }
    }
    await safeReply(ctx, '⏳ Создание сессии...\nЭто может занять 10-30 секунд');
    const sessionsPath = process.env.WA_SESSIONS_PATH || './wa-sessions';
    if (!fs.existsSync(sessionsPath)) {
        fs.mkdirSync(sessionsPath, { recursive: true });
    }
    const client = new whatsapp_web_js_1.Client({
        authStrategy: new whatsapp_web_js_1.LocalAuth({
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
        enabled: true,
        sentToday: 0,
        failedToday: 0,
    });
    let qrSent = false;
    client.on('qr', async (qr) => {
        const acc = waAccounts.get(accountId);
        if (acc?.status === 'connected')
            return;
        if (!qrSent) {
            qrSent = true;
            try {
                const qrDataUrl = await QRCode.toDataURL(qr);
                const base64Data = qrDataUrl.replace(/^data:image\/png;base64,/, '');
                const buffer = Buffer.from(base64Data, 'base64');
                await safeReply(ctx, '📱 *Подключение WhatsApp*\n\n' +
                    '1. Откройте WhatsApp на телефоне\n' +
                    '2. Нажмите Настройки → Связанные устройства\n' +
                    '3. Нажмите "Подключить устройство"\n' +
                    '4. Отсканируйте QR-код ниже\n\n' +
                    '⏰ QR-код действителен ~60 секунд', { parse_mode: 'Markdown' });
                await ctx.replyWithPhoto({ source: buffer }, { caption: '📱 *Отсканируйте этот QR-код*', parse_mode: 'Markdown' });
            }
            catch (e) {
                console.error('QR send error:', e);
            }
        }
    });
    client.on('ready', async () => {
        const acc = waAccounts.get(accountId);
        if (acc)
            acc.status = 'connected';
        await safeReply(ctx, `✅ *WhatsApp подключен!*\n\n📞 Номер: ${phone}\n\nТеперь вы можете:\n` +
            `• Настроить текст рассылки в "Аккаунты"\n` +
            `• Запустить рассылку в архивные чаты`, { parse_mode: 'Markdown' });
    });
    client.on('auth_failure', async (msg) => {
        const acc = waAccounts.get(accountId);
        if (acc)
            acc.status = 'disconnected';
        await safeReply(ctx, `❌ Ошибка авторизации: ${msg}`);
    });
    client.on('disconnected', async () => {
        const acc = waAccounts.get(accountId);
        if (acc)
            acc.status = 'disconnected';
        await safeReply(ctx, `⚠️ Аккаунт отключился`);
    });
    try {
        await client.initialize();
    }
    catch (error) {
        console.error('Initialize error:', error);
        const acc = waAccounts.get(accountId);
        if (acc)
            acc.status = 'disconnected';
        await safeReply(ctx, `❌ Ошибка: ${error.message}`);
    }
}
async function sendMessageWithRetry(client, chatId, baseText, messageNumber, maxRetries = 3) {
    let lastError = null;
    const chatIdStr = String(chatId);
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            if (!client.info?.wid)
                throw new Error('Client not ready');
            let messageText = baseText;
            if (messageNumber > 0)
                messageText = varyRussianText(baseText);
            if (attempt === 1)
                await simulateHumanTyping(client, chatIdStr);
            const result = await client.sendMessage(chatIdStr, messageText);
            console.log(`  Successfully sent to ${chatIdStr}, message ID: ${result.id}`);
            return true;
        }
        catch (error) {
            lastError = error;
            console.error(`  Attempt ${attempt} failed:`, error);
            if (attempt < maxRetries) {
                await new Promise(r => setTimeout(r, attempt * 10000));
            }
        }
    }
    return false;
}
async function startBroadcast(ctx, accountId, text) {
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected') {
        return safeReply(ctx, '❌ Аккаунт не подключен');
    }
    isBroadcasting = true;
    if (!acc.enabled) {
        isBroadcasting = false;
        return safeReply(ctx, '❌ Этот аккаунт выключен для рассылки');
    }
    if (acc.sentToday >= MAX_MESSAGES_PER_DAY) {
        isBroadcasting = false;
        return safeReply(ctx, `❌ Достигнут дневной лимит (${MAX_MESSAGES_PER_DAY} сообщений)`);
    }
    await safeReply(ctx, '🔍 Поиск архивных чатов...');
    try {
        const chats = await acc.client.getChats();
        const archivedChats = chats.filter((chat) => chat.archived);
        if (archivedChats.length === 0) {
            isBroadcasting = false;
            return safeReply(ctx, '❌ Архивные чаты не найдены');
        }
        const groups = archivedChats.filter((c) => c.isGroup);
        const individuals = archivedChats.filter((c) => !c.isGroup);
        const broadcastId = Date.now().toString();
        activeBroadcasts.set(broadcastId, { stop: false, sent: 0, failed: 0, total: archivedChats.length, accountId });
        const estimatedMin = Math.ceil(archivedChats.length * (FIXED_DELAY / 60000));
        const hours = Math.floor(estimatedMin / 60);
        const mins = estimatedMin % 60;
        await safeReply(ctx, `🔔 *Рассылка началась!*\n\n` +
            `📱 Аккаунт: ${acc.name || acc.phone}\n` +
            `📊 Найдено: ${archivedChats.length} чатов\n   👥 Групп: ${groups.length}\n   👤 Диалогов: ${individuals.length}\n\n` +
            `🛡️ *Защита:*\n   ⏱ Задержка: 10 мин (фиксированная)\n   📊 Лимит/день: ${MAX_MESSAGES_PER_DAY}\n\n` +
            `⏱ Примерное время: ~${hours}ч ${mins}мин\n\n` +
            `⏳ Рассылка по кругу пока не остановите...\n` +
            `🛑 Для остановки нажмите "⏹ Стоп"`, { parse_mode: 'Markdown' });
        let sent = 0;
        let failed = 0;
        let loopCount = 0;
        let chatIndex = 0;
        while (true) {
            const broadcast = activeBroadcasts.get(broadcastId);
            if (broadcast?.stop) {
                isBroadcasting = false;
                await safeReply(ctx, `⏹ *Рассылка остановлена*\n\n✅ Отправлено: ${sent}\n❌ Ошибок: ${failed}\n📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`, { parse_mode: 'Markdown' });
                activeBroadcasts.delete(broadcastId);
                return;
            }
            if (acc.sentToday >= MAX_MESSAGES_PER_DAY) {
                await safeReply(ctx, `⚠️ *Достигнут дневной лимит*\n\nОтправлено сегодня: ${acc.sentToday}\nМаксимум: ${MAX_MESSAGES_PER_DAY}\n\n⏳ Ожидаем сброса лимита...`, { parse_mode: 'Markdown' });
                await new Promise(r => setTimeout(r, 60 * 60 * 1000));
                continue;
            }
            if (chatIndex >= archivedChats.length) {
                chatIndex = 0;
                loopCount++;
                await safeReply(ctx, `🔄 *Новый круг рассылки #${loopCount + 1}*\n\n✅ Отправлено: ${sent}\n❌ Ошибок: ${failed}\n📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`, { parse_mode: 'Markdown' });
            }
            const chat = archivedChats[chatIndex];
            const chatId = String(chat.id?._serialized || chat.id);
            if (!chatId) {
                chatIndex++;
                continue;
            }
            const currentMsgNum = loopCount * archivedChats.length + chatIndex + 1;
            console.log(`[Круг ${loopCount + 1}, ${chatIndex + 1}/${archivedChats.length}] Sending to: ${chat.name || chatId}`);
            try {
                const success = await sendMessageWithRetry(acc.client, chatId, text, currentMsgNum, 3);
                if (success) {
                    sent++;
                    acc.sentToday++;
                }
                else {
                    failed++;
                    acc.failedToday++;
                }
                if (currentMsgNum % 3 === 0) {
                    await safeReply(ctx, `📊 Круг ${loopCount + 1} | Сообщение ${chatIndex + 1}/${archivedChats.length}\n` +
                        `✅ ${sent} | ❌ ${failed}\n` +
                        `📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`);
                }
            }
            catch (error) {
                failed++;
                acc.failedToday++;
            }
            chatIndex++;
            // ФИКСИРОВАННАЯ задержка 10 минут
            const delay = getFixedDelay();
            console.log(`Waiting ${Math.round(delay / 60000)} minutes before next message...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
    catch (error) {
        isBroadcasting = false;
        console.error('Broadcast error:', error);
        await safeReply(ctx, `❌ Ошибка: ${error.message}`);
    }
}
async function startJoinProcess(ctx, accountId, links) {
    const acc = waAccounts.get(accountId);
    if (!acc || acc.status !== 'connected')
        return safeReply(ctx, '❌ Аккаунт не подключен');
    const validLinks = [];
    for (const link of links) {
        const code = extractInviteCode(link);
        if (code)
            validLinks.push(code);
    }
    if (validLinks.length === 0)
        return safeReply(ctx, '❌ Не найдено валидных ссылок');
    const joinId = Date.now().toString();
    activeJoins.set(joinId, { stop: false, joined: 0, failed: 0, skippedApproval: 0, total: validLinks.length, accountId });
    await safeReply(ctx, `🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc.name || acc.phone}\n` +
        `📊 Найдено ссылок: ${validLinks.length}\n` +
        `🚫 Пропускаем чаты требующие одобрения\n\n` +
        `⏳ Начинаем...`, { parse_mode: 'Markdown' });
    let joined = 0, alreadyJoined = 0, failed = 0, skippedApproval = 0, joinsSinceBreak = 0;
    for (let i = 0; i < validLinks.length; i++) {
        const join = activeJoins.get(joinId);
        if (join?.stop) {
            await safeReply(ctx, `⏹ Остановлено\n✅ Присоединено: ${joined} | ❌ Ошибок: ${failed} | 🚫 Пропущено: ${skippedApproval}`);
            activeJoins.delete(joinId);
            return;
        }
        if (joinsSinceBreak >= MAX_JOINS_BEFORE_BREAK) {
            await safeReply(ctx, `⏸ *Перерыв 4 часа*\n\nПосле ${MAX_JOINS_BEFORE_BREAK} присоединений - перерыв\nОжидаем 4 часа...`, { parse_mode: 'Markdown' });
            await new Promise(r => setTimeout(r, JOIN_BREAK_DURATION));
            joinsSinceBreak = 0;
            await safeReply(ctx, `✅ Перерыв завершён, продолжаем...`);
        }
        const inviteCode = validLinks[i];
        console.log(`[${i + 1}/${validLinks.length}] Joining: ${inviteCode}`);
        try {
            const result = await joinGroupByInvite(acc.client, inviteCode);
            if (result.success) {
                if (result.alreadyJoined)
                    alreadyJoined++;
                else {
                    joined++;
                    joinsSinceBreak++;
                }
            }
            else if (result.needsApproval) {
                // Чат требует одобрения - пропускаем
                skippedApproval++;
                console.log(`  🚫 Skipped (requires approval)`);
            }
            else {
                failed++;
            }
            if (i % 3 === 0 || i === validLinks.length - 1) {
                await safeReply(ctx, `📊 ${i + 1}/${validLinks.length}\n` +
                    `✅ Присоединено: ${joined}\n` +
                    `⏭️ Уже в группе: ${alreadyJoined}\n` +
                    `❌ Ошибок: ${failed}\n` +
                    `🚫 Пропущено (одобрение): ${skippedApproval}`);
            }
        }
        catch (error) {
            failed++;
        }
        if (i < validLinks.length - 1) {
            const delay = getRandomJoinDelay();
            console.log(`Waiting ${Math.round(delay / 60000)} minutes before next join...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
    pendingGroupLinks.delete(accountId);
    await safeReply(ctx, `🏁 *Присоединение завершено*\n\n` +
        `✅ Присоединено: ${joined}\n` +
        `⏭️ Уже в группе: ${alreadyJoined}\n` +
        `❌ Ошибок: ${failed}\n` +
        `🚫 Пропущено (требовало одобрение): ${skippedApproval}`, { parse_mode: 'Markdown' });
    activeJoins.delete(joinId);
}
// Global error handler
bot.catch((err, ctx) => {
    console.error('Bot error:', err);
    safeReply(ctx, '⚠️ Ошибка. Попробуйте еще раз.');
});
async function setupBotMenu() {
    try {
        await bot.telegram.setMyCommands([
            { command: 'start', description: 'Главное меню' },
            { command: 'status', description: 'Статус бота' },
            { command: 'broadcast', description: 'Начать рассылку' },
            { command: 'stop', description: 'Остановить рассылку' },
            { command: 'accounts', description: 'Управление аккаунтами' }
        ]);
    }
    catch (e) { }
}
let isBroadcasting = false;
function startChromeAutoRestart() {
    setInterval(async () => {
        if (isBroadcasting)
            return;
        console.log('🔄 Scheduled Chrome restart...');
        for (const [id, acc] of waAccounts) {
            if (acc.status === 'connected') {
                try {
                    await acc.client.destroy();
                    acc.status = 'disconnected';
                }
                catch (e) { }
            }
        }
    }, 6 * 60 * 60 * 1000);
}
async function main() {
    console.log('🚀 Starting bot...');
    console.log('Admin IDs:', ADMIN_IDS);
    await setupBotMenu();
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