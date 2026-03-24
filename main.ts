import { Telegraf, Markup } from 'telegraf';
import { Client, LocalAuth } from 'whatsapp-web.js';
import * as QRCode from 'qrcode';
import * as fs from 'fs';
import * as path from 'path';
import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import express from 'express';
import 'dotenv/config';
import AdmZip from 'adm-zip';

// === EXPRESS СЕРВЕР ДЛЯ HEALTH CHECK ===
const app = express();
const PORT = process.env.PORT || 3000;

// Файл для хранения данных аккаунтов (восстановление после рестарта)
const ACCOUNTS_FILE_PATH = './data/accounts.json';

interface AccountData {
  phone: string;
  name: string;
  customMessage: string;
  enabled: boolean;
  sentToday: number;
  failedToday: number;
  broadcastDelay: number;
  joinDelay: number;
  autoJoinPending: boolean;
  messagesBeforePause: number;  // Лимит сообщений перед паузой
  pauseDurationMinutes: number;  // Длительность паузы в минутах
}

// Сохранить данные аккаунтов
function saveAccountsData(): void {
  try {
    const data: { [id: string]: AccountData } = {};
    waAccounts.forEach((acc, id) => {
      data[id] = {
        phone: acc.phone || '',
        name: acc.name || '',
        customMessage: acc.customMessage || '',
        enabled: acc.enabled,
        sentToday: acc.sentToday,
        failedToday: acc.failedToday,
        broadcastDelay: acc.broadcastDelay || 10,
        joinDelay: acc.joinDelay || 10,
        autoJoinPending: acc.autoJoinPending,
        messagesBeforePause: (acc as any).messagesBeforePause || 100,
        pauseDurationMinutes: (acc as any).pauseDurationMinutes || 10
      };
    });
    const dir = ACCOUNTS_FILE_PATH.substring(0, ACCOUNTS_FILE_PATH.lastIndexOf('/'));
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(ACCOUNTS_FILE_PATH, JSON.stringify(data, null, 2));
  } catch (error) {
    console.error('Error saving accounts data:', error);
  }
}

// Загрузить данные аккаунтов
function loadAccountsData(): { [id: string]: AccountData } {
  try {
    if (fs.existsSync(ACCOUNTS_FILE_PATH)) {
      const data = fs.readFileSync(ACCOUNTS_FILE_PATH, 'utf-8');
      return JSON.parse(data);
    }
  } catch (error) {
    console.error('Error loading accounts data:', error);
  }
  return {};
}

// Автоматическое восстановление аккаунтов
async function restoreExistingAccounts(): Promise<void> {
  const savedAccounts = loadAccountsData();
  // Railway автоматически сохраняет /data между деплоями!
  const sessionsBasePath = process.env.WA_SESSIONS_PATH || '/data/wa-sessions';

  for (const [accountId, accData] of Object.entries(savedAccounts)) {
    const sessionPath = `${sessionsBasePath}/${accountId}`;

    // Проверяем есть ли папка сессии
    if (!fs.existsSync(sessionPath)) {
      console.log(`⚠️ No session folder for ${accountId}, skipping...`);
      continue;
    }

    console.log(`🔄 Restoring account: ${accountId} (${accData.phone})`);

    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: accountId,
        dataPath: sessionPath,
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
          '--disable-web-security',
          '--disable-features=IsolateOrigins,site-per-process',
        ],
        ignoreHTTPSErrors: true,
      },
    });

    waAccounts.set(accountId, {
      client,
      status: 'connecting',
      phone: accData.phone,
      name: accData.name || accData.phone,
      customMessage: accData.customMessage || '',
      enabled: accData.enabled,
      sentToday: accData.sentToday,
      failedToday: accData.failedToday,
      broadcastDelay: accData.broadcastDelay || 10,
      joinDelay: accData.joinDelay || 10,
      isBroadcasting: false,
      isJoining: false,
      autoJoinPending: accData.autoJoinPending || false,
      messagesBeforePause: accData.messagesBeforePause || 100,
      pauseDurationMinutes: accData.pauseDurationMinutes || 10
    } as any);

    client.on('ready', async () => {
      const acc = waAccounts.get(accountId);
      if (acc) {
        acc.status = 'connected';
        console.log(`✅ Account restored: ${accountId} (${acc.phone})`);
        saveAccountsData();
      }
    });

    client.on('disconnected', async () => {
      const acc = waAccounts.get(accountId);
      if (acc) {
        acc.status = 'disconnected';
        console.log(`⚠️ Account disconnected: ${accountId}`);
        saveAccountsData();

        // ONE-TIME уведомление об отключении (не спамим)
        const now = Date.now();
        const lastNotified = lastDisconnectNotification.get(accountId) || 0;
        if (now - lastNotified > DISCONNECT_NOTIFICATION_COOLDOWN) {
          lastDisconnectNotification.set(accountId, now);
          // Уведомляем админов
          const msg = `⚠️ *Отключение аккаунта*\n\n📱 Аккаунт: ${acc.name || acc.phone || accountId}\n\n🔄 Бот автоматически восстановит сессию`;
          for (const adminId of ADMIN_IDS) {
            try {
              await bot.telegram.sendMessage(adminId, msg, { parse_mode: 'Markdown' });
            } catch (e) {
              // Игнорируем ошибки отправки
            }
          }
        }
      }
    });

    client.on('auth_failure', async (msg) => {
      const acc = waAccounts.get(accountId);
      if (acc) {
        acc.status = 'disconnected';
        console.log(`❌ Auth failure for ${accountId}: ${msg}`);
      }
    });

    // Задержка перед инициализацией
    await new Promise(r => setTimeout(r, 2000));

    // Инициализация с повторными попытками
    let initAttempts = 0;
    const maxInitAttempts = 3;
    let initSuccess = false;

    while (initAttempts < maxInitAttempts && !initSuccess) {
      try {
        initAttempts++;
        console.log(`🔄 Restoring ${accountId} (attempt ${initAttempts}/${maxInitAttempts})...`);
        await client.initialize();
        initSuccess = true;
        console.log(`✅ Account ${accountId} restored successfully`);
      } catch (error) {
        const errorMsg = (error as Error)?.message || String(error);
        console.error(`❌ Restore attempt ${initAttempts} failed for ${accountId}:`, errorMsg);

        // Проверяем критические ошибки
        if (errorMsg.includes('Execution context') ||
            errorMsg.includes('detached Frame') ||
            errorMsg.includes('Target closed') ||
            errorMsg.includes('Protocol error')) {
          // Пробуем снова
          if (initAttempts < maxInitAttempts) {
            await new Promise(r => setTimeout(r, 3000));
          }
        } else {
          // Другая ошибка - выходим
          console.log(`⚠️ Non-recoverable error for ${accountId}, skipping`);
          break;
        }
      }
    }

    if (!initSuccess) {
      console.error(`❌ Failed to restore ${accountId} after ${maxInitAttempts} attempts`);
      const acc = waAccounts.get(accountId);
      if (acc) acc.status = 'disconnected';
    }
  }

  // Ждём немного для инициализации
  await new Promise(r => setTimeout(r, 5000));
  console.log(`📱 Restored ${waAccounts.size} accounts`);
}

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
  enabled: boolean;
  sentToday: number;
  failedToday: number;
  broadcastDelay: number;     // Задержка для рассылки в минутах
  joinDelay: number;          // Задержка для присоединения в минутах
  isBroadcasting: boolean;    // Активна ли рассылка
  isJoining: boolean;          // Активен ли процесс присоединения
  autoJoinPending: boolean;    // Авто-повтор для pending групп
}>();

const userStates = new Map<number, { action: string; accountId?: string; chatCount?: number }>();
const activeBroadcasts = new Map<string, {
  stop: boolean;
  sent: number;
  failed: number;
  consecutiveErrors: number;
  total: number;
  accountId: string;
  messagesBeforePause: number;
  pauseDurationMinutes: number;
  isPaused: boolean;
  pauseEndTime?: number;
  messagesSincePause: number;
}>();
const pendingGroupLinks = new Map<string, string[]>();

const isAdmin = (userId: number) => ADMIN_IDS.includes(userId);

// === КОНФИГУРАЦИЯ АНТИ-БАНА ===
const FIXED_DELAY = 10 * 60 * 1000;  // ФИКСИРОВАННЫЕ 10 минут
const MAX_MESSAGES_PER_DAY = 999999; // ЛИМИТ ОТКЛЮЧЁН
const NIGHT_PAUSE_START = 24;       // Отключена
const NIGHT_PAUSE_END = 24;

// === ЗАЩИТА ОТ БАНА ===
const MAX_CONSECUTIVE_ERRORS = 3;      // После 3 ошибок - пауза 1 час
const ERROR_PAUSE_DURATION = 60;       // 60 минут пауза при ошибках
const MAX_ERROR_PAUSE_CYCLES = 2;      // После 2 таких пауз - полная остановка

// Конфигурация авто-присоединения
const JOIN_DELAY_MIN = 10 * 60 * 1000;
const JOIN_DELAY_MAX = 10 * 60 * 1000;
const MAX_JOINS_BEFORE_BREAK = 30;
const JOIN_BREAK_DURATION = 4 * 60 * 60 * 1000;
const MAX_PENDING_JOIN_REQUESTS = 5;  // Максимум 5 ожидающих заявок

const activeJoins = new Map<string, {
  stop: boolean;
  joined: number;
  failed: number;
  skippedApproval: number;
  total: number;
  accountId: string;
}>();

// === АКТИВНЫЕ ПРОЦЕССЫ ПУЛА ===
const activePoolJoins = new Map<string, {
  stop: boolean;
  joined: number;
  pending: number;
  failed: number;
  already: number;
  total: number;
  accountId: string;
}>();

// === СИСТЕМА PENDING ЗАЯВОК ===
interface PendingRequest {
  inviteCode: string;
  fullLink: string;
  addedAt: Date;
  status: 'pending' | 'approved' | 'rejected' | 'expired';
}

// Хранилище pending заявок: accountId -> PendingRequest[]
const pendingJoinRequests = new Map<string, PendingRequest[]>();

// Активный процесс обработки pending заявок
const activePendingProcessor = new Map<string, {
  stop: boolean;
  total: number;
  pending: number;
  approved: number;
  rejected: number;
}>();

// === СИСТЕМА УВЕДОМЛЕНИЙ ОБ ОТКЛЮЧЕНИИ ===
// Храним время последнего уведомления об отключении (one-time notification)
const lastDisconnectNotification = new Map<string, number>();
const DISCONNECT_NOTIFICATION_COOLDOWN = 60 * 60 * 1000; // 1 час между уведомлениями

// === ФУНКЦИЯ ПРЕДВАРИТЕЛЬНОЙ ПРОВЕРКИ ГРУППЫ (ВАРИАНТ A) ===
// Определяет тип ссылки ДО попытки присоединения
async function previewGroupInvite(client: Client, inviteCode: string): Promise<{
  type: 'working' | 'pending' | 'failed' | 'already' | 'unknown';
  groupName?: string;
  error?: string;
}> {
  try {
    // Проверяем что сессия валидна
    if (!client.info?.wid) {
      return { type: 'failed', error: 'Сессия неактивна' };
    }

    // Сначала проверяем, не состоим ли уже в группе
    const allChats = await client.getChats();
    const existingChat = allChats.find((chat: any) =>
      chat.isGroup && chat.link && chat.link.includes(inviteCode)
    );
    if (existingChat) {
      return { type: 'already', groupName: existingChat.name };
    }

    // Пытаемся получить информацию о группе через Web API
    // Используем queryExistingGroup для проверки
    try {
      // Пробуем создать приглашение - это позволяет проверить тип группы
      const groupInfo = await (client as any).inviteCodeInfo?.(inviteCode) ||
                        await (client as any).getGroupInviteLinkInfo?.(inviteCode);

      if (groupInfo) {
        // Группа существует и доступна для просмотра
        // Проверяем, требуется ли одобрение
        if (groupInfo.announce || groupInfo.restrict || groupInfo.joinApprovalMode) {
          return { type: 'pending', groupName: groupInfo.subject || 'Неизвестная группа' };
        }
        return { type: 'working', groupName: groupInfo.subject || 'Неизвестная группа' };
      }
    } catch (infoError) {
      // Игнорируем ошибки получения инфо
    }

    // Прямая попытка присоединения для определения типа
    try {
      await client.acceptInvite(inviteCode);
      return { type: 'working' };
    } catch (joinError: any) {
      const errMsg = (joinError.message || String(joinError)).toLowerCase();

      // Проверяем на "требуется одобрение"
      if (errMsg.includes('approval') ||
          errMsg.includes('join') ||
          errMsg.includes('require') ||
          errMsg.includes('admin') ||
          errMsg.includes('401') ||
          errMsg.includes('request')) {
        return { type: 'pending', error: 'Требуется одобрение администратора' };
      }

      // Проверяем на "недействительна/истекла"
      if (errMsg.includes('invalid') ||
          errMsg.includes('expired') ||
          errMsg.includes('not found') ||
          errMsg.includes('404') ||
          errMsg.includes('сброшено')) {
        return { type: 'failed', error: 'Ссылка недействительна или истекла' };
      }

      // Проверяем на "уже состоишь"
      if (errMsg.includes('already') || errMsg.includes('member')) {
        return { type: 'already' };
      }

      return { type: 'unknown', error: joinError.message || 'Неизвестная ошибка' };
    }
  } catch (error: any) {
    const errMsg = (error.message || String(error)).toLowerCase();

    // Анализируем ошибку
    if (errMsg.includes('invalid') || errMsg.includes('expired') || errMsg.includes('not found') || errMsg.includes('404') || errMsg.includes('сброшено')) {
      return { type: 'failed', error: 'Ссылка недействительна или истекла' };
    }

    if (errMsg.includes('approval') || errMsg.includes('join') || errMsg.includes('require') || errMsg.includes('admin') || errMsg.includes('401')) {
      return { type: 'pending', error: 'Требуется одобрение' };
    }

    return { type: 'unknown', error: error.message || 'Ошибка проверки' };
  }
}

// === СИСТЕМА ПЕРСИСТЕНТНОГО ПУЛА (ЕДИНЫЙ ПУЛ ДЛЯ ВСЕХ АККАУНТОВ) ===
const POOL_FILE_PATH = process.env.POOL_FILE_PATH || './data/pool.json';

interface PoolLink {
  inviteCode: string;
  fullLink: string;
  status: 'ready' | 'pending' | 'joined' | 'failed';
  addedAt: string;
  processedAt?: string;
  error?: string;
}

interface Pool {
  version: number;
  links: PoolLink[];  // Единый массив для всех ссылок
  globalStats: {
    total: number;
    ready: number;
    pending: number;
    joined: number;
    failed: number;
  };
}

// Инициализация пула
function initPool(): Pool {
  return {
    version: 2, // Обновленная версия
    links: [],
    globalStats: {
      total: 0,
      ready: 0,
      pending: 0,
      joined: 0,
      failed: 0
    }
  };
}

// Загрузить пул из файла
function loadPool(): Pool {
  try {
    if (fs.existsSync(POOL_FILE_PATH)) {
      const data = fs.readFileSync(POOL_FILE_PATH, 'utf-8');
      const pool = JSON.parse(data);

      // Миграция с версии 1 (accounts-based) на версию 2 (global)
      if (pool.version === 1) {
        console.log('🔄 Migrating pool from v1 to v2...');
        const newPool = initPool();
        // Собираем все ready ссылки из всех аккаунтов
        for (const accountId in pool.accounts) {
          const acc = pool.accounts[accountId];
          if (acc.ready) {
            newPool.links.push(...acc.ready);
            newPool.globalStats.ready += acc.ready.length;
          }
          if (acc.pending) {
            newPool.links.push(...acc.pending);
            newPool.globalStats.pending += acc.pending.length;
          }
          if (acc.joined) {
            newPool.links.push(...acc.joined);
            newPool.globalStats.joined += acc.joined.length;
          }
          if (acc.failed) {
            newPool.links.push(...acc.failed);
            newPool.globalStats.failed += acc.failed.length;
          }
        }
        newPool.globalStats.total = newPool.links.length;
        savePool(newPool);
        return newPool;
      }

      return pool;
    }
  } catch (error) {
    console.error('Error loading pool:', error);
  }
  return initPool();
}

// Сохранить пул в файл
function savePool(pool: Pool): void {
  try {
    const dir = POOL_FILE_PATH.substring(0, POOL_FILE_PATH.lastIndexOf('/'));
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(POOL_FILE_PATH, JSON.stringify(pool, null, 2));
  } catch (error) {
    console.error('Error saving pool:', error);
  }
}

// Глобальный пул (в памяти)
let globalPool: Pool = loadPool();

// Пересчитать статистику пула
function recalculatePoolStats(): void {
  globalPool.globalStats = {
    total: globalPool.links.length,
    ready: 0,
    pending: 0,
    joined: 0,
    failed: 0
  };
  for (const link of globalPool.links) {
    globalPool.globalStats[link.status]++;
  }
}

// Добавить ссылки в глобальный пул
function addLinksToPool(links: string[]): { added: number; duplicates: number } {
  let added = 0;
  let duplicates = 0;

  // Собираем все существующие коды
  const existingCodes = new Set<string>();
  for (const link of globalPool.links) {
    existingCodes.add(link.inviteCode);
  }

  for (const link of links) {
    const code = extractInviteCode(link);

    if (!code) continue;

    if (existingCodes.has(code)) {
      duplicates++;
      continue;
    }

    const poolLink: PoolLink = {
      inviteCode: code,
      fullLink: link,
      status: 'ready',
      addedAt: new Date().toISOString()
    };

    globalPool.links.push(poolLink);
    globalPool.globalStats.total++;
    globalPool.globalStats.ready++;
    existingCodes.add(code);
    added++;
  }

  savePool(globalPool);
  return { added, duplicates };
}

// Получить статистику пула
function getPoolStats(): { total: number; ready: number; pending: number; joined: number; failed: number } {
  return { ...globalPool.globalStats };
}

// Получить ссылки для обработки (РАНДОМНО из глобального пула)
function getReadyLinks(count: number): PoolLink[] {
  const readyLinks = globalPool.links.filter(l => l.status === 'ready');
  // Рандомно перемешиваем
  const shuffled = shuffleArray(readyLinks);
  return shuffled.slice(0, Math.min(count, shuffled.length));
}

// Обновить статус ссылки
function updateLinkStatus(inviteCode: string, status: PoolLink['status'], error?: string): void {
  const link = globalPool.links.find(l => l.inviteCode === inviteCode);
  if (!link) return;

  // Обновить глобальную статистику
  globalPool.globalStats[link.status]--;

  // Обновить статус
  link.status = status;
  link.processedAt = new Date().toISOString();
  if (error) link.error = error;

  globalPool.globalStats[status]++;

  savePool(globalPool);
}

// Получить все pending ссылки
function getAllPendingLinks(): PoolLink[] {
  return globalPool.links.filter(l => l.status === 'pending');
}

// Количество ready ссылок в глобальном пуле
function getReadyCount(): number {
  return globalPool.globalStats.ready;
}

// Количество pending ссылок в глобальном пуле
function getPendingCount(): number {
  return globalPool.globalStats.pending;
}

// Получить статистику глобального пула
function getGlobalPoolStats(): { ready: number; pending: number; joined: number; failed: number; total: number } {
  return { ...globalPool.globalStats };
}

// Очистить пул (только processed = joined + failed)
function clearProcessedPool(): void {
  const toRemove = globalPool.links.filter(l => l.status === 'joined' || l.status === 'failed');
  for (const link of toRemove) {
    globalPool.globalStats[link.status]--;
  }
  globalPool.links = globalPool.links.filter(l => l.status !== 'joined' && l.status !== 'failed');
  globalPool.globalStats.total = globalPool.links.length;
  savePool(globalPool);
}

// Очистить весь пул
function clearAllPool(): void {
  globalPool = initPool();
  savePool(globalPool);
}

// Добавить заявку в очередь
function addToPendingQueue(accountId: string, inviteCode: string, fullLink: string) {
  const queue = pendingJoinRequests.get(accountId) || [];
  // Проверяем есть ли уже эта заявка
  if (!queue.find(p => p.inviteCode === inviteCode)) {
    queue.push({
      inviteCode,
      fullLink,
      addedAt: new Date(),
      status: 'pending'
    });
    pendingJoinRequests.set(accountId, queue);
    console.log(`📋 Added to pending queue: ${inviteCode}`);
  }
}

// Получить количество активных pending заявок
function getActivePendingCount(accountId: string): number {
  const queue = pendingJoinRequests.get(accountId) || [];
  return queue.filter(p => p.status === 'pending').length;
}

// Получить следующую заявку для подачи
function getNextPendingRequest(accountId: string): PendingRequest | null {
  const queue = pendingJoinRequests.get(accountId) || [];
  return queue.find(p => p.status === 'pending') || null;
}

// Удалить заявку из очереди
function removeFromPendingQueue(accountId: string, inviteCode: string) {
  const queue = pendingJoinRequests.get(accountId) || [];
  const filtered = queue.filter(p => p.inviteCode !== inviteCode);
  pendingJoinRequests.set(accountId, filtered);
}

// === БЕЗОПАСНЫЕ ОБЕРТКИ ===
const safeReply = async (ctx: any, text: string, extra?: any) => {
  try {
    await ctx.reply(text, extra).catch(() => {});
  } catch (e) {
    console.error('Reply error:', e);
  }
};

const safeEdit = async (ctx: any, text: string, extra?: any) => {
  try {
    await ctx.editMessageText(text, extra).catch(() => {});
  } catch (e) {
    console.error('Edit error:', e);
  }
};

const safeAnswerCb = async (ctx: any, text?: string) => {
  try {
    await ctx.answerCbQuery(text).catch(() => {});
  } catch (e) {
    // Ignore
  }
};

// === ФУНКЦИИ АНТИ-БАНА ===

function getFixedDelay(): number {
  return FIXED_DELAY;
}

function isNightPause(): boolean {
  const hour = new Date().getHours();
  return hour >= NIGHT_PAUSE_START && hour < NIGHT_PAUSE_END;
}

function getTimeUntilMorning(): number {
  const now = new Date();
  const tomorrow = new Date(now);
  tomorrow.setDate(tomorrow.getDate() + 1);
  tomorrow.setHours(NIGHT_PAUSE_END, 0, 0, 0);
  return tomorrow.getTime() - now.getTime();
}

function varyRussianText(text: string): string {
  const replacements: { [key: string]: string } = {
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

async function simulateHumanTyping(client: Client, chatId: string, duration: number = 2000) {
  try {
    await client.sendPresenceAvailable();
    const typingTime = duration + Math.floor(Math.random() * 2000);
    await new Promise(r => setTimeout(r, typingTime));
  } catch (e) {}
}

// Функция для получения задержки с ±1 минутой рандомом
function getDelayWithRandom(baseDelayMinutes: number): number {
  // ±1 минута рандом
  const randomMinutes = baseDelayMinutes + (Math.random() * 2 - 1);
  return Math.round(randomMinutes * 60 * 1000); // в миллисекунды
}

function getRandomJoinDelay(accountId?: string): number {
  if (accountId) {
    const acc = waAccounts.get(accountId);
    if (acc) {
      return getDelayWithRandom(acc.joinDelay);
    }
  }
  return getDelayWithRandom(10); // По умолчанию 10 минут
}

// Получить задержку для рассылки с учётом настроек аккаунта
function getBroadcastDelay(accountId: string): number {
  const acc = waAccounts.get(accountId);
  if (acc) {
    return getDelayWithRandom(acc.broadcastDelay);
  }
  return getDelayWithRandom(10); // По умолчанию 10 минут
}

// Проверка валидности сессии WhatsApp
async function isSessionValid(client: Client): Promise<boolean> {
  try {
    // Проверяем базовые свойства клиента
    if (!client.info?.wid) {
      return false;
    }
    // Пытаемся получить список чатов - если сессия валидна, это работает
    await client.getChats();
    return true;
  } catch (error) {
    const errorMsg = (error as Error)?.message || String(error);
    // Detached Frame и другие ошибки сессии
    if (errorMsg.includes('detached Frame') || errorMsg.includes('Evaluation failed') || errorMsg.includes('not ready')) {
      return false;
    }
    // Другие ошибки могут означать что сессия ещё инициализируется
    return false;
  }
}

// Проверка и обновление статуса всех аккаунтов
async function refreshAccountsStatus(): Promise<void> {
  for (const [accountId, acc] of waAccounts) {
    if (acc.client && acc.status === 'connected') {
      const valid = await isSessionValid(acc.client);
      if (!valid) {
        acc.status = 'disconnected';
        console.log(`⚠️ Account ${accountId} session invalid - marked as disconnected`);
      }
    }
  }
  saveAccountsData();
}

function extractInviteCode(link: string): string | null {
  const patterns = [
    /chat\.whatsapp\.com\/([a-zA-Z0-9]{20,})/i,
    /invite\.whatsapp\.com\/([a-zA-Z0-9]{20,})/i,
  ];
  for (const pattern of patterns) {
    const match = link.match(pattern);
    if (match) return match[1];
  }
  if (/^[a-zA-Z0-9]{20,}$/.test(link.trim())) {
    return link.trim();
  }
  return null;
}

async function joinGroupByInvite(client: Client, inviteCode: string): Promise<{ success: boolean; error?: string; needsApproval?: boolean; alreadyJoined?: boolean; skipped?: boolean; sessionInvalid?: boolean; linkType?: 'working' | 'pending' | 'failed' | 'unknown' }> {
  try {
    // Проверяем что сессия валидна
    if (!client.info?.wid) {
      console.log(`  ⚠️ Session invalid - client not ready`);
      return { success: false, error: 'Сессия неактивна', sessionInvalid: true };
    }
    console.log(`  Checking/joining group with invite code: ${inviteCode}`);

    // === ВАРИАНТ A: Предварительная проверка типа ссылки ===
    const preview = await previewGroupInvite(client, inviteCode);
    console.log(`  📋 Preview: ${preview.type}${preview.groupName ? ` (${preview.groupName})` : ''}`);

    // Возвращаем тип ссылки
    if (preview.type === 'already') {
      return { success: true, alreadyJoined: true, linkType: 'working' };
    }

    if (preview.type === 'failed') {
      return { success: false, error: preview.error || 'Ссылка недействительна', linkType: 'failed' };
    }

    if (preview.type === 'pending') {
      return { success: false, error: preview.error || 'Требуется одобрение', needsApproval: true, linkType: 'pending' };
    }

    // Если working или unknown - пробуем присоединиться
    try {
      await client.acceptInvite(inviteCode);
      console.log(`  ✓ Successfully joined group`);
      return { success: true, linkType: 'working' };
    } catch (joinError: any) {
      const joinErrorMsg = joinError.message || String(joinError);

      if (joinErrorMsg.includes('detached Frame') || joinErrorMsg.includes('Evaluation failed')) {
        return { success: false, error: 'Сессия устарела', sessionInvalid: true, linkType: 'failed' };
      }
      if (joinErrorMsg.includes('approval') || joinErrorMsg.includes('join') || joinErrorMsg.includes('require') || joinErrorMsg.includes('Admin') || joinErrorMsg.includes('401')) {
        return { success: false, error: 'Требуется одобрение', needsApproval: true, linkType: 'pending' };
      }
      if (joinErrorMsg.includes('invalid') || joinErrorMsg.includes('expired') || joinErrorMsg.includes('not found')) {
        return { success: false, error: 'Ссылка недействительна', linkType: 'failed' };
      }
      if (joinErrorMsg.includes('already') || joinErrorMsg.includes('member')) {
        return { success: true, alreadyJoined: true, linkType: 'working' };
      }
      throw joinError;
    }
  } catch (error: any) {
    const errorMessage = error.message || String(error);
    console.error(`  ✗ Join failed:`, errorMessage);
    if (errorMessage.includes('detached Frame') || errorMessage.includes('Evaluation failed')) {
      return { success: false, error: 'Сессия устарела', sessionInvalid: true, linkType: 'failed' };
    }
    if (errorMessage.includes('invalid') || errorMessage.includes('expired')) {
      return { success: false, error: 'Ссылка недействительна', linkType: 'failed' };
    }
    if (errorMessage.includes('approval') || errorMessage.includes('join') || errorMessage.includes('require') || errorMessage.includes('Admin') || errorMessage.includes('401')) {
      return { success: false, error: 'Требуется одобрение', needsApproval: true, linkType: 'pending' };
    }
    if (errorMessage.includes('not found') || errorMessage.includes('404')) {
      return { success: false, error: 'Группа не найдена', linkType: 'failed' };
    }
    if (errorMessage.includes('already') || errorMessage.includes('member')) {
      return { success: true, alreadyJoined: true, linkType: 'working' };
    }
    return { success: false, error: errorMessage, linkType: 'unknown' };
  }
}
// === ФУНКЦИИ ПАРСИНГА ФАЙЛОВ ===

// Скачать файл по URL или получить локальный путь
async function downloadFile(filePathOrUrl: string, destPath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    // Если это локальный путь
    if (fs.existsSync(filePathOrUrl)) {
      fs.copyFileSync(filePathOrUrl, destPath);
      resolve(destPath);
      return;
    }

    // Если это URL
    const fileUrl = new URL(filePathOrUrl);
    const protocol = fileUrl.protocol === 'https:' ? https : http;

    protocol.get(fileUrl.href, (response) => {
      if (response.statusCode === 200) {
        const writer = fs.createWriteStream(destPath);
        response.pipe(writer);
        writer.on('finish', () => resolve(destPath));
        writer.on('error', reject);
      } else {
        reject(new Error(`HTTP ${response.statusCode}`));
      }
    }).on('error', reject);
  });
}

// Парсинг DOCX файла
function parseDocx(filePath: string): string[] {
  const links: string[] = [];
  try {
    const zip = new AdmZip(filePath);
    const content = zip.readAsText('word/document.xml');

    // Извлекаем все URL
    const urlRegex = /https?:\/\/[^\s<>\"]+/gi;
    const matches = content.match(urlRegex);

    if (matches) {
      for (const url of matches) {
        if (url.includes('chat.whatsapp') || url.includes('invite.whatsapp')) {
          // Убираем параметры после #
          const cleanUrl = url.split('#')[0];
          links.push(cleanUrl);
        }
      }
    }
  } catch (error) {
    console.error('Error parsing DOCX:', error);
  }
  return [...new Set(links)]; // Убираем дубликаты
}

// Парсинг TXT файла
function parseTxt(filePath: string): string[] {
  const links: string[] = [];
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    const lines = content.split(/[\r\n]+/);

    for (const line of lines) {
      const trimmed = line.trim();
      // Проверяем если это URL или просто код
      if (trimmed.includes('chat.whatsapp') || trimmed.includes('invite.whatsapp')) {
        const url = trimmed.split('#')[0].split('?')[0];
        links.push(url);
      } else if (/^[a-zA-Z0-9]{20,}$/.test(trimmed)) {
        // Просто код ссылки
        links.push(`https://chat.whatsapp.com/${trimmed}`);
      }
    }
  } catch (error) {
    console.error('Error parsing TXT:', error);
  }
  return [...new Set(links)]; // Убираем дубликаты
}

// Парсинг файла (определяет тип)
async function parseFile(filePath: string): Promise<string[]> {
  const ext = path.extname(filePath).toLowerCase();

  if (ext === '.docx') {
    return parseDocx(filePath);
  } else if (ext === '.txt' || ext === '.csv') {
    return parseTxt(filePath);
  } else {
    // Пробуем как TXT
    return parseTxt(filePath);
  }
}

// === МЕНЮ ===
const mainMenu = () => {
  const stats = getPoolStats();
  const poolIndicator = stats.total > 0 ? ` (${stats.ready} 🟢)` : '';

  return Markup.inlineKeyboard([
    [Markup.button.callback('📊 Пул чатов' + poolIndicator, 'pool_menu')],
    [Markup.button.callback('📱 Аккаунты', 'accounts')],
    [Markup.button.callback('📨 Рассылка', 'broadcast_menu')],
    [Markup.button.callback('🔗 Присоединение к чатам', 'join_menu')],
    [Markup.button.callback('📋 Pending заявки', 'pending_menu')],
    [Markup.button.callback('⚙️ Настройки', 'settings')],
  ]);
};

const accountsMenu = (accountId?: string) => {
  const buttons: any[][] = [];
  if (accountId) {
    const acc = waAccounts.get(accountId);
    if (acc) {
      const statusEmoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
      const enabledEmoji = acc.enabled ? '✅' : '❌';
      buttons.push([Markup.button.callback(`${enabledEmoji} Рассылка: ${acc.enabled ? 'ВКЛ' : 'ВЫКЛ'}`, `toggle_${accountId}`)]);
      if (acc.status === 'connected') {
        buttons.push([Markup.button.callback('📝 Изменить текст', `edit_msg_${accountId}`)]);
        // Настройки задержек
        buttons.push([Markup.button.callback(`⏱ Рассылка: ${acc.broadcastDelay} мин (±1)`, `set_broadcast_delay_${accountId}`)]);
        buttons.push([Markup.button.callback(`🔗 Присоединение: ${acc.joinDelay} мин (±1)`, `set_join_delay_${accountId}`)]);
        // Настройки защиты от бана
        const messagesBeforePause = (acc as any).messagesBeforePause || 100;
        const pauseDurationMinutes = (acc as any).pauseDurationMinutes || 10;
        buttons.push([Markup.button.callback(`🛡️ Пауза: ${messagesBeforePause} msg / ${pauseDurationMinutes} мин`, `set_pause_limit_${accountId}`)]);
        // Авто-повтор pending
        const autoText = acc.autoJoinPending ? '🔄 Авто-повтор: ВКЛ' : '🔄 Авто-повтор: ВЫКЛ';
        buttons.push([Markup.button.callback(autoText, `toggle_auto_pending_${accountId}`)]);
        buttons.push([Markup.button.callback('🔍 Просмотр чатов', `view_chats_${accountId}`)]);
      } else {
        // Для отключенных аккаунтов показываем кнопку переподключения
        buttons.push([Markup.button.callback('📷 Переподключить (QR)', `reconnect_qr_${accountId}`)]);
        buttons.push([Markup.button.callback('🔄 Проверить статус', `refresh_acc_${accountId}`)]);
      }
      buttons.push([Markup.button.callback('X Отвязать номер', `unbind_${accountId}`)]);
    }
    buttons.push([Markup.button.callback('< Back to list', 'accounts')]);
  } else {
    if (waAccounts.size > 0) {
      waAccounts.forEach((acc, id) => {
        const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
        const enabledEmoji = acc.enabled ? '✅' : '❌';
        // Все аккаунты кликабельны, проверка статуса в обработчике
        buttons.push([Markup.button.callback(`${emoji} ${enabledEmoji} ${acc.name || acc.phone || id}`, `acc_${id}`)]);
      });
      buttons.push([Markup.button.callback('🔄 Проверить все статусы', 'refresh_all_accounts')]);
    }
    buttons.push([Markup.button.callback('📷 QR-код (рекомендуется)', 'add_qr')]);
    buttons.push([Markup.button.callback('◀️ Назад', 'main')]);
  }
  return Markup.inlineKeyboard(buttons);
};

const broadcastSelectMenu = () => {
  const buttons: any[][] = [];
  let hasEnabled = false;
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected' && acc.enabled) {
      hasEnabled = true;
      buttons.push([Markup.button.callback(`📱 ${acc.name || acc.phone}`, `broadcast_acc_${id}`)]);
    }
  });
  if (!hasEnabled) {
    buttons.push([Markup.button.callback('❌ Нет включенных аккаунтов', 'main')]);
  }
  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);
  return Markup.inlineKeyboard(buttons);
};

const settingsMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('🔄 Перезапустить все сессии', 'restart_all')],
  [Markup.button.callback('📊 Статистика', 'stats')],
  [Markup.button.callback('◀️ Назад', 'main')],
]);

const joinSelectMenu = () => {
  const buttons: any[][] = [];
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected') {
      // Показываем статус активного процесса
      let statusIcon = '';
      if (acc.isJoining) statusIcon = ' 🔄';
      else if (acc.isBroadcasting) statusIcon = ' 📤';
      buttons.push([Markup.button.callback(`📱 ${acc.name || acc.phone}${statusIcon}`, `join_acc_${id}`)]);
    }
  });
  if (buttons.length === 0) {
    buttons.push([Markup.button.callback('❌ Нет подключенных аккаунтов', 'main')]);
  }
  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);
  return Markup.inlineKeyboard(buttons);
};

// === PENDING ЗАЯВКИ МЕНЮ ===
const pendingSelectMenu = () => {
  const buttons: any[][] = [];
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected') {
      const pendingCount = getActivePendingCount(id);
      const indicator = pendingCount > 0 ? ` (${pendingCount} 🔔)` : '';
      buttons.push([Markup.button.callback(`📱 ${acc.name || acc.phone}${indicator}`, `pending_acc_${id}`)]);
    }
  });
  if (buttons.length === 0) {
    buttons.push([Markup.button.callback('❌ Нет подключенных аккаунтов', 'main')]);
  }
  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);
  return Markup.inlineKeyboard(buttons);
};

const pendingAccMenu = (accountId: string) => {
  const queue = pendingJoinRequests.get(accountId) || [];
  const pendingItems = queue.filter(p => p.status === 'pending');
  const buttons: any[][] = [];
  const acc = waAccounts.get(accountId);

  // Если идёт процесс присоединения - показываем кнопку остановки
  if (acc?.isJoining) {
    buttons.push([Markup.button.callback('⏹ ОСТАНОВИТЬ присоединение', `stop_join_${accountId}`)]);
  }

  if (pendingItems.length > 0) {
    buttons.push([Markup.button.callback('▶️ Проверить заявки', `check_pending_${accountId}`)]);
    buttons.push([Markup.button.callback('🗑️ Очистить очередь', `clear_pending_${accountId}`)]);
  } else {
    buttons.push([Markup.button.callback('📭 Нет pending заявок', 'pending_menu')]);
  }
  buttons.push([Markup.button.callback('◀️ Назад', 'pending_menu')]);
  return Markup.inlineKeyboard(buttons);
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
      data: (ctx.callbackQuery as any).data,
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
  await safeReply(ctx,
    `${isBroadcasting ? '🟡' : '🟢'} *Статус бота*\n\n` +
    `📊 Статус: ${isBroadcasting ? 'Рассылка активна' : 'Работает'}\n` +
    `🖥 Аккаунтов: ${connected}/${waAccounts.size} подключено\n` +
    `✅ Включено для рассылки: ${enabled}\n` +
    `📤 Отправлено сегодня: ${totalSent}\n` +
    `⏱ Uptime: ${uptime} минут`,
    { parse_mode: 'Markdown' }
  );
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
  } else {
    await safeReply(ctx, '❌ Рассылка не найдена');
  }
});

// Main menu
bot.action('main', async (ctx) => {
  await safeAnswerCb(ctx);
  userStates.delete(ctx.from!.id);
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
  await safeEdit(ctx, text, { parse_mode: 'Markdown', ...accountsMenu() });
});

bot.action(/^acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
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
  if (acc.phone) text += `📞 Телефон: ${acc.phone}\n`;
  text += `\n📈 За сессию:\n`;
  text += `   ✅ Отправлено: ${acc.sentToday}\n`;
  text += `   ❌ Ошибок: ${acc.failedToday}\n`;
  if (acc.customMessage) {
    text += `\n📝 *Текст рассылки:*\n${acc.customMessage}\n`;
  } else {
    text += `\n📝 Текст не задан\n`;
  }
  await safeEdit(ctx, text, { parse_mode: 'Markdown', ...accountsMenu(accountId) });
});

bot.action(/^toggle_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
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

// === ПОЛУЧЕНИЕ ДОКУМЕНТОВ (ФАЙЛЫ) ===
bot.on('document', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;

  const doc = ctx.message.document;
  const fileName = doc.file_name || 'file';

  // Проверяем тип файла
  const ext = fileName.toLowerCase().split('.').pop();
  if (!['docx', 'txt', 'csv'].includes(ext || '')) {
    await safeReply(ctx, '❌ Неподдерживаемый формат.\n\n📋 Поддерживаемые форматы:\n• .docx\n• .txt\n• .csv');
    return;
  }

  // Проверяем подключенные аккаунты
  const connected = [...waAccounts.values()].filter(a => a.status === 'connected');
  if (connected.length === 0) {
    await safeReply(ctx, '❌ Нет подключенных аккаунтов WhatsApp.\n\nСначала подключите аккаунт в разделе "📱 Аккаунты"');
    return;
  }

  try {
    await safeReply(ctx, `📥 Получен файл: ${fileName}\n⏳ Парсинг ссылок...`);

    // Скачиваем файл через Telegram API
    const tempDir = '/tmp/wa-bot-uploads';
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }

    const filePath = `${tempDir}/${Date.now()}_${fileName}`;
    const fileInfo = await ctx.telegram.getFile(doc.file_id);
    const fileUrl = `https://api.telegram.org/file/bot${process.env.TELEGRAM_BOT_TOKEN}/${fileInfo.file_path}`;

    await new Promise<void>((resolve, reject) => {
      const request = https.get(fileUrl, (response) => {
        if (response.statusCode === 200) {
          const fileStream = fs.createWriteStream(filePath);
          response.pipe(fileStream);
          fileStream.on('finish', () => {
            fileStream.close();
            resolve();
          });
          fileStream.on('error', reject);
        } else {
          reject(new Error(`HTTP ${response.statusCode}`));
        }
      });
      request.on('error', reject);
    });

    // Парсим файл
    const links = await parseFile(filePath);

    if (links.length === 0) {
      await safeReply(ctx, '❌ В файле не найдены WhatsApp ссылки.');
      fs.unlinkSync(filePath);
      return;
    }

    // Добавляем в пул
    const result = addLinksToPool(links);

    // Удаляем временный файл
    fs.unlinkSync(filePath);

    // Формируем отчет
    let report = `✅ *Файл обработан!*\n\n`;
    report += `📄 Файл: ${fileName}\n`;
    report += `📊 Найдено ссылок: ${links.length}\n\n`;
    report += `✅ Добавлено в пул: ${result.added}\n`;
    if (result.duplicates > 0) {
      report += `⏭️ Дубликатов пропущено: ${result.duplicates}\n`;
    }

    const stats = getPoolStats();
    report += `\n📈 *Статистика пула:*\n`;
    report += `🟢 Готовы: ${stats.ready}\n`;
    report += `🔔 Pending: ${stats.pending}\n`;
    report += `✅ Присоединено: ${stats.joined}\n`;
    report += `❌ Ошибок: ${stats.failed}`;

    await safeReply(ctx, report, { parse_mode: 'Markdown' });

  } catch (error) {
    console.error('Error processing document:', error);
    await safeReply(ctx, `❌ Ошибка обработки файла: ${(error as Error).message}`);
  }
});

// === МЕНЮ ПУЛА ЧАТОВ ===
const poolMenu = () => {
  const stats = getPoolStats();

  let text = '📊 *Пул чатов*\n\n';
  text += `📈 *Статистика пула:*\n`;
  text += `🟢 Готовы: ${stats.ready}\n`;
  text += `🔔 Pending: ${stats.pending}\n`;
  text += `✅ Присоединено: ${stats.joined}\n`;
  text += `❌ Ошибок: ${stats.failed}\n\n`;
  text += `📱 *Выберите аккаунт:*`;

  const buttons: any[][] = [];

  // Показываем все подключенные аккаунты
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected') {
      buttons.push([Markup.button.callback(
        `📱 ${acc.name || acc.phone}`,
        `pool_acc_${id}`
      )]);
    }
  });

  if (buttons.length === 0) {
    buttons.push([Markup.button.callback('❌ Нет подключенных аккаунтов', 'main')]);
  }

  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);

  return { text, buttons };
};

bot.action('pool_menu', async (ctx) => {
  await safeAnswerCb(ctx);
  const { text, buttons } = poolMenu();
  await safeEdit(ctx, text, {
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: buttons }
  });
});

// Выбор аккаунта -> запрос количества
bot.action(/^pool_acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeEdit(ctx, '❌ Аккаунт не подключен', { ...poolMenu().buttons });
    return;
  }

  // Проверяем, не идёт ли уже процесс
  if (acc.isJoining) {
    await safeEdit(ctx,
      `⏳ *Присоединение уже идёт!*\n\n` +
      `📱 Аккаунт: ${acc.name || acc.phone}\n\n` +
      `Используйте "📋 Pending заявки" для остановки.`,
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [Markup.button.callback('📋 Pending заявки', 'pending_menu')],
            [Markup.button.callback('◀️ Назад', 'pool_menu')]
          ]
        }
      }
    );
    return;
  }

  // Запрашиваем количество чатов
  userStates.set(ctx.from!.id, { action: 'waiting_pool_join_count', accountId });

  const readyCount = getReadyCount();
  await safeEdit(ctx,
    `🔗 *Выбор количества*\n\n` +
    `📱 Аккаунт: ${acc.name || acc.phone}\n` +
    `🟢 Доступно в пуле: ${readyCount}\n\n` +
    `📝 *Введите количество чатов:*\n` +
    `Например: 5, 10, 20\n\n` +
    `🛡️ Защита:\n` +
    `• После ${MAX_JOINS_BEFORE_BREAK} присоединений - перерыв 4 часа`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('◀️ Назад', 'pool_menu')]
        ]
      }
    }
  );
});

bot.action('pool_process_pending', async (ctx) => {
  await safeAnswerCb(ctx);

  const stats = getPoolStats();
  if (stats.pending === 0) {
    await safeReply(ctx, '📭 Нет pending заявок');
    return;
  }

  const connectedAccounts = [...waAccounts.entries()].filter(([_, a]) => a.status === 'connected');
  if (connectedAccounts.length === 0) {
    await safeReply(ctx, '❌ Нет подключенных аккаунтов');
    return;
  }

  await safeReply(ctx, `🔔 *Проверка pending заявок*\n\n⏳ Обрабатываем...`, { parse_mode: 'Markdown' });

  let totalApproved = 0;
  let totalRejected = 0;

  const pendingLinks = getAllPendingLinks();

  for (const link of pendingLinks) {
    // Выбираем случайный аккаунт
    const [accountId, accData] = connectedAccounts[Math.floor(Math.random() * connectedAccounts.length)];

    try {
      const result = await joinGroupByInvite(accData.client, link.inviteCode);
      if (result.success) {
        updateLinkStatus(link.inviteCode, 'joined');
        totalApproved++;
      } else if (!result.needsApproval) {
        updateLinkStatus(link.inviteCode, 'failed', result.error);
        totalRejected++;
      }
    } catch (error) {
      updateLinkStatus(link.inviteCode, 'failed', (error as Error).message);
      totalRejected++;
    }
    await new Promise(r => setTimeout(r, 3000));
  }

  const newStats = getPoolStats();
  await safeReply(ctx,
    `🏁 *Проверка завершена*\n\n` +
    `✅ Присоединено: ${totalApproved}\n` +
    `❌ Отклонено: ${totalRejected}\n` +
    `🔔 Ещё pending: ${newStats.pending}`,
    { parse_mode: 'Markdown' }
  );
});

bot.action('pool_clear_processed', async (ctx) => {
  await safeAnswerCb(ctx);
  clearProcessedPool();
  await safeReply(ctx, '🗑️ Обработанные ссылки удалены');
  await safeEdit(ctx, '✅ Очередь очищена', {
    reply_markup: { inline_keyboard: [[Markup.button.callback('◀️ К пулу', 'pool_menu')]] }
  });
});

bot.action('pool_clear_all', async (ctx) => {
  await safeAnswerCb(ctx);
  await safeEdit(ctx,
    `⚠️ *Очистить весь пул?*\n\n` +
    `Это удалит ВСЕ ссылки!\n\n` +
    `Действие нельзя отменить!`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('✅ Да, очистить', 'pool_confirm_clear_all')],
          [Markup.button.callback('❌ Отмена', 'pool_menu')]
        ]
      }
    }
  );
});

bot.action('pool_confirm_clear_all', async (ctx) => {
  await safeAnswerCb(ctx);
  clearAllPool();
  await safeReply(ctx, '🗑️ Весь пул очищен!');
  await safeEdit(ctx, '✅ Пул очищен', {
    reply_markup: { inline_keyboard: [[Markup.button.callback('◀️ К пулу', 'pool_menu')]] }
  });
});

// Обработчики для рандомного присоединения из глобального пула
for (const count of [10, 20, 30, 50]) {
  bot.action(`pool_join_${count}`, async (ctx) => {
    await safeAnswerCb(ctx);

    const stats = getPoolStats();
    if (stats.ready === 0) {
      await safeReply(ctx, '📭 Нет готовых ссылок в пуле');
      return;
    }

    // Получаем рандомные ссылки из глобального пула
    const shuffledLinks = getReadyLinks(count);

    await safeReply(ctx,
      `🚀 *Рандомное присоединение*\n\n` +
      `📊 Выбрано: ${shuffledLinks.length} ссылок\n` +
      `🟢 В пуле осталось: ${stats.ready - shuffledLinks.length}\n\n` +
      `⏳ Обрабатываем...`,
      { parse_mode: 'Markdown' }
    );

    let totalJoined = 0, totalPending = 0, totalFailed = 0, totalAlready = 0;

    for (let i = 0; i < shuffledLinks.length; i++) {
      const link = shuffledLinks[i];

      // Для каждой ссылки выбираем случайный подключенный аккаунт
      const connectedAccounts = [...waAccounts.entries()].filter(([_, a]) => a.status === 'connected');
      if (connectedAccounts.length === 0) {
        await safeReply(ctx, '❌ Нет подключенных аккаунтов');
        break;
      }

      const randomIndex = Math.floor(Math.random() * connectedAccounts.length);
      const [accountId, accData] = connectedAccounts[randomIndex];

      try {
        const result = await joinGroupByInvite(accData.client, link.inviteCode);

        if (result.success) {
          if (result.alreadyJoined) {
            totalAlready++;
          } else {
            totalJoined++;
          }
          updateLinkStatus(link.inviteCode, 'joined');
        } else if (result.needsApproval) {
          totalPending++;
          updateLinkStatus(link.inviteCode, 'pending');
        } else {
          totalFailed++;
          updateLinkStatus(link.inviteCode, 'failed', result.error);
        }

        // Обновляем прогресс каждые 5 ссылок
        if ((i + 1) % 5 === 0 || i === shuffledLinks.length - 1) {
          await safeReply(ctx,
            `📊 Прогресс: ${i + 1}/${shuffledLinks.length}\n` +
            `✅ ${totalJoined} | ❌ ${totalFailed} | 🔔 ${totalPending}`
          );
        }
      } catch (error) {
        totalFailed++;
        updateLinkStatus(link.inviteCode, 'failed', (error as Error).message);
      }

      await new Promise(r => setTimeout(r, JOIN_DELAY_MIN));
    }

    const newStats = getPoolStats();
    let finalText = `🏁 *Завершено*\n\n`;
    finalText += `✅ Присоединено: ${totalJoined}\n`;
    finalText += `🔔 Pending: ${totalPending}\n`;
    finalText += `❌ Ошибок: ${totalFailed}\n`;
    finalText += `⏭️ Уже в группе: ${totalAlready}\n`;
    finalText += `\n📊 Осталось в пуле: 🟢 ${newStats.ready}`;

    await safeReply(ctx, finalText, { parse_mode: 'Markdown' });
  });
}

bot.action('add_account', async (ctx) => {
  await safeAnswerCb(ctx);
  await safeEdit(ctx,
    '📱 *Добавление аккаунта*\n\n📷 *Подключение через QR-код*\n   ✅ Работает стабильно\n\n⚠️ *Код подтверждения временно недоступен*\n   WhatsApp изменил интерфейс',
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('📷 QR-код', 'add_qr')],
          [Markup.button.callback('❌ Отмена', 'accounts')]
        ]
      }
    }
  );
});

bot.action('add_qr', async (ctx) => {
  await safeAnswerCb(ctx);
  userStates.set(ctx.from!.id, { action: 'waiting_phone_qr' });
  await safeEdit(ctx,
    '📷 *Подключение через QR-код*\n\n' +
    'Введите номер телефона в формате:\n' +
    '+79991234567\n\n' +
    'После ввода бот отправит QR-код для сканирования.',
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'add_account')]])
    }
  );
});

bot.action(/^unbind_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  await safeEdit(ctx,
    `⚠️ *Отвязать номер?*\n\nАккаунт будет удален и отключен от WhatsApp.`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('✅ Да, отвязать', `confirm_unbind_${accountId}`)],
          [Markup.button.callback('❌ Отмена', `acc_${accountId}`)]
        ]
      }
    }
  );
});

bot.action(/^confirm_unbind_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (acc?.client) {
    try { await acc.client.logout(); await acc.client.destroy(); } catch (e) {}
  }
  // Удаляем ПАПКУ конкретного аккаунта (включая accountId в пути)
  // Используем /data который Railway автоматически сохраняет между деплоями!
  const sessionsPath = `${process.env.WA_SESSIONS_PATH || '/data/wa-sessions'}/${accountId}`;
  if (fs.existsSync(sessionsPath)) {
    fs.rmSync(sessionsPath, { recursive: true, force: true });
  }
  waAccounts.delete(accountId);
  await safeReply(ctx, '✅ Аккаунт отвязан');
  await safeEdit(ctx, '✅ Аккаунт удален', { ...accountsMenu() });
});

bot.action(/^edit_msg_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  userStates.set(ctx.from!.id, { action: 'waiting_custom_message', accountId });
  const acc = waAccounts.get(accountId);
  const currentMsg = acc?.customMessage || '';
  await safeEdit(ctx,
    `📝 *Текст для аккаунта ${acc?.name || acc?.phone || accountId}*\n\n` +
    `Текущий текст: ${currentMsg || 'не задан'}\n\n` +
    `Введите новый текст для рассылки:`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', `acc_${accountId}`)]])
    }
  );
});

// Настройка задержки рассылки для аккаунта
bot.action(/^set_broadcast_delay_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  userStates.set(ctx.from!.id, { action: 'waiting_account_broadcast_delay', accountId });
  const acc = waAccounts.get(accountId);
  await safeEdit(ctx,
    `⏱ *Задержка рассылки*\n\n` +
    `📱 Аккаунт: ${acc?.name || acc?.phone || accountId}\n` +
    `📊 Текущая: ${acc?.broadcastDelay || 10} мин (±1 мин)\n\n` +
    `Введите новую задержку (1-60 минут):`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', `acc_${accountId}`)]])
    }
  );
});

// Настройка задержки присоединения для аккаунта
bot.action(/^set_join_delay_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  userStates.set(ctx.from!.id, { action: 'waiting_account_join_delay', accountId });
  const acc = waAccounts.get(accountId);
  await safeEdit(ctx,
    `🔗 *Задержка присоединения*\n\n` +
    `📱 Аккаунт: ${acc?.name || acc?.phone || accountId}\n` +
    `📊 Текущая: ${acc?.joinDelay || 10} мин (±1 мин)\n\n` +
    `Введите новую задержку (1-60 минут):`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', `acc_${accountId}`)]])
    }
  );
});

// Переключение авто-повтора для pending заявок
bot.action(/^toggle_auto_pending_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (acc) {
    acc.autoJoinPending = !acc.autoJoinPending;
    saveAccountsData();
    await safeReply(ctx, `✅ Авто-повтор для pending: ${acc.autoJoinPending ? 'ВКЛЮЧЁН' : 'ВЫКЛЮЧЁН'}`);
  }
  await safeEdit(ctx, '📱 *Настройки обновлены*\n\nНажмите "◀️ Назад" для возврата', {
    parse_mode: 'Markdown',
    ...accountsMenu(accountId)
  });
});

bot.action(/^view_chats_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx, 'Загрузка чатов...');
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeEdit(ctx, '❌ Аккаунт не подключен', { ...accountsMenu(accountId) });
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
    const showChats = archived.slice(0, 10);
    showChats.forEach((chat: any) => {
      const emoji = chat.isGroup ? '👥' : '👤';
      text += `${emoji} ${chat.name || chat.id._serialized}\n`;
    });
    if (archived.length > 10) text += `\n... и ещё ${archived.length - 10}`;
    await safeEdit(ctx, text, { parse_mode: 'Markdown', ...accountsMenu(accountId) });
  } catch (error) {
    console.error('Error getting chats:', error);
    await safeReply(ctx, '❌ Ошибка при получении чатов');
  }
});

// Проверка статуса одного аккаунта
bot.action(/^refresh_acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx, 'Проверяем...');
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (!acc) {
    await safeEdit(ctx, '❌ Аккаунт не найден', { ...accountsMenu() });
    return;
  }

  const wasConnected = acc.status === 'connected';
  if (acc.client) {
    const valid = await isSessionValid(acc.client);
    if (valid && acc.status !== 'connected') {
      acc.status = 'connected';
      await safeReply(ctx, `✅ Статус обновлён: аккаунт подключен!`);
    } else if (!valid && acc.status === 'connected') {
      acc.status = 'disconnected';
      await safeReply(ctx, `⚠️ Статус обновлён: аккаунт отключен (сессия устарела)`);
    } else if (valid) {
      await safeReply(ctx, `✅ Аккаунт подключен и активен`);
    } else {
      await safeReply(ctx, `🔴 Аккаунт отключен. Нажмите "❌ Отвязать номер" и подключите заново через QR.`);
    }
    saveAccountsData();
  }
  await safeEdit(ctx, `📱 *${acc.name || acc.phone}*\n\n📊 Статус: ${acc.status === 'connected' ? '🟢 Подключен' : '🔴 Отключен'}\n\nНажмите "◀️ Назад к списку"`, { parse_mode: 'Markdown', ...accountsMenu(accountId) });
});

// Проверка статуса всех аккаунтов
bot.action('refresh_all_accounts', async (ctx) => {
  await safeAnswerCb(ctx, 'Проверяем все аккаунты...');
  await refreshAccountsStatus();
  const connected = [...waAccounts.values()].filter(a => a.status === 'connected').length;
  const total = waAccounts.size;
  await safeEdit(ctx,
    `🔄 *Проверка завершена*\n\n` +
    `📱 Подключено: ${connected}/${total}\n\n` +
    (connected === 0 ? `⚠️ Нет активных аккаунтов!\n` : '') +
    `Нажмите "◀️ Назад" для просмотра списка`,
    { parse_mode: 'Markdown', ...accountsMenu() }
  );
});

// === НАСТРОЙКА ЛИМИТА СООБЩЕНИЙ И ПАУЗЫ ===
bot.action(/^set_pause_limit_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeEdit(ctx, '❌ Аккаунт не подключен', { ...accountsMenu(accountId) });
    return;
  }

  // Запрашиваем лимит сообщений
  userStates.set(ctx.from!.id, { action: 'waiting_pause_messages', accountId });
  const currentMessages = (acc as any).messagesBeforePause || 100;
  const currentPause = (acc as any).pauseDurationMinutes || 10;

  await safeEdit(ctx,
    `🛡️ *Настройка защиты от бана*\n\n` +
    `📊 Текущие настройки:\n` +
    `   📝 Лимит сообщений: ${currentMessages}\n` +
    `   ⏱ Длительность паузы: ${currentPause} мин\n\n` +
    `📝 *Введите лимит сообщений:*\n` +
    `Рекомендуется: 50-200 сообщений\n` +
    `Диапазон: 10-500`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('◀️ Назад', `acc_${accountId}`)]
        ]
      }
    }
  );
});

// Переподключение через QR для отключенных аккаунтов
bot.action(/^reconnect_qr_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (!acc) {
    await safeEdit(ctx, '❌ Аккаунт не найден', { ...accountsMenu() });
    return;
  }

  await safeEdit(ctx,
    `📷 *Переподключение аккаунта*\n\n` +
    `📱 Аккаунт: ${acc.name || acc.phone || accountId}\n` +
    `⚠️ Статус: ${acc.status === 'connected' ? '🟢 Подключен' : '🔴 Отключен'}\n\n` +
    `Для переподключения:\n` +
    `1. Откройте WhatsApp на телефоне\n` +
    `2. Нажмите "📷 QR-код (рекомендуется)" в меню\n` +
    `3. Отсканируйте код WhatsApp\n\n` +
    `Или нажмите "◀️ Назад" и выберите "🔄 Проверить статус"`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('📷 QR-код (рекомендуется)', 'add_qr')],
          [Markup.button.callback('◀️ Назад', `acc_${accountId}`)]
        ]
      }
    }
  );
});

// === BROADCAST ===
bot.action('broadcast_menu', async (ctx) => {
  await safeAnswerCb(ctx);
  const enabledAccounts = [...waAccounts.values()].filter(a => a.status === 'connected' && a.enabled);
  let text = '📨 *Рассылка*\n\n';
  if (enabledAccounts.length === 0) {
    text += '❌ Нет включенных аккаунтов для рассылки\n\nВключите аккаунты в разделе "Аккаунты"';
  } else {
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
  if (!accountId) return;
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
    userStates.set(ctx.from!.id, { action: 'waiting_broadcast_text', accountId });
    await safeEdit(ctx,
      `📨 *Рассылка с ${acc.name || acc.phone}*\n\n⚠️ Текст не настроен!\n\nВведите текст для рассылки:`,
      {
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'broadcast_menu')]])
      }
    );
    return;
  }
  // Текст уже есть - спрашиваем задержку
  userStates.set(ctx.from!.id, { action: 'waiting_broadcast_delay', accountId });
  await safeEdit(ctx,
    `📨 *Рассылка с ${acc.name || acc.phone}*\n\n` +
    `📝 *Текст:*\n${acc.customMessage.substring(0, 100)}${acc.customMessage.length > 100 ? '...' : ''}\n\n` +
    `⏱ Введите задержку между сообщениями (в минутах):\n` +
    `Например: 5 или 10\n\n` +
    `±1 минута рандом будет добавлена автоматически.`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('◀️ Назад', 'broadcast_menu')]
        ]
      }
    }
  );
});

bot.action(/^confirm_broadcast_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
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
  text += `   ⏱ Задержка: ${Math.round(JOIN_DELAY_MIN/60000)} мин между присоединениями\n\n`;
  text += `Выберите аккаунт для присоединения:`;
  await safeEdit(ctx, text, { parse_mode: 'Markdown', ...joinSelectMenu() });
});

bot.action(/^join_acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeEdit(ctx, '❌ Аккаунт не подключен', { ...joinSelectMenu() });
    return;
  }
  // Проверяем, не идёт ли уже процесс присоединения
  if (acc.isJoining) {
    await safeEdit(ctx,
      `⏳ *Присоединение уже идёт!*\n\n` +
      `📱 Аккаунт: ${acc.name || acc.phone}\n\n` +
      `Используйте "📋 Pending заявки" для остановки.`,
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [Markup.button.callback('📋 Pending заявки', 'pending_menu')],
            [Markup.button.callback('◀️ Назад', 'join_menu')]
          ]
        }
      }
    );
    return;
  }

  // Сначала спрашиваем количество чатов
  userStates.set(ctx.from!.id, { action: 'waiting_join_count', accountId });
  await safeEdit(ctx,
    `🔗 *Присоединение к чатам*\n\n` +
    `📱 Аккаунт: ${acc.name || acc.phone}\n\n` +
    `📊 Введите количество чатов для присоединения:\n` +
    `Например: 5 или 10\n\n` +
    `🛡️ Защита от бана:\n` +
    `• Максимум ${MAX_PENDING_JOIN_REQUESTS} pending заявок\n` +
    `• После ${MAX_JOINS_BEFORE_BREAK} присоединений - перерыв 4 часа`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('◀️ Назад', 'join_menu')]
        ]
      }
    }
  );
});

bot.action(/^confirm_join_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
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

// === PENDING ЗАЯВКИ ===
bot.action('pending_menu', async (ctx) => {
  await safeAnswerCb(ctx);
  let text = '📋 *Pending заявки*\n\n';
  text += `🛡️ *Логика работы:*\n`;
  text += `• Присоединение → сразу вступаем\n`;
  text += `• Требует одобрение → в очередь\n`;
  text += `• Максимум ${MAX_PENDING_JOIN_REQUESTS} активных заявок\n`;
  text += `• После одобрения → вступаем\n\n`;
  text += `Выберите аккаунт:`;
  await safeEdit(ctx, text, { parse_mode: 'Markdown', ...pendingSelectMenu() });
});

bot.action(/^pending_acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeEdit(ctx, '❌ Аккаунт не подключен', { ...pendingSelectMenu() });
    return;
  }
  const queue = pendingJoinRequests.get(accountId) || [];
  const pendingItems = queue.filter(p => p.status === 'pending');
  const activeCount = pendingItems.length;

  let text = `📋 *Pending заявки*\n\n`;
  text += `📱 Аккаунт: ${acc.name || acc.phone}\n`;
  text += `⏳ Активных: ${activeCount}/${MAX_PENDING_JOIN_REQUESTS}\n\n`;

  if (pendingItems.length > 0) {
    text += `📋 *В очереди:*\n`;
    pendingItems.forEach((item, idx) => {
      const timeAgo = Math.floor((Date.now() - item.addedAt.getTime()) / 60000);
      text += `${idx + 1}. ${item.inviteCode.substring(0, 15)}...\n   ⏱ ${timeAgo} мин назад\n`;
    });
  } else {
    text += `📭 Очередь пуста\n`;
  }

  await safeEdit(ctx, text, { parse_mode: 'Markdown', ...pendingAccMenu(accountId) });
});

bot.action(/^check_pending_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeReply(ctx, '❌ Аккаунт не подключен');
    return;
  }
  await checkPendingRequests(ctx, accountId);
});

bot.action(/^clear_pending_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;
  pendingJoinRequests.delete(accountId);
  await safeReply(ctx, '🗑️ Очередь pending заявок очищена');
  await safeEdit(ctx, '✅ Очередь очищена!', { ...pendingAccMenu(accountId) });
});

// === STOP JOIN ===
bot.action(/^stop_join_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc) {
    await safeEdit(ctx, '❌ Аккаунт не найден', { ...pendingAccMenu(accountId) });
    return;
  }

  // Останавливаем процесс присоединения
  acc.isJoining = false;

  // Останавливаем все активные join операции для этого аккаунта
  for (const [joinId, joinData] of activeJoins.entries()) {
    if (joinData.accountId === accountId) {
      joinData.stop = true;
    }
  }

  // Останавливаем активные процессы пула
  for (const [poolJoinId, poolData] of activePoolJoins.entries()) {
    if (poolData.accountId === accountId) {
      poolData.stop = true;
    }
  }

  await safeReply(ctx, `⏹ Присоединение для ${acc.name || acc.phone} остановлено!`);
  await safeEdit(ctx, `✅ Присоединение остановлено!\n📱 Аккаунт: ${acc.name || acc.phone}`, { ...pendingAccMenu(accountId) });
});

// === STOP BROADCAST ===
bot.action(/^stop_broadcast_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const broadcastId = ctx.match?.[1];
  if (!broadcastId) return;

  const broadcast = activeBroadcasts.get(broadcastId);
  if (!broadcast) {
    await safeEdit(ctx, '❌ Рассылка не найдена или уже завершена', {});
    return;
  }

  broadcast.stop = true;

  const acc = waAccounts.get(broadcast.accountId);
  await safeReply(ctx, `⏹ Команда остановки отправлена для рассылки!`);
  await safeEdit(ctx, `⏹ *Рассылка останавливается...*\n\n📱 Аккаунт: ${acc?.name || acc?.phone || 'неизвестен'}\n\n⏳ Дождитесь завершения текущего сообщения...`, { parse_mode: 'Markdown' });
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
      } catch (e) {}
    }
  }
  await safeReply(ctx, '✅ Все сессии перезапущены');
  await safeEdit(ctx, '✅ Сессии перезапущены', { ...settingsMenu() });
});

// === TEXT HANDLER ===
bot.on('text', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;
  const state = userStates.get(ctx.from!.id);
  if (!state) return;

  try {
    if (state.action === 'waiting_phone_qr') {
      const phone = ctx.message.text.trim();
      const cleanPhone = phone.replace(/[^0-9+]/g, '');
      if (cleanPhone.length < 10) {
        await safeReply(ctx, '❌ Неверный формат номера. Введите в формате +79991234567');
        return;
      }
      await addNewAccount(ctx, cleanPhone, 'qr');
      userStates.delete(ctx.from!.id);
      return;
    }

    if (state.action === 'waiting_custom_message') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        acc.customMessage = ctx.message.text;
        await safeReply(ctx, `✅ Текст сохранен для аккаунта ${acc.name || acc.phone}`);
        await safeEdit(ctx, `✅ Текст сохранен!\n\n📝 ${ctx.message.text}`, { parse_mode: 'Markdown' });
      }
      userStates.delete(ctx.from!.id);
      return;
    }

    if (state.action === 'waiting_broadcast_text') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        acc.customMessage = ctx.message.text;
        userStates.set(ctx.from!.id, { action: 'waiting_broadcast_delay', accountId });
        await safeReply(ctx,
          `✅ Текст сохранен!\n\n` +
          `📝 Текст: "${ctx.message.text.substring(0, 50)}${ctx.message.text.length > 50 ? '...' : ''}"\n\n` +
          `⏱ Введите задержку между сообщениями (в минутах):\n` +
          `Например: 5 или 10`,
          { parse_mode: 'Markdown' }
        );
      }
      return;
    }

    // Новый обработчик для ввода задержки рассылки
    if (state.action === 'waiting_broadcast_delay') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const delayMinutes = parseInt(ctx.message.text.trim());
      if (isNaN(delayMinutes) || delayMinutes < 1 || delayMinutes > 60) {
        await safeReply(ctx, '❌ Введите число от 1 до 60 минут');
        return;
      }
      acc.broadcastDelay = delayMinutes;
      saveAccountsData();

      userStates.delete(ctx.from!.id);
      await safeReply(ctx,
        `✅ Задержка установлена: ${delayMinutes} мин (±1 мин рандом)\n\n` +
        `📨 *Подтверждение рассылки*\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `⏱ Задержка: ${delayMinutes} мин (±1 мин)\n\n` +
        `Нажмите "Да, начать" для запуска рассылки.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback(`✅ Да, начать (${delayMinutes} мин)`, `confirm_broadcast_${accountId}`)],
              [Markup.button.callback('❌ Отмена', 'broadcast_menu')]
            ]
          }
        }
      );
      return;
    }

    // Ввод количества чатов для присоединения
    if (state.action === 'waiting_join_count') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const count = parseInt(ctx.message.text.trim());
      if (isNaN(count) || count < 1 || count > 100) {
        await safeReply(ctx, '❌ Введите число от 1 до 100');
        return;
      }
      // Сохраняем количество в состояние и переходим к задержке
      userStates.set(ctx.from!.id, { action: 'waiting_join_delay', accountId, chatCount: count });
      await safeReply(ctx,
        `✅ Количество: ${count} чатов\n\n` +
        `🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `📊 Количество чатов: ${count}\n\n` +
        `⏱ Введите задержку между присоединениями (в минутах):\n` +
        `Например: 5 или 10\n\n` +
        `±1 минута рандом будет добавлена автоматически.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('◀️ Назад', 'join_menu')]
            ]
          }
        }
      );
      return;
    }

    // Ввод количества чатов из меню пула
    if (state.action === 'waiting_pool_join_count') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const count = parseInt(ctx.message.text.trim());
      if (isNaN(count) || count < 1 || count > 100) {
        await safeReply(ctx, '❌ Введите число от 1 до 100');
        return;
      }

      // Переходим к вводу задержки
      userStates.set(ctx.from!.id, { action: 'waiting_pool_join_delay', accountId, chatCount: count });

      await safeReply(ctx,
        `✅ Количество: ${count} чатов\n\n` +
        `🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `📊 Количество чатов: ${count}\n\n` +
        `⏱ Введите задержку между присоединениями (в минутах):\n` +
        `Например: 5 или 10\n\n` +
        `±1 минута рандом будет добавлена автоматически.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('◀️ Назад в пул', 'pool_menu')]
            ]
          }
        }
      );
      return;
    }

    // Ввод задержки из меню пула -> автоматически берем из пула
    if (state.action === 'waiting_pool_join_delay') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const delayMinutes = parseInt(ctx.message.text.trim());
      if (isNaN(delayMinutes) || delayMinutes < 1 || delayMinutes > 60) {
        await safeReply(ctx, '❌ Введите число от 1 до 60 минут');
        return;
      }
      acc!.joinDelay = delayMinutes;
      saveAccountsData();

      const chatCount = (state as any).chatCount || 10;
      userStates.delete(ctx.from!.id);

      // Получаем рандомные ссылки из глобального пула
      const shuffledLinks = getReadyLinks(chatCount);

      if (shuffledLinks.length === 0) {
        await safeReply(ctx, '❌ В пуле нет готовых ссылок!');
        return;
      }

      // Создаем активный процесс пула
      const poolJoinId = Date.now().toString();
      activePoolJoins.set(poolJoinId, {
        stop: false,
        joined: 0,
        pending: 0,
        failed: 0,
        already: 0,
        total: shuffledLinks.length,
        accountId: accountId!
      });
      acc!.isJoining = true;

      await safeReply(ctx,
        `🚀 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `📊 Количество чатов: ${shuffledLinks.length}\n` +
        `⏱ Задержка: ${delayMinutes} мин (±1 мин)\n\n` +
        `⏳ Начинаем присоединение...`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('⏹ СТОП ПРИСОЕДИНЕНИЕ', `stop_join_${accountId}`)]
            ]
          }
        }
      );

      // Запускаем процесс присоединения
      let totalJoined = 0, totalPending = 0, totalFailed = 0, totalAlready = 0;

      for (let i = 0; i < shuffledLinks.length; i++) {
        const poolJoin = activePoolJoins.get(poolJoinId);

        // Проверяем флаг остановки
        if (poolJoin?.stop) {
          acc!.isJoining = false;
          activePoolJoins.delete(poolJoinId);
          await safeReply(ctx,
            `⏹ *Присоединение остановлено!*\n\n` +
            `✅ Присоединено: ${totalJoined}\n` +
            `🔔 Pending: ${totalPending}\n` +
            `❌ Ошибок: ${totalFailed}\n` +
            `⏭️ Уже в группе: ${totalAlready}`
          );
          return;
        }

        const link = shuffledLinks[i];

        try {
          const result = await joinGroupByInvite(acc!.client, link.inviteCode);

          if (result.success) {
            if (result.alreadyJoined) {
              totalAlready++;
            } else {
              totalJoined++;
            }
            updateLinkStatus(link.inviteCode, 'joined');
          } else if (result.needsApproval) {
            totalPending++;
            updateLinkStatus(link.inviteCode, 'pending');
          } else {
            totalFailed++;
            updateLinkStatus(link.inviteCode, 'failed', result.error);
          }

          // Обновляем статистику в activePoolJoins
          if (poolJoin) {
            poolJoin.joined = totalJoined;
            poolJoin.pending = totalPending;
            poolJoin.failed = totalFailed;
            poolJoin.already = totalAlready;
          }

          // Обновляем прогресс каждые 3 ссылки
          if ((i + 1) % 3 === 0 || i === shuffledLinks.length - 1) {
            await safeReply(ctx,
              `📊 Прогресс: ${i + 1}/${shuffledLinks.length}\n` +
              `✅ ${totalJoined} | ❌ ${totalFailed} | 🔔 ${totalPending}\n\n` +
              `⏳ Продолжаем...`,
              {
                reply_markup: {
                  inline_keyboard: [
                    [Markup.button.callback('⏹ СТОП', `stop_join_${accountId}`)]
                  ]
                }
              }
            );
          }
        } catch (error) {
          totalFailed++;
          updateLinkStatus(link.inviteCode, 'failed', (error as Error).message);
        }

        // Задержка между присоединениями
        const randomDelay = (delayMinutes * 60000) + (Math.random() * 60000 - 30000);
        await new Promise(r => setTimeout(r, Math.max(30000, randomDelay)));
      }

      // Очищаем состояние
      acc!.isJoining = false;
      activePoolJoins.delete(poolJoinId);

      const newStats = getPoolStats();
      await safeReply(ctx,
        `🏁 *Завершено*\n\n` +
        `✅ Присоединено: ${totalJoined}\n` +
        `🔔 Pending: ${totalPending}\n` +
        `❌ Ошибок: ${totalFailed}\n` +
        `⏭️ Уже в группе: ${totalAlready}\n\n` +
        `📊 Осталось в пуле: 🟢 ${newStats.ready}`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    // Ввод задержки для присоединения (старый метод)
    if (state.action === 'waiting_join_delay') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const delayMinutes = parseInt(ctx.message.text.trim());
      if (isNaN(delayMinutes) || delayMinutes < 1 || delayMinutes > 60) {
        await safeReply(ctx, '❌ Введите число от 1 до 60 минут');
        return;
      }
      acc.joinDelay = delayMinutes;
      saveAccountsData();

      // Получаем количество чатов из состояния
      const chatCount = (state as any).chatCount || 10;

      userStates.delete(ctx.from!.id);
      await safeReply(ctx,
        `✅ Настройки сохранены!\n\n` +
        `🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `📊 Количество чатов: ${chatCount}\n` +
        `⏱ Задержка: ${delayMinutes} мин (±1 мин)\n\n` +
        `Введите ссылки на группы (${chatCount} штук через пробел).`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('◀️ Назад', 'join_menu')]
            ]
          }
        }
      );
      // Сохраняем состояние для ожидания ссылок с количеством
      userStates.set(ctx.from!.id, { action: 'waiting_group_links', accountId, chatCount });
      return;
    }

    // Настройка задержки рассылки из меню аккаунта
    if (state.action === 'waiting_account_broadcast_delay') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const delayMinutes = parseInt(ctx.message.text.trim());
      if (isNaN(delayMinutes) || delayMinutes < 1 || delayMinutes > 60) {
        await safeReply(ctx, '❌ Введите число от 1 до 60 минут');
        return;
      }
      acc.broadcastDelay = delayMinutes;
      saveAccountsData();
      await safeReply(ctx,
        `✅ Задержка рассылки установлена: ${delayMinutes} мин (±1 мин рандом)\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `⏱ Новая задержка: ${delayMinutes} мин`
      );
      userStates.delete(ctx.from!.id);
      await safeEdit(ctx, '✅ Задержка рассылки обновлена!', { ...accountsMenu(accountId) });
      return;
    }

    // Настройка задержки присоединения из меню аккаунта
    if (state.action === 'waiting_account_join_delay') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const delayMinutes = parseInt(ctx.message.text.trim());
      if (isNaN(delayMinutes) || delayMinutes < 1 || delayMinutes > 60) {
        await safeReply(ctx, '❌ Введите число от 1 до 60 минут');
        return;
      }
      acc.joinDelay = delayMinutes;
      saveAccountsData();
      await safeReply(ctx,
        `✅ Задержка присоединения установлена: ${delayMinutes} мин (±1 мин рандом)\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `⏱ Новая задержка: ${delayMinutes} мин`
      );
      userStates.delete(ctx.from!.id);
      await safeEdit(ctx, '✅ Задержка присоединения обновлена!', { ...accountsMenu(accountId) });
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
      pendingGroupLinks.set(accountId!, links);
      await safeReply(ctx,
        `✅ Найдено ссылок: ${links.length}\n\nНажмите "Да, начать" для присоединения к чатам.`,
        {
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('✅ Да, начать', `confirm_join_${accountId}`)],
              [Markup.button.callback('❌ Отмена', 'join_menu')]
            ]
          }
        }
      );
      userStates.delete(ctx.from!.id);
      return;
    }

    // === НАСТРОЙКА ЛИМИТА СООБЩЕНИЙ ДЛЯ ПАУЗЫ ===
    if (state.action === 'waiting_pause_messages') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (!acc) {
        userStates.delete(ctx.from!.id);
        await safeReply(ctx, '❌ Аккаунт не найден');
        return;
      }

      const messagesCount = parseInt(ctx.message.text.trim());
      if (isNaN(messagesCount) || messagesCount < 10 || messagesCount > 500) {
        await safeReply(ctx, '❌ Введите число от 10 до 500');
        return;
      }

      // Сохраняем лимит сообщений и запрашиваем длительность паузы
      (acc as any).messagesBeforePause = messagesCount;
      saveAccountsData();
      userStates.set(ctx.from!.id, { action: 'waiting_pause_duration', accountId });

      await safeReply(ctx,
        `✅ Лимит сообщений: ${messagesCount}\n\n` +
        `🛡️ *Настройка защиты от бана*\n\n` +
        `📝 Лимит: ${messagesCount} сообщений\n\n` +
        `⏱ Введите длительность паузы (в минутах):\n` +
        `Рекомендуется: 30-120 минут\n` +
        `Диапазон: 10-300`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('◀️ Назад', `acc_${accountId}`)]
            ]
          }
        }
      );
      return;
    }

    // === НАСТРОЙКА ДЛИТЕЛЬНОСТИ ПАУЗЫ ===
    if (state.action === 'waiting_pause_duration') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (!acc) {
        userStates.delete(ctx.from!.id);
        await safeReply(ctx, '❌ Аккаунт не найден');
        return;
      }

      const pauseMinutes = parseInt(ctx.message.text.trim());
      if (isNaN(pauseMinutes) || pauseMinutes < 10 || pauseMinutes > 300) {
        await safeReply(ctx, '❌ Введите число от 10 до 300 минут');
        return;
      }

      // Сохраняем длительность паузы
      (acc as any).pauseDurationMinutes = pauseMinutes;
      saveAccountsData();
      userStates.delete(ctx.from!.id);

      const messagesLimit = (acc as any).messagesBeforePause || 100;

      await safeReply(ctx,
        `✅ *Настройки защиты сохранены!*\n\n` +
        `🛡️ *Защита от бана*\n\n` +
        `📝 После ${messagesLimit} сообщений → пауза ${pauseMinutes} мин\n` +
        `❌ После 3 ошибок → пауза 1 час\n` +
        `🔴 После 2 таких пауз → полная остановка\n\n` +
        `📱 Аккаунт: ${acc.name || acc.phone}`,
        { parse_mode: 'Markdown' }
      );

      await safeEdit(ctx,
        `✅ *Защита от бана настроена!*\n\n` +
        `🛡️ *Текущие настройки:*\n` +
        `📝 Лимит: ${messagesLimit} msg\n` +
        `⏱ Пауза: ${pauseMinutes} мин`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('◀️ К аккаунту', `acc_${accountId}`)]
            ]
          }
        }
      );
      return;
    }
  } catch (error) {
    console.error('Error in text handler:', error);
    await safeReply(ctx, '❌ Ошибка: ' + (error as Error).message);
    userStates.delete(ctx.from!.id);
  }
});

// === FUNCTIONS ===
async function addNewAccount(ctx: any, phone: string, authMethod: 'qr' | 'pairing' = 'qr') {
  const accountId = `wa_${phone.replace(/\+/g, '')}`;
  if (waAccounts.has(accountId)) {
    const existing = waAccounts.get(accountId);
    if (existing?.status === 'connected') {
      await safeReply(ctx, '✅ Этот номер уже подключен!');
      return;
    }
    if (existing?.client) {
      try { await existing.client.destroy(); } catch (e) {}
    }
  }

  await safeReply(ctx, '⏳ Создание сессии...\nЭто может занять 10-30 секунд');

  // УНИКАЛЬНАЯ папка для каждого аккаунта - критически важно для мультиаккаунта!
  // Используем /data который Railway автоматически сохраняет между деплоями!
  const sessionsPath = `${process.env.WA_SESSIONS_PATH || '/data/wa-sessions'}/${accountId}`;

  // Очищаем старую сессию если есть проблемы
  if (fs.existsSync(sessionsPath)) {
    try {
      const sessionFiles = fs.readdirSync(sessionsPath);
      // Если есть старый SessionFormat folder - удаляем
      if (sessionFiles.some(f => f.includes('Session'))) {
        console.log(`🗑️ Clearing old session for ${accountId}...`);
        fs.rmSync(sessionsPath, { recursive: true, force: true });
        fs.mkdirSync(sessionsPath, { recursive: true });
      }
    } catch (e) {
      console.log(`⚠️ Could not clear session folder: ${e}`);
    }
  } else {
    fs.mkdirSync(sessionsPath, { recursive: true });
  }

  const client = new Client({
    authStrategy: new LocalAuth({
      clientId: accountId,
      dataPath: sessionsPath,  // Уникальная папка для каждого аккаунта
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
        '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
      ],
      ignoreHTTPSErrors: true,
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
    broadcastDelay: 10,  // По умолчанию 10 минут
    joinDelay: 10,      // По умолчанию 10 минут
    isBroadcasting: false,
    isJoining: false,
    autoJoinPending: true,
    messagesBeforePause: 100,  // По умолчанию 100 сообщений
    pauseDurationMinutes: 10   // По умолчанию 10 минут
  } as any);

  let qrAttempts = 0;
  const maxQrAttempts = 3;

  client.on('qr', async (qr) => {
    const acc = waAccounts.get(accountId);
    if (acc?.status === 'connected') return;

    // QR-код меняется каждые ~60 секунд,允许最多3次重试
    if (qrAttempts >= maxQrAttempts) {
      console.log(`⚠️ Max QR attempts reached for ${phone}`);
      if (ctx) {
        await safeReply(ctx,
          `⏰ *Время истекло*\n\n` +
          `QR-код действует ~60 секунд. Попробуйте снова через "📱 Аккаунты" → "📷 QR-код"`,
          { parse_mode: 'Markdown' }
        );
      }
      return;
    }

    qrAttempts++;
    try {
      const qrDataUrl = await QRCode.toDataURL(qr);
      const base64Data = qrDataUrl.replace(/^data:image\/png;base64,/, '');
      const buffer = Buffer.from(base64Data, 'base64');
      const attemptText = qrAttempts > 1 ? ` (попытка ${qrAttempts}/${maxQrAttempts})` : '';

      if (ctx) {
        await safeReply(ctx,
          `📱 *Подключение WhatsApp*${attemptText}\n\n` +
          `1. Откройте WhatsApp на телефоне\n` +
          `2. Нажмите Настройки → Связанные устройства\n` +
          `3. Нажмите "Подключить устройство"\n` +
          `4. Отсканируйте QR-код ниже\n\n` +
          `⏰ QR-код действителен ~60 секунд\n` +
          (qrAttempts > 1 ? `⚠️ Если QR не работает, попробуйте снова позже` : ''),
          { parse_mode: 'Markdown' }
        );
        await ctx.replyWithPhoto({ source: buffer }, { caption: `📱 *Отсканируйте этот QR-код*${attemptText}`, parse_mode: 'Markdown' });
      }
    } catch (e) {
      console.error('QR send error:', e);
    }
  });

  client.on('ready', async () => {
    const acc = waAccounts.get(accountId);
    if (acc) {
      acc.status = 'connected';
      saveAccountsData();  // Сохраняем после подключения
    }
    console.log(`✅ WhatsApp account connected: ${phone}`);
    if (ctx) {
      await safeReply(ctx,
        `✅ *WhatsApp подключен!*\n\n📞 Номер: ${phone}\n\nТеперь вы можете:\n` +
        `• Настроить текст рассылки в "Аккаунты"\n` +
        `• Запустить рассылку в архивные чаты`,
        { parse_mode: 'Markdown' }
      );
    }
  });

  client.on('auth_failure', async (msg) => {
    console.log(`❌ Auth failure for ${phone}: ${msg}`);
    const acc = waAccounts.get(accountId);
    if (acc) acc.status = 'disconnected';
    if (ctx) {
      await safeReply(ctx, `❌ Ошибка авторизации: ${msg}`);
    }
  });

  client.on('disconnected', async () => {
    console.log(`⚠️ Account disconnected: ${phone}`);
    const acc = waAccounts.get(accountId);
    if (acc) {
      acc.status = 'disconnected';
      // ONE-TIME уведомление об отключении (не спамим)
      const now = Date.now();
      const lastNotified = lastDisconnectNotification.get(accountId) || 0;
      if (now - lastNotified > DISCONNECT_NOTIFICATION_COOLDOWN) {
        lastDisconnectNotification.set(accountId, now);
        const msg = `⚠️ *Отключение аккаунта*\n\n📱 Аккаунт: ${acc.name || acc.phone || phone}\n\n🔄 Бот автоматически восстановит сессию`;
        for (const adminId of ADMIN_IDS) {
          try {
            await bot.telegram.sendMessage(adminId, msg, { parse_mode: 'Markdown' });
          } catch (e) {
            // Игнорируем ошибки отправки
          }
        }
      }
    }
    if (ctx) {
      await safeReply(ctx, `⚠️ Аккаунт отключился`);
    }
  });

  // Небольшая задержка перед инициализацией
  await new Promise(r => setTimeout(r, 2000));

  // Инициализация с повторными попытками
  let initAttempts = 0;
  const maxInitAttempts = 2;
  let lastError: Error | null = null;

  while (initAttempts < maxInitAttempts) {
    try {
      initAttempts++;
      console.log(`🔄 Initializing client (attempt ${initAttempts}/${maxInitAttempts})...`);

      if (ctx) {
        await safeReply(ctx, `⏳ Попытка подключения ${initAttempts}/${maxInitAttempts}...\nЭто может занять 10-30 секунд`);
      }

      await client.initialize();
      console.log(`✅ Client initialized successfully`);
      return; // Успех - выходим
    } catch (error) {
      lastError = error as Error;
      console.error(`❌ Initialize attempt ${initAttempts} failed:`, lastError.message);

      // Проверяем тип ошибки
      const errorMsg = lastError.message || '';
      if (errorMsg.includes('Execution context') || errorMsg.includes('detached Frame')) {
        console.log(`🔄 Session stale, will retry with fresh session...`);
        // Ждём перед повторной попыткой
        if (initAttempts < maxInitAttempts) {
          await new Promise(r => setTimeout(r, 3000)); // 3 секунды
        }
      } else if (errorMsg.includes('Target closed') || errorMsg.includes('Protocol error')) {
        console.log(`🔄 Browser closed unexpectedly, retrying...`);
        if (initAttempts < maxInitAttempts) {
          await new Promise(r => setTimeout(r, 2000));
        }
      } else {
        // Другие ошибки - возможно QR код нужен
        break;
      }
    }
  }

  // Все попытки исчерпаны
  console.error(`❌ All ${maxInitAttempts} initialization attempts failed`);
  const acc = waAccounts.get(accountId);
  if (acc) acc.status = 'disconnected';

  const finalMsg = lastError?.message || 'Неизвестная ошибка';
  if (ctx) {
    await safeReply(ctx,
      `❌ *Ошибка подключения*\n\n` +
      `Номер: ${phone}\n` +
      `Ошибка: ${finalMsg.substring(0, 100)}\n\n` +
      `💡 *Решения:*\n` +
      `1. Откройте WhatsApp на телефоне\n` +
      `2. Удалите связанное устройство (если есть)\n` +
      `3. Попробуйте снова через 1-2 минуты\n\n` +
      `Или попробуйте другой номер телефона.`,
      { parse_mode: 'Markdown' }
    );
  }
}

async function sendMessageWithRetry(
  client: Client,
  chatId: string,
  baseText: string,
  messageNumber: number,
  maxRetries: number = 3
): Promise<boolean> {
  let lastError: Error | null = null;
  const chatIdStr = String(chatId);

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      if (!client.info?.wid) throw new Error('Client not ready');
      let messageText = baseText;
      if (messageNumber > 0) messageText = varyRussianText(baseText);
      if (attempt === 1) await simulateHumanTyping(client, chatIdStr);
      const result = await client.sendMessage(chatIdStr, messageText);
      console.log(`  Successfully sent to ${chatIdStr}, message ID: ${result.id}`);
      return true;
    } catch (error) {
      lastError = error as Error;
      console.error(`  Attempt ${attempt} failed:`, error);
      if (attempt < maxRetries) {
        await new Promise(r => setTimeout(r, attempt * 10000));
      }
    }
  }
  return false;
}

async function startBroadcast(ctx: any, accountId: string, text: string) {
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    return safeReply(ctx, '❌ Аккаунт не подключен');
  }

  // Проверяем, не идёт ли уже рассылка
  if (acc.isBroadcasting) {
    return safeReply(ctx, '⏳ На этом аккаунте уже идёт рассылка!');
  }
  // NOTE: присоединение и рассылка могут идти одновременно на одном аккаунте

  isBroadcasting = true;
  acc.isBroadcasting = true;

  if (!acc.enabled) {
    isBroadcasting = false;
    acc.isBroadcasting = false;
    return safeReply(ctx, '❌ Этот аккаунт выключен для рассылки');
  }

  await safeReply(ctx, '🔍 Поиск архивных чатов...');

  try {
    // Проверяем подключение аккаунта
    if (acc.status !== 'connected') {
      isBroadcasting = false;
      acc.isBroadcasting = false;
      return safeReply(ctx, '❌ Аккаунт отключился! Используйте "🔄 Проверить статус" для восстановления.');
    }

    const chats = await acc.client.getChats();
    const archivedChats = chats.filter((chat: any) => chat.archived);
    if (archivedChats.length === 0) {
      isBroadcasting = false;
      acc.isBroadcasting = false;
      return safeReply(ctx, '❌ Архивные чаты не найдены');
    }

    const groups = archivedChats.filter((c: any) => c.isGroup);
    const individuals = archivedChats.filter((c: any) => !c.isGroup);
    const broadcastId = Date.now().toString();

    // Получаем настройки из аккаунта или используем значения по умолчанию
    const messagesBeforePause = (acc as any).messagesBeforePause || 100;
    const pauseDurationMinutes = (acc as any).pauseDurationMinutes || 10;

    activeBroadcasts.set(broadcastId, {
      stop: false,
      sent: 0,
      failed: 0,
      consecutiveErrors: 0,
      total: archivedChats.length,
      accountId,
      messagesBeforePause,
      pauseDurationMinutes,
      isPaused: false,
      messagesSincePause: 0
    });

    const broadcastDelay = getBroadcastDelay(accountId);
    const estimatedMin = Math.ceil(archivedChats.length * (broadcastDelay / 60000));
    const hours = Math.floor(estimatedMin / 60);
    const mins = estimatedMin % 60;

    await safeReply(ctx,
      `🔔 *Рассылка началась!*\n\n` +
      `📱 Аккаунт: ${acc.name || acc.phone}\n` +
      `📊 Найдено: ${archivedChats.length} чатов\n   👥 Групп: ${groups.length}\n   👤 Диалогов: ${individuals.length}\n\n` +
      `🛡️ *Защита от бана:*\n   ⏱ Задержка: ${acc.broadcastDelay} мин (±1 мин)\n   📊 Пауза после: ${messagesBeforePause} сообщений\n   ⏸ Длительность паузы: ${pauseDurationMinutes} мин\n   ❌ Стоп после: ${MAX_CONSECUTIVE_ERRORS} ошибок подряд\n\n` +
      `⏱ Примерное время: ~${hours}ч ${mins}мин\n\n` +
      `⏳ Рассылка по кругу пока не остановите...`,
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [Markup.button.callback('⏹ СТОП РАССЫЛКУ', `stop_broadcast_${broadcastId}`)]
          ]
        }
      }
    );

    let sent = 0;
    let failed = 0;
    let loopCount = 0;
    let chatIndex = 0;
    let errorPauseCycles = 0;

    while (true) {
      const broadcast = activeBroadcasts.get(broadcastId);
      if (!broadcast) {
        // Рассылка была удалена извне
        isBroadcasting = false;
        acc.isBroadcasting = false;
        return;
      }

      if (broadcast.stop) {
        isBroadcasting = false;
        acc.isBroadcasting = false;
        await safeReply(ctx,
          `⏹ *Рассылка остановлена*\n\n✅ Отправлено: ${sent}\n❌ Ошибок: ${failed}\n📊 За сегодня: ${acc.sentToday}`,
          { parse_mode: 'Markdown' }
        );
        activeBroadcasts.delete(broadcastId);
        return;
      }

      // === ПРОВЕРКА ОТКЛЮЧЕНИЯ АККАУНТА ===
      if (acc.status !== 'connected') {
        isBroadcasting = false;
        acc.isBroadcasting = false;
        broadcast.stop = true;
        await safeReply(ctx,
          `⚠️ *Аккаунт отключился!*\n\n` +
          `✅ Отправлено до отключения: ${sent}\n` +
          `❌ Ошибок: ${failed}\n\n` +
          `🔄 Используйте "🔄 Проверить статус" для восстановления и продолжения рассылки.`,
          { parse_mode: 'Markdown' }
        );
        activeBroadcasts.delete(broadcastId);
        return;
      }

      // === ПРОВЕРКА ПАУЗЫ ===
      if (broadcast.isPaused) {
        const now = Date.now();
        if (broadcast.pauseEndTime && now < broadcast.pauseEndTime) {
          const remainingMin = Math.ceil((broadcast.pauseEndTime - now) / 60000);
          // Обновляем статус каждые 30 секунд
          if (remainingMin % 1 === 0) {
            await safeReply(ctx,
              `⏸ *ПАУЗА*\n\n` +
              `⏱ Осталось: ~${remainingMin} мин\n` +
              `📊 Отправлено: ${sent} | ❌ Ошибок: ${failed}\n` +
              `📊 За сегодня: ${acc.sentToday}`,
              {
                parse_mode: 'Markdown',
                reply_markup: {
                  inline_keyboard: [
                    [Markup.button.callback('⏹ СТОП РАССЫЛКУ', `stop_broadcast_${broadcastId}`)]
                  ]
                }
              }
            );
          }
          await new Promise(r => setTimeout(r, 30000));
          continue;
        } else {
          // Пауза закончилась
          broadcast.isPaused = false;
          broadcast.pauseEndTime = undefined;
          broadcast.messagesSincePause = 0;
          broadcast.consecutiveErrors = 0;
          await safeReply(ctx,
            `✅ *Пауза завершена!*\n\n` +
            `📊 Продолжаем рассылку...\n` +
            `📊 Отправлено: ${sent} | ❌ Ошибок: ${failed}`,
            { parse_mode: 'Markdown' }
          );
        }
      }

      // === ПРОВЕРКА ЛИМИТА СООБЩЕНИЙ ПЕРЕД ПАУЗОЙ ===
      if (broadcast.messagesSincePause >= broadcast.messagesBeforePause) {
        broadcast.isPaused = true;
        broadcast.pauseEndTime = Date.now() + (broadcast.pauseDurationMinutes * 60000);
        broadcast.messagesSincePause = 0;
        await safeReply(ctx,
          `⏸ *ПАУЗА*\n\n` +
          `📊 Достигнут лимит ${broadcast.messagesBeforePause} сообщений\n` +
          `⏱ Пауза: ${broadcast.pauseDurationMinutes} мин\n\n` +
          `📊 За сегодня: ${acc.sentToday}`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [Markup.button.callback('⏹ СТОП РАССЫЛКУ', `stop_broadcast_${broadcastId}`)]
              ]
            }
          }
        );
        await new Promise(r => setTimeout(r, broadcast.pauseDurationMinutes * 60000));
        continue;
      }

      if (chatIndex >= archivedChats.length) {
        chatIndex = 0;
        loopCount++;
        await safeReply(ctx,
          `🔄 *Новый круг рассылки #${loopCount + 1}*\n\n` +
          `✅ Отправлено: ${sent}\n` +
          `❌ Ошибок: ${failed}\n` +
          `📊 За сегодня: ${acc.sentToday}`,
          {
            parse_mode: 'Markdown',
            reply_markup: {
              inline_keyboard: [
                [Markup.button.callback('⏹ СТОП РАССЫЛКУ', `stop_broadcast_${broadcastId}`)]
              ]
            }
          }
        );
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
        // Проверяем подключение перед каждым сообщением
        if (acc.status !== 'connected') {
          broadcast.stop = true;
          isBroadcasting = false;
          acc.isBroadcasting = false;
          await safeReply(ctx,
            `⚠️ *Аккаунт отключился!*\n\n` +
            `✅ Отправлено: ${sent}\n` +
            `❌ Ошибок: ${failed}`,
            { parse_mode: 'Markdown' }
          );
          activeBroadcasts.delete(broadcastId);
          return;
        }

        const success = await sendMessageWithRetry(acc.client, chatId, text, currentMsgNum, 3);
        if (success) {
          sent++;
          acc.sentToday++;
          broadcast.sent++;
          broadcast.consecutiveErrors = 0; // Сбрасываем счетчик ошибок при успехе
          broadcast.messagesSincePause++;
        } else {
          failed++;
          acc.failedToday++;
          broadcast.failed++;
          broadcast.consecutiveErrors++;

          // === ЗАЩИТА ОТ БАНА: 3 ошибки подряд ===
          if (broadcast.consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
            errorPauseCycles++;
            if (errorPauseCycles >= MAX_ERROR_PAUSE_CYCLES) {
              // После 2 циклов пауз - полная остановка
              broadcast.stop = true;
              isBroadcasting = false;
              acc.isBroadcasting = false;
              await safeReply(ctx,
                `🛑 *РАССЫЛКА ОСТАНОВЛЕНА*\n\n` +
                `❌ Слишком много ошибок (${MAX_CONSECUTIVE_ERRORS} подряд x${errorPauseCycles})\n` +
                `📊 Отправлено: ${sent}\n` +
                `❌ Ошибок: ${failed}\n` +
                `📊 За сегодня: ${acc.sentToday}\n\n` +
                `⚠️ Аккаунт возможно заблокирован.\n` +
                `🔄 Используйте "🔄 Проверить статус" для диагностики.`,
                { parse_mode: 'Markdown' }
              );
              activeBroadcasts.delete(broadcastId);
              return;
            } else {
              // Пауза 1 час
              broadcast.isPaused = true;
              broadcast.pauseEndTime = Date.now() + (ERROR_PAUSE_DURATION * 60000);
              broadcast.consecutiveErrors = 0;
              await safeReply(ctx,
                `⏸ *ПАУЗА ИЗ-ЗА ОШИБОК*\n\n` +
                `❌ ${MAX_CONSECUTIVE_ERRORS} ошибки подряд!\n` +
                `⏱ Пауза: ${ERROR_PAUSE_DURATION} минут\n` +
                `📊 Цикл: ${errorPauseCycles}/${MAX_ERROR_PAUSE_CYCLES}\n\n` +
                `⚠️ Если ошибки повторятся - рассылка будет остановлена.`,
                {
                  parse_mode: 'Markdown',
                  reply_markup: {
                    inline_keyboard: [
                      [Markup.button.callback('⏹ СТОП РАССЫЛКУ', `stop_broadcast_${broadcastId}`)]
                    ]
                  }
                }
              );
              await new Promise(r => setTimeout(r, ERROR_PAUSE_DURATION * 60000));
              continue;
            }
          }
        }

        // Обновляем прогресс каждые 3 сообщения
        if (currentMsgNum % 3 === 0) {
          let pauseInfo = '';
          if (broadcast.isPaused) {
            const remainingMin = broadcast.pauseEndTime ? Math.ceil((broadcast.pauseEndTime - Date.now()) / 60000) : 0;
            pauseInfo = `\n⏸ До паузы: ${broadcast.messagesBeforePause - broadcast.messagesSincePause}`;
          }
          await safeReply(ctx,
            `📊 Круг ${loopCount + 1} | ${chatIndex + 1}/${archivedChats.length}\n` +
            `✅ ${sent} | ❌ ${failed}${pauseInfo}\n` +
            `📊 За сегодня: ${acc.sentToday}`,
            {
              reply_markup: {
                inline_keyboard: [
                  [Markup.button.callback('⏹ СТОП', `stop_broadcast_${broadcastId}`)]
                ]
              }
            }
          );
        }
      } catch (error) {
        failed++;
        acc.failedToday++;
        broadcast.failed++;
        broadcast.consecutiveErrors++;
        console.error(`Message send error:`, error);
      }

      chatIndex++;

      // Задержка с учётом настроек аккаунта (±1 минута)
      const delay = getBroadcastDelay(accountId);
      console.log(`Waiting ${Math.round(delay / 60000)} minutes before next message...`);
      await new Promise(r => setTimeout(r, delay));
    }
  } catch (error) {
    isBroadcasting = false;
    acc.isBroadcasting = false;
    console.error('Broadcast error:', error);
    await safeReply(ctx, `❌ Ошибка: ${(error as Error).message}`);
  }
}

// Функция для перемешивания массива (Fisher-Yates)
function shuffleArray<T>(array: T[]): T[] {
  const shuffled = [...array];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}

async function startJoinProcess(ctx: any, accountId: string, links: string[], maxToJoin?: number) {
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') return safeReply(ctx, '❌ Аккаунт не подключен');

  // Проверяем, не идёт ли уже присоединение
  if (acc.isJoining) {
    return safeReply(ctx, '⏳ На этом аккаунте уже идёт присоединение к чатам!');
  }
  // NOTE: присоединение и рассылка могут идти одновременно на одном аккаунте

  acc.isJoining = true;

  const validLinks: string[] = [];
  for (const link of links) {
    const code = extractInviteCode(link);
    if (code) validLinks.push(code);
  }
  if (validLinks.length === 0) {
    acc.isJoining = false;
    return safeReply(ctx, '❌ Не найдено валидных ссылок');
  }

  // Перемешиваем ссылки для рандомного выбора
  const shuffledLinks = shuffleArray(validLinks);

  // Если указано maxToJoin - берём только нужное количество
  const linksToJoin = maxToJoin ? shuffledLinks.slice(0, maxToJoin) : shuffledLinks;

  // Сохраняем оригинальные ссылки для pending
  const linksMap = new Map<string, string>();
  for (let i = 0; i < validLinks.length; i++) {
    linksMap.set(validLinks[i], links[i]);
  }

  const joinId = Date.now().toString();
  activeJoins.set(joinId, { stop: false, joined: 0, failed: 0, skippedApproval: 0, total: linksToJoin.length, accountId });

  await safeReply(ctx,
    `🔗 *Присоединение к чатам*\n\n` +
    `📱 Аккаунт: ${acc.name || acc.phone}\n` +
    `📊 Всего в пуле: ${validLinks.length}\n` +
    `🎯 Выбрано для присоединения: ${linksToJoin.length}\n\n` +
    `🛡️ *Логика:*\n` +
    `• Рандомный выбор из пула\n` +
    `• Сразу вступаем (без одобрения)\n` +
    `• Требует одобрение → в pending очередь\n` +
    `• Максимум ${MAX_PENDING_JOIN_REQUESTS} pending заявок\n` +
    `• Задержка: ${acc.joinDelay} мин (±1 мин)\n\n` +
    `⏳ Начинаем...`,
    { parse_mode: 'Markdown' }
  );

  let joined = 0, alreadyJoined = 0, failed = 0, addedToPending = 0, joinsSinceBreak = 0;

  for (let i = 0; i < linksToJoin.length; i++) {
    const join = activeJoins.get(joinId);
    if (join?.stop) {
      await safeReply(ctx, `⏹ Остановлено\n✅ Присоединено: ${joined} | ❌ Ошибок: ${failed} | 📋 В pending: ${addedToPending}`);
      activeJoins.delete(joinId);
      return;
    }

    if (joinsSinceBreak >= MAX_JOINS_BEFORE_BREAK) {
      await safeReply(ctx,
        `⏸ *Перерыв 4 часа*\n\nПосле ${MAX_JOINS_BEFORE_BREAK} присоединений - перерыв\nОжидаем 4 часа...`,
        { parse_mode: 'Markdown' }
      );
      await new Promise(r => setTimeout(r, JOIN_BREAK_DURATION));
      joinsSinceBreak = 0;
      await safeReply(ctx, `✅ Перерыв завершён, продолжаем...`);
    }

    // Проверяем количество pending заявок
    const currentPending = getActivePendingCount(accountId);
    if (currentPending >= MAX_PENDING_JOIN_REQUESTS) {
      // Превышен лимит pending - пропускаем остальные ссылки
      const remaining = linksToJoin.length - i;
      await safeReply(ctx,
        `⚠️ *Лимит pending заявок достигнут (${MAX_PENDING_JOIN_REQUESTS})*\n\n` +
        `📋 Осталось ссылок: ${remaining}\n` +
        `📋 В pending очереди: ${currentPending}\n\n` +
        `⏳ Продолжите после одобрения заявок\n` +
        `🔔 Используйте "📋 Pending заявки" для проверки`,
        { parse_mode: 'Markdown' }
      );
      break;
    }

    const inviteCode = linksToJoin[i];
    console.log(`[${i+1}/${linksToJoin.length}] Joining: ${inviteCode} (pending: ${currentPending}/${MAX_PENDING_JOIN_REQUESTS})`);

    try {
      const result = await joinGroupByInvite(acc.client, inviteCode);
      if (result.sessionInvalid) {
        // Сессия устарела - останавливаем и просим переподключить
        acc.status = 'disconnected';
        await safeReply(ctx,
          `⚠️ *Сессия аккаунта устарела!*\n\n` +
          `📱 Номер: ${acc.phone}\n` +
          `❌ Присоединено: ${joined}\n` +
          `⏭️ Уже в группе: ${alreadyJoined}\n` +
          `❌ Ошибок: ${failed}\n` +
          `📋 В pending: ${addedToPending}\n\n` +
          `🔄 Для продолжения:\n` +
          `1. Перейдите в "📱 Аккаунты"\n` +
          `2. Нажмите "❌ Отвязать номер"\n` +
          `3. Подключите номер заново через QR`,
          { parse_mode: 'Markdown' }
        );
        activeJoins.delete(joinId);
        pendingGroupLinks.delete(accountId);
        acc.isJoining = false;
        return;
      }
      if (result.success) {
        if (result.alreadyJoined) alreadyJoined++;
        else { joined++; joinsSinceBreak++; }
      } else if (result.needsApproval) {
        // Чат требует одобрения - добавляем в pending очередь
        const fullLink = linksMap.get(inviteCode) || `https://chat.whatsapp.com/${inviteCode}`;
        addToPendingQueue(accountId, inviteCode, fullLink);
        addedToPending++;
        console.log(`  📋 Added to pending queue`);
      } else {
        failed++;
      }

      if (i % 3 === 0 || i === linksToJoin.length - 1) {
        await safeReply(ctx,
          `📊 ${i + 1}/${linksToJoin.length}\n` +
          `✅ Присоединено: ${joined}\n` +
          `⏭️ Уже в группе: ${alreadyJoined}\n` +
          `❌ Ошибок: ${failed}\n` +
          `📋 В pending: ${addedToPending}`
        );
      }
    } catch (error) {
      failed++;
    }

    if (i < linksToJoin.length - 1) {
      const delay = getRandomJoinDelay(accountId);
      console.log(`Waiting ${Math.round(delay / 60000)} minutes before next join...`);
      await new Promise(r => setTimeout(r, delay));
    }
  }

  pendingGroupLinks.delete(accountId);
  acc.isJoining = false;

  const pendingCount = getActivePendingCount(accountId);
  let finalText = `🏁 *Присоединение завершено*\n\n` +
    `✅ Присоединено: ${joined}\n` +
    `⏭️ Уже в группе: ${alreadyJoined}\n` +
    `❌ Ошибок: ${failed}\n` +
    `📋 Добавлено в pending: ${addedToPending}`;

  if (pendingCount > 0) {
    finalText += `\n\n📋 *В pending очереди:* ${pendingCount}\n🔔 Одобрите заявки в WhatsApp, затем нажмите "Проверить заявки"`;
  }

  await safeReply(ctx, finalText, { parse_mode: 'Markdown' });
  activeJoins.delete(joinId);
}

// Функция проверки pending заявок с авто-продолжением
async function checkPendingRequests(ctx: any, accountId: string) {
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') return safeReply(ctx, '❌ Аккаунт не подключен');

  const queue = pendingJoinRequests.get(accountId) || [];
  const pendingItems = queue.filter(p => p.status === 'pending');

  if (pendingItems.length === 0) {
    await safeReply(ctx, '📭 Нет pending заявок для проверки');
    return;
  }

  const processorId = Date.now().toString();
  activePendingProcessor.set(processorId, {
    stop: false,
    total: pendingItems.length,
    pending: pendingItems.length,
    approved: 0,
    rejected: 0
  });

  await safeReply(ctx,
    `🔍 *Проверка pending заявок*\n\n` +
    `📋 Всего: ${pendingItems.length}\n` +
    `🔄 Проверяем и автоматически вступаем при одобрении...`,
    { parse_mode: 'Markdown' }
  );

  let approved = 0;
  let stillPending = 0;
  let rejected = 0;

  for (let i = 0; i < pendingItems.length; i++) {
    const processor = activePendingProcessor.get(processorId);
    if (processor?.stop) break;

    const item = pendingItems[i];
    console.log(`[Check ${i+1}/${pendingItems.length}] Checking: ${item.inviteCode}`);

    try {
      // Пытаемся присоединиться повторно
      const result = await joinGroupByInvite(acc.client, item.inviteCode);

      if (result.success) {
        // Заявка одобрена!
        approved++;
        item.status = 'approved';
        console.log(`  ✅ Request approved: ${item.inviteCode}`);
        // Уведомляем пользователя об одобрении
        await safeReply(ctx,
          `🎉 *Заявка одобрена!*\n\n` +
          `✅ Вы автоматически вступили в группу\n` +
          `📋 Осталось pending: ${pendingItems.length - i - 1 + stillPending}`,
          { parse_mode: 'Markdown' }
        );
      } else if (result.needsApproval) {
        // Всё ещё требует одобрения
        stillPending++;
        console.log(`  ⏳ Still pending: ${item.inviteCode}`);
      } else {
        // Заявка отклонена или истекла
        rejected++;
        item.status = 'rejected';
        console.log(`  ❌ Request rejected/expired: ${item.inviteCode}`);
      }
    } catch (error) {
      console.error(`  ❌ Error checking:`, error);
      rejected++;
      item.status = 'rejected';
    }

    // Обновляем прогресс каждые 3 заявки
    if (i % 3 === 0 || i === pendingItems.length - 1) {
      await safeReply(ctx,
        `📊 Проверено: ${i + 1}/${pendingItems.length}\n` +
        `✅ Одобрено: ${approved}\n` +
        `⏳ Ещё pending: ${stillPending}\n` +
        `❌ Отклонено: ${rejected}`
      );
    }

    // Задержка между проверками
    if (i < pendingItems.length - 1) {
      await new Promise(r => setTimeout(r, 5000)); // 5 секунд
    }
  }

  // Удаляем обработанные заявки
  const updatedQueue = queue.filter(p => p.status === 'pending');
  pendingJoinRequests.set(accountId, updatedQueue);

  const remainingPending = updatedQueue.length;

  await safeReply(ctx,
    `🏁 *Проверка завершена*\n\n` +
    `✅ Одобрено: ${approved}\n` +
    `⏳ Ещё pending: ${remainingPending}\n` +
    `❌ Отклонено: ${rejected}\n\n` +
    (remainingPending > 0 ? `⏳ Дождитесь одобрения остальных заявок\n🔔 Затем нажмите "Проверить заявки" снова` : `🎉 Все заявки обработаны!`),
    { parse_mode: 'Markdown' }
  );

  activePendingProcessor.delete(processorId);
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
  } catch (e) {}
}

let isBroadcasting = false;

function startChromeAutoRestart() {
  setInterval(async () => {
    if (isBroadcasting) return;
    console.log('🔄 Scheduled Chrome restart...');
    for (const [id, acc] of waAccounts) {
      if (acc.status === 'connected') {
        try {
          await acc.client.destroy();
          acc.status = 'disconnected';
        } catch (e) {}
      }
    }
  }, 6 * 60 * 60 * 1000);
}

// Периодическая проверка и поддержание сессий живыми + авто-переподключение
function startSessionKeepalive() {
  // Каждые 15 минут проверяем все сессии
  setInterval(async () => {
    console.log('🔍 Checking session health...');
    for (const [id, acc] of waAccounts) {
      if (acc.status === 'connected' && acc.client) {
        try {
          // Пинг - проверяем что сессия жива
          const state = await acc.client.getState();
          console.log(`  ✅ Session ${id}: ${state}`);
        } catch (e) {
          const errorMsg = (e as Error).message || '';
          console.log(`  ⚠️ Session ${id} is stale: ${errorMsg}`);

          // Пытаемся восстановить сессию
          if (errorMsg.includes('Execution context') || errorMsg.includes('detached') || errorMsg.includes('stale')) {
            console.log(`  🔄 Attempting to reconnect session ${id}...`);
            try {
              acc.status = 'connecting';
              await acc.client.destroy();

              // Создаём новый клиент для этого аккаунта
              const sessionsBasePath = process.env.WA_SESSIONS_PATH || '/data/wa-sessions';
              const sessionPath = `${sessionsBasePath}/${id}`;

              const newClient = new Client({
                authStrategy: new LocalAuth({
                  clientId: id,
                  dataPath: sessionPath,
                }),
                puppeteer: {
                  headless: true,
                  executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium',
                  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--single-process', '--disable-gpu'],
                },
              });

              // Копируем обработчики событий
              newClient.on('ready', async () => {
                acc.client = newClient;
                acc.status = 'connected';
                console.log(`  ✅ Session ${id} reconnected successfully!`);
              });

              newClient.on('disconnected', async () => {
                acc.status = 'disconnected';
                console.log(`  ⚠️ Session ${id} disconnected again`);
                // ONE-TIME уведомление
                const now = Date.now();
                const lastNotified = lastDisconnectNotification.get(id) || 0;
                if (now - lastNotified > DISCONNECT_NOTIFICATION_COOLDOWN) {
                  lastDisconnectNotification.set(id, now);
                  const msg = `⚠️ *Отключение аккаунта*\n\n📱 Аккаунт: ${acc.name || acc.phone || id}\n\n🔄 Бот автоматически восстановит сессию`;
                  for (const adminId of ADMIN_IDS) {
                    try {
                      await bot.telegram.sendMessage(adminId, msg, { parse_mode: 'Markdown' });
                    } catch (e) {}
                  }
                }
              });

              await newClient.initialize();
            } catch (reconnectError) {
              console.log(`  ❌ Failed to reconnect ${id}: ${(reconnectError as Error).message}`);
              acc.status = 'disconnected';
            }
          }
        }
      } else if (acc.status === 'disconnected' && acc.phone) {
        // Пробуем автоматически переподключить отключенные аккаунты каждые 30 минут
        console.log(`  🔄 Will attempt to reconnect ${id} on next cycle...`);
      }
    }
  }, 15 * 60 * 1000); // 15 минут
}

// Автоматический повторный вход для pending заявок
function startAutoRejoinForAllAccounts() {
  // Каждые 5 минут проверяем pending заявки
  setInterval(async () => {
    console.log('🔄 Checking pending requests for auto-rejoin...');

    for (const [accountId, acc] of waAccounts) {
      // Пропускаем если авто-повтор выключен или аккаунт не подключен
      if (!acc.autoJoinPending || acc.status !== 'connected') {
        continue;
      }

      const queue = pendingJoinRequests.get(accountId) || [];
      const pendingItems = queue.filter(p => p.status === 'pending');

      // Если нет pending заявок, пробуем добавить новые из пула
      if (pendingItems.length === 0 && getReadyLinks( 1).length > 0) {
        console.log(`  🔄 Auto-rejoin for ${acc.phone || accountId}: Adding new links from pool`);

        // Получаем рандомные ссылки из пула
        const readyLinks = getReadyLinks( 10); // Берём до 10 ссылок
        if (readyLinks.length > 0) {
          const shuffledLinks = shuffleArray(readyLinks);
          const linksToJoin = shuffledLinks.slice(0, MAX_PENDING_JOIN_REQUESTS);

          // Добавляем в pending очередь
          for (const link of linksToJoin) {
            const inviteCode = extractInviteCode(link.fullLink);
            if (inviteCode) {
              addToPendingQueue(accountId, inviteCode, link.fullLink);
              console.log(`    📋 Added: ${inviteCode}`);
            }
          }

          // Запускаем процесс присоединения
          try {
            await autoJoinNextPending(accountId, linksToJoin.map(l => ({
              inviteCode: extractInviteCode(l.fullLink)!,
              fullLink: l.fullLink
            })));
          } catch (e) {
            console.log(`  ❌ Auto-rejoin error for ${accountId}: ${(e as Error).message}`);
          }
        }
      } else if (pendingItems.length > 0) {
        // Есть pending заявки - проверяем их статус
        console.log(`  🔍 Checking ${pendingItems.length} pending requests for ${acc.phone || accountId}`);

        // Проверяем каждый pending запрос
        for (const item of pendingItems) {
          try {
            const result = await joinGroupByInvite(acc.client, item.inviteCode);

            if (result.success) {
              // Заявка одобрена!
              item.status = 'approved';
              console.log(`    ✅ Approved: ${item.inviteCode}`);
            } else if (!result.needsApproval) {
              // Заявка отклонена или истекла
              item.status = 'rejected';
              console.log(`    ❌ Rejected/expired: ${item.inviteCode}`);

              // Автоматически пробуем следующую ссылку из пула
              const moreLinks = getReadyLinks( 1);
              if (moreLinks.length > 0) {
                const shuffled = shuffleArray(moreLinks);
                const link = shuffled[0];
                const inviteCode = extractInviteCode(link.fullLink);
                if (inviteCode) {
                  addToPendingQueue(accountId, inviteCode, link.fullLink);
                  console.log(`    📋 Auto-added replacement: ${inviteCode}`);

                  // Сразу пробуем присоединиться
                  await joinGroupByInvite(acc.client, inviteCode);
                }
              }
            }
          } catch (e) {
            console.log(`    ⚠️ Error checking ${item.inviteCode}: ${(e as Error).message}`);
          }

          // Небольшая задержка между проверками
          await new Promise(r => setTimeout(r, 2000));
        }

        // Удаляем обработанные заявки из очереди
        const updatedQueue = queue.filter(p => p.status === 'pending');
        pendingJoinRequests.set(accountId, updatedQueue);
      }
    }
  }, 5 * 60 * 1000); // 5 минут
}

// Автоматическое присоединение к следующему pending чату
async function autoJoinNextPending(accountId: string, links: { inviteCode: string; fullLink: string }[]) {
  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') return;

  const maxToJoin = Math.min(links.length, MAX_PENDING_JOIN_REQUESTS);

  for (let i = 0; i < maxToJoin; i++) {
    const link = links[i];

    // Проверяем лимит pending
    if (getPendingCount() >= MAX_PENDING_JOIN_REQUESTS) {
      break;
    }

    try {
      const result = await joinGroupByInvite(acc.client, link.inviteCode);

      if (result.needsApproval) {
        addToPendingQueue(accountId, link.inviteCode, link.fullLink);
        updateLinkStatus( link.inviteCode, 'pending');
      } else if (result.success) {
        updateLinkStatus( link.inviteCode, 'joined');
      } else {
        updateLinkStatus( link.inviteCode, 'failed', result.error || 'Unknown error');
      }
    } catch (e) {
      updateLinkStatus( link.inviteCode, 'failed', (e as Error).message);
    }

    // Задержка между попытками
    await new Promise(r => setTimeout(r, JOIN_DELAY_MIN));
  }
}

async function main() {
  console.log('🚀 Starting bot...');
  console.log('Admin IDs:', ADMIN_IDS);

  // Восстанавливаем существующие аккаунты
  await restoreExistingAccounts();

  // Ждём немного для инициализации сессий
  await new Promise(r => setTimeout(r, 3000));

  // Проверяем и обновляем статусы аккаунтов
  await refreshAccountsStatus();

  await setupBotMenu();
  startChromeAutoRestart();

  // Запускаем периодическую проверку сессий для поддержания их живыми
  startSessionKeepalive();

  // Запускаем авто-повтор pending заявок
  startAutoRejoinForAllAccounts();

  await bot.launch();
  console.log('✅ Bot started');
}

// Глобальные обработчики ошибок для предотвращения крашей
// Эти ошибки часто происходят в puppeteer и не должны крашить бота

// Функция проверки критических ошибок puppeteer
function isPuppeteerCriticalError(msg: string): boolean {
  const criticalPatterns = [
    'Execution context was destroyed',
    'Execution context',
    'Target closed',
    'Protocol error',
    'Navigation',
    'detached Frame',
    'TargetCrashed',
    'Page Crashed',
    'crashed',
    'Session closed',
    'Session closed',
  ];
  return criticalPatterns.some(pattern => msg.toLowerCase().includes(pattern.toLowerCase()));
}

process.on('uncaughtException', (error) => {
  const errorMsg = error.message || '';
  console.error('⚠️ Uncaught Exception:', errorMsg);

  // Execution context errors - логируем но не крашим бота
  if (isPuppeteerCriticalError(errorMsg)) {
    console.log('🔄 Ignoring puppeteer navigation/execution error - session will be reconnected');
    return; // Не выходим из процесса
  }

  // Для других ошибок - выходим
  console.error('❌ Fatal error, exiting...');
  console.error(error.stack);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  const reasonStr = String(reason);
  console.error('⚠️ Unhandled Rejection at:', promise, 'reason:', reasonStr);

  // Игнорируем известные ошибки puppeteer
  if (isPuppeteerCriticalError(reasonStr)) {
    console.log('🔄 Ignoring puppeteer rejection - session will be reconnected');
    return;
  }
});

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
