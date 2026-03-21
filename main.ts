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
        autoJoinPending: acc.autoJoinPending
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
        ],
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
      autoJoinPending: accData.autoJoinPending || false
    });

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
      }
    });

    client.on('auth_failure', async (msg) => {
      const acc = waAccounts.get(accountId);
      if (acc) {
        acc.status = 'disconnected';
        console.log(`❌ Auth failure for ${accountId}: ${msg}`);
      }
    });

    try {
      await client.initialize();
    } catch (error) {
      console.error(`Failed to initialize ${accountId}:`, error);
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
  total: number;
  accountId: string;
}>();
const pendingGroupLinks = new Map<string, string[]>();

const isAdmin = (userId: number) => ADMIN_IDS.includes(userId);

// === КОНФИГУРАЦИЯ АНТИ-БАНА ===
const FIXED_DELAY = 10 * 60 * 1000;  // ФИКСИРОВАННЫЕ 10 минут
const MAX_MESSAGES_PER_DAY = 150;
const NIGHT_PAUSE_START = 24;       // Отключена
const NIGHT_PAUSE_END = 24;

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

// === СИСТЕМА ПЕРСИСТЕНТНОГО ПУЛА ===
const POOL_FILE_PATH = process.env.POOL_FILE_PATH || './data/pool.json';

interface PoolLink {
  inviteCode: string;
  fullLink: string;
  status: 'ready' | 'pending' | 'joined' | 'failed';
  addedAt: string;
  processedAt?: string;
  accountId?: string;
  error?: string;
}

interface Pool {
  version: number;
  accounts: {
    [accountId: string]: {
      ready: PoolLink[];
      pending: PoolLink[];
      joined: PoolLink[];
      failed: PoolLink[];
    };
  };
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
    version: 1,
    accounts: {},
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
      return JSON.parse(data);
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

// Инициализировать аккаунт в пуле
function initAccountInPool(accountId: string): void {
  if (!globalPool.accounts[accountId]) {
    globalPool.accounts[accountId] = {
      ready: [],
      pending: [],
      joined: [],
      failed: []
    };
  }
}

// Добавить ссылки в пул (для всех аккаунтов)
function addLinksToPool(links: string[]): { added: number; duplicates: number } {
  let added = 0;
  let duplicates = 0;

  // Собираем все существующие коды
  const existingCodes = new Set<string>();
  for (const accId in globalPool.accounts) {
    const acc = globalPool.accounts[accId];
    acc.ready.forEach(l => existingCodes.add(l.inviteCode));
    acc.pending.forEach(l => existingCodes.add(l.inviteCode));
    acc.joined.forEach(l => existingCodes.add(l.inviteCode));
    acc.failed.forEach(l => existingCodes.add(l.inviteCode));
  }

  // Получаем все подключенные аккаунты
  const connectedAccounts = [...waAccounts.entries()]
    .filter(([_, a]) => a.status === 'connected')
    .map(([id, _]) => id);

  if (connectedAccounts.length === 0) {
    return { added: 0, duplicates: 0 };
  }

  // Распределяем ссылки между аккаунтами
  for (let i = 0; i < links.length; i++) {
    const link = links[i];
    const code = extractInviteCode(link);

    if (!code) continue;

    if (existingCodes.has(code)) {
      duplicates++;
      continue;
    }

    // Выбираем аккаунт по round-robin
    const accountId = connectedAccounts[i % connectedAccounts.length];
    initAccountInPool(accountId);

    const poolLink: PoolLink = {
      inviteCode: code,
      fullLink: link,
      status: 'ready',
      addedAt: new Date().toISOString()
    };

    globalPool.accounts[accountId].ready.push(poolLink);
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

// Получить ссылки для обработки
function getReadyLinks(accountId: string, count: number): PoolLink[] {
  initAccountInPool(accountId);
  const ready = globalPool.accounts[accountId].ready;
  // Берем рандомные ссылки
  const shuffled = [...ready].sort(() => Math.random() - 0.5);
  return shuffled.slice(0, Math.min(count, shuffled.length));
}

// Обновить статус ссылки
function updateLinkStatus(accountId: string, inviteCode: string, status: PoolLink['status'], error?: string): void {
  const acc = globalPool.accounts[accountId];
  if (!acc) return;

  // Найти ссылку
  let link: PoolLink | undefined;
  let sourceArray: PoolLink[] | undefined;

  for (const arr of [acc.ready, acc.pending, acc.joined, acc.failed]) {
    const found = arr.find(l => l.inviteCode === inviteCode);
    if (found) {
      link = found;
      sourceArray = arr;
      break;
    }
  }

  if (!link || !sourceArray) return;

  // Удалить из старого массива
  const idx = sourceArray.findIndex(l => l.inviteCode === inviteCode);
  if (idx > -1) sourceArray.splice(idx, 1);

  // Обновить глобальную статистику
  globalPool.globalStats[link.status]--;

  // Добавить в новый массив
  link.status = status;
  link.processedAt = new Date().toISOString();
  link.accountId = accountId;
  if (error) link.error = error;

  globalPool.accounts[accountId][status].push(link);
  globalPool.globalStats[status]++;

  savePool(globalPool);
}

// Получить pending ссылки аккаунта
function getAccountPendingLinks(accountId: string): PoolLink[] {
  initAccountInPool(accountId);
  return globalPool.accounts[accountId].pending;
}

// Количество ready ссылок
function getReadyCount(accountId: string): number {
  initAccountInPool(accountId);
  return globalPool.accounts[accountId].ready.length;
}

// Количество pending ссылок
function getPendingCount(accountId: string): number {
  initAccountInPool(accountId);
  return globalPool.accounts[accountId].pending.length;
}

// Получить всех connected аккаунтов с их статистикой
function getAccountPoolStats(): { accountId: string; phone: string; ready: number; pending: number; joined: number; failed: number }[] {
  const stats: { accountId: string; phone: string; ready: number; pending: number; joined: number; failed: number }[] = [];

  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected') {
      initAccountInPool(id);
      const accPool = globalPool.accounts[id];
      stats.push({
        accountId: id,
        phone: acc.phone || acc.name || id,
        ready: accPool.ready.length,
        pending: accPool.pending.length,
        joined: accPool.joined.length,
        failed: accPool.failed.length
      });
    }
  });

  return stats;
}

// Очистить пул (только processed = joined + failed)
function clearProcessedPool(): void {
  for (const accId in globalPool.accounts) {
    const acc = globalPool.accounts[accId];
    globalPool.globalStats.joined -= acc.joined.length;
    globalPool.globalStats.failed -= acc.failed.length;
    acc.joined = [];
    acc.failed = [];
  }
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

async function joinGroupByInvite(client: Client, inviteCode: string): Promise<{ success: boolean; error?: string; needsApproval?: boolean; alreadyJoined?: boolean; skipped?: boolean; sessionInvalid?: boolean }> {
  try {
    // Проверяем что сессия валидна
    if (!client.info?.wid) {
      console.log(`  ⚠️ Session invalid - client not ready`);
      return { success: false, error: 'Сессия неактивна', sessionInvalid: true };
    }

    console.log(`  Checking/joining group with invite code: ${inviteCode}`);
    const allChats = await client.getChats();
    const existingChat = allChats.find((chat: any) =>
      chat.isGroup && chat.link && chat.link.includes(inviteCode)
    );
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
    } catch (joinError: any) {
      const joinErrorMsg = joinError.message || String(joinError);

      // Detached Frame - сессия стала невалидной
      if (joinErrorMsg.includes('detached Frame') || joinErrorMsg.includes('Evaluation failed')) {
        console.log(`  ⚠️ Session became invalid (detached Frame) - needs re-auth`);
        return { success: false, error: 'Сессия устарела, требуется переподключение', sessionInvalid: true };
      }

      // Если требуется одобрение - добавляем в pending очередь
      if (joinErrorMsg.includes('approval') || joinErrorMsg.includes('join') || joinErrorMsg.includes('require') || joinErrorMsg.includes('Admin') || joinErrorMsg.includes('401')) {
        console.log(`  ⚠️ Group requires approval - adding to pending queue`);
        return { success: false, error: 'Требуется одобрение', needsApproval: true };
      }

      // Для других ошибок - пробросим их выше
      throw joinError;
    }
  } catch (error: any) {
    const errorMessage = error.message || String(error);
    console.error(`  ✗ Join failed:`, errorMessage);

    // Detached Frame
    if (errorMessage.includes('detached Frame') || errorMessage.includes('Evaluation failed')) {
      return { success: false, error: 'Сессия устарела, требуется переподключение', sessionInvalid: true };
    }
    if (errorMessage.includes('invalid') || errorMessage.includes('expired')) {
      return { success: false, error: 'Ссылка недействительна или истекла' };
    }
    if (errorMessage.includes('approval') || errorMessage.includes('join') || errorMessage.includes('require') || errorMessage.includes('Admin') || errorMessage.includes('401')) {
      console.log(`  ⚠️ Group requires approval - adding to pending queue`);
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
        // Авто-повтор pending
        const autoText = acc.autoJoinPending ? '🔄 Авто-повтор: ВКЛ' : '🔄 Авто-повтор: ВЫКЛ';
        buttons.push([Markup.button.callback(autoText, `toggle_auto_pending_${accountId}`)]);
        buttons.push([Markup.button.callback('🔍 Просмотр чатов', `view_chats_${accountId}`)]);
      }
      buttons.push([Markup.button.callback('🔄 Проверить статус', `refresh_acc_${accountId}`)]);
      buttons.push([Markup.button.callback('X Отвязать номер', `unbind_${accountId}`)]);
    }
    buttons.push([Markup.button.callback('< Back to list', 'accounts')]);
  } else {
    if (waAccounts.size > 0) {
      waAccounts.forEach((acc, id) => {
        const emoji = acc.status === 'connected' ? '🟢' : acc.status === 'connecting' ? '🔄' : '🔴';
        const enabledEmoji = acc.enabled ? '✅' : '❌';
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
  const accStats = getAccountPoolStats();

  let text = '📊 *Пул чатов*\n\n';
  text += `📈 *Общая статистика:*\n`;
  text += `🟢 Готовы: ${stats.ready}\n`;
  text += `🔔 Pending: ${stats.pending}\n`;
  text += `✅ Присоединено: ${stats.joined}\n`;
  text += `❌ Ошибок: ${stats.failed}\n\n`;

  if (accStats.length > 0) {
    text += `📱 *Выберите номер для присоединения:*\n`;
  }

  const buttons: any[][] = [];

  // Показываем все номера с их статистикой
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected') {
      const pendingCount = getPendingCount(id);
      const readyCount = getReadyCount(id);
      let statusIcon = '';
      if (acc.isJoining) statusIcon = ' 🔄';
      else if (acc.isBroadcasting) statusIcon = ' 📤';
      buttons.push([Markup.button.callback(
        `📱 ${acc.name || acc.phone}${statusIcon} | 🟢${readyCount} 🔔${pendingCount}`,
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

// Выбор номера в пуле - спрашиваем количество чатов
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
      `Используйте "📋 Pending заявки" для остановки или проверки.`,
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [Markup.button.callback('📋 Pending заявки', 'pending_menu')],
            [Markup.button.callback('◀️ Назад в пул', 'pool_menu')]
          ]
        }
      }
    );
    return;
  }

  // Запрашиваем количество чатов
  userStates.set(ctx.from!.id, { action: 'waiting_pool_join_count', accountId });

  const readyCount = getReadyCount(accountId);
  await safeEdit(ctx,
    `📊 *Выбор количества чатов*\n\n` +
    `📱 Аккаунт: ${acc.name || acc.phone}\n` +
    `🟢 Доступно в пуле: ${readyCount}\n\n` +
    `📝 Введите количество чатов для присоединения:\n` +
    `Например: 5, 10, 20\n\n` +
    `🛡️ Защита от бана:\n` +
    `• Максимум ${MAX_PENDING_JOIN_REQUESTS} pending заявок\n` +
    `• После ${MAX_JOINS_BEFORE_BREAK} присоединений - перерыв 4 часа`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('◀️ Назад в пул', 'pool_menu')]
        ]
      }
    }
  );
});

bot.action('pool_process_pending', async (ctx) => {
  await safeAnswerCb(ctx);
  const accStats = getAccountPoolStats();

  if (accStats.length === 0) {
    await safeReply(ctx, '❌ Нет подключенных аккаунтов');
    return;
  }

  const stats = getPoolStats();
  if (stats.pending === 0) {
    await safeReply(ctx, '📭 Нет pending заявок');
    return;
  }

  await safeReply(ctx, `🔔 *Проверка pending заявок*\n\n⏳ Обрабатываем...`, { parse_mode: 'Markdown' });

  let totalApproved = 0;
  let totalRejected = 0;

  for (const acc of accStats) {
    if (acc.pending === 0) continue;
    const accData = waAccounts.get(acc.accountId);
    if (!accData || accData.status !== 'connected') continue;

    const pendingLinks = getAccountPendingLinks(acc.accountId);

    for (const link of pendingLinks) {
      try {
        const result = await joinGroupByInvite(accData.client, link.inviteCode);
        if (result.success) {
          updateLinkStatus(acc.accountId, link.inviteCode, 'joined');
          totalApproved++;
        } else if (result.needsApproval) {
          // Оставляем pending
        } else {
          updateLinkStatus(acc.accountId, link.inviteCode, 'failed', result.error);
          totalRejected++;
        }
      } catch (error) {
        updateLinkStatus(acc.accountId, link.inviteCode, 'failed', (error as Error).message);
        totalRejected++;
      }
      await new Promise(r => setTimeout(r, 3000));
    }
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

// Обработчики для разного количества
for (const count of [10, 20, 30, 50]) {
  bot.action(`pool_join_${count}`, async (ctx) => {
    await safeAnswerCb(ctx);
    const accStats = getAccountPoolStats();
    if (accStats.length === 0) {
      await safeReply(ctx, '❌ Нет подключенных аккаунтов');
      return;
    }

    const stats = getPoolStats();
    if (stats.ready === 0) {
      await safeReply(ctx, '📭 Нет готовых ссылок');
      return;
    }

    const actualCount = Math.min(count, stats.ready);

    await safeReply(ctx,
      `🚀 *Присоединение ${actualCount} ссылок*\n\n` +
      `📱 Аккаунтов: ${accStats.length}\n` +
      `📊 Распределение: ~${Math.ceil(actualCount / accStats.length)} ссылок на аккаунт\n\n` +
      `⏳ Начинаем процесс...`,
      { parse_mode: 'Markdown' }
    );

    let totalJoined = 0, totalPending = 0, totalFailed = 0, totalAlready = 0, totalSessionErrors = 0;
    let hasSessionError = false;

    for (const acc of accStats) {
      const accData = waAccounts.get(acc.accountId);
      if (!accData || accData.status !== 'connected') {
        await safeReply(ctx, `⚠️ Аккаунт ${acc.phone} не подключен, пропускаем...`);
        continue;
      }

      const linksForAcc = Math.ceil(actualCount / accStats.length);
      const readyLinks = getReadyLinks(acc.accountId, linksForAcc);

      await safeReply(ctx,
        `📱 *${acc.phone}*\n` +
        `🔗 Ссылок для обработки: ${readyLinks.length}\n` +
        `⏳ Обрабатываем...`,
        { parse_mode: 'Markdown' }
      );

      for (let i = 0; i < readyLinks.length; i++) {
        const link = readyLinks[i];

        if (getPendingCount(acc.accountId) >= MAX_PENDING_JOIN_REQUESTS) {
          await safeReply(ctx, `⚠️ Достигнут лимит pending заявок для ${acc.phone}`);
          break;
        }

        try {
          const result = await joinGroupByInvite(accData.client, link.inviteCode);

          if (result.sessionInvalid) {
            // Сессия устарела
            hasSessionError = true;
            totalSessionErrors++;
            accData.status = 'disconnected';
            updateLinkStatus(acc.accountId, link.inviteCode, 'failed', 'Сессия устарела');
            await safeReply(ctx,
              `⚠️ *${acc.phone}*: Сессия устарела!\n` +
              `⏹ Остановка для этого аккаунта.\n` +
              `💡 Перейдите в "📱 Аккаунты" → "🔄 Проверить статус" или переподключите номер.`,
              { parse_mode: 'Markdown' }
            );
            break;
          }

          if (result.success) {
            if (result.alreadyJoined) {
              totalAlready++;
            } else {
              totalJoined++;
            }
            updateLinkStatus(acc.accountId, link.inviteCode, 'joined');
          } else if (result.needsApproval) {
            totalPending++;
            updateLinkStatus(acc.accountId, link.inviteCode, 'pending');
          } else {
            totalFailed++;
            updateLinkStatus(acc.accountId, link.inviteCode, 'failed', result.error);
          }

          // Обновляем прогресс каждые 5 ссылок
          if ((i + 1) % 5 === 0 || i === readyLinks.length - 1) {
            await safeReply(ctx,
              `📊 *${acc.phone}*\n` +
              `🔗 ${i + 1}/${readyLinks.length}\n` +
              `✅ +${totalJoined} | ❌ +${totalFailed} | 🔔 +${totalPending}`
            );
          }
        } catch (error) {
          totalFailed++;
          updateLinkStatus(acc.accountId, link.inviteCode, 'failed', (error as Error).message);
        }

        await new Promise(r => setTimeout(r, JOIN_DELAY_MIN));
      }
    }

    const newStats = getPoolStats();
    let finalText = `🏁 *Завершено*\n\n`;
    finalText += `✅ Присоединено: ${totalJoined}\n`;
    finalText += `🔔 Pending: ${totalPending}\n`;
    finalText += `❌ Ошибок: ${totalFailed}\n`;
    finalText += `⏭️ Уже в группе: ${totalAlready}\n`;
    if (totalSessionErrors > 0) {
      finalText += `⚠️ Сессий устарело: ${totalSessionErrors}\n`;
    }
    finalText += `\n📊 Осталось: 🟢 ${newStats.ready} | 🔔 ${newStats.pending}`;
    if (hasSessionError) {
      finalText += `\n\n💡 Для продолжения:\n1. Перейдите в "📱 Аккаунты"\n2. Проверьте/переподключите аккаунты с устаревшими сессиями`;
    }

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

  await safeReply(ctx, `⏹ Присоединение для ${acc.name || acc.phone} остановлено!`);
  await safeEdit(ctx, `✅ Присоединение остановлено!\n📱 Аккаунт: ${acc.name || acc.phone}`, { ...pendingAccMenu(accountId) });
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

    // Ввод задержки из меню пула
    if (state.action === 'waiting_pool_join_delay') {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      const delayMinutes = parseInt(ctx.message.text.trim());
      if (isNaN(delayMinutes) || delayMinutes < 1 || delayMinutes > 60) {
        await safeReply(ctx, '❌ Введите число от 1 до 60 минут');
        return;
      }
      acc.joinDelay = delayMinutes;
      saveAccountsData();

      const chatCount = (state as any).chatCount || 10;

      userStates.delete(ctx.from!.id);
      await safeReply(ctx,
        `✅ Настройки сохранены!\n\n` +
        `🔗 *Присоединение к чатам*\n\n` +
        `📱 Аккаунт: ${acc!.name || acc!.phone}\n` +
        `📊 Количество чатов: ${chatCount}\n` +
        `⏱ Задержка: ${delayMinutes} мин (±1 мин)\n\n` +
        `📝 Введите ссылки на группы (${chatCount} штук через пробел).\n` +
        `Формат: https://chat.whatsapp.com/Код`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('◀️ Назад в пул', 'pool_menu')]
            ]
          }
        }
      );
      userStates.set(ctx.from!.id, { action: 'waiting_pool_links', accountId, chatCount });
      return;
    }

    // Ввод ссылок из меню пула
    if (state.action === 'waiting_pool_links') {
      const accountId = state.accountId;
      const chatCount = (state as any).chatCount || 10;
      const text = ctx.message.text.trim();
      const links = text.split(/[\s\n]+/).filter(l => l.length > 0);

      if (links.length === 0) {
        await safeReply(ctx, '❌ Не найдены ссылки. Введите ссылки формата: https://chat.whatsapp.com/Код');
        return;
      }

      pendingGroupLinks.set(accountId!, links);
      userStates.delete(ctx.from!.id);

      await safeReply(ctx,
        `✅ Найдено ссылок: ${links.length}\n\n` +
        `📱 Аккаунт: ${waAccounts.get(accountId!)?.name || waAccounts.get(accountId!)?.phone}\n` +
        `📊 Количество: ${chatCount}\n\n` +
        `Нажмите "✅ Да, начать" для присоединения к чатам.`,
        {
          reply_markup: {
            inline_keyboard: [
              [Markup.button.callback('✅ Да, начать', `confirm_join_${accountId}`)],
              [Markup.button.callback('◀️ Назад в пул', 'pool_menu')]
            ]
          }
        }
      );
      return;
    }

    // Ввод задержки для присоединения
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
  if (!fs.existsSync(sessionsPath)) {
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
    broadcastDelay: 10,  // По умолчанию 10 минут
    joinDelay: 10,      // По умолчанию 10 минут
    isBroadcasting: false,
    isJoining: false,
    autoJoinPending: true
  });

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
    if (acc) acc.status = 'disconnected';
    if (ctx) {
      await safeReply(ctx, `⚠️ Аккаунт отключился`);
    }
  });

  try {
    await client.initialize();
  } catch (error) {
    console.error('Initialize error:', error);
    const acc = waAccounts.get(accountId);
    if (acc) acc.status = 'disconnected';
    await safeReply(ctx, `❌ Ошибка: ${(error as Error).message}`);
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
  if (acc.sentToday >= MAX_MESSAGES_PER_DAY) {
    isBroadcasting = false;
    acc.isBroadcasting = false;
    return safeReply(ctx, `❌ Достигнут дневной лимит (${MAX_MESSAGES_PER_DAY} сообщений)`);
  }

  await safeReply(ctx, '🔍 Поиск архивных чатов...');

  try {
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
    activeBroadcasts.set(broadcastId, { stop: false, sent: 0, failed: 0, total: archivedChats.length, accountId });

    const broadcastDelay = getBroadcastDelay(accountId);
    const estimatedMin = Math.ceil(archivedChats.length * (broadcastDelay / 60000));
    const hours = Math.floor(estimatedMin / 60);
    const mins = estimatedMin % 60;

    await safeReply(ctx,
      `🔔 *Рассылка началась!*\n\n` +
      `📱 Аккаунт: ${acc.name || acc.phone}\n` +
      `📊 Найдено: ${archivedChats.length} чатов\n   👥 Групп: ${groups.length}\n   👤 Диалогов: ${individuals.length}\n\n` +
      `🛡️ *Защита:*\n   ⏱ Задержка: ${acc.broadcastDelay} мин (±1 мин)\n   📊 Лимит/день: ${MAX_MESSAGES_PER_DAY}\n\n` +
      `⏱ Примерное время: ~${hours}ч ${mins}мин\n\n` +
      `⏳ Рассылка по кругу пока не остановите...\n` +
      `🛑 Для остановки нажмите "⏹ Стоп"`,
      { parse_mode: 'Markdown' }
    );

    let sent = 0;
    let failed = 0;
    let loopCount = 0;
    let chatIndex = 0;

    while (true) {
      const broadcast = activeBroadcasts.get(broadcastId);
      if (broadcast?.stop) {
        isBroadcasting = false;
        acc.isBroadcasting = false;
        await safeReply(ctx,
          `⏹ *Рассылка остановлена*\n\n✅ Отправлено: ${sent}\n❌ Ошибок: ${failed}\n📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`,
          { parse_mode: 'Markdown' }
        );
        activeBroadcasts.delete(broadcastId);
        return;
      }

      if (acc.sentToday >= MAX_MESSAGES_PER_DAY) {
        await safeReply(ctx,
          `⚠️ *Достигнут дневной лимит*\n\nОтправлено сегодня: ${acc.sentToday}\nМаксимум: ${MAX_MESSAGES_PER_DAY}\n\n⏳ Ожидаем сброса лимита...`,
          { parse_mode: 'Markdown' }
        );
        await new Promise(r => setTimeout(r, 60 * 60 * 1000));
        continue;
      }

      if (chatIndex >= archivedChats.length) {
        chatIndex = 0;
        loopCount++;
        await safeReply(ctx,
          `🔄 *Новый круг рассылки #${loopCount + 1}*\n\n✅ Отправлено: ${sent}\n❌ Ошибок: ${failed}\n📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`,
          { parse_mode: 'Markdown' }
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
        const success = await sendMessageWithRetry(acc.client, chatId, text, currentMsgNum, 3);
        if (success) {
          sent++;
          acc.sentToday++;
        } else {
          failed++;
          acc.failedToday++;
        }

        if (currentMsgNum % 3 === 0) {
          await safeReply(ctx,
            `📊 Круг ${loopCount + 1} | Сообщение ${chatIndex + 1}/${archivedChats.length}\n` +
            `✅ ${sent} | ❌ ${failed}\n` +
            `📊 За сегодня: ${acc.sentToday}/${MAX_MESSAGES_PER_DAY}`
          );
        }
      } catch (error) {
        failed++;
        acc.failedToday++;
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

async function startJoinProcess(ctx: any, accountId: string, links: string[]) {
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

  const joinId = Date.now().toString();
  activeJoins.set(joinId, { stop: false, joined: 0, failed: 0, skippedApproval: 0, total: validLinks.length, accountId });

  await safeReply(ctx,
    `🔗 *Присоединение к чатам*\n\n` +
    `📱 Аккаунт: ${acc.name || acc.phone}\n` +
    `📊 Найдено ссылок: ${validLinks.length}\n\n` +
    `🛡️ *Логика:*\n` +
    `• Сразу вступаем (без одобрения)\n` +
    `• Требует одобрение → в pending очередь\n` +
    `• Максимум ${MAX_PENDING_JOIN_REQUESTS} pending заявок\n` +
    `• Задержка: ${acc.joinDelay} мин (±1 мин)\n\n` +
    `⏳ Начинаем...`,
    { parse_mode: 'Markdown' }
  );

  let joined = 0, alreadyJoined = 0, failed = 0, addedToPending = 0, joinsSinceBreak = 0;

  for (let i = 0; i < validLinks.length; i++) {
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
      const remaining = validLinks.length - i;
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

    const inviteCode = validLinks[i];
    console.log(`[${i+1}/${validLinks.length}] Joining: ${inviteCode} (pending: ${currentPending}/${MAX_PENDING_JOIN_REQUESTS})`);

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
        addToPendingQueue(accountId, inviteCode, links[i]);
        addedToPending++;
        console.log(`  📋 Added to pending queue`);
      } else {
        failed++;
      }

      if (i % 3 === 0 || i === validLinks.length - 1) {
        await safeReply(ctx,
          `📊 ${i + 1}/${validLinks.length}\n` +
          `✅ Присоединено: ${joined}\n` +
          `⏭️ Уже в группе: ${alreadyJoined}\n` +
          `❌ Ошибок: ${failed}\n` +
          `📋 В pending: ${addedToPending}`
        );
      }
    } catch (error) {
      failed++;
    }

    if (i < validLinks.length - 1) {
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
