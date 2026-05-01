import { Telegraf, Markup } from 'telegraf';
import { Client, LocalAuth } from 'whatsapp-web.js';
import * as QRCode from 'qrcode';
import * as fs from 'fs';
import * as path from 'path';
import express from 'express';
import 'dotenv/config';

const app = express();
const PORT = process.env.PORT || 3000;
const ACCOUNTS_FILE_PATH = './data/accounts.json';

interface AccountData {
  phone: string;
  name: string;
  customMessage: string;
  enabled: boolean;
  sentToday: number;
  failedToday: number;
  broadcastDelay: number;
  messagesBeforePause: number;
  pauseDurationMinutes: number;
  proxy?: string;
}

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
        messagesBeforePause: (acc as any).messagesBeforePause || 100,
        pauseDurationMinutes: (acc as any).pauseDurationMinutes || 10,
        proxy: (acc as any).proxy || ''
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

function createClient(accountId: string): Client {
  const sessionsBasePath = process.env.WA_SESSIONS_PATH || '/data/wa-sessions';
  const sessionPath = `${sessionsBasePath}/${accountId}`;

  const puppeteerOptions: any = {
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
      '--disable-background-networking',
      '--disable-extensions',
      '--mute-audio',
      // RAM optimization args
      '--disable-logging',
      '--log-level=3',
      '--disable-cache',
      '--disable-application-cache',
      '--disable-images',
      '--disable-javascript-harmony-shipping',
      '--disable-renderer-backgrounding',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-hang-monitor',
      '--disable-prompt-on-repost',
      '--disable-collapse-stateful-node',
      '--disable-ipc-flooding-protection',
    ],
    ignoreHTTPSErrors: true,
  };

  return new Client({
    authStrategy: new LocalAuth({
      clientId: accountId,
      dataPath: sessionPath,
    }),
    puppeteer: puppeteerOptions,
  });
}

async function restoreExistingAccounts(): Promise<void> {
  const savedAccounts = loadAccountsData();
  const sessionsBasePath = process.env.WA_SESSIONS_PATH || '/data/wa-sessions';

  for (const [accountId, accData] of Object.entries(savedAccounts)) {
    const sessionPath = `${sessionsBasePath}/${accountId}`;

    if (!fs.existsSync(sessionPath)) {
      console.log(`No session folder for ${accountId}, skipping...`);
      continue;
    }

    console.log(`Restoring account: ${accountId} (${accData.phone})`);

    const client = createClient(accountId);

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
      isBroadcasting: false,
      isSleeping: false,
      lastActivity: Date.now(),
      sleepCooldown: 0,
      messagesBeforePause: accData.messagesBeforePause || 100,
      pauseDurationMinutes: accData.pauseDurationMinutes || 10,
      proxy: accData.proxy || ''
    } as any);

    client.on('ready', async () => {
      const acc = waAccounts.get(accountId);
      if (acc) {
        acc.status = 'connected';
        acc.lastActivity = Date.now();
        console.log(`Account restored: ${accountId} (${acc.phone})`);
        saveAccountsData();
      }
    });

    client.on('disconnected', async () => {
      const acc = waAccounts.get(accountId);
      if (acc) {
        acc.status = 'disconnected';
        console.log(`Account disconnected: ${accountId}`);
        saveAccountsData();
      }
    });

    client.on('auth_failure', async (msg) => {
      const acc = waAccounts.get(accountId);
      if (acc) {
        acc.status = 'disconnected';
        console.log(`Auth failure for ${accountId}: ${msg}`);
      }
    });

    await new Promise(r => setTimeout(r, 2000));

    let initAttempts = 0;
    const maxInitAttempts = 3;
    let initSuccess = false;

    while (initAttempts < maxInitAttempts && !initSuccess) {
      try {
        initAttempts++;
        console.log(`Restoring ${accountId} (attempt ${initAttempts}/${maxInitAttempts})...`);
        await client.initialize();
        initSuccess = true;
        console.log(`Account ${accountId} restored successfully`);
      } catch (error) {
        const errorMsg = (error as Error)?.message || String(error);
        console.error(`Restore attempt ${initAttempts} failed for ${accountId}:`, errorMsg);

        if (errorMsg.includes('Execution context') ||
            errorMsg.includes('detached Frame') ||
            errorMsg.includes('Target closed') ||
            errorMsg.includes('Protocol error')) {
          if (initAttempts < maxInitAttempts) {
            await new Promise(r => setTimeout(r, 3000));
          }
        } else {
          console.log(`Non-recoverable error for ${accountId}, skipping`);
          break;
        }
      }
    }

    if (!initSuccess) {
      console.error(`Failed to restore ${accountId} after ${maxInitAttempts} attempts`);
      const acc = waAccounts.get(accountId);
      if (acc) acc.status = 'disconnected';
    }
  }

  await new Promise(r => setTimeout(r, 5000));
  console.log(`Restored ${waAccounts.size} accounts`);
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

app.listen(PORT, () => {
  console.log(`Health check server running on port ${PORT}`);
});

const bot = new Telegraf(process.env.TELEGRAM_BOT_TOKEN!);

const ADMIN_IDS = (process.env.ADMIN_TELEGRAM_IDS || '')
  .split(',')
  .map(id => parseInt(id.trim()))
  .filter(id => !isNaN(id));

const waAccounts = new Map<string, {
  client: Client;
  status: 'disconnected' | 'connecting' | 'connected' | 'sleeping';
  phone?: string;
  name: string;
  customMessage: string;
  enabled: boolean;
  sentToday: number;
  failedToday: number;
  broadcastDelay: number;
  isBroadcasting: boolean;
  isSleeping: boolean;
  lastActivity: number;
  sleepCooldown: number;
  messagesBeforePause: number;
  pauseDurationMinutes: number;
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

const isAdmin = (userId: number) => ADMIN_IDS.includes(userId);

const FIXED_DELAY = 10 * 60 * 1000;
const MAX_CONSECUTIVE_ERRORS = 3;
const ERROR_PAUSE_DURATION = 60;

const SLEEP_INACTIVITY_THRESHOLD = 30 * 60 * 1000;
const SLEEP_WAKEUP_INTERVAL = 10 * 60 * 1000;
const MAX_ACTIVE_ACCOUNTS = 2;

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
  } catch (e) {}
};

function getDelayWithRandom(baseDelayMinutes: number): number {
  const randomMinutes = baseDelayMinutes + (Math.random() * 2 - 1);
  return Math.round(randomMinutes * 60 * 1000);
}

function getBroadcastDelay(accountId: string): number {
  const acc = waAccounts.get(accountId);
  if (acc) {
    return getDelayWithRandom(acc.broadcastDelay);
  }
  return getDelayWithRandom(10);
}

async function isSessionValid(client: Client): Promise<boolean> {
  try {
    if (!client.info?.wid) return false;
    await client.getChats();
    return true;
  } catch (error) {
    return false;
  }
}

function updateAccountActivity(accountId: string): void {
  const acc = waAccounts.get(accountId);
  if (acc) {
    acc.lastActivity = Date.now();
  }
}

async function sleepAccount(accountId: string): Promise<void> {
  const acc = waAccounts.get(accountId);
  if (!acc || acc.isSleeping) return;

  console.log(`Putting account ${accountId} to sleep...`);

  try {
    saveAccountsData();

    if (acc.client) {
      try {
        await acc.client.destroy();
      } catch (e) {}
    }

    acc.isSleeping = true;
    acc.status = 'sleeping';
    acc.client = null as any;

    console.log(`Account ${accountId} is now sleeping (memory freed)`);
  } catch (error) {
    console.error(`Failed to sleep account ${accountId}:`, error);
  }
}

async function wakeupAccount(accountId: string): Promise<boolean> {
  const acc = waAccounts.get(accountId);
  if (!acc || !acc.isSleeping) return false;

  console.log(`Waking up account ${accountId}...`);

  try {
    const client = createClient(accountId);

    acc.client = client;
    acc.status = 'connecting';
    acc.isSleeping = false;
    acc.lastActivity = Date.now();

    client.on('ready', async () => {
      const account = waAccounts.get(accountId);
      if (account) {
        account.status = 'connected';
        account.lastActivity = Date.now();
        console.log(`Account ${accountId} woke up successfully!`);
        saveAccountsData();
      }
    });

    client.on('disconnected', async () => {
      const account = waAccounts.get(accountId);
      if (account) {
        account.status = 'disconnected';
        console.log(`Account ${accountId} disconnected after wakeup`);
        saveAccountsData();
      }
    });

    await client.initialize();
    return true;
  } catch (error) {
    console.error(`Failed to wakeup account ${accountId}:`, error);
    acc.status = 'disconnected';
    acc.isSleeping = false;
    return false;
  }
}

async function sleepManager(): Promise<void> {
  const now = Date.now();

  for (const [accountId, acc] of waAccounts) {
    if (acc.isSleeping) continue;

    if (acc.status === 'connected' && !acc.isBroadcasting) {
      const inactiveTime = now - (acc.lastActivity || 0);

      if (inactiveTime >= SLEEP_INACTIVITY_THRESHOLD) {
        const activeAccounts = [...waAccounts.values()].filter(a =>
          a.status === 'connected' && !a.isSleeping &&
          (a.isBroadcasting || (now - (a.lastActivity || 0) < SLEEP_INACTIVITY_THRESHOLD))
        );

        if (activeAccounts.length > MAX_ACTIVE_ACCOUNTS) {
          await sleepAccount(accountId);
        }
      }
    }
  }
}

setInterval(() => {
  sleepManager().catch(err => console.error('Sleep manager error:', err));
}, SLEEP_WAKEUP_INTERVAL);

const mainMenu = () => {
  return Markup.inlineKeyboard([
    [Markup.button.callback('Accounts', 'accounts')],
    [Markup.button.callback('Broadcast', 'broadcast_menu')],
    [Markup.button.callback('Stats', 'stats')],
    [Markup.button.callback('Settings', 'settings')],
  ]);
};

const accountsMenu = (accountId?: string) => {
  const buttons: any[][] = [];
  if (accountId) {
    const acc = waAccounts.get(accountId);
    if (acc) {
      const statusEmoji = acc.status === 'connected' ? 'OK' :
                         acc.status === 'connecting' ? '...' :
                         acc.status === 'sleeping' ? 'ZZZ' : 'ERR';
      const enabledEmoji = acc.enabled ? 'ON' : 'OFF';

      buttons.push([Markup.button.callback(`Broadcast: ${enabledEmoji}`, `toggle_${accountId}`)]);

      if (acc.status === 'connected') {
        buttons.push([Markup.button.callback('Edit text', `edit_msg_${accountId}`)]);
        buttons.push([Markup.button.callback(`Delay: ${acc.broadcastDelay} min`, `set_broadcast_delay_${accountId}`)]);
        const messagesBeforePause = (acc as any).messagesBeforePause || 100;
        const pauseDurationMinutes = (acc as any).pauseDurationMinutes || 10;
        buttons.push([Markup.button.callback(`Pause: ${messagesBeforePause} msg / ${pauseDurationMinutes} min`, `set_pause_limit_${accountId}`)]);
        const proxyStatus = (acc as any).proxy ? 'ON' : 'OFF';
        buttons.push([Markup.button.callback(`Proxy: ${proxyStatus}`, `set_proxy_${accountId}`)]);
        buttons.push([Markup.button.callback('View chats', `view_chats_${accountId}`)]);
      } else if (acc.isSleeping) {
        buttons.push([Markup.button.callback('Wake up', `wakeup_${accountId}`)]);
        buttons.push([Markup.button.callback('Reconnect (QR)', `reconnect_qr_${accountId}`)]);
      } else {
        buttons.push([Markup.button.callback('Reconnect (QR)', `reconnect_qr_${accountId}`)]);
        buttons.push([Markup.button.callback('Check status', `refresh_acc_${accountId}`)]);
      }
      buttons.push([Markup.button.callback('Unbind', `unbind_${accountId}`)]);
    }
    buttons.push([Markup.button.callback('< Back', 'accounts')]);
  } else {
    if (waAccounts.size > 0) {
      waAccounts.forEach((acc, id) => {
        const emoji = acc.status === 'connected' ? 'OK' :
                      acc.status === 'connecting' ? '...' :
                      acc.status === 'sleeping' ? 'ZZZ' : 'ERR';
        const enabledEmoji = acc.enabled ? 'ON' : 'OFF';
        buttons.push([Markup.button.callback(`${emoji} ${enabledEmoji} ${acc.name || acc.phone || id}`, `acc_${id}`)]);
      });
      buttons.push([Markup.button.callback('Refresh all', 'refresh_all_accounts')]);
    }
    buttons.push([Markup.button.callback('QR Code', 'add_qr')]);
    buttons.push([Markup.button.callback('Back', 'main')]);
  }
  return Markup.inlineKeyboard(buttons);
};

const broadcastSelectMenu = () => {
  const buttons: any[][] = [];
  let hasEnabled = false;
  waAccounts.forEach((acc, id) => {
    if (acc.status === 'connected' && acc.enabled) {
      hasEnabled = true;
      buttons.push([Markup.button.callback(`${acc.name || acc.phone}`, `broadcast_acc_${id}`)]);
    }
  });
  if (!hasEnabled) {
    buttons.push([Markup.button.callback('No enabled accounts', 'main')]);
  }
  buttons.push([Markup.button.callback('Back', 'main')]);
  return Markup.inlineKeyboard(buttons);
};

const settingsMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('Restart all', 'restart_all')],
  [Markup.button.callback('Stats', 'stats')],
  [Markup.button.callback('Back', 'main')],
]);

bot.use(async (ctx, next) => {
  if (ctx.from && !isAdmin(ctx.from.id)) {
    return safeReply(ctx, 'Access denied');
  }
  await next();
});

bot.use(async (ctx, next) => {
  await next().catch(err => {
    console.error('[Error]', err);
    safeReply(ctx, 'Error: ' + err.message);
  });
});

bot.start(async (ctx) => {
  await ctx.reply('WhatsApp Broadcaster\n\nSelect action:', {
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
    `Status\n\n` +
    `Accounts: ${connected}/${waAccounts.size} connected\n` +
    `Enabled: ${enabled}\n` +
    `Sent today: ${totalSent}\n` +
    `Uptime: ${uptime} min`,
    { parse_mode: 'Markdown' }
  );
});

bot.command('stop', async (ctx) => {
  const args = ctx.message.text.split(' ');
  if (args.length < 2) {
    await safeReply(ctx, 'Usage: /stop <broadcast_id>');
    return;
  }
  const broadcastId = args[1];
  const broadcast = activeBroadcasts.get(broadcastId);
  if (broadcast) {
    broadcast.stop = true;
    await safeReply(ctx, `Stop command sent!`);
  } else {
    await safeReply(ctx, 'Broadcast not found');
  }
});

bot.action('main', async (ctx) => {
  await safeAnswerCb(ctx);
  await safeEdit(ctx, 'Main Menu\n\nSelect action:', {
    parse_mode: 'Markdown',
    ...mainMenu()
  });
});

bot.action('accounts', async (ctx) => {
  await safeAnswerCb(ctx);
  const text = `Accounts\n\n` +
    `Total: ${waAccounts.size}\n` +
    `Connected: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}\n` +
    `Enabled: ${[...waAccounts.values()].filter(a => a.enabled).length}`;

  await safeEdit(ctx, text, { parse_mode: 'Markdown', ...accountsMenu() });
});

bot.action('broadcast_menu', async (ctx) => {
  await safeAnswerCb(ctx);
  await safeEdit(ctx, 'Broadcast\n\nSelect account:', {
    parse_mode: 'Markdown',
    ...broadcastSelectMenu()
  });
});

bot.action('stats', async (ctx) => {
  await safeAnswerCb(ctx);

  let text = `Stats\n\n`;
  text += `Total accounts: ${waAccounts.size}\n`;
  text += `Connected: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}\n`;
  text += `Enabled: ${[...waAccounts.values()].filter(a => a.enabled).length}\n\n`;

  if (waAccounts.size === 0) {
    text += `No accounts\n`;
  } else {
    waAccounts.forEach((acc, id) => {
      const status = acc.status === 'connected' ? 'OK' : 'ERR';
      text += `${status} ${acc.name || acc.phone || id}: ${acc.sentToday} sent, ${acc.failedToday} failed\n`;
    });
  }

  text += `\nUptime: ${Math.floor(process.uptime() / 60)} min`;

  await safeEdit(ctx, text, { parse_mode: 'Markdown', ...settingsMenu() });
});

bot.action('settings', async (ctx) => {
  await safeAnswerCb(ctx);
  const text = `Settings\n\n` +
    `Total accounts: ${waAccounts.size}\n` +
    `Connected: ${[...waAccounts.values()].filter(a => a.status === 'connected').length}`;

  await safeEdit(ctx, text, {
    parse_mode: 'Markdown',
    ...settingsMenu()
  });
});

bot.action(/^acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc) {
    await safeEdit(ctx, 'Account not found', { ...accountsMenu() });
    return;
  }

  const statusEmoji = acc.status === 'connected' ? 'OK' :
                     acc.status === 'connecting' ? '...' :
                     acc.status === 'sleeping' ? 'ZZZ' : 'ERR';
  const statusText = acc.status === 'connected' ? 'Connected' :
                    acc.status === 'sleeping' ? 'Sleeping' : 'Disconnected';

  const text = `${acc.name || acc.phone}\n\n` +
    `Status: ${statusEmoji} ${statusText}\n` +
    `Broadcast: ${acc.enabled ? 'Enabled' : 'Disabled'}\n` +
    `Sent: ${acc.sentToday}\n` +
    `Failed: ${acc.failedToday}`;

  await safeEdit(ctx, text, {
    parse_mode: 'Markdown',
    ...accountsMenu(accountId)
  });
});

bot.action('refresh_all_accounts', async (ctx) => {
  await safeAnswerCb(ctx, 'Checking...');

  for (const [accountId, acc] of waAccounts) {
    if (acc.client && acc.status === 'connected') {
      try {
        await acc.client.getChats();
      } catch (e) {
        acc.status = 'disconnected';
      }
    }
  }

  saveAccountsData();

  const connected = [...waAccounts.values()].filter(a => a.status === 'connected').length;
  await safeEdit(ctx,
    `Check complete\n\nConnected: ${connected}/${waAccounts.size}`,
    { parse_mode: 'Markdown', ...accountsMenu() }
  );
});

bot.action(/^refresh_acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx, 'Checking...');
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc) {
    await safeEdit(ctx, 'Account not found', { ...accountsMenu() });
    return;
  }

  if (acc.isSleeping) {
    await safeReply(ctx, 'Account is sleeping. Press "Wake up"');
    return;
  }

  if (acc.client) {
    try {
      await acc.client.getChats();
      await safeReply(ctx, `Account is active`);
    } catch (e) {
      acc.status = 'disconnected';
      await safeReply(ctx, `Account disconnected`);
    }
    saveAccountsData();
  }

  await safeEdit(ctx, `Status: ${acc.status}`, { ...accountsMenu(accountId) });
});

bot.action(/^toggle_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc) return;

  acc.enabled = !acc.enabled;
  saveAccountsData();

  await safeReply(ctx, `Broadcast ${acc.enabled ? 'enabled' : 'disabled'}`);
  await safeEdit(ctx, `${acc.name || acc.phone}`, { ...accountsMenu(accountId) });
});

bot.action(/^edit_msg_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  userStates.set(ctx.from!.id, { action: 'waiting_custom_message', accountId });
  const acc = waAccounts.get(accountId);
  const currentMsg = acc?.customMessage || '';

  await safeEdit(ctx,
    `Text for ${acc?.name || acc?.phone || 'account'}\n\n` +
    `Current: ${currentMsg || 'not set'}\n\n` +
    `Enter new text:`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('Cancel', `acc_${accountId}`)]])
    }
  );
});

bot.action(/^set_broadcast_delay_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  userStates.set(ctx.from!.id, { action: 'waiting_broadcast_delay', accountId });
  const acc = waAccounts.get(accountId);

  await safeEdit(ctx,
    `Broadcast delay\n\n` +
    `Current: ${acc?.broadcastDelay || 10} min\n\n` +
    `Enter (1-60 minutes):`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('Cancel', `acc_${accountId}`)]])
    }
  );
});

bot.action(/^set_pause_limit_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  userStates.set(ctx.from!.id, { action: 'waiting_pause_limit', accountId });
  const acc = waAccounts.get(accountId);
  const mp = (acc as any).messagesBeforePause || 100;
  const pd = (acc as any).pauseDurationMinutes || 10;

  await safeEdit(ctx,
    `Pause settings\n\n` +
    `Current: ${mp} msg -> pause ${pd} min\n\n` +
    `Enter: <limit> <pause>\n` +
    `Example: 50 5`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('Cancel', `acc_${accountId}`)]])
    }
  );
});

bot.action(/^set_proxy_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  userStates.set(ctx.from!.id, { action: 'waiting_proxy', accountId });
  const acc = waAccounts.get(accountId);
  const currentProxy = (acc as any).proxy || 'Not set';

  await safeEdit(ctx,
    `Proxy settings\n\n` +
    `Current: ${currentProxy}\n\n` +
    `Enter proxy URL:\n` +
    `http://user:pass@host:port\n` +
    `or leave empty to remove`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('Cancel', `acc_${accountId}`)]])
    }
  );
});

bot.action(/^unbind_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  await safeEdit(ctx,
    `Unbind number?\n\nAccount will be removed.`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [Markup.button.callback('Yes, unbind', `confirm_unbind_${accountId}`)],
          [Markup.button.callback('Cancel', `acc_${accountId}`)]
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

  const sessionsPath = `${process.env.WA_SESSIONS_PATH || '/data/wa-sessions'}/${accountId}`;
  if (fs.existsSync(sessionsPath)) {
    fs.rmSync(sessionsPath, { recursive: true, force: true });
  }

  waAccounts.delete(accountId);
  await safeReply(ctx, 'Account unbound');
  await safeEdit(ctx, 'Account removed', { ...accountsMenu() });
});

bot.action(/^wakeup_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx, 'Waking up...');
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc || !acc.isSleeping) {
    await safeReply(ctx, 'Account not sleeping');
    return;
  }

  const success = await wakeupAccount(accountId);
  if (success) {
    await safeReply(ctx, `Account waking up! Connecting...`);
  } else {
    await safeReply(ctx, 'Failed to wake up');
  }
});

bot.action('restart_all', async (ctx) => {
  await safeAnswerCb(ctx, 'Restarting...');

  let restarted = 0;
  for (const [id, acc] of waAccounts) {
    if (acc.status === 'disconnected' && acc.phone) {
      try {
        const sessionsPath = `${process.env.WA_SESSIONS_PATH || '/data/wa-sessions'}/${id}`;

        if (!fs.existsSync(sessionsPath)) continue;

        const client = createClient(id);

        acc.client = client;
        acc.status = 'connecting';

        client.on('ready', () => {
          acc.status = 'connected';
          acc.lastActivity = Date.now();
          console.log(`Session ${id} reconnected!`);
        });

        client.on('disconnected', () => {
          acc.status = 'disconnected';
        });

        await client.initialize();
        restarted++;
      } catch (e) {
        console.log(`Failed: ${id}`);
      }
    }
  }

  await safeReply(ctx, `Restarted: ${restarted}`);
});

bot.action('add_qr', async (ctx) => {
  await safeAnswerCb(ctx);
  userStates.set(ctx.from!.id, { action: 'waiting_phone_qr' });
  await safeEdit(ctx,
    'Connect via QR\n\n' +
    'Enter phone number:\n+79991234567',
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('Cancel', 'accounts')]])
    }
  );
});

bot.action(/^view_chats_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx, 'Loading...');
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeReply(ctx, 'Account not connected');
    return;
  }

  try {
    const chats = await acc.client.getChats();
    const groups = chats.filter((c: any) => c.isGroup);

    await safeReply(ctx,
      `Chats for ${acc.name || acc.phone}\n\n` +
      `Total chats: ${chats.length}\n` +
      `Groups: ${groups.length}`,
      { parse_mode: 'Markdown' }
    );
  } catch (e) {
    await safeReply(ctx, 'Error getting chats');
  }
});

bot.action(/^broadcast_acc_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const accountId = ctx.match?.[1];
  if (!accountId) return;

  const acc = waAccounts.get(accountId);
  if (!acc || acc.status !== 'connected') {
    await safeReply(ctx, 'Account not connected');
    return;
  }

  if (!acc.enabled) {
    await safeReply(ctx, 'Broadcast disabled. Enable in settings.');
    return;
  }

  userStates.set(ctx.from!.id, { action: 'waiting_broadcast_count', accountId });

  await safeEdit(ctx,
    `Broadcast\n\n` +
    `Account: ${acc.name || acc.phone}\n` +
    `Text: ${acc.customMessage || 'not set'}\n` +
    `Delay: ${acc.broadcastDelay} min\n\n` +
    `Enter number of chats:`,
    {
      parse_mode: 'Markdown',
      ...Markup.inlineKeyboard([[Markup.button.callback('Cancel', 'broadcast_menu')]])
    }
  );
});

bot.action(/^stop_broadcast_(.+)$/, async (ctx) => {
  await safeAnswerCb(ctx);
  const broadcastId = ctx.match?.[1];
  if (!broadcastId) return;

  const broadcast = activeBroadcasts.get(broadcastId);
  if (!broadcast) {
    await safeReply(ctx, 'Broadcast not found');
    return;
  }

  broadcast.stop = true;
  await safeReply(ctx, `Stop command sent!`);
});

bot.on('text', async (ctx) => {
  const state = userStates.get(ctx.from!.id);
  if (!state) return;

  const text = ctx.message.text.trim();

  switch (state.action) {
    case 'waiting_phone_qr': {
      const phone = text.replace(/[^\d+]/g, '');
      if (phone.length < 10) {
        await safeReply(ctx, 'Invalid phone format');
        return;
      }

      userStates.delete(ctx.from!.id);

      const accountId = `wa_${Date.now()}`;
      const sessionsPath = `${process.env.WA_SESSIONS_PATH || '/data/wa-sessions'}/${accountId}`;

      if (!fs.existsSync(sessionsPath)) {
        fs.mkdirSync(sessionsPath, { recursive: true });
      }

      const client = createClient(accountId);

      waAccounts.set(accountId, {
        client,
        status: 'connecting',
        phone,
        name: phone,
        customMessage: '',
        enabled: true,
        sentToday: 0,
        failedToday: 0,
        broadcastDelay: 10,
        isBroadcasting: false,
        isSleeping: false,
        lastActivity: Date.now(),
        sleepCooldown: 0,
        messagesBeforePause: 100,
        pauseDurationMinutes: 10,
        proxy: ''
      } as any);

      let qrAttempts = 0;
      const maxQrAttempts = 3;

      client.on('qr', async (qr) => {
        const acc = waAccounts.get(accountId);
        if (acc?.status === 'connected') return;

        qrAttempts++;
        if (qrAttempts > maxQrAttempts) {
          await safeReply(ctx, 'QR timeout. Try again.');
          return;
        }

        try {
          const qrImage = await QRCode.toDataURL(qr);
          await ctx.replyWithPhoto({ source: Buffer.from(qrImage.split(',')[1], 'base64') }, {
            caption: `QR code ${qrAttempts}/${maxQrAttempts}\nScan within 60 seconds`
          });
        } catch (e) {
          console.error('QR send error:', e);
        }
      });

      client.on('ready', async () => {
        const acc = waAccounts.get(accountId);
        if (acc) {
          acc.status = 'connected';
          acc.lastActivity = Date.now();
          console.log(`Account ready: ${accountId}`);
          saveAccountsData();

          for (const adminId of ADMIN_IDS) {
            try {
              await bot.telegram.sendMessage(adminId, `Account connected\n\n${phone}`, { parse_mode: 'Markdown' });
            } catch (e) {}
          }
        }
      });

      client.on('disconnected', () => {
        const acc = waAccounts.get(accountId);
        if (acc) {
          acc.status = 'disconnected';
          saveAccountsData();
        }
      });

      client.on('auth_failure', async (msg) => {
        const acc = waAccounts.get(accountId);
        if (acc) {
          acc.status = 'disconnected';
          console.log(`Auth failure: ${msg}`);
        }
        await safeReply(ctx, 'Auth error. Try again.');
      });

      await safeReply(ctx, `Waiting for QR code...`);
      await client.initialize();
      break;
    }

    case 'waiting_custom_message': {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        acc.customMessage = text;
        saveAccountsData();
        await safeReply(ctx, 'Text saved');
        await safeEdit(ctx, `${acc.name || acc.phone}`, { ...accountsMenu(accountId) });
      }
      userStates.delete(ctx.from!.id);
      break;
    }

    case 'waiting_broadcast_delay': {
      const delay = parseInt(text);
      if (isNaN(delay) || delay < 1 || delay > 60) {
        await safeReply(ctx, 'Enter number 1-60');
        return;
      }

      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        acc.broadcastDelay = delay;
        saveAccountsData();
        await safeReply(ctx, `Delay: ${delay} min`);
        await safeEdit(ctx, `${acc.name || acc.phone}`, { ...accountsMenu(accountId) });
      }
      userStates.delete(ctx.from!.id);
      break;
    }

    case 'waiting_pause_limit': {
      const parts = text.split(' ');
      if (parts.length < 2) {
        await safeReply(ctx, 'Enter: limit pause');
        return;
      }

      const limit = parseInt(parts[0]);
      const pause = parseInt(parts[1]);

      if (isNaN(limit) || isNaN(pause)) {
        await safeReply(ctx, 'Invalid numbers');
        return;
      }

      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        (acc as any).messagesBeforePause = limit;
        (acc as any).pauseDurationMinutes = pause;
        saveAccountsData();
        await safeReply(ctx, `Pause: ${limit} msg -> ${pause} min`);
        await safeEdit(ctx, `${acc.name || acc.phone}`, { ...accountsMenu(accountId) });
      }
      userStates.delete(ctx.from!.id);
      break;
    }

    case 'waiting_proxy': {
      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (acc) {
        const proxy = text.trim();
        if (proxy) {
          // Validate proxy format
          if (proxy.startsWith('http://') || proxy.startsWith('https://') || proxy.startsWith('socks')) {
            (acc as any).proxy = proxy;
            saveAccountsData();
            await safeReply(ctx, `Proxy set: ${proxy}`);
          } else {
            await safeReply(ctx, 'Invalid proxy format. Use: http://user:pass@host:port');
            return;
          }
        } else {
          (acc as any).proxy = '';
          saveAccountsData();
          await safeReply(ctx, 'Proxy removed');
        }
        await safeEdit(ctx, `${acc.name || acc.phone}`, { ...accountsMenu(accountId) });
      }
      userStates.delete(ctx.from!.id);
      break;
    }

    case 'waiting_broadcast_count': {
      const count = parseInt(text);
      if (isNaN(count) || count < 1) {
        await safeReply(ctx, 'Enter positive number');
        return;
      }

      const accountId = state.accountId;
      const acc = waAccounts.get(accountId!);
      if (!acc || acc.status !== 'connected') {
        await safeReply(ctx, 'Account not connected');
        userStates.delete(ctx.from!.id);
        return;
      }

      userStates.delete(ctx.from!.id);

      const broadcastId = `bc_${Date.now()}`;
      activeBroadcasts.set(broadcastId, {
        stop: false,
        sent: 0,
        failed: 0,
        consecutiveErrors: 0,
        total: count,
        accountId: accountId!,
        messagesBeforePause: (acc as any).messagesBeforePause || 100,
        pauseDurationMinutes: (acc as any).pauseDurationMinutes || 10,
        isPaused: false,
        messagesSincePause: 0
      });

      acc.isBroadcasting = true;
      updateAccountActivity(accountId!);

      await safeReply(ctx,
        `Broadcast started\n\n` +
        `Account: ${acc.name || acc.phone}\n` +
        `Target: ${count} chats\n` +
        `Delay: ${acc.broadcastDelay} min\n` +
        `ID: ${broadcastId}`,
        { parse_mode: 'Markdown' }
      );

      startBroadcast(accountId!, broadcastId);
      break;
    }
  }
});

async function startBroadcast(accountId: string, broadcastId: string): Promise<void> {
  const acc = waAccounts.get(accountId);
  const broadcast = activeBroadcasts.get(broadcastId);

  if (!acc || !broadcast) return;

  try {
    const allChats = await acc.client.getChats();
    const chats = allChats.filter((c: any) => c.isGroup).slice(0, broadcast.total);

    broadcast.total = Math.min(chats.length, broadcast.total);

    await safeReplyAdmin(
      `Broadcast: ${broadcast.total} groups found\nStarting...`
    );

    for (let i = 0; i < chats.length; i++) {
      if (broadcast.stop) {
        await safeReplyAdmin(`Broadcast stopped!\nSent: ${broadcast.sent}/${broadcast.total}`);
        break;
      }

      if (broadcast.isPaused) {
        if (Date.now() < (broadcast.pauseEndTime || 0)) {
          await new Promise(r => setTimeout(r, 10000));
          i--;
          continue;
        } else {
          broadcast.isPaused = false;
          broadcast.messagesSincePause = 0;
        }
      }

      const chat = chats[i];

      try {
        const message = acc.customMessage || 'Hello!';
        await chat.sendMessage(message);

        broadcast.sent++;
        acc.sentToday++;
        broadcast.consecutiveErrors = 0;
        broadcast.messagesSincePause++;

        console.log(`Sent [${broadcast.sent}/${broadcast.total}] to ${chat.name}`);

        if (broadcast.messagesSincePause >= broadcast.messagesBeforePause) {
          broadcast.isPaused = true;
          broadcast.pauseEndTime = Date.now() + (broadcast.pauseDurationMinutes * 60 * 1000);
          console.log(`Pausing for ${broadcast.pauseDurationMinutes} minutes`);
        }

      } catch (e) {
        broadcast.failed++;
        broadcast.consecutiveErrors++;
        acc.failedToday++;
        console.error(`Failed to send to ${chat.name}:`, e);

        if (broadcast.consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          broadcast.isPaused = true;
          broadcast.pauseEndTime = Date.now() + (ERROR_PAUSE_DURATION * 60 * 1000);
          broadcast.consecutiveErrors = 0;
          console.log(`Too many errors, pausing for ${ERROR_PAUSE_DURATION} minutes`);
        }
      }

      updateAccountActivity(accountId);

      const delay = getBroadcastDelay(accountId);
      await new Promise(r => setTimeout(r, delay));
    }

    acc.isBroadcasting = false;
    saveAccountsData();

    await safeReplyAdmin(
      `Broadcast complete\n\n` +
      `Sent: ${broadcast.sent}\n` +
      `Failed: ${broadcast.failed}\n` +
      `Total: ${broadcast.total}`,
      { parse_mode: 'Markdown' }
    );

  } catch (error) {
    console.error('Broadcast error:', error);
    acc.isBroadcasting = false;
    saveAccountsData();

    await safeReplyAdmin(
      `Broadcast error\n\n${error}`,
      { parse_mode: 'Markdown' }
    );
  }

  activeBroadcasts.delete(broadcastId);
}

async function safeReplyAdmin(text: string, extra?: any): Promise<void> {
  try {
    for (const adminId of ADMIN_IDS) {
      await bot.telegram.sendMessage(adminId, text, { parse_mode: 'Markdown', ...extra });
    }
  } catch (e) {
    console.error('Reply error:', e);
  }
}

async function main() {
  console.log('Starting WhatsApp Bot...');
  await restoreExistingAccounts();
  bot.launch();
  console.log('Bot started!');
}

main().catch(console.error);

process.on('SIGTERM', () => {
  console.log('SIGTERM received, stopping...');
  bot.stop('SIGTERM');
  process.exit(0);
});
