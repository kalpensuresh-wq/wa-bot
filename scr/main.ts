import { Telegraf, Markup } from 'telegraf';
import { PrismaClient } from '@prisma/client';
import { Client, LocalAuth, MessageMedia } from 'whatsapp-web.js';
import * as QRCode from 'qrcode';
import * as fs from 'fs';
import 'dotenv/config';

const prisma = new PrismaClient();
const bot = new Telegraf(process.env.TELEGRAM_BOT_TOKEN!);

const ADMIN_IDS = (process.env.ADMIN_TELEGRAM_IDS || '')
  .split(',')
  .map(id => parseInt(id.trim()))
  .filter(id => !isNaN(id));

// Хранилище WhatsApp клиентов
const waClients = new Map<string, { client: Client; status: string; qr?: string }>();

// Хранилище состояний пользователей
const userStates = new Map<number, { action?: string; data?: any }>();

// Проверка админа
const isAdmin = (userId: number) => ADMIN_IDS.includes(userId);

// Главное меню
const mainMenu = () => Markup.inlineKeyboard([
  [Markup.button.callback('📱 Аккаунты', 'accounts')],
  [Markup.button.callback('📋 Группы', 'groups')],
  [Markup.button.callback('📨 Рассылка', 'broadcast')],
]);

// Команда /start
bot.start(async (ctx) => {
  if (!isAdmin(ctx.from!.id)) {
    return ctx.reply('⛔ Нет доступа');
  }
  await ctx.reply('🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
    parse_mode: 'Markdown',
    ...mainMenu()
  });
});

// Список аккаунтов
bot.action('accounts', async (ctx) => {
  await ctx.answerCbQuery();
  const accounts = await prisma.account.findMany({
    include: { _count: { select: { groups: true } } }
  });

  let text = '📱 *Аккаунты WhatsApp*\n\n';
  if (accounts.length === 0) {
    text += 'Нет аккаунтов';
  } else {
    accounts.forEach((acc, i) => {
      const wa = waClients.get(acc.id);
      const status = wa?.status || acc.status;
      const emoji = status === 'connected' ? '🟢' : status === 'qr' ? '📱' : '🔴';
      text += `${i + 1}. ${emoji} *${acc.name}*\n`;
      text += `   📋 Групп: ${acc._count.groups}\n\n`;
    });
  }

  const buttons = accounts.map(acc => [
    Markup.button.callback(`${acc.name}`, `acc_${acc.id}`)
  ]);
  buttons.push([Markup.button.callback('➕ Добавить', 'add_account')]);
  buttons.push([Markup.button.callback('◀️ Назад', 'main')]);

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard(buttons)
  });
});

// Добавить аккаунт
bot.action('add_account', async (ctx) => {
  await ctx.answerCbQuery();
  userStates.set(ctx.from!.id, { action: 'waiting_account_name' });
  await ctx.editMessageText('📱 Введите название для аккаунта:', {
    ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'accounts')]])
  });
});

// Просмотр аккаунта
bot.action(/^acc_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const accountId = ctx.match[1];
  const account = await prisma.account.findUnique({
    where: { id: accountId },
    include: { _count: { select: { groups: true } } }
  });

  if (!account) {
    return ctx.reply('❌ Аккаунт не найден');
  }

  const wa = waClients.get(accountId);
  const status = wa?.status || account.status;
  const emoji = status === 'connected' ? '🟢' : status === 'qr' ? '📱' : '🔴';
  const statusText = status === 'connected' ? 'Подключен' : status === 'qr' ? 'Ожидает QR' : 'Отключен';

  let text = `📱 *${account.name}*\n\n`;
  text += `${emoji} Статус: ${statusText}\n`;
  text += `📞 Телефон: ${account.phone || 'Не определен'}\n`;
  text += `📋 Групп: ${account._count.groups}\n`;

  const buttons: any[][] = [];
  if (status === 'connected') {
    buttons.push([Markup.button.callback('🔄 Синхр. группы', `sync_${accountId}`)]);
    buttons.push([Markup.button.callback('🔌 Отключить', `disconnect_${accountId}`)]);
  } else {
    buttons.push([Markup.button.callback('🔗 Подключить', `connect_${accountId}`)]);
  }
  buttons.push([Markup.button.callback('🗑 Удалить', `delete_${accountId}`)]);
  buttons.push([Markup.button.callback('◀️ Назад', 'accounts')]);

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard(buttons)
  });
});

// Подключить аккаунт
bot.action(/^connect_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery('Инициализация...');
  const accountId = ctx.match[1];

  await ctx.editMessageText('⏳ Запуск WhatsApp клиента...');

  try {
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

    waClients.set(accountId, { client, status: 'connecting' });

    client.on('qr', async (qr) => {
      console.log('QR received for', accountId);
      const waClient = waClients.get(accountId);
      if (waClient) {
        waClient.status = 'qr';
        waClient.qr = qr;
      }
      
      try {
        const qrDataUrl = await QRCode.toDataURL(qr);
        const base64Data = qrDataUrl.replace(/^data:image\/png;base64,/, '');
        const buffer = Buffer.from(base64Data, 'base64');
        
        await ctx.replyWithPhoto({ source: buffer }, {
          caption: '📱 *Отсканируйте QR-код*\n\nWhatsApp → Связанные устройства → Привязать',
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
        waClient.qr = undefined;
      }

      const info = client.info;
      await prisma.account.update({
        where: { id: accountId },
        data: { 
          status: 'connected',
          phone: info?.wid?.user || null
        }
      });

      await ctx.reply('✅ Аккаунт подключен!');
    });

    client.on('auth_failure', async () => {
      console.log('Auth failure for', accountId);
      const waClient = waClients.get(accountId);
      if (waClient) waClient.status = 'disconnected';
      
      await prisma.account.update({
        where: { id: accountId },
        data: { status: 'disconnected' }
      });

      await ctx.reply('❌ Ошибка авторизации');
    });

    client.on('disconnected', async () => {
      console.log('Disconnected:', accountId);
      const waClient = waClients.get(accountId);
      if (waClient) waClient.status = 'disconnected';
      
      await prisma.account.update({
        where: { id: accountId },
        data: { status: 'disconnected' }
      });
    });

    await client.initialize();
  } catch (error: any) {
    console.error('Connect error:', error);
    await ctx.reply(`❌ Ошибка: ${error.message}`);
  }
});

// Отключить аккаунт
bot.action(/^disconnect_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const accountId = ctx.match[1];
  
  const waClient = waClients.get(accountId);
  if (waClient) {
    try {
      await waClient.client.destroy();
    } catch (e) {}
    waClients.delete(accountId);
  }

  await prisma.account.update({
    where: { id: accountId },
    data: { status: 'disconnected' }
  });

  await ctx.reply('✅ Отключено');
});

// Синхронизация групп
bot.action(/^sync_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery('Синхронизация...');
  const accountId = ctx.match[1];

  const waClient = waClients.get(accountId);
  if (!waClient || waClient.status !== 'connected') {
    return ctx.reply('❌ Аккаунт не подключен');
  }

  try {
    const chats = await waClient.client.getChats();
    const archivedGroups = chats.filter(chat => chat.isGroup && chat.archived);

    let count = 0;
    for (const group of archivedGroups) {
      await prisma.group.upsert({
        where: {
          waId_accountId: {
            waId: group.id._serialized,
            accountId
          }
        },
        update: { name: group.name },
        create: {
          waId: group.id._serialized,
          name: group.name,
          accountId
        }
      });
      count++;
    }

    await ctx.reply(`✅ Синхронизировано ${count} групп`);
  } catch (error: any) {
    await ctx.reply(`❌ Ошибка: ${error.message}`);
  }
});

// Удалить аккаунт
bot.action(/^delete_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const accountId = ctx.match[1];

  await ctx.editMessageText('⚠️ Удалить аккаунт?', {
    ...Markup.inlineKeyboard([
      [Markup.button.callback('✅ Да', `confirm_del_${accountId}`)],
      [Markup.button.callback('❌ Нет', `acc_${accountId}`)]
    ])
  });
});

bot.action(/^confirm_del_(.+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const accountId = ctx.match[1];

  const waClient = waClients.get(accountId);
  if (waClient) {
    try { await waClient.client.destroy(); } catch (e) {}
    waClients.delete(accountId);
  }

  await prisma.account.delete({ where: { id: accountId } });
  await ctx.reply('✅ Удалено');
});

// Список групп
bot.action('groups', async (ctx) => {
  await ctx.answerCbQuery();
  const groups = await prisma.group.findMany({
    include: { account: true },
    take: 20
  });

  let text = '📋 *Архивные группы*\n\n';
  if (groups.length === 0) {
    text += 'Нет групп. Подключите аккаунт и синхронизируйте.';
  } else {
    text += `Всего: ${groups.length}\n\n`;
    groups.forEach(g => {
      text += `• ${g.name}\n`;
    });
  }

  await ctx.editMessageText(text, {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard([[Markup.button.callback('◀️ Назад', 'main')]])
  });
});

// Рассылка
bot.action('broadcast', async (ctx) => {
  await ctx.answerCbQuery();
  userStates.set(ctx.from!.id, { action: 'waiting_broadcast_text' });

  await ctx.editMessageText('📨 *Создание рассылки*\n\nОтправьте текст или видео с подписью:', {
    parse_mode: 'Markdown',
    ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'main')]])
  });
});

// Назад в главное меню
bot.action('main', async (ctx) => {
  await ctx.answerCbQuery();
  userStates.delete(ctx.from!.id);
  await ctx.editMessageText('🤖 *WhatsApp Broadcaster*\n\nВыберите действие:', {
    parse_mode: 'Markdown',
    ...mainMenu()
  });
});

// Обработка текстовых сообщений
bot.on('text', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;

  const state = userStates.get(ctx.from!.id);
  if (!state) return;

  if (state.action === 'waiting_account_name') {
    const name = ctx.message.text;
    const account = await prisma.account.create({
      data: { name }
    });
    userStates.delete(ctx.from!.id);
    await ctx.reply(`✅ Аккаунт "${name}" создан!`);
    return;
  }

  if (state.action === 'waiting_broadcast_text') {
    const text = ctx.message.text;
    await startBroadcast(ctx, text);
    userStates.delete(ctx.from!.id);
    return;
  }
});

// Обработка видео
bot.on('video', async (ctx) => {
  if (!isAdmin(ctx.from!.id)) return;

  const state = userStates.get(ctx.from!.id);
  if (state?.action === 'waiting_broadcast_text') {
    const caption = ctx.message.caption || '';
    await startBroadcast(ctx, caption, ctx.message.video.file_id);
    userStates.delete(ctx.from!.id);
  }
});

// Функция запуска рассылки
async function startBroadcast(ctx: any, text: string, mediaFileId?: string) {
  const groups = await prisma.group.findMany({
    include: { account: true }
  });

  if (groups.length === 0) {
    return ctx.reply('❌ Нет групп для рассылки');
  }

  const connectedGroups = groups.filter(g => {
    const wa = waClients.get(g.accountId);
    return wa && wa.status === 'connected';
  });

  if (connectedGroups.length === 0) {
    return ctx.reply('❌ Нет подключенных аккаунтов');
  }

  const progressMsg = await ctx.reply(`▶️ Рассылка: 0/${connectedGroups.length}`);
  
  let sent = 0;
  let failed = 0;

  for (const group of connectedGroups) {
    const wa = waClients.get(group.accountId);
    if (!wa || wa.status !== 'connected') {
      failed++;
      continue;
    }

    try {
      await wa.client.sendMessage(group.waId, text);
      sent++;

      try {
        await ctx.telegram.editMessageText(
          ctx.chat.id,
          progressMsg.message_id,
          undefined,
          `▶️ Рассылка: ${sent + failed}/${connectedGroups.length}\n✅ ${sent} | ❌ ${failed}`
        );
      } catch (e) {}

      // Задержка 2-5 сек
      await new Promise(r => setTimeout(r, 2000 + Math.random() * 3000));
    } catch (error) {
      console.error('Send error:', error);
      failed++;
    }
  }

  await ctx.telegram.editMessageText(
    ctx.chat.id,
    progressMsg.message_id,
    undefined,
    `🏁 *Завершено*\n\n✅ Отправлено: ${sent}\n❌ Ошибок: ${failed}`,
    { parse_mode: 'Markdown' }
  );
}

// Запуск
async function main() {
  console.log('🚀 Starting bot...');
  await bot.launch();
  console.log('✅ Bot started');
}

main().catch(console.error);

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
