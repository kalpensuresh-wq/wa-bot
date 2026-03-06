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