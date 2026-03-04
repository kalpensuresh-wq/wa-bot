# WhatsApp Archive Bot

Telegram-бот для рассылки в архивные группы WhatsApp.

## Деплой на Railway

1. Создай репо на GitHub и загрузи файлы
2. railway.app → New Project → Deploy from GitHub
3. Добавь PostgreSQL: + New → Database → PostgreSQL
4. Добавь Variables:
   - `TELEGRAM_BOT_TOKEN` - от @BotFather
   - `ADMIN_TELEGRAM_IDS` - твой Telegram ID
5. Deploy!

## Команды бота

- Аккаунты - добавить/подключить WhatsApp
- Группы - просмотр синхронизированных групп  
- Рассылка - отправить сообщение во все группы
