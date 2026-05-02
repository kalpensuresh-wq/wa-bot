FROM node:18-slim

RUN apt-get update && apt-get install -y \
    chromium \
    fonts-ipafont-gothic \
    fonts-wqy-zenhei \
    fonts-thai-tlwg \
    fonts-kacst \
    fonts-freefont-ttf \
    libxss1 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV NODE_ENV=production
ENV TELEGRAM_BOT_TOKEN=8723493896:AAH-n__fHgNyy46vVu2SS94nk5sefpQ_u1g
ENV ADMIN_TELEGRAM_IDS=7985029246

WORKDIR /app

COPY package.json ./

RUN npm install

COPY . .

RUN npm run build

# Force rebuild - cache clear
RUN rm -rf /app/dist && npm run build

RUN mkdir -p /app/wa-sessions && chmod -R 777 /app/wa-sessions

ENV PORT=3000

EXPOSE 3000

CMD npm run start:prod
