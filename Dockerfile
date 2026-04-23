FROM node:22-bookworm

WORKDIR /app

# Install Playwright system dependencies at OS level first
RUN npx playwright install-deps chromium

# Install app dependencies
COPY package*.json ./
RUN npm ci

# Install Playwright chromium binary
RUN npx playwright install chromium

# Copy app source
COPY . .

# Build NestJS
RUN npm run build

ENV NODE_ENV=production
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

EXPOSE 3000

CMD ["node", "dist/main.js"]