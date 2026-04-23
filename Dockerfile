FROM node:22-bookworm

WORKDIR /app

# Install dependencies first (better caching)
COPY package*.json ./
RUN npm ci

# Install Playwright + system deps
RUN npx playwright install --with-deps chromium

# Copy app source
COPY . .

# Build NestJS
RUN npm run build

ENV NODE_ENV=production

# Railway uses PORT env var
EXPOSE 3000

CMD ["node", "dist/main.js"]