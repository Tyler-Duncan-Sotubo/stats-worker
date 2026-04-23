FROM node:22-bookworm

WORKDIR /app

# Install app dependencies first (better layer caching)
COPY package*.json ./
RUN npm ci

# Install Playwright deps + binary
RUN npx playwright install-deps chromium && \
    npx playwright install chromium

# Copy source (exclude dist via .dockerignore)
COPY . .

# Build and verify output exists
RUN npm run build && \
    ls -la dist/ && \
    test -f dist/main.js || (echo "ERROR: dist/main.js not found after build" && exit 1)

ENV NODE_ENV=production
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

EXPOSE 3000

CMD ["node", "dist/main.js"]