FROM oven/bun:1.3.3
WORKDIR /app
COPY package.json ./
COPY bun.lock ./
RUN bun install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
RUN bun run build
ENV NODE_ENV=production
EXPOSE 3000
CMD ["bun", "dist/main.js"]
