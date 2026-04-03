FROM node:18-alpine AS build

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
RUN npm run build

FROM node:18-alpine

WORKDIR /app
COPY --from=build /app/dist ./dist
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/package.json ./
COPY --from=build /app/vite.config.ts ./
COPY --from=build /app/tsconfig.json ./

EXPOSE 4173

CMD ["npm", "run", "preview"]
