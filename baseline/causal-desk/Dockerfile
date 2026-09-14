FROM node:20-alpine
WORKDIR /app

# su-exec：入口脚本里从 root 降权到 node（alpine 自带仓库）
RUN apk add --no-cache su-exec

# 仅依赖 express（纯 JS，无原生编译）
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund
COPY src ./src
COPY web ./web
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 数据目录：预先创建并交给运行用户 node(uid 1000)。
# 全新命名卷首次挂载时会继承镜像目录属主；旧卷/绑定挂载则由入口脚本兜底修属主。
RUN mkdir -p /data && chown -R node:node /data
VOLUME ["/data"]

ENV NODE_ENV=production
# 入口以 root 启动 → chown /data → 降权 node 运行业务进程
ENTRYPOINT ["docker-entrypoint.sh"]
