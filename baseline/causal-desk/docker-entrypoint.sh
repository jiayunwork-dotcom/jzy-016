#!/bin/sh
set -e

# 以 root 启动时（compose 默认）：确保数据目录归 node(uid 1000) 所有。
# 这同时修两类情况：
#   1) 旧版本镜像创建的命名卷，挂载点是 root 属主；
#   2) 用户把宿主机目录 bind mount 进来且属主不是 1000。
# 修正后立即降权到 node，业务进程永不以 root 运行。
if [ "$(id -u)" = '0' ]; then
  mkdir -p /data
  chown -R node:node /data
  exec su-exec node "$0" "$@"
fi

exec "$@"
