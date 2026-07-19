#!/bin/bash
set -e

# ============================================================
#  bitmagnet IPv6 双栈 DHT 爬虫 — 一键部署脚本
#  支持 ARM64 / x86_64
#  用法: bash deploy_bitmagnet.sh
# ============================================================

RED="\033[0;31m"; GREEN="\033[0;32m"; YELLOW="\033[1;33m"; BLUE="\033[0;34m"; NC="\033[0m"
info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }
step()  { echo -e "\n${BLUE}=== $1 ===${NC}"; }

# ============================================================
# 检测架构
# ============================================================
detect_arch() {
    ARCH=$(uname -m)
    case "$ARCH" in
        aarch64|arm64)      IMAGE_TAG="arm64" ;;
        x86_64|amd64)       IMAGE_TAG="amd64" ;;
        *)                  error "不支持的架构: $ARCH"; exit 1 ;;
    esac
    info "检测到架构: $ARCH -> bitmagnet-ipv6:${IMAGE_TAG}"
}

# ============================================================
# 加载镜像
# ============================================================
load_image() {
    local img_file="bitmagnet-ipv6-fixed-${IMAGE_TAG}.tar"
    
    if [ ! -f "$img_file" ]; then
        error "镜像文件不存在: $img_file"
        echo "  请先从 GitHub Releases 下载："
        echo "  https://github.com/yesterday666/bitmagnetPRO/releases"
        exit 1
    fi
    
    info "导入 Docker 镜像..."
    docker load -i "$img_file"
    docker tag bitmagnet-ipv6:latest bitmagnet:ipv6-v2
    info "镜像导入完成"
}

# ============================================================
# 交互式配置
# ============================================================
configure() {
    step "配置参数"
    
    # PostgreSQL
    echo ""
    echo "--- PostgreSQL 数据库配置 ---"
    read -p "  数据库地址 [127.0.0.1]: " PG_HOST; PG_HOST=${PG_HOST:-127.0.0.1}
    read -p "  数据库端口 [5432]: " PG_PORT; PG_PORT=${PG_PORT:-5432}
    read -p "  数据库名   [bitmagnet]: " PG_DB; PG_DB=${PG_DB:-bitmagnet}
    read -p "  数据库用户 [postgres]: " PG_USER; PG_USER=${PG_USER:-postgres}
    read -sp "  数据库密码: " PG_PASS; echo ""
    
    # DHT
    echo ""
    echo "--- DHT 爬虫配置 ---"
    read -p "  DHT 端口 [39651]: " DHT_PORT; DHT_PORT=${DHT_PORT:-39651}
    read -p "  Web 端口 [3333]: " WEB_PORT; WEB_PORT=${WEB_PORT:-3333}
    read -p "  并发倍数 (1-50) [18]: " SCALING; SCALING=${SCALING:-18}
    
    # 存储
    echo ""
    echo "--- 数据存储 ---"
    read -p "  配置目录 [/DATA/AppData/bitmagnet]: " CFG_DIR; CFG_DIR=${CFG_DIR:-/DATA/AppData/bitmagnet}
    
    # 确认
    echo ""
    echo "============================================"
    echo "  配置确认"
    echo "============================================"
    echo "  PostgreSQL: ${PG_USER}@${PG_HOST}:${PG_PORT}/${PG_DB}"
    echo "  DHT 端口:   UDP ${DHT_PORT}"
    echo "  Web 端口:   TCP ${WEB_PORT}"
    echo "  并发倍数:   ${SCALING}"
    echo "  配置目录:   ${CFG_DIR}"
    echo "============================================"
    read -p "确认部署? (y/N): " CONFIRM
    [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ] && { info "已取消"; exit 0; }
}

# ============================================================
# 部署
# ============================================================
deploy() {
    step "开始部署"
    
    # 停止旧容器
    if docker ps -a --format "{{.Names}}" | grep -q "^bitmagnet$"; then
        info "停止旧容器..."
        docker stop bitmagnet 2>/dev/null || true
        docker rm bitmagnet 2>/dev/null || true
    fi
    
    # 创建配置目录
    mkdir -p "${CFG_DIR}/root/.config/bitmagnet"
    
    # 写默认配置
    cat > "${CFG_DIR}/root/.config/bitmagnet/config.yml" << YMLEOF
dht_crawler:
  scaling_factor: ${SCALING}
  save_files_threshold: 50
YMLEOF
    
    # 启动
    info "启动容器..."
    docker run -d \
        --name bitmagnet \
        --network host \
        --hostname "$(hostname)" \
        --restart unless-stopped \
        -e HOSTNAME="$(hostname)" \
        -e DHT_SERVER_ADDR="::" \
        -e DHT_SERVER_PORT="${DHT_PORT}" \
        -e HTTP_SERVER_LOCAL_ADDRESS=":${WEB_PORT}" \
        -e POSTGRES_HOST="${PG_HOST}" \
        -e POSTGRES_PORT="${PG_PORT}" \
        -e POSTGRES_DB="${PG_DB}" \
        -e POSTGRES_USER="${PG_USER}" \
        -e POSTGRES_PASSWORD="${PG_PASS}" \
        -e TMDB_ENABLED="false" \
        -e DHT_CRAWLER_SCALING_FACTOR="${SCALING}" \
        -v "${CFG_DIR}/root/.config/bitmagnet:/root/.config/bitmagnet" \
        bitmagnet:ipv6-v2 \
        worker run --all
    
    sleep 3
    
    # 验证
    if docker ps --format "{{.Names}}" | grep -q "^bitmagnet$"; then
        info "容器已启动"
    else
        error "容器启动失败！"
        docker logs bitmagnet --tail=20
        exit 1
    fi
    
    step "部署完成！"
    echo ""
    echo "  Web 界面: http://$(hostname -I | awk '{print $1}'):${WEB_PORT}"
    echo "  查看状态: curl http://localhost:${WEB_PORT}/status"
    echo "  查看日志: docker logs -f bitmagnet"
    echo ""
    echo "  等待 2-3 分钟 DHT 引导后，访问 Web 界面搜索种子"
}

# ============================================================
# 主流程
# ============================================================
main() {
    echo "============================================"
    echo "  bitmagnet IPv6 双栈 DHT 爬虫 — 部署脚本"
    echo "============================================"
    
    if [ "$EUID" -ne 0 ]; then
        error "请以 root 权限运行"
        echo "  sudo bash $0"
        exit 1
    fi
    
    if ! command -v docker &>/dev/null; then
        error "未检测到 Docker，请先安装"
        echo "  curl -fsSL https://get.docker.com | sh"
        exit 1
    fi
    
    detect_arch
    load_image
    configure
    deploy
}

main "$@"
