#!/bin/bash
# Buildroot/Luckfox management entrypoint for pyMC Repeater

set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
INSTALL_DIR="/opt/pymc_repeater"
VENV_DIR="$INSTALL_DIR/venv"
VENV_PIP="$VENV_DIR/bin/pip"
VENV_PYTHON="$VENV_DIR/bin/python"
CONFIG_DIR="/etc/pymc_repeater"
LOG_DIR="/var/log/pymc_repeater"
DATA_DIR="/var/lib/pymc_repeater"
SERVICE_USER="repeater"
INIT_SCRIPT="/etc/init.d/S80pymc-repeater"
PIDFILE="/var/run/pymc-repeater.pid"
LOGFILE="$LOG_DIR/repeater.log"
SERVICE_NAME="pymc-repeater"
SILENT_MODE="${PYMC_SILENT:-${SILENT:-}}"

R2_BASE_URL="https://wheel.pymc.dev/pymc_build_deps"
R2_ENABLED=1

stage() {
    printf '\n==> %s\n' "$1"
}

info() {
    printf '  - %s\n' "$1"
}

warn() {
    printf '  - %s\n' "$1" >&2
}

fail() {
    printf '%s\n' "$1" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

is_buildroot() {
    [ -f /etc/pymc-image-build-id ] && return 0
    [ -f /etc/os-release ] && grep -q '^ID=buildroot$' /etc/os-release 2>/dev/null && return 0
    return 1
}

ensure_root() {
    [ "${EUID}" -eq 0 ] || fail "This command must be run as root."
}

group_exists() {
    grep -q "^$1:" /etc/group 2>/dev/null
}

ensure_group_line() {
    local group_name="$1"
    local gid="$2"
    group_exists "$group_name" && return 0
    printf '%s:x:%s:\n' "$group_name" "$gid" >> /etc/group
}

ensure_service_user() {
    if id "$SERVICE_USER" >/dev/null 2>&1; then
        return 0
    fi

    if command -v useradd >/dev/null 2>&1; then
        useradd --system --home "$DATA_DIR" --shell /sbin/nologin "$SERVICE_USER"
        return 0
    fi

    ensure_group_line "$SERVICE_USER" 990
    printf '%s:x:990:990::%s:/sbin/nologin\n' "$SERVICE_USER" "$DATA_DIR" >> /etc/passwd
    if [ -f /etc/shadow ]; then
        printf '%s:!:19000:0:99999:7:::\n' "$SERVICE_USER" >> /etc/shadow
    fi
}

add_user_to_group() {
    local user_name="$1"
    local group_name="$2"
    local current_line current_members gid escaped_line new_members

    group_exists "$group_name" || return 0
    current_line=$(grep "^${group_name}:" /etc/group 2>/dev/null || true)
    [ -n "$current_line" ] || return 0
    current_members=$(printf '%s' "$current_line" | cut -d: -f4)
    case ",${current_members}," in
        *,"${user_name}",*) return 0 ;;
    esac

    if [ -n "$current_members" ]; then
        new_members="${current_members},${user_name}"
    else
        new_members="${user_name}"
    fi
    gid=$(printf '%s' "$current_line" | cut -d: -f3)
    escaped_line=$(printf '%s\n' "$current_line" | sed 's/[].[^$\\*]/\\&/g')
    sed -i "s/^${escaped_line}\$/${group_name}:x:${gid}:${new_members}/" /etc/group
}

install_system_packages() {
    if is_buildroot; then
        info "Buildroot image detected; using preinstalled packages."
        return 0
    fi

    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        libffi-dev libusb-1.0-0 sudo jq pip python3-venv python3-rrdtool wget swig build-essential python3-dev
}

ensure_yq() {
    local yq_version="v4.40.5"
    local yq_binary="yq_linux_arm64"

    if command -v yq >/dev/null 2>&1 && yq --version 2>&1 | grep -q 'mikefarah/yq'; then
        return 0
    fi

    case "$(uname -m)" in
        x86_64) yq_binary="yq_linux_amd64" ;;
        armv7*|armv6*|armhf) yq_binary="yq_linux_arm" ;;
        aarch64|arm64) yq_binary="yq_linux_arm64" ;;
    esac

    wget -qO /usr/local/bin/yq "https://github.com/mikefarah/yq/releases/download/${yq_version}/${yq_binary}"
    chmod +x /usr/local/bin/yq
}

ensure_venv() {
    if [ ! -x "$VENV_PYTHON" ]; then
        stage "Creating virtual environment"
        python3 -m venv --system-site-packages "$VENV_DIR"
        "$VENV_PIP" install --upgrade pip setuptools wheel >/dev/null 2>&1 || true
    fi
}

preinstall_r2_wheels() {
    local machine_arch arch_tag platform_tag py_tag wheel_base

    [ "$R2_ENABLED" -eq 1 ] || return 0

    machine_arch=$(uname -m)
    case "$machine_arch" in
        aarch64) arch_tag="arm64"; platform_tag="aarch64" ;;
        armv7l|armv7) arch_tag="armv7"; platform_tag="armv7l" ;;
        x86_64) arch_tag="x86_64"; platform_tag="x86_64" ;;
        *) return 0 ;;
    esac

    py_tag=$("$VENV_PYTHON" -c 'import sys; v=f"cp{sys.version_info.major}{sys.version_info.minor}"; print(f"{v}-{v}")' 2>/dev/null || echo "cp311-cp311")
    wheel_base="${R2_BASE_URL}/${arch_tag}/${platform_tag}/${py_tag}"
    "$VENV_PIP" install --find-links "${wheel_base}/index.html" --no-cache-dir \
        "pycryptodome>=3.23.0" "PyNaCl>=1.5.0" cffi "pyyaml>=6.0.0" >/dev/null 2>&1 || true
}

create_init_script() {
    cat > "$INIT_SCRIPT" <<EOF
#!/bin/sh
DAEMON="${VENV_PYTHON}"
PIDFILE="${PIDFILE}"
LOGFILE="${LOGFILE}"
WORKDIR="${DATA_DIR}"
CONFIG_FILE="${CONFIG_DIR}/config.yaml"
RUN_AS="${SERVICE_USER}"

start() {
    mkdir -p "$(dirname "\$PIDFILE")" "$(dirname "\$LOGFILE")" "\$WORKDIR"
    if [ -f "\$PIDFILE" ] && kill -0 "$(cat "\$PIDFILE")" 2>/dev/null; then
        echo "${SERVICE_NAME} is already running."
        return 0
    fi
    start-stop-daemon --start --quiet --background --make-pidfile --pidfile "\$PIDFILE" \\
        --chuid "\$RUN_AS" --exec /bin/sh -- -c "cd \"\$WORKDIR\" && exec \"\$DAEMON\" -m repeater.main --config \"\$CONFIG_FILE\" >>\"\$LOGFILE\" 2>&1"
}

stop() {
    if [ ! -f "\$PIDFILE" ]; then
        echo "${SERVICE_NAME} is not running."
        return 0
    fi
    start-stop-daemon --stop --quiet --retry 5 --pidfile "\$PIDFILE" || true
    rm -f "\$PIDFILE"
}

status() {
    if [ -f "\$PIDFILE" ] && kill -0 "$(cat "\$PIDFILE")" 2>/dev/null; then
        echo "${SERVICE_NAME} is running."
        return 0
    fi
    echo "${SERVICE_NAME} is stopped."
    return 1
}

case "\${1:-}" in
    start) start ;;
    stop) stop ;;
    restart) stop; start ;;
    status) status ;;
    *)
        echo "Usage: \$0 {start|stop|restart|status}"
        exit 1
        ;;
esac
EOF
    chmod 0755 "$INIT_SCRIPT"
}

service_exists() {
    [ -x "$INIT_SCRIPT" ]
}

is_installed() {
    [ -d "$INSTALL_DIR" ] && service_exists
}

is_running() {
    [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null
}

get_version() {
    if [ -x "$VENV_PYTHON" ]; then
        "$VENV_PYTHON" -c "from importlib.metadata import version; print(version('pymc_repeater'))" 2>/dev/null || echo "not installed"
    else
        echo "not installed"
    fi
}

doctor() {
    stage "Checking Buildroot image baseline"

    for cmd in bash git python3 dialog jq wget sudo sqlite3 start-stop-daemon; do
        if command -v "$cmd" >/dev/null 2>&1; then
            info "found $cmd"
        else
            warn "missing $cmd"
        fi
    done

    if python3 -m venv --help >/dev/null 2>&1; then
        info "python venv support available"
    else
        warn "python venv support missing"
    fi

    if python3 - <<'PY'
modules = [
    "sqlite3",
    "yaml",
    "cherrypy",
    "cherrypy_cors",
    "autocommand",
    "jaraco.collections",
    "jaraco.text",
    "paho.mqtt.client",
    "psutil",
    "jwt",
    "ws4py",
    "nacl",
    "periphery",
    "spidev",
    "serial",
    "usb",
    "Crypto",
]
for module in modules:
    __import__(module)
PY
    then
        info "python runtime packages are present"
    else
        warn "python runtime packages are missing"
    fi

    for path in /dev/spidev* /dev/gpiochip*; do
        [ -e "$path" ] && info "detected $path"
    done
}

install_repeater() {
    local git_version machine_arch arch_tag platform_tag py_tag wheel_base ip_address

    ensure_root
    stage "Preparing Buildroot installation"
    install_system_packages
    ensure_service_user

    for grp in plugdev dialout gpio i2c spi; do
        add_user_to_group "$SERVICE_USER" "$grp"
    done

    mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$LOG_DIR" "$DATA_DIR" "$DATA_DIR/.config/pymc_repeater"
    chown -R "$SERVICE_USER:$SERVICE_USER" "$CONFIG_DIR" "$LOG_DIR" "$DATA_DIR"
    chmod 755 "$INSTALL_DIR" "$DATA_DIR"
    chmod 750 "$CONFIG_DIR" "$LOG_DIR"

    cp "$SCRIPT_DIR/config.yaml.example" "$CONFIG_DIR/config.yaml.example"
    [ -f "$CONFIG_DIR/config.yaml" ] || cp "$SCRIPT_DIR/config.yaml.example" "$CONFIG_DIR/config.yaml"
    cp "$SCRIPT_DIR/radio-settings.json" "$DATA_DIR/" 2>/dev/null || true
    cp "$SCRIPT_DIR/radio-presets.json" "$DATA_DIR/" 2>/dev/null || true

    ensure_yq
    ensure_venv

    if [ -d "$SCRIPT_DIR/.git" ]; then
        git -C "$SCRIPT_DIR" fetch --tags 2>/dev/null || true
        git_version=$(python3 -m setuptools_scm 2>/dev/null || echo "1.0.5")
        export SETUPTOOLS_SCM_PRETEND_VERSION="$git_version"
    else
        export SETUPTOOLS_SCM_PRETEND_VERSION="1.0.5"
    fi

    if ! grep -q "Luckfox Pico" /proc/device-tree/model 2>/dev/null; then
        export PIP_ONLY_BINARY=pycryptodome,cffi,PyNaCl,psutil
    fi

    preinstall_r2_wheels

    stage "Installing pyMC Repeater into venv"
    (cd "$SCRIPT_DIR" && "$VENV_PIP" install --upgrade --no-cache-dir .[hardware])

    create_init_script

    stage "Starting service"
    "$INIT_SCRIPT" restart

    ip_address=$(hostname -I | awk '{print $1}')
    if is_running; then
        printf '\nService is running on: http://%s:8000\n' "${ip_address}"
    else
        fail "Installation completed but the service failed to start. Check: sh $0 logs"
    fi
}

upgrade_repeater() {
    ensure_root
    is_installed || fail "Service is not installed."

    ensure_venv
    preinstall_r2_wheels

    stage "Upgrading pyMC Repeater"
    (cd "$SCRIPT_DIR" && "$VENV_PIP" install --upgrade --no-cache-dir .[hardware])
    "$INIT_SCRIPT" restart
}

uninstall_repeater() {
    ensure_root

    stage "Removing service"
    service_exists && "$INIT_SCRIPT" stop || true
    rm -f "$INIT_SCRIPT"
    rm -rf "$INSTALL_DIR" "$CONFIG_DIR" "$LOG_DIR" "$DATA_DIR"
}

manage_service() {
    local action="$1"
    ensure_root
    service_exists || fail "Service is not installed."
    "$INIT_SCRIPT" "$action"
}

show_status() {
    local ip_address version
    version=$(get_version)
    ip_address=$(hostname -I | awk '{print $1}')

    if ! is_installed; then
        printf 'Installation Status: Not Installed\n'
        return 0
    fi

    printf 'Installation Status: Installed\n'
    printf 'Version: %s\n' "$version"
    printf 'Install Directory: %s\n' "$INSTALL_DIR"
    printf 'Config Directory: %s\n' "$CONFIG_DIR"
    printf 'Log File: %s\n' "$LOGFILE"
    printf 'Dashboard: http://%s:8000\n' "$ip_address"
    if is_running; then
        printf 'Service Status: Running\n'
    else
        printf 'Service Status: Stopped\n'
    fi
}

show_logs() {
    mkdir -p "$LOG_DIR"
    touch "$LOGFILE"
    tail -f "$LOGFILE"
}

run_debug() {
    ensure_root
    mkdir -p "$LOG_DIR" "$DATA_DIR"
    exec "$VENV_PYTHON" -m repeater.main --config "$CONFIG_DIR/config.yaml"
}

delegate_to_stock_manage() {
    exec bash "$SCRIPT_DIR/manage.sh" "$@"
}

usage() {
    cat <<'EOF'
Usage: bash buildroot-manage.sh <command>

Commands:
  doctor      Check Buildroot/Luckfox prerequisites
  install     Install pyMC Repeater on the Buildroot image
  upgrade     Upgrade the Buildroot installation from the checked-out repo
  config      Run the stock interactive config flow
  start       Start the init.d service
  stop        Stop the init.d service
  restart     Restart the init.d service
  status      Show Buildroot service status
  logs        Tail the Buildroot log file
  uninstall   Remove the Buildroot installation
  debug       Run repeater.main in the foreground
EOF
}

case "${1:-}" in
    doctor)
        doctor
        ;;
    install)
        install_repeater
        ;;
    upgrade)
        upgrade_repeater
        ;;
    config)
        shift
        delegate_to_stock_manage config "$@"
        ;;
    start|stop|restart)
        manage_service "$1"
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    uninstall)
        uninstall_repeater
        ;;
    debug)
        run_debug
        ;;
    ""|help|-h|--help)
        usage
        ;;
    *)
        fail "Unknown command: ${1}"
        ;;
esac
