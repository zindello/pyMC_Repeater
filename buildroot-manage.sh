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
SERVICE_USER="root"
INIT_SCRIPT="/etc/init.d/S80pymc-repeater"
PIDFILE="/var/run/pymc-repeater.pid"
LOGFILE="$LOG_DIR/repeater.log"
SERVICE_NAME="pymc-repeater"
SILENT_MODE="${PYMC_SILENT:-${SILENT:-}}"

R2_BASE_URL="https://wheel.pymc.dev/pymc_build_deps"
PIWHEELS_INDEX_URL="https://www.piwheels.org/simple"
R2_ENABLED=1
PYMC_CORE_REPO="${PYMC_CORE_REPO:-https://github.com/rightup/pyMC_core.git}"
PYMC_CORE_REF="${PYMC_CORE_REF:-dev}"
set_wheel_dependencies() {
    set -- \
        "pyyaml>=6.0.0" \
        "cherrypy>=18.0.0" \
        "cherrypy-cors==1.7.0" \
        "paho-mqtt>=1.6.0" \
        "psutil>=5.9.0" \
        "pyjwt>=2.8.0" \
        "ws4py>=0.6.0" \
        "pycryptodome>=3.23.0" \
        "PyNaCl>=1.5.0" \
        "cffi" \
        "pyserial" \
        "pyusb" \
        "spidev" \
        "python-periphery" \
        "autocommand" \
        "jaraco.collections" \
        "jaraco.text" \
        "jaraco.context" \
        "tempora" \
        "zc.lockfile" \
        "httpagentparser>=1.5"
    printf '%s\n' "$@"
}

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
    [ "$(id -u 2>/dev/null || echo 1)" -eq 0 ] || fail "This command must be run as root."
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
    if [ "$SERVICE_USER" = "root" ]; then
        return 0
    fi

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

    if [ "$user_name" = "root" ]; then
        return 0
    fi

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

ensure_venv() {
    local recreate=0

    if [ -f "$VENV_DIR/pyvenv.cfg" ] && grep -Eq '^include-system-site-packages *= *true$' "$VENV_DIR/pyvenv.cfg"; then
        stage "Rebuilding virtual environment"
        info "Existing venv uses system site-packages and is not supported on Buildroot."
        recreate=1
    fi

    if [ "$recreate" -eq 1 ]; then
        rm -rf "$VENV_DIR"
    fi

    if [ ! -x "$VENV_PYTHON" ]; then
        stage "Creating virtual environment"
        info "Creating $VENV_DIR"
        info "This can take a minute on Buildroot flash storage."
        python3 -m venv "$VENV_DIR"
        info "Bootstrapping pip, setuptools, and wheel"
        "$VENV_PIP" install --upgrade --no-cache-dir pip setuptools wheel
        info "Virtual environment is ready"
    else
        info "Using existing virtual environment at $VENV_DIR"
    fi
}

ensure_venv_build_backend() {
    if "$VENV_PYTHON" - <<'PY'
import setuptools
import setuptools.build_meta
import wheel
PY
    then
        info "venv build backend is ready"
        return 0
    fi

    stage "Rebuilding virtual environment"
    warn "Existing venv is contaminated or incomplete; recreating it cleanly."
    rm -rf "$VENV_DIR"
    python3 -m venv "$VENV_DIR"
    "$VENV_PIP" install --upgrade --no-cache-dir pip setuptools wheel

    if "$VENV_PYTHON" - <<'PY'
import setuptools
import setuptools.build_meta
import wheel
PY
    then
        info "venv build backend repaired"
        return 0
    fi

    fail "Unable to prepare an isolated venv with setuptools.build_meta on this Buildroot image."
}

get_r2_wheel_base() {
    local machine_arch arch_tag platform_tag py_tag wheel_base

    [ "$R2_ENABLED" -eq 1 ] || return 1

    machine_arch=$(uname -m)
    case "$machine_arch" in
        aarch64) arch_tag="arm64"; platform_tag="aarch64" ;;
        armv7l|armv7) arch_tag="armv7"; platform_tag="armv7l" ;;
        x86_64) arch_tag="x86_64"; platform_tag="x86_64" ;;
        *) return 1 ;;
    esac

    py_tag=$("$VENV_PYTHON" -c 'import sys; v=f"cp{sys.version_info.major}{sys.version_info.minor}"; print(f"{v}-{v}")' 2>/dev/null || echo "cp311-cp311")
    wheel_base="${R2_BASE_URL}/${arch_tag}/${platform_tag}/${py_tag}"
    printf '%s\n' "$wheel_base"
}

preinstall_r2_wheels() {
    local wheel_base

    wheel_base=$(get_r2_wheel_base 2>/dev/null || true)
    [ -n "$wheel_base" ] || return 0

    stage "Checking optional wheel cache"
    info "Trying prebuilt Python wheels from ${wheel_base}/index.html"
    "$VENV_PIP" install --find-links "${wheel_base}/index.html" --only-binary=:all: --no-cache-dir \
        "pycryptodome>=3.23.0" "PyNaCl>=1.5.0" cffi "pyyaml>=6.0.0" >/dev/null 2>&1 || true
    info "Wheel cache step finished"
}

install_buildroot_dependencies() {
    local wheel_base
    local deps

    wheel_base=$(get_r2_wheel_base 2>/dev/null || true)
    deps=$(set_wheel_dependencies)
    stage "Installing Python dependency wheels"
    if [ -n "$wheel_base" ]; then
        info "Using Rightup wheels: ${wheel_base}/index.html"
    fi
    info "Using piwheels fallback: ${PIWHEELS_INDEX_URL}"

    if [ -n "$wheel_base" ]; then
        # shellcheck disable=SC2086
        "$VENV_PIP" install --upgrade --no-cache-dir --only-binary=:all: \
            --find-links "${wheel_base}/index.html" \
            --extra-index-url "${PIWHEELS_INDEX_URL}" \
            $deps
    else
        # shellcheck disable=SC2086
        "$VENV_PIP" install --upgrade --no-cache-dir --only-binary=:all: \
            --extra-index-url "${PIWHEELS_INDEX_URL}" \
            $deps
    fi
}

install_core_into_venv() {
    local core_repo core_spec

    core_repo="$PYMC_CORE_REPO"
    case "$core_repo" in
        *.git) ;;
        *) core_repo="${core_repo}.git" ;;
    esac
    core_spec="pyMC_core[hardware] @ git+${core_repo}@${PYMC_CORE_REF}"
    stage "Installing pyMC_core"
    info "Repo: ${PYMC_CORE_REPO}"
    info "Ref: ${PYMC_CORE_REF}"
    "$VENV_PIP" install --upgrade --no-cache-dir --no-deps --no-build-isolation "$core_spec"
}

install_repeater_package() {
    stage "Installing pyMC Repeater into venv"
    info "Installing checked-out repo without re-resolving dependencies"
    "$VENV_PIP" install --upgrade --no-cache-dir --no-deps --no-build-isolation "$SCRIPT_DIR"
}

create_init_script() {
    cat > "$INIT_SCRIPT" <<'EOF'
#!/bin/sh
DAEMON="__DAEMON__"
PIDFILE="__PIDFILE__"
LOGFILE="__LOGFILE__"
WORKDIR="__WORKDIR__"
CONFIG_FILE="__CONFIG_FILE__"
RUN_AS="__RUN_AS__"

start() {
    mkdir -p "$(dirname "$PIDFILE")" "$(dirname "$LOGFILE")" "$WORKDIR"
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        echo "__SERVICE_NAME__ is already running."
        return 0
    fi
    start-stop-daemon --start --quiet --background --make-pidfile --pidfile "$PIDFILE" \
        --chuid "$RUN_AS" --exec /bin/sh -- -c "cd \"$WORKDIR\" && exec \"$DAEMON\" -m repeater.main --config \"$CONFIG_FILE\" >>\"$LOGFILE\" 2>&1"
}

stop() {
    if [ ! -f "$PIDFILE" ]; then
        echo "__SERVICE_NAME__ is not running."
        return 0
    fi
    start-stop-daemon --stop --quiet --retry 5 --pidfile "$PIDFILE" >/dev/null 2>&1 || true
    rm -f "$PIDFILE"
}

status() {
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        echo "__SERVICE_NAME__ is running."
        return 0
    fi
    echo "__SERVICE_NAME__ is stopped."
    return 1
}

case "${1:-}" in
    start) start ;;
    stop) stop ;;
    restart) stop; start ;;
    status) status ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac
EOF
    sed -i \
        -e "s|__DAEMON__|${VENV_PYTHON}|g" \
        -e "s|__PIDFILE__|${PIDFILE}|g" \
        -e "s|__LOGFILE__|${LOGFILE}|g" \
        -e "s|__WORKDIR__|${DATA_DIR}|g" \
        -e "s|__CONFIG_FILE__|${CONFIG_DIR}/config.yaml|g" \
        -e "s|__RUN_AS__|${SERVICE_USER}|g" \
        -e "s|__SERVICE_NAME__|${SERVICE_NAME}|g" \
        "$INIT_SCRIPT"
    chmod 0755 "$INIT_SCRIPT"
}

get_primary_ip() {
    ip -o -4 addr show dev eth0 2>/dev/null | awk 'NR==1 { sub(/\/.*/, "", $4); print $4; exit }'
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

check_venv_runtime() {
    "$VENV_PYTHON" - <<'PY'
checks = [
    ("import yaml", "PyYAML"),
    ("import cherrypy", "CherryPy"),
    ("import cherrypy_cors", "cherrypy-cors"),
    ("import paho.mqtt.client", "paho-mqtt"),
    ("import psutil", "psutil"),
    ("import jwt", "PyJWT"),
    ("import ws4py", "ws4py"),
    ("import nacl", "PyNaCl"),
    ("import periphery", "python-periphery"),
    ("import spidev", "spidev"),
    ("import serial", "pyserial"),
    ("import usb", "pyusb"),
    ("from Crypto.Cipher import AES", "pycryptodome AES backend"),
]
failed = []
for stmt, label in checks:
    try:
        exec(stmt, {})
    except Exception as exc:
        failed.append((label, exc))

if failed:
    print("Buildroot runtime validation failed:")
    for label, exc in failed:
        print(f" - {label}: {exc}")
    raise SystemExit(1)
PY
}

install_repeater() {
    local git_version ip_address

    ensure_root
    stage "Preparing Buildroot installation"
    install_system_packages
    ensure_service_user

    stage "Preparing directories and config"
    info "Install dir: $INSTALL_DIR"
    info "Config dir: $CONFIG_DIR"
    info "Data dir: $DATA_DIR"
    mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$LOG_DIR" "$DATA_DIR" "$DATA_DIR/.config/pymc_repeater"
    chown -R root:root "$CONFIG_DIR" "$LOG_DIR" "$DATA_DIR"
    chmod 755 "$INSTALL_DIR" "$DATA_DIR"
    chmod 750 "$CONFIG_DIR" "$LOG_DIR"

    cp "$SCRIPT_DIR/config.yaml.example" "$CONFIG_DIR/config.yaml.example"
    [ -f "$CONFIG_DIR/config.yaml" ] || cp "$SCRIPT_DIR/config.yaml.example" "$CONFIG_DIR/config.yaml"
    cp "$SCRIPT_DIR/radio-settings.json" "$DATA_DIR/" 2>/dev/null || true
    cp "$SCRIPT_DIR/radio-presets.json" "$DATA_DIR/" 2>/dev/null || true

    ensure_venv
    ensure_venv_build_backend

    if [ -d "$SCRIPT_DIR/.git" ]; then
        stage "Inspecting checked-out repo version"
        info "Fetching tags for setuptools_scm version detection"
        git -C "$SCRIPT_DIR" fetch --tags 2>/dev/null || true
        git_version=$(python3 -m setuptools_scm 2>/dev/null || echo "1.0.5")
        export SETUPTOOLS_SCM_PRETEND_VERSION="$git_version"
        info "Using version: $git_version"
    else
        export SETUPTOOLS_SCM_PRETEND_VERSION="1.0.5"
        info "Using fallback version: 1.0.5"
    fi

    preinstall_r2_wheels
    install_buildroot_dependencies
    install_core_into_venv
    install_repeater_package

    stage "Validating installed runtime"
    if check_venv_runtime; then
        info "Installed Python runtime looks usable"
    else
        fail "Installed packages are present but one or more native modules are unusable on this image."
    fi

    stage "Writing Buildroot init service"
    create_init_script

    stage "Starting service"
    "$INIT_SCRIPT" restart

    ip_address=$(get_primary_ip)
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
    ensure_venv_build_backend
    preinstall_r2_wheels

    stage "Upgrading pyMC Repeater"
    install_buildroot_dependencies
    install_core_into_venv
    install_repeater_package
    stage "Validating installed runtime"
    if check_venv_runtime; then
        info "Installed Python runtime looks usable"
    else
        fail "Installed packages are present but one or more native modules are unusable on this image."
    fi
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
    ip_address=$(get_primary_ip)

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
