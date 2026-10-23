#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印带颜色的信息
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在
check_command() {
    if ! command -v $1 &> /dev/null; then
        print_error "$1 is not installed"
        return 1
    fi
    return 0
}

# 检测系统类型
detect_os() {
    if [ -f /etc/redhat-release ]; then
        # 红帽系发行版
        if grep -q "CentOS" /etc/redhat-release; then
            OS="CentOS"
            VER=$(grep -oE '[0-9]+\.[0-9]+' /etc/redhat-release)
        elif grep -q "Red Hat" /etc/redhat-release; then
            OS="Red Hat Enterprise Linux"
            VER=$(grep -oE '[0-9]+\.[0-9]+' /etc/redhat-release)
        elif grep -q "Fedora" /etc/redhat-release; then
            OS="Fedora"
            VER=$(grep -oE '[0-9]+' /etc/redhat-release)
        else
            OS="Unknown RedHat-based"
            VER="Unknown"
        fi
    elif [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$NAME
        VER=$VERSION_ID
    else
        print_error "Cannot determine OS type"
        exit 1
    fi

    print_info "Detected OS: $OS $VER"
}

# 安装依赖
install_dependencies() {
    print_info "Installing dependencies..."
    
    if [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]] || [[ "$OS" == *"Fedora"* ]]; then
        # 红帽系发行版
        if command -v dnf &> /dev/null; then
            sudo dnf install -y gcc-c++ make git
        else
            sudo yum install -y gcc-c++ make git
        fi
    elif [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        sudo apt-get install -y g++ make git
    else
        print_error "Unsupported OS type"
        exit 1
    fi
}

# 安装 CLI11
install_cli11() {
    print_info "Installing CLI11..."

    # 创建目标目录
    sudo mkdir -p /usr/local/include/CLI
    sudo mkdir -p /usr/local/include/CLI/impl

    # Clone into a private temporary directory. The previous version cloned next
    # to the caller's working directory and switched into it with an unchecked
    # `cd`; when the clone failed the copies below ran from the wrong directory
    # under sudo, and the trailing `cd ..; rm -rf CLI11` could remove an
    # unrelated directory of the same name.
    local workdir
    if ! workdir=$(mktemp -d); then
        print_error "Cannot create a temporary directory"
        return 1
    fi
    trap 'rm -rf "${workdir:-}"' RETURN

    if ! git clone --depth 1 https://github.com/CLIUtils/CLI11.git "$workdir/CLI11"; then
        print_error "Failed to clone CLI11"
        return 1
    fi

    # 复制头文件
    if ! sudo cp "$workdir"/CLI11/include/CLI/*.hpp /usr/local/include/CLI/; then
        print_error "Failed to install CLI11 headers"
        return 1
    fi
    if ! sudo cp "$workdir"/CLI11/include/CLI/impl/*.hpp /usr/local/include/CLI/impl/; then
        print_error "Failed to install CLI11 impl headers"
        return 1
    fi
}


# 安装 indicators
install_indicators() {
    print_info "Installing indicators..."

    # 创建目标目录
    sudo mkdir -p /usr/local/include/indicators

    # 同 CLI11：在私有临时目录里 clone，失败即中止，不触碰调用者的目录。
    local workdir
    if ! workdir=$(mktemp -d); then
        print_error "Cannot create a temporary directory"
        return 1
    fi
    trap 'rm -rf "${workdir:-}"' RETURN

    if ! git clone --depth 1 https://github.com/p-ranav/indicators.git "$workdir/indicators"; then
        print_error "Failed to clone indicators"
        return 1
    fi

    # 复制头文件
    if ! sudo cp -r "$workdir"/indicators/include/indicators/* /usr/local/include/indicators/; then
        print_error "Failed to install indicators headers"
        return 1
    fi
    # 确保 indicators.hpp 在正确的位置
    if ! sudo cp "$workdir"/indicators/include/indicators.hpp /usr/local/include/; then
        print_error "Failed to install indicators.hpp"
        return 1
    fi
}

# 创建必要的目录
create_directories() {
    print_info "Creating necessary directories..."
    
    # 创建日志目录
    mkdir -p logs
    
    # 创建测试目录
    mkdir -p test
}

# 检查编译环境
check_environment() {
    print_info "Checking build environment..."
    
    # 检查必要的命令
    for cmd in g++ make git; do
        if ! check_command $cmd; then
            print_error "Required command '$cmd' is not installed"
            exit 1
        fi
    done
    
    # 检查 libssh2，如果不存在则尝试安装
    if ! pkg-config --exists libssh2; then
        print_warn "libssh2 is not installed, attempting to install..."
        
        if [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]] || [[ "$OS" == *"Fedora"* ]]; then
            # 红帽系发行版
            if command -v dnf &> /dev/null; then
                sudo dnf install -y libssh2-devel
            else
                sudo yum install -y libssh2-devel
            fi
        elif [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
            sudo apt-get install -y libssh2-1-dev
        else
            print_error "Cannot install libssh2 on unsupported OS type"
            exit 1
        fi
        
        # 再次检查是否安装成功
        if ! pkg-config --exists libssh2; then
            print_error "Failed to install libssh2"
            exit 1
        fi
        print_info "Successfully installed libssh2"
    fi
}

# 编译项目
build_project() {
    print_info "Building project..."
    
    make clean
    if ! make; then
        print_error "Build failed"
        exit 1
    fi
}

# 主函数
main() {
    print_info "Starting installation process..."
    
    # 检测系统类型
    detect_os
    
    # 检查环境
    check_environment
    
    # 安装依赖
    install_dependencies
    
    # 安装 CLI11
    install_cli11 || { print_error "CLI11 installation failed"; return 1; }
    
    # 安装 indicators
    install_indicators || { print_error "indicators installation failed"; return 1; }
    
    # 创建目录
    create_directories || { print_error "Failed to create directories"; return 1; }
    
    # 编译项目
    build_project || { print_error "Project build failed"; return 1; }
    
    
    print_info "Installation completed successfully!"
}

# 执行主函数
main 