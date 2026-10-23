#!/bin/bash

# 设置颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查参数
if [ $# -ne 1 ]; then
    echo -e "${RED}Usage: $0 <binary_path>${NC}"
    exit 1
fi

BINARY_PATH=$1
LIB_DIR="lib"

# 检查二进制文件是否存在
if [ ! -f "$BINARY_PATH" ]; then
    echo -e "${RED}Error: Binary file $BINARY_PATH does not exist${NC}"
    exit 1
fi

# 创建lib目录
echo -e "${YELLOW}Creating lib directory...${NC}"
mkdir -p "$LIB_DIR"

# 获取所有依赖的库
#
# 原来写成  ldd | grep -v vdso | awk '{print $3}' | grep -v "not found"：
# "not found" 是在取完第三个字段之后才过滤的，而那时那一行已经是 "not"，
# 于是缺库既不会报错，还会往列表里塞一个叫 "not" 的垃圾项。这里先按整行判断
# 是否缺失，缺失就直接失败——缺库正是最需要知道的事，打包一套不全的库只会让
# 问题推迟到运行时。
echo -e "${YELLOW}Collecting library dependencies...${NC}"

LIBS=""
MISSING=""

while IFS= read -r line; do
    case $line in
        *linux-vdso.so.1*)
            continue
            ;;
    esac

    # 形如 "name => /path/to/lib (0x...)"，或没有 "=>" 的加载器行
    case $line in
        *"=> not found"*)
            name=$(printf '%s\n' "$line" | awk '{print $1}')
            MISSING="$MISSING $name"
            continue
            ;;
        *"=>"*)
            path=$(printf '%s\n' "$line" | awk '{print $3}')
            # 有些平台这一行是 "name => /path (addr)"，$3 就是路径
            if [ -n "$path" ] && [ -f "$path" ]; then
                LIBS="$LIBS $path"
            else
                name=$(printf '%s\n' "$line" | awk '{print $1}')
                MISSING="$MISSING $name"
            fi
            ;;
        *)
            # 没有 "=>" 的行是加载器本身（如 /lib64/ld-linux-x86-64.so.2）
            path=$(printf '%s\n' "$line" | awk '{print $1}')
            case $path in
                /*)
                    if [ -f "$path" ]; then
                        LIBS="$LIBS $path"
                    fi
                    ;;
            esac
            ;;
    esac
done <<EOF
$(ldd "$BINARY_PATH")
EOF

if [ -n "$MISSING" ]; then
    echo -e "${RED}Error: these libraries are not found, cannot collect a complete set:${NC}"
    for name in $MISSING; do
        echo -e "${RED}  $name${NC}"
    done
    exit 1
fi

# 复制库文件
for lib in $LIBS; do
    echo -e "${GREEN}Copying $lib${NC}"
    cp -L "$lib" "$LIB_DIR/"
done

# 创建ld.so.conf文件
echo -e "${YELLOW}Creating ld.so.conf...${NC}"
echo "$(pwd)/$LIB_DIR" > $LIB_DIR/ld.so.conf

# 创建打包脚本
echo -e "${YELLOW}Creating package script...${NC}"
cat > $LIB_DIR/package.sh << 'EOF'
#!/bin/bash
tar -czf libs.tar.gz *
echo "Libraries have been packaged into libs.tar.gz"
EOF

chmod +x $LIB_DIR/package.sh

echo -e "${GREEN}Done! Libraries have been collected in $LIB_DIR directory${NC}"
echo -e "${YELLOW}To package the libraries, run:${NC}"
echo -e "cd $LIB_DIR && ./package.sh" 