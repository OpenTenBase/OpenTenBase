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
mkdir -p $LIB_DIR

# 获取所有依赖的库
echo -e "${YELLOW}Collecting library dependencies...${NC}"
LIBS=$(ldd $BINARY_PATH | grep -v "linux-vdso.so.1" | awk '{print $3}' | grep -v "not found")

# 复制库文件
for lib in $LIBS; do
    if [ -f "$lib" ]; then
        echo -e "${GREEN}Copying $lib${NC}"
        cp -L "$lib" $LIB_DIR/
    else
        echo -e "${RED}Warning: Library $lib not found${NC}"
    fi
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