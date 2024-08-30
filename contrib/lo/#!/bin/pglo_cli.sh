#!/bin/bash

# 替换以下变量为你的数据库信息
DB_NAME=""          # 数据库名称
DB_USER=""          # 数据库用户
DB_HOST="localhost" # 数据库主机，默认为 'localhost'
PSQL="psql"         # psql 命令的路径

# 显示用法信息
usage() {
    echo "用法: $0 {import|list|export|delete} [参数]"
    exit 1
}

# 检查是否传入了必要的参数
check_args() {
    if [[ -z "$DB_NAME" ]] || [[ -z "$DB_USER" ]]; then
        echo "错误：请在脚本中更新你的 DB_NAME 和 DB_USER。"
        exit 1
    fi
}

# 导入大型对象到 PostgreSQL
import_lo() {
    local file_path="$1"
    if [[ -z "$file_path" ]]; then
        echo "请提供要导入的文件路径。"
        exit 1
    fi
    OID=$($PSQL -U $DB_USER -d $DB_NAME -h $DB_HOST -t -c "SELECT lo_import('$file_path');")
    echo "大型对象已导入，OID: $OID"
}

# 在 PostgreSQL 中列出所有大型对象
list_lo() {
    $PSQL -U $DB_USER -d $DB_NAME -h $DB_HOST -c "SELECT loid FROM pg_largeobject_metadata;"
}

# 从 PostgreSQL 中导出大型对象
export_lo() {
    local oid="$1"
    local file_path="$2"
    if [[ -z "$oid" ]] || [[ -z "$file_path" ]]; then
        echo "请提供要导出的 OID 和文件路径。"
        exit 1
    fi
    $PSQL -U $DB_USER -d $DB_NAME -h $DB_HOST -c "SELECT lo_export($oid, '$file_path');"
    echo "大型对象 OID $oid 已导出到 $file_path"
}

# 从 PostgreSQL 中删除大型对象
delete_lo() {
    local oid="$1"
    if [[ -z "$oid" ]]; then
        echo "请提供要删除的大型对象的 OID。"
        exit 1
    fi
    $PSQL -U $DB_USER -d $DB_NAME -h $DB_HOST -c "SELECT lo_unlink($oid);"
    echo "大型对象 OID $oid 已删除。"
}

check_args

# 解析命令行参数
case "$1" in
    import)
        import_lo "$2"
        ;;
    list)
        list_lo
        ;;
    export)
        export_lo "$2" "$3"
        ;;
    delete)
        delete_lo "$2"
        ;;
    *)
        usage
