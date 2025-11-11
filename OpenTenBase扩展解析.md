# OpenTenBase扩展解析



## 1. 简介

OpenTenBase是腾讯云数据库团队在 PostgreSQL基础上研发的企业级分布式HTAP开源数据库，拥有一个强大的插件系统，允许个人开发者为OpenTenBase添加新功能。



## 2. 扩展结构



### 2.1  控制文件

控制文件是一个定义了扩展元数据的文件，文件后缀为.control，通常由comment，default_version，module_pathname，relocatable，schema组成。

comment: 是对扩展的简短描述,帮助用户了解扩展的功能。

default_version：这指定了在创建扩展时如果没有指定版本将使用的默认版本。

module_pathname：这是扩展的共享库模块的路径。在数据库中使用扩展时需要加载该路径才能使用扩展。

relocatable：值为布尔类型，表示扩展是否可重定位。可重定位的扩展在创建后可以移动到不同的模式（schema）。

schema：安装扩展对象的模式。



下面是adminpack扩展的控制文件示例

```
# adminpack extension
comment = 'administrative functions for PostgreSQL'
default_version = '1.0'
module_pathname = '$libdir/adminpack' # $libdir 是数据库库目录的占位符。当加载扩展时，数据库会用实际的库目录路径替换 $libdir。
relocatable = false # 被设置为 false，这意味着一旦创建，它就不能被移动到不同的模式。
schema = pg_catalog # 这是 PostgreSQL 的一个系统模式，包含了必要的系统表、视图和函数。
```



### 2.2 SQL脚本文件

SQL脚本文件是用于定义和创建扩展所提供的数据库对象，比如函数、数据类型、操作符、索引方法、表这些。SQL脚本文件可以算做是扩展的比较的核心部分，因为它包含了实现扩展功能的所有必要SQL命令。另外，在一个扩展中，可以有一个SQL脚本文件，也可以有多个脚本文件。当然，如果你不需要对数据库进行操作，你也可以直接舍弃SQL脚本文件。



以下是一个脚本函数示例，函数的实现在C语言文件中。

```sql
CREATE FUNCTION pg_catalog.pg_file_write(text, text, bool) 
RETURNS bigint   						# 表示返回一个bigint 类型的值

AS 'MODULE_PATHNAME', 'pg_file_write'   # MODULE_PATHNAME是一个宏，在运行时会被替换为实际的模块路径名，pg_file_write是C语言函数的名称，表											# 示这个 PostgreSQL 函数是通过调用 C 语言函数来实现的。
LANGUAGE C VOLATILE STRICT;             # 表明函数体是用C语言编写的
```



### 2.3 C语言文件

C语言文件可以用来编写自定义的函数和数据类型。这些函数和数据类型可能会直接操作数据库的内部结构，或者执行一些SQL无法高效完成的复杂计算，也可以用来实现自定义的操作符和索引方法。这些操作符和索引方法可以提供对特定数据类型的高效处理，或者支持新的查询优化策略。OpenTenBase数据库中还有一些钩子函数，在插件中可以注册这些回调函数来为系统的各个处理环节插入逻辑代码用来实现想要的功能。这样就可以在不修改内核代码的情况下，通过插件为内核增加功能。

需要注意的是，之前在SQL脚本中定义的函数要在C语言文件中实现，比如上面的pg_file_write函数

```c
Datum   //可以容纳任何 PostgreSQL 数据类型值的通用指针类型
pg_file_write(PG_FUNCTION_ARGS)
{
    FILE       *f;              // 文件指针
    char       *filename;       // 文件名
    text       *data;           // 要写入的数据
    int64        count = 0;      // 实际写入的字节数

    requireSuperuser();          // 确保调用者是超级用户

    filename = convert_and_check_filename(PG_GETARG_TEXT_PP(0), false);
    // 转换并检查文件名参数
    data = PG_GETARG_TEXT_PP(1);
    // 获取要写入的数据

    if (!PG_GETARG_BOOL(2))
    {
        // 如果第三个参数为false，检查文件是否已存在
        struct stat fst;

        if (stat(filename, &fst) >= 0)
            ereport(ERROR,
                    (ERRCODE_DUPLICATE_FILE,
                     errmsg("file \"%s\" exists", filename)));

        f = AllocateFile(filename, "wb");
        // 如果文件不存在，以写入模式打开文件
    }
    else
        f = AllocateFile(filename, "ab");
    // 如果第三个参数为true，以追加模式打开文件

    if (!f)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open file \"%s\" for writing: %m",
                        filename)));
    // 如果打开文件失败，报告错误

    count = fwrite(VARDATA_ANY(data), 1, VARSIZE_ANY_EXHDR(data), f);
    // 写入数据到文件
    if (count != VARSIZE_ANY_EXHDR(data) || FreeFile(f))
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not write file \"%s\": %m", filename)));
    // 如果写入失败或关闭文件失败，报告错误

    PG_RETURN_INT64(count);
    // 返回实际写入的字节数
}
```



### 2.4 Makefile文件

一个扩展可以没有C语言文件，可以没有SQL脚本，可以没有control，但是不能没有Makeflie。Makeflie文件是一个构建脚本，它指导着编译过程，告诉构建系统如何编译和安装扩展。通常包含了一系列的规则，这些规则定义了目标（例如，扩展的共享库文件）以及如何从源代码生成这些目标。

扩展一般的Makefile文件都是一个模版

```
MODULE_big	= name  # 用于指定主模块的名称，通常与扩展的名称相同。
OBJS		= verify_nbtree.o $(WIN32RES) # 指定要编译的目标文件列表

EXTENSION = name # 扩展的名称
DATA = name--1.0.sql # 指定了扩展的SQL脚本文件
PGFILEDESC = ""  # 关于扩展的简短描述,会在安装或列出扩展信息时显示

REGRESS = check name_test # 指定了用于测试扩展的回归测试脚本 ,非必须

ifdef USE_PGXS
PG_CONFIG = pg_config  # PGXS是一个构建框架，简化扩展的构建过程，使用pg_config工具来定位PGXS脚本
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/name # 扩展的文件目录
top_builddir = ../.. # 顶级构建目录的路径
include $(top_builddir)/src/Makefile.global 
include $(top_srcdir)/contrib/contrib-global.mk
endif
```



## 3.扩展安装和使用

将扩展文件夹放到contrib文件夹下

```
make -sj && make install
```

生成*.o和\*.so 文件



安装命令：

```
CREATE EXTENSION  extension_name;
```



加载文件夹

```
load 'libdir/name' # name:扩展名
```

