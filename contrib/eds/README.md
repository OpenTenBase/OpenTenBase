### branch:master --->>> eds 

此扩展是一个最基本的OpenTenBase extension，没有hook钩子等用法。
可以直接整合进OpenTenBase的contrib目录中。

```txt
├── eds--1.0.sql         定义了extension所涉及的表，涉及的函数等
├── eds.c		实现接口的代码
├── eds.control		版本控制信息
├── eds.h		头文件
├── expected		测试所需的目录
│   └── eds.out
├── Makefile		
├── README.md	
└── sql			测试所需的创建环境sql文件
    └── eds.sql
```
```sql
make
make install
make installcheck
```

