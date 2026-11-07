# OpenTenBase FQS 源码探索台

一个无需安装前端依赖的交互网页，帮助理解 v5.0 快速查询下推（Fast Query Shipping）。

## 打开

下载或克隆本分支，用浏览器打开本目录的 `index.html`。源码片段、解释与计划快照随页面打包，可离线使用。

## 内容

- 机制总览：五阶段自动走读。
- 计划生成：可点击的 FQS 条件判断与标准规划回退关系图。
- 远程收发：区分直接调用、回调绑定和生命周期转换。
- 关键数据结构：ExecNodes、RemoteQuery、RemoteQueryState、ResponseCombiner。
- 真实计划与依据：同一条 SELECT 在 FQS on/off 下的 EXPLAIN JSON，以及原始源码位置与哈希。

## 验证与边界

整理了 17 个函数、17 条关系和 10 个源文件，其中 14 条直接调用经过静态核对。五个视图生成和 25 处源码跳转目标已检查；浏览器视觉验收尚未完成。

真实计划来自只读事务中的 EXPLAIN，无 ANALYZE，不据此声称性能提升或运行时调用次数。页面展示的是静态源码分析与计划快照，不连接数据库。DN 内部执行器、完整标准分布式规划、GTM/MVCC 和错误恢复未展开。

本目录代码片段的原项目版权与许可见 `COPYRIGHT.txt`。

相关成果说明：https://github.com/OpenTenBase/OpenTenBase/issues/200#issuecomment-6704958234
