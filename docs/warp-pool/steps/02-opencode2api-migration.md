# opencode2api 迁移示例

## 目标

提供从 opencode2api 自研 WarpPool/nodePool 迁移到 warp-pool 库的示例,验证库在真实业务中的接入性,并给出迁移指引。

## 改动点

- docs 示例文档(库项目,不建 examples 目录):替换 warp_amz.go + pool.go 中 WarpPool 相关逻辑为 warp-pool import 的对照示例
- README 增补迁移章节

## 验收点

- 示例代码可编译运行,行为对齐原 WarpPool(轮询/健康检查/重播)
- 原 nodePool 亲和逻辑映射到 DialContextWithKey,迁移后业务代码显著缩短
