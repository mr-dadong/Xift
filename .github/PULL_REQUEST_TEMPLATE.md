<!--
中文或英文填写均可，不需要的小节直接删除。
Fill in Chinese or English; delete any section you don't need.
-->

## 摘要 / Summary

<!-- 做了什么、为什么。/ What this does and why. -->

## 关联 / Related

Closes #

## 类型 / Type

<!-- 只保留一项，其余删除 / keep one, delete the rest -->
- [ ] ✨ 新功能 / New feature
- [ ] 🐛 Bug 修复 / Bug fix
- [ ] ♻️ 重构（不改变外部行为）/ Refactor (no behavior change)
- [ ] ⚡ 性能优化 / Performance improvement
- [ ] 📝 文档 / Documentation
- [ ] 🧪 测试 / Tests
- [ ] 🔧 构建、CI 与工具链 / Build, CI and tooling
- [ ] 📦 依赖或工程配置 / Dependencies or project config
- [ ] 💥 破坏性变更 / Breaking change

## 实现说明 / How It Works

<!-- 关键设计决策、方案取舍、受影响的核心包与平台差异。
     Key design decisions, trade-offs, affected packages and platform differences. -->

## 性能影响 / Performance Impact

<!-- 涉及热路径时，给出改动前后的 benchmark 数据。
     Benchmarks before/after for hot paths. -->

## 破坏性变更与兼容性 / Breaking Changes & Compatibility

<!-- 影响哪些平台、公开接口或磁盘格式，以及迁移方式；没有请写「无」。
     Affected platforms, public APIs or on-disk formats, plus the migration path; otherwise write "None". -->

## 截图或录屏 / Screenshots or Recordings

<!-- UI 改动请附修改前后对比。
     Attach before/after comparisons for UI changes. -->

## 验证 / Verification

- [ ] `gofmt -l .`、`go vet ./...` 通过 / pass
- [ ] `CGO_ENABLED=0 go test ./...`、`go test -race -count=1 ./...` 通过 / pass
- [ ] 无 `import "C"`，`CGO_ENABLED=0` 可构建 / no `import "C"`, zero-CGO build
- [ ] 新行为有测试（先红后绿）/ tests for new behavior (red → green)
- [ ] 守护进程对用户磁盘只读；热路径零分配 / daemon read-only; hot paths allocation-free
