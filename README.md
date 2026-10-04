# 市政饮用水污染响应

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8334`。

- `app.py`：服务生命周期和依赖组装。
- `src/domain.py`：水源、污染物、区域、来源校验与批次/回执归一化。
- `src/rules.py`：污染评分、通知去重、停水、切换水源、冲洗、消毒、复检、恢复状态机，以及消毒批次状态机、支路拓扑下游 closure 和污染来源更新失效规则。
- `src/repository.py`：SQLite、事务、重复保护、乐观版本、审计链，以及管网支路、消毒机组、消毒批次台账（排队号、占用、回执、重试、失效）和旧数据升级。
- `src/service.py`：身份、角色、支路边界权限和用例编排。
- `src/http_api.py`：JSON 接口与静态首页。
- `src/audit.py`：审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8334
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。测试覆盖完整响应流程、重复事件、重复通知、复检阈值、权限和版本冲突。内置规则不替代真实水质模型、法定通报渠道或供水控制系统的联锁。

## 消毒批次联动账

冲洗和消毒共用一台移动消毒机组，污染事件、管网支路、消毒批次记成一本联动账：

- **开工占用**：`POST /api/items/<id>/batches` 开工即占用机组与支路；`POST /api/batches/<id>/receipt` 回执带回实际加药量。
- **先到领批，后到取号**：同一机组两人同时提交，先到者开工领批，后到者拿到排队号；机组释放后按原顺序自动派发，不跳号。
- **支路占用可见**：同一支路上有未结案批次时，新批次返回 `branch_occupied`，不会重复开工。
- **回执失败保留**：`success:false` 的回执保留未完成批次（`receipt_failed`），机组仍占用；`POST /api/batches/<id>/retry` 按原顺序重试，只对账已记录的实际加药量，不再下发加药指令（不重复加药）。
- **来源更新整批失效**：`POST /api/items/<id>/sources` 提交覆盖区域污染来源后，该事件在受影响支路（含下游）上的所有批次立即失效，已恢复区域一起退回 `reinspection_pending` 待复检，不清空历史样本、只清除放行结论。
- **旧数据升级**：启动时按单区支路补齐 `pipe_branches`、补齐默认机组，不改动历史业务记录，可重复执行。
- **支路边界权限**：现场角色（`field_operator`、`lab`）越过 X-Region 支路边界返回 `branch_scope_denied`（403）；监管（`regulator`）可跨支路 `POST /api/batches/<id>/release` 放行。

支路拓扑通过 `POST /api/branches/<code>/upstream` 配置（`upstream_code` 为上游支路，`null` 表示独立支路）。另提供 `GET /api/branches`、`GET /api/units`、`GET /api/batches`、`GET /api/batches/<id>` 查询。
