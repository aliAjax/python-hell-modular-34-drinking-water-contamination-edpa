# 市政饮用水污染响应

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8334`。

- `app.py`：服务生命周期和依赖组装。
- `src/domain.py`：水源、污染物、区域和来源校验。
- `src/rules.py`：污染评分、通知去重、停水、切换水源、冲洗、消毒、复检和恢复状态机。
- `src/repository.py`：SQLite、事务、重复保护、乐观版本、审计链，以及管网支路/移动消毒机组/消毒批次联动账。
- `src/service.py`：身份、角色、支路边界和用例编排。
- `src/http_api.py`：JSON 接口与静态首页。
- `src/audit.py`：审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8334
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。测试覆盖完整响应流程、重复事件、重复通知、复检阈值、权限和版本冲突。内置规则不替代真实水质模型、法定通报渠道或供水控制系统的联锁。

## 冲洗/消毒联动账

冲洗与消毒共用一台移动消毒机组，污染事件、管网支路、消毒批次在同一事务内联动：

- `POST /api/units` / `POST /api/branches`：登记移动消毒机组与管网支路（`downstream_codes` 描述上下游）。
- `POST /api/batches`：批次开工同时占用机组和支路。上一批未回执（机组在役）或支路被占时，后到者不报错，拿到排队号 `queue_no`；在役批次回执成功后按排队顺序自动领批（先到先得，支路仍忙则顺延）。
- `POST /api/batches/<id>/receipts`：回执带回实际加药量 `dose`；同一 `receipt_id` 重试不重复加药。回执失败（`success:false`）保留未完成批次并继续占用机组，按原顺序重试。
- 污染来源更新（`POST /api/items/<id>/sources` 携带 `zone_id`）：覆盖该区域的在役/已完成消毒批次立即失效（`invalidated`），下游已恢复区域一并退回待复检 `pending_retest`，新污染区回到 `affected`；无关支路不受影响。
- `POST /api/items/<id>/retests`：复检合格转 `restored`，不合格重回 `affected`。
- 权限：监管（`regulator`）可跨支路放行；现场角色（`field_operator` 等）越过支路 `region` 边界返回 `403 branch_boundary_denied`。
- 旧数据升级：首次打开旧库时按单区支路（`BR-<zone_id>`）补齐拓扑、补登共用机组 `U-LEGACY`，历史消毒/恢复记录以完成批次和区域状态兼容落账；升级幂等、可重复执行。
- 查询：`GET /api/branches`、`GET /api/units/<code>`、`GET /api/batches?unit=&status=`、`GET /api/batches/<id>`。
