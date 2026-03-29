# Netlify + Railway 上线前核对单

## A. 代码与构建产物

- [ ] 前端发布目录仅使用 `frontend-prototype/` 下资源
- [ ] 页面内无 `../reusable/...` 之类越级引用
- [ ] 后端镜像构建上下文已由 `.dockerignore` 排除模板/文档/备份目录
- [ ] Python 语法检查通过：`python -m compileall -q backend scripts`

## B. Railway（后端）环境变量

- [ ] `DB_HOST`
- [ ] `DB_PORT=4000`（TiDB 默认）
- [ ] `DB_USER`
- [ ] `DB_PASSWORD`
- [ ] `DB_NAME=trans_fields_mapping`
- [ ] `DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt`
- [ ] `DB_SSL_DISABLED=false`
- [ ] `DB_SSL_VERIFY_CERT=true`
- [ ] `DB_SSL_VERIFY_IDENTITY=true`
- [ ] `DEFAULT_ADMIN_USERNAME=admin`
- [ ] `DEFAULT_ADMIN_PASSWORD=<强密码>`（首次引导必填）
- [ ] `AUTH_COOKIE_NAME=trans_fields_mapping_session`
- [ ] `AUTH_COOKIE_SECURE=true`
- [ ] `AUTH_COOKIE_SAMESITE=none`
- [ ] `AUTH_COOKIE_DOMAIN=`（留空通常即可）
- [ ] `CORS_ALLOW_ORIGINS=https://<your-netlify-domain>`

## C. Netlify（前端）

- [ ] Publish directory: `frontend-prototype`
- [ ] `frontend-prototype/runtime-config.js` 已设置后端地址，或通过运行时注入
- [ ] Netlify 域名与 `CORS_ALLOW_ORIGINS` 完整一致（协议+域名）

## D. 安全与健壮性

- [ ] 默认管理员密码未使用硬编码（已改为环境变量引导）
- [ ] `scripts/check_backend_health.sh` 返回 200/401/403 之一即判定 API 可达
- [ ] 若未提供 DD03L 元数据文件，确认允许跳过（或设置 `DD03L_METADATA_REQUIRED=true` 强制校验）

## E. 上线后 5 分钟验收

- [ ] 打开 Netlify 首页成功
- [ ] 登录成功（管理员账号）
- [ ] 访问核心 API 返回正常（含鉴权场景）
- [ ] 关键功能抽样：查询、导入、路径分析
- [ ] 浏览器 Console 无关键报错
