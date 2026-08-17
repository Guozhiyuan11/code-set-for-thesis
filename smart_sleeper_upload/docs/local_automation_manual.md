# SMART Sleeper 本地自动化与 Supabase 使用手册

## 1. 当前实现

Windows 每 15 分钟启动一次 `run_pipeline.py`：

```text
SMART Sleeper HTTP JSON
  -> 解码整份数据
  -> 静态/动态过滤
  -> 排除 Supabase 已处理过的完全重复记录
  -> 调用 ingest-dynamic-v2
  -> Supabase 原子保存运行记录、非重复数据、报告、事件和状态
```

关键行为：

- 数据来源只有 SMART Sleeper 接口，不再读取 EMS。
- 每次都重新读取和过滤接口返回的整份数据，不依赖本地进度文件。
- 每条数据通过稳定 ID 和内容指纹判断是否完全重复。
- 完全重复的数据不会再次写入或更新 `processed_records`。
- 即使全部数据都重复，程序仍调用 Supabase，并在 `ingest_runs` 留下一条本轮运行记录。
- 每轮属于一个 15 分钟执行窗口，例如 `09:00`、`09:15`。同一窗口内重试会复用同一条运行记录，避免重复日志。
- 正常模式只写 Supabase，不生成本地结果。只有 `--skip-supabase` 调试模式才生成本地文件。

Windows 任务名：

```text
SmartSleeperPipeline
```

## 2. 配置和使用

先进入项目目录：

```powershell
Set-Location C:\Users\Asus\Desktop\smart_sleeper_demo
```

真实凭据只写在 `.env`：

```text
SMART_SLEEPER_URL=https://meshnetdev.thearcsgroup.com/smartsleepertest2/app/DeviceGroups/SmartSleeper/influx/json
SMART_SLEEPER_USERNAME=...
SMART_SLEEPER_PASSWORD=...
SUPABASE_URL=https://你的项目.supabase.co
SUPABASE_SECRET_KEY=...
```

### 手动运行一次

```powershell
.\.uv-python\cpython-3.11.14-windows-x86_64-none\python.exe run_pipeline.py `
  --dynamic-config config\dynamic_filtering.json `
  --dynamic-mode shadow
```

成功输出示例：

```text
result_destination: supabase
execution_slot: 2026-07-10T09:15:00+00:00
processed_records: 32
records_submitted: 0
duplicate_records_omitted: 32
supabase_load_performed: True
ingest_run_id: ...
inserted_records: 0
updated_records: 0
state_saved: True
```

`records_submitted: 0` 不是失败。它表示 32 条数据都已存在，因此没有重复写数据，但本轮运行记录仍已写入 Supabase。

### 安装或更新每 15 分钟任务

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\install_local_schedule.ps1
```

重复执行安装命令只会更新同名任务。

立即触发一次：

```powershell
Start-ScheduledTask -TaskName SmartSleeperPipeline
```

暂停或恢复：

```powershell
Disable-ScheduledTask -TaskName SmartSleeperPipeline
Enable-ScheduledTask -TaskName SmartSleeperPipeline
```

### 可选：只在本地调试

```powershell
.\.uv-python\cpython-3.11.14-windows-x86_64-none\python.exe run_pipeline.py `
  --input-json data\smart_sleeper_data.json `
  --skip-supabase `
  --dynamic-config config\dynamic_filtering.json `
  --dynamic-mode shadow
```

这个命令不会写 Supabase；结果位于 `smart_sleeper_local_output`。

## 3. 监测方法

### 检查 Windows 调度

```powershell
Get-ScheduledTask -TaskName SmartSleeperPipeline |
  Select-Object TaskName, State

Get-ScheduledTaskInfo -TaskName SmartSleeperPipeline |
  Select-Object LastRunTime, LastTaskResult, NextRunTime
```

判断标准：

- `State = Ready`：正常，表示任务已安装且当前空闲。
- `State = Running`：只在程序正在执行的几秒钟内出现，通常很难刚好看到。
- `LastTaskResult = 0`：上一次执行成功。
- `LastRunTime`：应每 15 分钟更新一次。
- `NextRunTime`：应比上一次约晚 15 分钟。

### 检查每 15 分钟的 Supabase 运行记录

在 Supabase SQL Editor 执行：

```sql
select
  created_at,
  status,
  quality_report->>'execution_slot' as execution_slot,
  quality_report->>'total_decoded_records' as records_processed,
  quality_report->>'records_submitted' as records_submitted,
  quality_report->>'duplicate_records_omitted' as duplicates_omitted,
  inserted_record_count,
  updated_record_count,
  skipped_record_count
from public.ingest_runs
where source_name = 'smart_sleeper'
order by created_at desc
limit 20;
```

固定数据正常情况下，每 15 分钟会看到一行新的 `execution_slot`；`duplicates_omitted` 可以是 `32`，而插入和更新数量都是 `0`。

### 检查实际过滤数据

```sql
select
  source_record_id,
  device_id,
  source_time_utc,
  filtering_mode,
  created_at,
  updated_at
from public.processed_records
where source_type = 'smart_sleeper'
order by source_time_utc desc
limit 20;
```

完全重复时，这里的行数和 `updated_at` 不应每 15 分钟变化；变化的是 `ingest_runs`。

还可以查看：

- `Edge Functions -> ingest-dynamic-v2 -> Logs`：请求与错误。
- `dynamic_filter_state`：远程动态过滤状态。
- `anomaly_events`：异常事件。

## 4. 常见问题

| 现象 | 含义或处理 |
|---|---|
| 任务一直显示 `Ready` | 正常；程序运行很快，应检查 `LastRunTime` 和 `LastTaskResult` |
| `records_submitted: 0` | 数据完全重复，已跳过数据写入；查看 `ingest_run_id` 确认运行已记录 |
| `LastTaskResult` 不是 `0` | 手动运行一次同样命令，查看终端中的具体错误 |
| `Supabase ingestion failed` | 检查 `.env`、Supabase Edge Function Logs 和 Secret Key |
| `source authentication failed` | 检查 SMART Sleeper 用户名、密码和 URL |

## 5. 安全

- 不要提交、截图或分享 `.env`。
- `SUPABASE_SECRET_KEY` 只能用于本机后端任务，不能放入浏览器代码。
- 密钥轮换后更新 `.env`，下一轮任务会重新读取。
