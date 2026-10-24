# 媒体归档工作台

Windows Flutter 桌面端示例：读卡器文件复制到工作区，转码指定视频格式，再通过 STS 凭证并发上传至 OSS。

## 项目结构

```text
lib/
  main.dart
  models/app_config.dart
  services/
    app_paths.dart
    config_repository.dart
    copy_service.dart
    ffmpeg_service.dart
    log_store.dart
    log_severity.dart
    oss_service.dart
    pipeline_manager.dart
    pipeline_types.dart
    process_runner.dart
  ui/pipeline_page.dart
windows/
  runner/                  Flutter Windows 原生启动器
tools/
  ffmpeg.exe               需自行放入
  ossutil.exe              需自行放入
tool/
  pipeline_smoke.dart     配置/日志、FFmpeg 进度解析、并发、暂停/取消/重试、工作区隔离 smoke test
  process_fixture.dart   ProcessRunner 子进程 argv/输出/并行终止 fixture
```

## 运行与打包

将 Windows x64 版本的 `ffmpeg.exe` 和 `ossutil.exe` 放入项目根目录的 `tools/`。打包后，将 `tools/` 整个目录复制到 `untitled.exe` 同级目录；默认工具路径是相对于 exe 目录的 `tools\\ffmpeg.exe` 和 `tools\\ossutil.exe`。也可以在界面里填绝对路径。

在 Windows 开发环境执行：

```powershell
flutter pub get
flutter run -d windows
flutter build windows --release
```

服务层 smoke test 不需要启动 Flutter 测试 runner：

```powershell
dart --packages=.dart_tool/package_config.json tool/pipeline_smoke.dart
```

输出目录通常为 `build\\windows\\x64\\runner\\Release\\`。配置文件写入 `%APPDATA%\\MediaIngest\\config.json`；AccessKey Secret 和 STS Token 以明文 JSON 保存，请限制 Windows 用户目录访问权限，并避免将配置文件提交到版本库。

## 运行语义

- FFmpeg 参数模板按 argv 规则解析，不经过 shell；可用引号包含有空格的单个参数。程序自动加入 `-progress pipe:1`，从 `out_time_us`/`out_time_ms` 和 FFmpeg 的 `Duration` 日志估算进度。
- 每次任务使用配置工作区下独立的 `run_<id>/` 子目录，只上传本次扫描复制的文件，不会把上次失败遗留的内容混入任务。转码输出名包含来源后缀（如 `clip_from_mov.mp4`），FFmpeg 使用禁止覆盖模式，避免同名 `.mov`/`.avi` 并行转码覆盖彼此或覆盖已有文件；成功后只在本次子目录内删除原视频。读卡器原文件不会被修改。
- 暂停为协作式暂停：等待当前复制/转码/上传文件结束后，不再开始下一文件；取消或关闭窗口会终止所有活跃的外部进程并等待 worker 收尾。复制中的文件 I/O 在当前文件结束后响应取消。
- 复制、转码、上传分别使用有界 worker 池并行执行。转码并发默认为 2（上限 8），上传并发默认为 10（上限 32）；失败时停止派发新文件，结束同阶段其他子进程并等待 worker 收尾后再报告失败。OSS 单文件默认重试 2 次，退避时间逐次增加且受暂停/取消控制；配置可设为 0 到 5 次。
- 上传并发数表示并行启动的 ossutil 文件上传进程数；每个进程使用 `-j 1`，避免并发参数被叠乘。总体阶段进度按复制 30%、转码 30%、上传 35%、清理 5% 加权；关闭清理时上传阶段改为 40%，保证总权重仍为 100%。复制阶段按扫描得到的文件字节数计算，复制期间 `File.copy` 不提供单文件百分比，因此该文件显示不确定进度，阶段总体值按已完成字节推进。转码阶段按源视频字节数近似加权，单视频百分比仍由 FFmpeg 媒体时长估算；对编码复杂度差异较大的素材，阶段总体百分比是工作量估算而非精确剩余时间。上传阶段先以最多 32 个异步 worker 统计大小，再按已传输字节加权；该统计支持暂停和取消。没有待转码视频或待上传文件时，对应阶段会明确完成，不会让总体进度留下空白区间。任务区显示执行中 worker 数及各文件状态。外部工具未报告百分比时显示不确定进度；乱序/重复进度不会让单文件进度倒退。UI 将高频状态事件合并到约每秒 20 次刷新，日志仍独立追加到磁盘。
- 上传重试时单文件进度条会从新尝试重新开始，但总体进度保留文件已取得的最高有效进度，不因一次迟到失败而回退；重试等待阶段仍显示为活动 worker。暂停、恢复、取消请求及最终任务结果也会写入全局日志。日志级别筛选可识别常见的 `[ERROR]`、`WARN:`/`WARNING:` 控制台标签。
- OSS 命令按 ossutil v1 常见参数构造：`cp local oss://bucket/key -e endpoint -i id -k secret -t token -j 1`。如果使用 ossutil v2 或企业定制版本，请按版本调整 `OssService` 的参数映射。额外参数字段支持空格分隔，不支持带空格的额外参数值。
- 外部进程以独立参数列表启动（`runInShell: false`），路径含空格无需自行加引号。Windows Dart runtime 使用非 shell 子进程启动方式，避免弹出交互控制台窗口。
- 每次应用运行的完整日志异步追加到 `%APPDATA%\\MediaIngest\\logs\\run_*.log`，约每秒 flush，退出时关闭；日志面板仅保留最近 1500 行，支持文本搜索和信息/警告/错误筛选，清屏不会删除文件，并可打开日志目录。凭证值会在展示和保存日志前做脱敏。此示例不负责自动刷新过期 STS Token，需要在任务开始前填入有效凭证。
# dataUpdateTools
# dataUpdateTools
