import 'dart:async';
import 'dart:io';
import 'package:flutter/material.dart';
import '../models/app_config.dart';
import '../services/config_repository.dart';
import '../services/log_store.dart';
import '../services/log_severity.dart';
import '../services/pipeline_manager.dart';
import '../services/pipeline_types.dart';

class PipelinePage extends StatefulWidget {
  const PipelinePage({
    super.key,
    this.configRepository,
    this.logStore,
    this.pipelineManager,
  });

  final ConfigRepository? configRepository;
  final LogStore? logStore;
  final PipelineManager? pipelineManager;

  @override
  State<PipelinePage> createState() => _PipelinePageState();
}

enum _LogFilter { all, info, warning, error }

class _PipelinePageState extends State<PipelinePage> {
  late final ConfigRepository _repo;
  late final LogStore _logStore;
  late final PipelineManager _manager;
  final _formKey = GlobalKey<FormState>();
  final _controllers = <String, TextEditingController>{};
  final _logs = <String>[];
  final _pendingLogs = <String>[];
  final _logSearchController = TextEditingController();
  late final ScrollController _logScrollController;
  Timer? _logUiTimer;
  Timer? _pipelineUiTimer;
  PipelineSnapshot? _pendingPipelineState;
  bool _followLog = true;
  late final StreamSubscription<PipelineSnapshot> _subscription;
  Future<void>? _activeRun;
  PipelineSnapshot _state = const PipelineSnapshot();
  AppConfig _config = const AppConfig();
  bool _loading = true;
  String? _logFilePath;
  String? _logSaveError;
  String? _actionError;
  String _logSearchQuery = '';
  _LogFilter _logFilter = _LogFilter.all;

  static const _fields = <(String, String, bool)>[
    ('sourcePath', '读卡器源目录', false),
    ('workspacePath', '本地工作区', false),
    ('bucket', 'OSS Bucket', false),
    ('prefix', 'OSS 对象前缀', false),
    ('endpoint', 'OSS Endpoint', false),
    ('accessKeyId', 'AccessKey ID', false),
    ('accessKeySecret', 'AccessKey Secret', true),
    ('securityToken', 'STS Security Token', true),
    ('ffmpegPath', 'FFmpeg 程序路径', false),
    ('ossutilPath', 'ossutil 程序路径', false),
    ('ffmpegArgs', 'FFmpeg 编码参数模板', false),
    ('videoExtensions', '转码视频后缀（逗号分隔）', false),
    ('outputExtension', '转码输出后缀', false),
    ('uploadConcurrency', '上传并发数', false),
    ('uploadRetries', '上传失败重试次数', false),
    ('transcodeConcurrency', '转码并发数', false),
    ('copyConcurrency', '复制并发数', false),
    ('cleanupConcurrency', '清理并发数', false),
    ('ossExtraArgs', '额外 ossutil 参数（空格分隔）', false),
  ];

  @override
  void initState() {
    super.initState();
    _repo = widget.configRepository ?? ConfigRepository();
    _logStore = widget.logStore ?? LogStore();
    _manager = widget.pipelineManager ?? PipelineManager();
    _logScrollController = ScrollController()
      ..addListener(() {
        if (!_logScrollController.hasClients) return;
        final position = _logScrollController.position;
        _followLog = position.maxScrollExtent - position.pixels < 48;
      });
    _logUiTimer = Timer.periodic(
      const Duration(milliseconds: 150),
      (_) => _flushLogUi(),
    );
    _pipelineUiTimer = Timer.periodic(const Duration(milliseconds: 50), (_) {
      final latest = _pendingPipelineState;
      if (!mounted || latest == null) return;
      _pendingPipelineState = null;
      setState(() => _state = latest);
    });
    _subscription = _manager.events.listen((event) {
      _pendingPipelineState = event;
    });
    _load();
  }

  Future<void> _load() async {
    try {
      await _logStore.startSession();
      _logFilePath = _logStore.filePath;
    } catch (error) {
      _logSaveError = error.toString();
    }
    try {
      _config = await _repo.load();
    } catch (error) {
      _config = const AppConfig();
      _actionError = '读取配置失败：$error';
      _append(_actionError!);
    }
    for (final (key, _, _) in _fields) {
      _controllers[key] = TextEditingController(text: _value(_config, key));
    }
    if (mounted) setState(() => _loading = false);
  }

  String _value(AppConfig c, String key) => switch (key) {
    'sourcePath' => c.sourcePath,
    'workspacePath' => c.workspacePath,
    'bucket' => c.bucket,
    'prefix' => c.prefix,
    'endpoint' => c.endpoint,
    'accessKeyId' => c.accessKeyId,
    'accessKeySecret' => c.accessKeySecret,
    'securityToken' => c.securityToken,
    'ffmpegPath' => c.ffmpegPath,
    'ossutilPath' => c.ossutilPath,
    'ffmpegArgs' => c.ffmpegArgs,
    'videoExtensions' => c.videoExtensions.join(', '),
    'outputExtension' => c.outputExtension,
    'uploadConcurrency' => '${c.uploadConcurrency}',
    'uploadRetries' => '${c.uploadRetries}',
    'transcodeConcurrency' => '${c.transcodeConcurrency}',
    'copyConcurrency' => '${c.copyConcurrency}',
    'cleanupConcurrency' => '${c.cleanupConcurrency}',
    'ossExtraArgs' => c.ossExtraArgs,
    _ => '',
  };

  AppConfig _readForm() => _config.copyWith(
    sourcePath: _text('sourcePath'),
    workspacePath: _text('workspacePath'),
    bucket: _text('bucket'),
    prefix: _text('prefix'),
    endpoint: _text('endpoint'),
    accessKeyId: _text('accessKeyId'),
    accessKeySecret: _text('accessKeySecret'),
    securityToken: _text('securityToken'),
    ffmpegPath: _text('ffmpegPath'),
    ossutilPath: _text('ossutilPath'),
    ffmpegArgs: _text('ffmpegArgs'),
    videoExtensions: _text('videoExtensions')
        .split(',')
        .map((e) => e.trim().toLowerCase())
        .where((e) => e.isNotEmpty)
        .map((e) => e.startsWith('.') ? e : '.$e')
        .toList(),
    outputExtension: _text('outputExtension'),
    uploadConcurrency: int.tryParse(_text('uploadConcurrency')) ?? 1,
    uploadRetries: int.tryParse(_text('uploadRetries')) ?? 0,
    transcodeConcurrency: int.tryParse(_text('transcodeConcurrency')) ?? 1,
    copyConcurrency: int.tryParse(_text('copyConcurrency')) ?? 1,
    cleanupConcurrency: int.tryParse(_text('cleanupConcurrency')) ?? 1,
    ossExtraArgs: _text('ossExtraArgs'),
  );
  String _text(String key) => _controllers[key]?.text.trim() ?? '';

  List<String> get _visibleLogs {
    final query = _logSearchQuery.toLowerCase();
    return _logs
        .where((line) {
          if (query.isNotEmpty && !line.toLowerCase().contains(query)) {
            return false;
          }
          if (_logFilter == _LogFilter.all) return true;
          final severity = classifyLogSeverity(line);
          return switch (_logFilter) {
            _LogFilter.all => true,
            _LogFilter.info => severity == LogSeverity.info,
            _LogFilter.warning => severity == LogSeverity.warning,
            _LogFilter.error => severity == LogSeverity.error,
          };
        })
        .toList(growable: false);
  }

  Future<void> _save() async {
    if (!(_formKey.currentState?.validate() ?? false)) return;
    _config = _readForm();
    try {
      await _repo.save(_config);
      _append('配置已保存：${await _repo.file}');
      if (mounted) setState(() => _actionError = null);
    } catch (error) {
      _actionError = '配置保存失败：$error';
      _append(_actionError!);
      if (mounted) {
        setState(() {});
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text(_actionError!)));
      }
    }
  }

  Future<void> _start() async {
    if (!(_formKey.currentState?.validate() ?? false)) return;
    _config = _readForm();
    const requiredFields = <String, String>{
      'sourcePath': '源目录',
      'workspacePath': '本地工作区',
      'bucket': 'OSS Bucket',
      'endpoint': 'OSS Endpoint',
      'accessKeyId': 'AccessKey ID',
      'accessKeySecret': 'AccessKey Secret',
      'securityToken': 'STS Security Token',
      'ossutilPath': 'ossutil 程序路径',
    };
    final missing = requiredFields.entries
        .where((field) => _text(field.key).isEmpty)
        .map((field) => field.value)
        .toList();
    if (missing.isNotEmpty) {
      final message = '任务未启动，缺少配置：${missing.join('、')}';
      _append(message);
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text(message)));
      }
      return;
    }
    try {
      await _repo.save(_config);
      if (mounted) setState(() => _actionError = null);
    } catch (error) {
      _actionError = '任务未启动，配置保存失败：$error';
      _append(_actionError!);
      if (mounted) {
        setState(() {});
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text(_actionError!)));
      }
      return;
    }
    try {
      final run = _manager.run(_config, log: _append);
      _activeRun = run;
      await run;
    } catch (_) {
      // 具体错误已经写入全局日志，避免再弹出重复提示。
    } finally {
      _activeRun = null;
    }
  }

  void _append(String line) {
    final stamp = DateTime.now().toIso8601String().substring(11, 19);
    final entry = '[$stamp] $line';
    if (mounted) _pendingLogs.add(entry);
    unawaited(
      _logStore.append(entry).catchError((Object error) {
        if (mounted) setState(() => _logSaveError = error.toString());
      }),
    );
  }

  void _flushLogUi() {
    if (!mounted) return;
    final saveError = _logStore.lastError?.toString();
    final logStateChanged = saveError != null && saveError != _logSaveError;
    if (_pendingLogs.isEmpty) {
      if (logStateChanged) setState(() => _logSaveError = saveError);
      return;
    }
    final batch = List<String>.from(_pendingLogs);
    _pendingLogs.clear();
    setState(() {
      if (logStateChanged) _logSaveError = saveError;
      _logs.addAll(batch);
      if (_logs.length > 1500) _logs.removeRange(0, _logs.length - 1500);
    });
    if (_followLog) {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        if (_logScrollController.hasClients) {
          _logScrollController.jumpTo(
            _logScrollController.position.maxScrollExtent,
          );
        }
      });
    }
  }

  void _clearDisplayedLogs() {
    setState(() {
      _logs.clear();
      _pendingLogs.clear();
    });
  }

  Future<void> _openLogDirectory() async {
    final path = _logStore.directoryPath;
    if (path == null) return;
    try {
      await Process.start('explorer.exe', [path], runInShell: false);
    } catch (error) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('无法打开日志目录：$error')));
      }
    }
  }

  @override
  void dispose() {
    _subscription.cancel();
    for (final c in _controllers.values) {
      c.dispose();
    }
    _logSearchController.dispose();
    _logUiTimer?.cancel();
    _pipelineUiTimer?.cancel();
    _logScrollController.dispose();
    unawaited(_shutdown());
    super.dispose();
  }

  Future<void> _shutdown() async {
    await _manager.cancel();
    final activeRun = _activeRun;
    if (activeRun != null) {
      try {
        await activeRun;
      } catch (_) {
        // run() already records task errors before it completes.
      }
    }
    await _manager.close();
    try {
      await _logStore.close();
    } catch (_) {
      // The window is closing; there is no UI left to report a final flush error.
    }
  }

  @override
  Widget build(BuildContext context) {
    if (_loading) {
      return const Scaffold(body: Center(child: CircularProgressIndicator()));
    }
    final running = _manager.running;
    return Scaffold(
      appBar: AppBar(
        title: const Text('媒体归档工作台'),
        actions: [
          IconButton(
            tooltip: '保存配置',
            onPressed: running ? null : _save,
            icon: const Icon(Icons.save_outlined),
          ),
        ],
      ),
      body: Padding(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Expanded(flex: 6, child: _configPanel()),
            const SizedBox(height: 16),
            _progressPanel(),
            const SizedBox(height: 16),
            Expanded(flex: 4, child: _logPanel()),
          ],
        ),
      ),
    );
  }

  Widget _configPanel() => Card(
    elevation: 0,
    child: Form(
      key: _formKey,
      child: Scrollbar(
        child: GridView.builder(
          padding: const EdgeInsets.all(16),
          itemCount: _fields.length + 1,
          gridDelegate: const SliverGridDelegateWithMaxCrossAxisExtent(
            maxCrossAxisExtent: 460,
            mainAxisExtent: 76,
            crossAxisSpacing: 14,
            mainAxisSpacing: 6,
          ),
          itemBuilder: (context, index) {
            if (index == _fields.length) {
              return Align(
                alignment: Alignment.centerLeft,
                child: Row(
                  children: [
                    const Text('上传成功后删除工作区'),
                    const SizedBox(width: 8),
                    Switch(
                      value: _config.deleteAfterUpload,
                      onChanged: _manager.running
                          ? null
                          : (value) => setState(
                              () => _config = _config.copyWith(
                                deleteAfterUpload: value,
                              ),
                            ),
                    ),
                  ],
                ),
              );
            }
            final (key, label, secret) = _fields[index];
            final numeric =
                key == 'uploadConcurrency' ||
                key == 'uploadRetries' ||
                key == 'transcodeConcurrency' ||
                key == 'copyConcurrency' ||
                key == 'cleanupConcurrency';
            return TextFormField(
              controller: _controllers[key],
              obscureText: secret,
              keyboardType: numeric ? TextInputType.number : TextInputType.text,
              enabled: !_manager.running,
              validator: (value) {
                if (key == 'outputExtension') {
                  final extension = value?.trim() ?? '';
                  final normalized = extension.startsWith('.')
                      ? extension
                      : '.$extension';
                  if (!RegExp(r'^\.[a-zA-Z0-9]{1,10}$').hasMatch(normalized)) {
                    return '请输入有效的扩展名，例如 .mp4';
                  }
                }
                if (numeric) {
                  final parsed = int.tryParse(value ?? '');
                  final isRetries = key == 'uploadRetries';
                  final min = isRetries ? 0 : 1;
                  final max = key == 'transcodeConcurrency'
                      ? 8
                      : key == 'uploadRetries'
                      ? 5
                      : 32;
                  if (parsed == null || parsed < min || parsed > max) {
                    return '请输入 $min 到 $max 之间的整数';
                  }
                }
                return null;
              },
              decoration: InputDecoration(labelText: label),
            );
          },
        ),
      ),
    ),
  );

  Widget _progressPanel() {
    final stage = switch (_state.stage) {
      PipelineStage.copying => '扫描与复制',
      PipelineStage.transcoding => '视频转码',
      PipelineStage.uploading => 'OSS 上传',
      PipelineStage.cleaning => '清理',
      PipelineStage.completed => '已完成',
      PipelineStage.failed => '失败',
      PipelineStage.cancelled => '已取消',
      PipelineStage.idle => '就绪',
    };
    final running = _manager.running;
    final scanning =
        _state.stage == PipelineStage.copying &&
        _state.message.startsWith('扫描中');
    return Card(
      elevation: 0,
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            _stageFlow(),
            const SizedBox(height: 14),
            Row(
              children: [
                Text(stage, style: Theme.of(context).textTheme.titleMedium),
                const Spacer(),
                if (running && !_state.paused)
                  IconButton(
                    tooltip: '暂停',
                    onPressed: _manager.pause,
                    icon: const Icon(Icons.pause_circle_outline),
                  )
                else if (running)
                  IconButton(
                    tooltip: '继续',
                    onPressed: _manager.resume,
                    icon: const Icon(Icons.play_arrow),
                  ),
                if (running)
                  IconButton(
                    tooltip: '取消任务',
                    onPressed: _manager.cancel,
                    icon: const Icon(Icons.stop_circle_outlined),
                  ),
                FilledButton.icon(
                  onPressed: running ? null : _start,
                  icon: const Icon(Icons.play_arrow),
                  label: const Text('开始任务'),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Row(
              children: [
                const SizedBox(width: 92, child: Text('总体进度')),
                Expanded(
                  child: LinearProgressIndicator(
                    value: scanning ? null : _state.overall,
                  ),
                ),
                const SizedBox(width: 12),
                Text(
                  scanning
                      ? '扫描中'
                      : '${(_state.overall * 100).toStringAsFixed(1)}%',
                ),
              ],
            ),
            const SizedBox(height: 8),
            Row(
              children: [
                const SizedBox(width: 92, child: Text('当前文件')),
                Expanded(
                  child: Text(
                    _state.currentFile.isEmpty
                        ? _state.message
                        : '${_state.currentFile}  ·  ${_state.message}',
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                  ),
                ),
                if (!scanning) ...[
                  const SizedBox(width: 12),
                  SizedBox(
                    width: 120,
                    child: LinearProgressIndicator(value: _state.fileProgress),
                  ),
                ],
              ],
            ),
            if (_state.totalFiles > 0)
              Padding(
                padding: const EdgeInsets.only(left: 92, top: 4),
                child: Text(
                  '${_state.completedFiles} / ${_state.totalFiles} 个文件',
                  style: Theme.of(context).textTheme.bodySmall,
                ),
              ),
            if (_state.activeTasks.isNotEmpty) ...[
              const SizedBox(height: 10),
              Row(
                children: [
                  const Text('并行文件任务'),
                  const Spacer(),
                  Text(
                    '${_state.activeTaskCount} 个执行中 · ${_state.activeTasks.length} 项可见',
                    style: Theme.of(context).textTheme.bodySmall,
                  ),
                ],
              ),
              const SizedBox(height: 4),
              SizedBox(
                height: 104,
                child: ListView.builder(
                  itemCount: _state.activeTasks.length,
                  itemBuilder: (context, index) {
                    final task = _state.activeTasks[index];
                    return Padding(
                      padding: const EdgeInsets.symmetric(vertical: 3),
                      child: Row(
                        children: [
                          SizedBox(
                            width: 220,
                            child: Text(
                              task.name,
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                              style: Theme.of(context).textTheme.bodySmall,
                            ),
                          ),
                          Expanded(
                            child: LinearProgressIndicator(
                              value: task.progress,
                            ),
                          ),
                          SizedBox(
                            width: 74,
                            child: Text(
                              task.status,
                              textAlign: TextAlign.end,
                              style: Theme.of(context).textTheme.bodySmall,
                            ),
                          ),
                        ],
                      ),
                    );
                  },
                ),
              ),
            ],
          ],
        ),
      ),
    );
  }

  Widget _stageFlow() {
    const labels = ['扫描与复制', '视频转码', 'OSS 上传', '清理'];
    final terminal =
        _state.stage == PipelineStage.failed ||
        _state.stage == PipelineStage.cancelled;
    final visualStage = terminal
        ? (_state.originStage ?? PipelineStage.idle)
        : _state.stage;
    final current = switch (visualStage) {
      PipelineStage.copying => 0,
      PipelineStage.transcoding => 1,
      PipelineStage.uploading => 2,
      PipelineStage.cleaning => 3,
      PipelineStage.completed => 4,
      _ => -1,
    };
    return Row(
      children: [
        for (var i = 0; i < labels.length; i++) ...[
          Expanded(
            child: Row(
              children: [
                CircleAvatar(
                  radius: 13,
                  backgroundColor: i < current
                      ? Theme.of(context).colorScheme.primary
                      : i == current && terminal
                      ? Theme.of(context).colorScheme.error
                      : i == current
                      ? Theme.of(context).colorScheme.tertiary
                      : Theme.of(context).colorScheme.surfaceContainerHighest,
                  child: Icon(
                    i < current
                        ? Icons.check
                        : i == current && terminal
                        ? Icons.priority_high
                        : Icons.circle,
                    size: i < current || (i == current && terminal) ? 15 : 8,
                    color: i <= current
                        ? Colors.white
                        : Theme.of(context).colorScheme.onSurfaceVariant,
                  ),
                ),
                const SizedBox(width: 7),
                Flexible(
                  child: Text(
                    labels[i],
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                    style: Theme.of(context).textTheme.labelMedium,
                  ),
                ),
                if (i < labels.length - 1)
                  Expanded(
                    child: Container(
                      height: 1,
                      margin: const EdgeInsets.symmetric(horizontal: 8),
                      color: i < current
                          ? Theme.of(context).colorScheme.primary
                          : Colors.black12,
                    ),
                  ),
              ],
            ),
          ),
        ],
      ],
    );
  }

  Widget _logPanel() => Card(
    elevation: 0,
    child: Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(16, 10, 8, 4),
          child: Row(
            children: [
              Text('运行日志 ${_visibleLogs.length}/${_logs.length}'),
              const Spacer(),
              if (_logFilePath != null)
                Padding(
                  padding: const EdgeInsets.only(right: 8),
                  child: Tooltip(
                    message: _logFilePath!,
                    child: Text(
                      '已保存到本机',
                      style: Theme.of(context).textTheme.bodySmall,
                    ),
                  ),
                ),
              IconButton(
                tooltip: '打开日志目录',
                onPressed: _logFilePath == null ? null : _openLogDirectory,
                icon: const Icon(Icons.folder_open_outlined, size: 19),
              ),
              IconButton(
                tooltip: '清空当前显示（不删除日志文件）',
                onPressed: _clearDisplayedLogs,
                icon: const Icon(Icons.delete_outline, size: 19),
              ),
            ],
          ),
        ),
        Padding(
          padding: const EdgeInsets.fromLTRB(14, 4, 14, 10),
          child: LayoutBuilder(
            builder: (context, constraints) {
              final search = TextField(
                controller: _logSearchController,
                onChanged: (value) => setState(() => _logSearchQuery = value),
                decoration: InputDecoration(
                  labelText: '搜索当前日志',
                  prefixIcon: const Icon(Icons.search),
                  suffixIcon: _logSearchQuery.isEmpty
                      ? null
                      : IconButton(
                          tooltip: '清除搜索',
                          onPressed: () {
                            _logSearchController.clear();
                            setState(() => _logSearchQuery = '');
                          },
                          icon: const Icon(Icons.close),
                        ),
                ),
              );
              final filter = SegmentedButton<_LogFilter>(
                showSelectedIcon: false,
                segments: const [
                  ButtonSegment(value: _LogFilter.all, label: Text('全部')),
                  ButtonSegment(value: _LogFilter.info, label: Text('信息')),
                  ButtonSegment(value: _LogFilter.warning, label: Text('警告')),
                  ButtonSegment(value: _LogFilter.error, label: Text('错误')),
                ],
                selected: {_logFilter},
                onSelectionChanged: (selection) =>
                    setState(() => _logFilter = selection.first),
              );
              if (constraints.maxWidth < 760) {
                return Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    search,
                    const SizedBox(height: 8),
                    Align(alignment: Alignment.centerRight, child: filter),
                  ],
                );
              }
              return Row(
                children: [
                  Expanded(child: search),
                  const SizedBox(width: 12),
                  filter,
                ],
              );
            },
          ),
        ),
        const Divider(height: 1),
        if (_logSaveError != null)
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 4),
            child: Text(
              '日志保存错误：$_logSaveError',
              style: TextStyle(color: Theme.of(context).colorScheme.error),
            ),
          ),
        if (_actionError != null)
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 4),
            child: Text(
              _actionError!,
              maxLines: 2,
              overflow: TextOverflow.ellipsis,
              style: TextStyle(color: Theme.of(context).colorScheme.error),
            ),
          ),
        Expanded(
          child: _visibleLogs.isEmpty
              ? Center(
                  child: Text(
                    _logs.isEmpty ? '暂无运行日志' : '没有匹配的日志',
                    style: Theme.of(context).textTheme.bodySmall,
                  ),
                )
              : Scrollbar(
                  child: ListView.builder(
                    controller: _logScrollController,
                    itemCount: _visibleLogs.length,
                    itemBuilder: (_, i) => Padding(
                      padding: const EdgeInsets.symmetric(
                        horizontal: 14,
                        vertical: 2,
                      ),
                      child: SelectableText(
                        _visibleLogs[i],
                        style: const TextStyle(
                          fontFamily: 'Consolas',
                          fontSize: 12,
                        ),
                      ),
                    ),
                  ),
                ),
        ),
      ],
    ),
  );
}
