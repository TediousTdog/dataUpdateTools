import 'dart:async';
import 'dart:io';
import 'package:flutter/material.dart';
import 'package:file_picker/file_picker.dart';
import '../models/app_config.dart';
import '../models/oss_authorization.dart';
import '../services/config_repository.dart';
import '../services/log_store.dart';
import '../services/log_severity.dart';
import '../services/pipeline_manager.dart';
import '../services/pipeline_types.dart';
import '../services/demo_pipeline_manager.dart';
import '../services/oss_service.dart';
import '../services/process_runner.dart';
import 'file_alignment_page.dart';

class PipelinePage extends StatefulWidget {
  const PipelinePage({
    super.key,
    this.configRepository,
    this.logStore,
    this.pipelineManager,
    this.demoMode = false,
    this.demoPipelineManager,
  });

  final ConfigRepository? configRepository;
  final LogStore? logStore;
  final PipelineManager? pipelineManager;
  final bool demoMode;
  final DemoPipelineManager? demoPipelineManager;

  @override
  State<PipelinePage> createState() => _PipelinePageState();
}

enum _LogFilter { all, info, warning, error }

class _PipelinePageState extends State<PipelinePage> {
  late final ConfigRepository _repo;
  late final LogStore _logStore;
  late final dynamic _manager;
  late final OssService _ossBrowser;
  final _formKey = GlobalKey<FormState>();
  final _controllers = <String, TextEditingController>{};
  final _logs = <String>[];
  final _pendingLogs = <String>[];
  final _logSearchController = TextEditingController();
  late final ScrollController _logScrollController;
  late final Future<void> _loadFuture;
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
  String? _logSessionError;
  String? _logAppendError;
  String? _renderedLogError;
  String? _actionError;
  String _logSearchQuery = '';
  _LogFilter _logFilter = _LogFilter.all;
  bool _disposed = false;
  bool _starting = false;
  bool _showAdvanced = false;
  bool _ossListing = false;
  List<String> _ossEntries = const [];
  String? _ossListingError;
  String _activeSection = 'browser';
  String? _selectedObjectKey;
  bool _downloading = false;
  bool _authRequired = false;

  static const _advancedKeys = <String>{
    'endpoint',
    'accessKeyId',
    'accessKeySecret',
    'securityToken',
    'ffmpegPath',
    'ossutilPath',
    'ffmpegArgs',
    'videoExtensions',
    'outputExtension',
    'multipartParallel',
    'bigFileThresholdMb',
    'partSizeMb',
    'uploadRetries',
    'transcodeConcurrency',
    'copyConcurrency',
    'cleanupConcurrency',
    'ossExtraArgs',
  };

  static const _transcodePresets = <String, String>{
    '硬件加速（推荐）': '-c:v h264_nvenc -preset fast -c:a copy',
    '兼容模式（CPU）': '-c:v libx264 -preset veryfast -crf 23 -c:a aac',
    '高质量（CPU）': '-c:v libx264 -preset medium -crf 18 -c:a aac',
  };

  static const _fields = <(String, String, bool)>[
    ('sourcePath', '读卡器源目录', false),
    ('workspacePath', '本地工作区', false),
    ('bucket', 'OSS Bucket', false),
    ('prefix', 'OSS 上传路径（子目录）', false),
    ('endpoint', 'OSS Endpoint', false),
    ('authorizationCode', 'OSS 授权码（Base64 JSON）', true),
    ('accessKeyId', 'AccessKey ID', false),
    ('accessKeySecret', 'AccessKey Secret', true),
    ('securityToken', 'STS Security Token', true),
    ('ffmpegPath', 'FFmpeg 程序路径', false),
    ('ossutilPath', 'ossutil 程序路径', false),
    ('ffmpegArgs', 'FFmpeg 编码参数模板', false),
    ('videoExtensions', '转码视频后缀（逗号分隔）', false),
    ('outputExtension', '转码输出后缀', false),
    ('uploadConcurrency', '上传并发数', false),
    ('multipartParallel', '单文件分片并行数', false),
    ('bigFileThresholdMb', '大文件阈值（MB）', false),
    ('partSizeMb', '分片大小（MB）', false),
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
    _manager = widget.demoMode
        ? (widget.demoPipelineManager ?? DemoPipelineManager())
        : (widget.pipelineManager ?? PipelineManager());
    _ossBrowser = OssService(ProcessRunner());
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
    _loadFuture = _load();
  }

  Future<void> _load() async {
    try {
      await _logStore.startSession();
      _logFilePath = _logStore.filePath;
    } catch (error) {
      _logSessionError = '无法启动日志会话：$error';
    }
    try {
      _config = await _repo.load();
    } catch (error) {
      _config = const AppConfig();
      _actionError = '读取配置失败：$error';
      _append(_actionError!);
    }
    // 窗口可能在异步读取配置期间被关闭；不要在 dispose 后创建控制器或
    // 调用 setState，避免泄漏控制器及 “setState after dispose” 异常。
    if (!mounted || _disposed) return;
    for (final (key, _, _) in _fields) {
      _controllers[key] = TextEditingController(text: _value(_config, key));
    }
    _authRequired =
        _config.authorizationCode.trim().isEmpty &&
        (_config.accessKeyId.isEmpty || _config.accessKeySecret.isEmpty);
    if (mounted) setState(() => _loading = false);
    if (mounted &&
        (_text('authorizationCode').isNotEmpty || _text('bucket').isNotEmpty)) {
      unawaited(_refreshOssListing());
    }
  }

  String _value(AppConfig c, String key) => switch (key) {
    'sourcePath' => c.sourcePath,
    'workspacePath' => c.workspacePath,
    'bucket' => c.bucket,
    'prefix' => c.prefix,
    'endpoint' => c.endpoint,
    'authorizationCode' => c.authorizationCode,
    'accessKeyId' => c.accessKeyId,
    'accessKeySecret' => c.accessKeySecret,
    'securityToken' => c.securityToken,
    'ffmpegPath' => c.ffmpegPath,
    'ossutilPath' => c.ossutilPath,
    'ffmpegArgs' => c.ffmpegArgs,
    'videoExtensions' => c.videoExtensions.join(', '),
    'outputExtension' => c.outputExtension,
    'uploadConcurrency' => '${c.uploadConcurrency}',
    'multipartParallel' => '${c.multipartParallel}',
    'bigFileThresholdMb' => '${c.bigFileThresholdMb}',
    'partSizeMb' => '${c.partSizeMb}',
    'uploadRetries' => '${c.uploadRetries}',
    'transcodeConcurrency' => '${c.transcodeConcurrency}',
    'copyConcurrency' => '${c.copyConcurrency}',
    'cleanupConcurrency' => '${c.cleanupConcurrency}',
    'ossExtraArgs' => c.ossExtraArgs,
    _ => '',
  };

  AppConfig _readForm() {
    final authorizationCode = _text('authorizationCode');
    final useAuthorizationCode = authorizationCode.isNotEmpty;
    return _config.copyWith(
      sourcePath: _text('sourcePath'),
      workspacePath: _text('workspacePath'),
      // 授权码只负责提供默认 Bucket/Endpoint/凭证；OSS 前缀允许用户单独覆盖，
      // 这样可以把同一份授权码上传到不同的对象目录。
      bucket: useAuthorizationCode ? '' : _text('bucket'),
      prefix: _text('prefix'),
      endpoint: useAuthorizationCode ? '' : _text('endpoint'),
      authorizationCode: authorizationCode,
      accessKeyId: useAuthorizationCode ? '' : _text('accessKeyId'),
      accessKeySecret: useAuthorizationCode ? '' : _text('accessKeySecret'),
      securityToken: useAuthorizationCode ? '' : _text('securityToken'),
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
      multipartParallel: int.tryParse(_text('multipartParallel')) ?? 1,
      bigFileThresholdMb: int.tryParse(_text('bigFileThresholdMb')) ?? 1,
      partSizeMb: int.tryParse(_text('partSizeMb')) ?? 1,
      uploadRetries: int.tryParse(_text('uploadRetries')) ?? 0,
      transcodeConcurrency: int.tryParse(_text('transcodeConcurrency')) ?? 1,
      copyConcurrency: int.tryParse(_text('copyConcurrency')) ?? 1,
      cleanupConcurrency: int.tryParse(_text('cleanupConcurrency')) ?? 1,
      ossExtraArgs: _text('ossExtraArgs'),
    );
  }

  String _text(String key) => _controllers[key]?.text.trim() ?? '';

  Future<void> _pickDirectory(String key, String title) async {
    if (_manager.running || _starting) return;
    final selected = await FilePicker.platform.getDirectoryPath(
      dialogTitle: title,
      lockParentWindow: true,
    );
    if (!mounted || selected == null || selected.trim().isEmpty) return;
    _controllers[key]?.value = TextEditingValue(
      text: selected,
      selection: TextSelection.collapsed(offset: selected.length),
    );
    setState(() {});
  }

  Future<void> _selectOssPrefix() async {
    if (_manager.running || _starting) return;
    final controller = TextEditingController(text: _text('prefix'));
    final value = await showDialog<String>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('选择 OSS 上传路径'),
        content: TextField(
          controller: controller,
          autofocus: true,
          decoration: const InputDecoration(
            labelText: '对象前缀（子目录）',
            hintText: '例如：照片/2026/10',
            helperText: '不要填写 oss:// 和 Bucket，只填写 Bucket 后面的目录',
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(context, controller.text.trim()),
            child: const Text('使用此路径'),
          ),
        ],
      ),
    );
    controller.dispose();
    if (!mounted || value == null) return;
    _controllers['prefix']?.text = value;
    setState(() {});
  }

  Future<void> _refreshOssListing() async {
    if (_ossListing || _manager.running || _starting) return;
    if (widget.demoMode) {
      setState(() {
        _ossEntries = const ['演示目录/照片/', '演示目录/视频/', '演示目录/README.txt'];
        _ossListingError = null;
      });
      _append('演示模式：OSS 原目录列表为模拟数据');
      return;
    }
    setState(() {
      _ossListing = true;
      _ossListingError = null;
    });
    try {
      final config = _readForm().resolveAuthorization();
      final entries = await _ossBrowser.listPrefix(
        config: config,
        onLog: (line) => _append('[OSS 目录] $line'),
      );
      if (!mounted) return;
      setState(() => _ossEntries = entries);
      _append('OSS 原目录读取完成：${entries.length} 条');
    } catch (error) {
      if (!mounted) return;
      setState(() => _ossListingError = error.toString());
      _append('OSS 原目录读取失败：$error');
    } finally {
      if (mounted) setState(() => _ossListing = false);
    }
  }

  Future<void> _switchAuthorization() async {
    if (_manager.running || _starting) return;
    final controller = TextEditingController(text: _text('authorizationCode'));
    final code = await showDialog<String>(
      context: context,
      builder: (dialogContext) => AlertDialog(
        title: const Text('切换 OSS 授权码'),
        content: SizedBox(
          width: 560,
          child: TextField(
            controller: controller,
            minLines: 3,
            maxLines: 6,
            decoration: const InputDecoration(
              labelText: '粘贴新的授权码',
              helperText: '授权码会保存在本机配置中；不要分享给他人。',
            ),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(dialogContext),
            child: const Text('取消'),
          ),
          FilledButton(
            onPressed: () {
              try {
                OssAuthorization.parse(controller.text);
                Navigator.pop(dialogContext, controller.text.trim());
              } on FormatException catch (error) {
                ScaffoldMessenger.of(context).showSnackBar(
                  SnackBar(content: Text('授权码无效：${error.message}')),
                );
              }
            },
            child: const Text('验证并切换'),
          ),
        ],
      ),
    );
    controller.dispose();
    if (!mounted || code == null) return;
    await _applyAuthorizationCode(code);
  }

  Future<void> _applyAuthorizationCode(String code) async {
    try {
      OssAuthorization.parse(code);
    } on FormatException catch (error) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('授权码无效：${error.message}')));
      }
      return;
    }
    _controllers['authorizationCode']?.text = code;
    _controllers['prefix']?.clear();
    _selectedObjectKey = null;
    try {
      await _save();
      if (!mounted) return;
      setState(() {
        _activeSection = 'browser';
        _authRequired = false;
      });
      await _refreshOssListing();
    } catch (_) {
      // _save 已通过现有错误提示显示失败原因。
    }
  }

  Future<void> _downloadSelectedObject() async {
    final key = _selectedObjectKey;
    if (key == null || _downloading) return;
    final directory = await FilePicker.platform.getDirectoryPath(
      dialogTitle: '选择下载保存位置',
      lockParentWindow: true,
    );
    if (!mounted || directory == null) return;
    setState(() => _downloading = true);
    try {
      await _ossBrowser.downloadObject(
        config: _readForm(),
        objectKey: key,
        localDirectory: directory,
        onLog: (line) => _append('[OSS 下载] $line'),
      );
      _append('下载完成：$key → $directory');
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text('文件下载完成')));
      }
    } catch (error) {
      _append('下载失败：$error');
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('下载失败：$error')));
      }
    } finally {
      if (mounted) setState(() => _downloading = false);
    }
  }

  Future<void> _pickExecutable(String key, String title) async {
    if (widget.demoMode) return;
    if (_manager.running || _starting) return;
    final result = await FilePicker.platform.pickFiles(
      dialogTitle: title,
      type: FileType.custom,
      allowedExtensions: const ['exe'],
      allowMultiple: false,
      lockParentWindow: true,
    );
    final selected = result?.files.single.path;
    if (!mounted || selected == null || selected.trim().isEmpty) return;
    _controllers[key]?.value = TextEditingValue(
      text: selected,
      selection: TextSelection.collapsed(offset: selected.length),
    );
    setState(() {});
  }

  String? _presetFor(String args) {
    for (final entry in _transcodePresets.entries) {
      if (entry.value == args) return entry.key;
    }
    return null;
  }

  String _authorizationPreview() {
    final code = _text('authorizationCode');
    if (code.isEmpty) return '授权码留空时，使用下方手动 OSS 配置。';
    try {
      final auth = OssAuthorization.parse(code);
      return '已解析：${auth.bucket}/${auth.prefix}\n${auth.endpoint}';
    } on FormatException catch (error) {
      return '授权码无效：${error.message}';
    }
  }

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
    if (_starting || _manager.running) return;
    if (!(_formKey.currentState?.validate() ?? false)) return;
    if (mounted) setState(() => _starting = true);
    try {
      _config = _readForm();
      if (widget.demoMode) {
        _append('macOS 演示模式：跳过真实目录、授权码和外部程序校验');
        final run = _manager.run(_config, log: _append);
        _activeRun = run;
        await run;
        return;
      }
      final requiredFields = <String, String>{
        'sourcePath': '源目录',
        'workspacePath': '本地工作区',
        'ossutilPath': 'ossutil 程序路径',
      };
      if (_text('authorizationCode').isEmpty) {
        requiredFields.addAll(const {
          'bucket': 'OSS Bucket',
          'endpoint': 'OSS Endpoint',
          'accessKeyId': 'AccessKey ID',
          'accessKeySecret': 'AccessKey Secret',
        });
      }
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
        // 在写入配置/启动流水线前先解析一次，错误会直接反馈到界面；
        // PipelineManager 仍会再次解析，以保证非 UI 调用也具备同样保护。
        _config.resolveAuthorization();
      } on FormatException catch (error) {
        final message = '授权码解析失败：${error.message}';
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
      }
    } finally {
      _activeRun = null;
      if (mounted) setState(() => _starting = false);
    }
  }

  void _append(String line) {
    final stamp = DateTime.now().toIso8601String().substring(11, 19);
    final entry = '[$stamp] $line';
    if (mounted && !_disposed) _pendingLogs.add(entry);
    unawaited(
      _logStore.append(entry).catchError((Object error) {
        _logAppendError = '日志追加失败：$error';
        if (mounted) setState(() {});
      }),
    );
  }

  void _flushLogUi() {
    if (!mounted) return;
    final logErrors = <String>[
      ?_logSessionError,
      ?_logAppendError,
      if (_logStore.lastError != null) '日志文件写入失败：${_logStore.lastError}',
      if (_manager.lastLogError != null) '日志回调失败：${_manager.lastLogError}',
    ];
    final logError = logErrors.isEmpty ? null : logErrors.join('\n');
    final logStateChanged = logError != _renderedLogError;
    if (_pendingLogs.isEmpty) {
      if (logStateChanged) {
        setState(() => _renderedLogError = logError);
      }
      return;
    }
    final batch = List<String>.from(_pendingLogs);
    _pendingLogs.clear();
    setState(() {
      if (logStateChanged) _renderedLogError = logError;
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
    _disposed = true;
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
    // 等待初次配置/日志会话初始化完成，避免 close() 与 startSession()
    // 并发操作同一个 LogStore。
    try {
      await _loadFuture;
    } catch (_) {
      // 初始化错误已经通过 UI/日志路径尽力记录；关闭流程仍需继续。
    }
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
    final busy = running || _starting;
    return Scaffold(
      appBar: AppBar(
        title: Row(
          children: [
            const Text('媒体归档工作台'),
            if (widget.demoMode) ...[
              const SizedBox(width: 10),
              Chip(
                avatar: Icon(Icons.visibility_outlined, size: 16),
                label: Text('演示模式'),
                visualDensity: VisualDensity.compact,
              ),
            ],
          ],
        ),
        actions: [
          _sectionButton('browser', 'OSS 文件', Icons.folder_open_outlined),
          _sectionButton('upload', '上传任务', Icons.cloud_upload_outlined),
          _sectionButton('download', '下载文件', Icons.download_outlined),
          _sectionButton('settings', '设置', Icons.settings_outlined),
          _sectionButton('logs', '运行日志', Icons.article_outlined),
          IconButton(
            tooltip: '文件对齐与分组整理',
            onPressed: busy
                ? null
                : () => Navigator.of(context).push(
                    MaterialPageRoute(
                      builder: (_) => const FileAlignmentPage(),
                    ),
                  ),
            icon: const Icon(Icons.fact_check_outlined),
          ),
          IconButton(
            tooltip: '切换授权码',
            onPressed: busy ? null : _switchAuthorization,
            icon: const Icon(Icons.key_outlined),
          ),
        ],
      ),
      body: Padding(
        padding: const EdgeInsets.all(20),
        child: _authRequired && !widget.demoMode
            ? _authorizationEntryPage()
            : switch (_activeSection) {
                'browser' => _browserPage(),
                'upload' => _uploadPage(),
                'download' => _downloadPage(),
                'settings' => _settingsPage(),
                'logs' => _logPanel(),
                _ => _browserPage(),
              },
      ),
    );
  }

  Widget _sectionButton(String section, String label, IconData icon) {
    final active = _activeSection == section;
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 2),
      child: TextButton.icon(
        onPressed: () => setState(() => _activeSection = section),
        icon: Icon(icon, size: 18),
        label: Text(label),
        style: TextButton.styleFrom(
          backgroundColor: active
              ? Theme.of(context).colorScheme.secondaryContainer
              : null,
          foregroundColor: active
              ? Theme.of(context).colorScheme.onSecondaryContainer
              : null,
        ),
      ),
    );
  }

  Widget _authorizationEntryPage() => Center(
    child: ConstrainedBox(
      constraints: const BoxConstraints(maxWidth: 620),
      child: Card(
        child: Padding(
          padding: const EdgeInsets.all(28),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              const Icon(Icons.cloud_circle_outlined, size: 56),
              const SizedBox(height: 14),
              Text(
                '连接阿里云 OSS',
                textAlign: TextAlign.center,
                style: Theme.of(context).textTheme.headlineSmall,
              ),
              const SizedBox(height: 8),
              const Text(
                '粘贴服务方提供的授权码，即可直接进入授权码对应的 OSS 路径。',
                textAlign: TextAlign.center,
              ),
              const SizedBox(height: 20),
              TextField(
                controller: _controllers['authorizationCode'],
                minLines: 3,
                maxLines: 5,
                decoration: const InputDecoration(
                  labelText: 'OSS 授权码',
                  hintText: '粘贴完整授权码（Base64 JSON）',
                  helperText: '授权信息仅保存在本机配置文件中。',
                ),
                onChanged: (_) => setState(() {}),
              ),
              const SizedBox(height: 12),
              Text(
                _authorizationPreview(),
                style: Theme.of(context).textTheme.bodySmall?.copyWith(
                  color: _authorizationPreview().startsWith('授权码无效')
                      ? Theme.of(context).colorScheme.error
                      : Theme.of(context).colorScheme.primary,
                ),
              ),
              const SizedBox(height: 14),
              FilledButton.icon(
                onPressed: _text('authorizationCode').isEmpty
                    ? null
                    : () => _applyAuthorizationCode(_text('authorizationCode')),
                icon: const Icon(Icons.login),
                label: const Text('验证并进入 OSS'),
              ),
            ],
          ),
        ),
      ),
    ),
  );

  Widget _browserPage() => Row(
    crossAxisAlignment: CrossAxisAlignment.stretch,
    children: [
      SizedBox(width: 390, child: _locationPanel()),
      const SizedBox(width: 16),
      Expanded(child: _ossExplorerPanel()),
    ],
  );

  Widget _settingsPage() => Column(
    crossAxisAlignment: CrossAxisAlignment.stretch,
    children: [
      Row(
        children: [
          Text('设置', style: Theme.of(context).textTheme.titleLarge),
          const Spacer(),
          OutlinedButton.icon(
            onPressed: _manager.running || _starting ? null : _save,
            icon: const Icon(Icons.save_outlined),
            label: const Text('保存设置'),
          ),
        ],
      ),
      const SizedBox(height: 12),
      Expanded(child: _configPanel()),
    ],
  );

  Widget _uploadPage() => Column(
    crossAxisAlignment: CrossAxisAlignment.stretch,
    children: [
      Text('上传任务', style: Theme.of(context).textTheme.titleLarge),
      const SizedBox(height: 12),
      Expanded(child: _progressPanel()),
    ],
  );

  Widget _downloadPage() => Column(
    crossAxisAlignment: CrossAxisAlignment.stretch,
    children: [
      Row(
        children: [
          Expanded(
            child: Text('下载文件', style: Theme.of(context).textTheme.titleLarge),
          ),
          FilledButton.icon(
            onPressed: _selectedObjectKey == null || _downloading
                ? null
                : _downloadSelectedObject,
            icon: _downloading
                ? const SizedBox.square(
                    dimension: 18,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.download),
            label: Text(_downloading ? '正在下载…' : '下载选中文件'),
          ),
        ],
      ),
      const SizedBox(height: 8),
      Text(
        _selectedObjectKey == null
            ? '请先到“OSS 文件”页面选择一个文件对象。支持任意文件类型。'
            : '已选择：$_selectedObjectKey',
      ),
      const SizedBox(height: 12),
      Expanded(child: _ossExplorerPanel()),
    ],
  );

  List<({String name, String? key, bool directory, String raw})>
  _visibleOssItems() {
    AppConfig effective;
    try {
      effective = _readForm().resolveAuthorization();
    } on FormatException {
      return _ossEntries
          .map((line) => (name: line, key: null, directory: false, raw: line))
          .toList();
    }
    final bucket = effective.bucket;
    final current = effective.prefix.replaceAll(RegExp(r'^/+|/+$'), '');
    final children = <String, ({String? key, bool directory, String raw})>{};
    final objectPattern = RegExp(r'oss://([^/\s]+)/([^\s]*)');
    for (final line in _ossEntries) {
      final match = objectPattern.firstMatch(line);
      if (match == null || match.group(1) != bucket) continue;
      var key = match.group(2)!.replaceAll(RegExp(r'[,;]+$'), '');
      try {
        key = Uri.decodeComponent(key);
      } on ArgumentError {
        // 对象名可能包含未编码的百分号，原样保留即可继续浏览。
      }
      final directoryMarker = key.endsWith('/');
      final normalized = key.replaceAll(RegExp(r'^/+|/+$'), '');
      final remainder = current.isEmpty
          ? normalized
          : normalized.startsWith('$current/')
          ? normalized.substring(current.length + 1)
          : '';
      if (remainder.isEmpty) continue;
      final slash = remainder.indexOf('/');
      if (slash >= 0) {
        final name = remainder.substring(0, slash);
        final childKey = [if (current.isNotEmpty) current, name].join('/');
        children[name] = (key: childKey, directory: true, raw: line);
      } else if (directoryMarker) {
        final name = remainder;
        children[name] = (key: normalized, directory: true, raw: line);
      } else {
        final name = remainder;
        children.putIfAbsent(
          name,
          () => (key: normalized, directory: false, raw: line),
        );
      }
    }
    final items =
        children.entries
            .map(
              (entry) => (
                name: entry.key,
                key: entry.value.key,
                directory: entry.value.directory,
                raw: entry.value.raw,
              ),
            )
            .toList()
          ..sort((a, b) {
            if (a.directory != b.directory) return a.directory ? -1 : 1;
            return a.name.toLowerCase().compareTo(b.name.toLowerCase());
          });
    return items;
  }

  Future<void> _openOssFolder(String key) async {
    _controllers['prefix']?.text = key;
    _selectedObjectKey = null;
    setState(() {});
    await _refreshOssListing();
  }

  Future<void> _goOssParent() async {
    final effective = _readForm().resolveAuthorization();
    final current = effective.prefix.replaceAll(RegExp(r'^/+|/+$'), '');
    final authRoot = _text('authorizationCode').isEmpty
        ? ''
        : OssAuthorization.parse(_text('authorizationCode')).prefix;
    if (current == authRoot || current.isEmpty) return;
    final parent = current.contains('/')
        ? current.substring(0, current.lastIndexOf('/'))
        : authRoot;
    await _openOssFolder(parent);
  }

  Widget _ossExplorerPanel() {
    AppConfig? effective;
    try {
      effective = _readForm().resolveAuthorization();
    } on FormatException {
      effective = null;
    }
    final path = effective == null
        ? '请先输入有效授权码'
        : 'oss://${effective.bucket}/${effective.prefix.isEmpty ? '' : '${effective.prefix}/'}';
    final items = _visibleOssItems();
    return Card(
      elevation: 0,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          Padding(
            padding: const EdgeInsets.fromLTRB(18, 12, 8, 8),
            child: Row(
              children: [
                IconButton(
                  tooltip: '返回上级文件夹',
                  onPressed: _ossListing || effective == null
                      ? null
                      : _goOssParent,
                  icon: const Icon(Icons.arrow_upward),
                ),
                const Icon(Icons.cloud_outlined),
                const SizedBox(width: 10),
                Expanded(
                  child: Text(
                    path,
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                  ),
                ),
                IconButton(
                  tooltip: '刷新当前文件夹',
                  onPressed: effective == null ? null : _refreshOssListing,
                  icon: const Icon(Icons.refresh),
                ),
                IconButton(
                  tooltip: '选择当前文件夹作为上传目标',
                  onPressed: effective == null
                      ? null
                      : () {
                          ScaffoldMessenger.of(
                            context,
                          ).showSnackBar(SnackBar(content: Text('上传目标：$path')));
                        },
                  icon: const Icon(Icons.check_circle_outline),
                ),
              ],
            ),
          ),
          const Divider(height: 1),
          if (_ossListingError != null)
            Padding(
              padding: const EdgeInsets.all(16),
              child: Text(
                _ossListingError!,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              ),
            )
          else if (_ossListing)
            const Expanded(child: Center(child: CircularProgressIndicator()))
          else if (effective == null)
            const Expanded(child: Center(child: Text('使用右上角“切换授权码”进入 OSS')))
          else if (items.isEmpty)
            const Expanded(child: Center(child: Text('此文件夹为空，或尚未刷新目录')))
          else
            Expanded(
              child: ListView.separated(
                itemCount: items.length,
                separatorBuilder: (_, _) =>
                    const Divider(height: 1, indent: 56),
                itemBuilder: (context, index) {
                  final item = items[index];
                  final selected =
                      item.key != null && item.key == _selectedObjectKey;
                  return ListTile(
                    selected: selected,
                    leading: Icon(
                      item.directory
                          ? Icons.folder_outlined
                          : Icons.insert_drive_file_outlined,
                    ),
                    title: Text(
                      item.name,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                    subtitle: item.directory
                        ? const Text('文件夹')
                        : Text(
                            item.raw,
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                          ),
                    trailing: item.directory
                        ? const Icon(Icons.chevron_right)
                        : selected
                        ? const Icon(Icons.check_circle)
                        : null,
                    onTap: item.key == null
                        ? null
                        : item.directory
                        ? () => _openOssFolder(item.key!)
                        : () => setState(() => _selectedObjectKey = item.key),
                  );
                },
              ),
            ),
          Padding(
            padding: const EdgeInsets.all(12),
            child: Text(
              '${items.where((item) => item.directory).length} 个文件夹 · '
              '${items.where((item) => !item.directory).length} 个文件（不限格式）',
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ),
        ],
      ),
    );
  }

  Widget _locationPanel() {
    final source = _text('sourcePath');
    final workspace = _text('workspacePath');
    var bucket = _text('bucket');
    var endpoint = _text('endpoint');
    if (_text('authorizationCode').isNotEmpty) {
      try {
        final auth = OssAuthorization.parse(_text('authorizationCode'));
        bucket = auth.bucket;
        endpoint = auth.endpoint;
      } on FormatException catch (_) {
        // 授权码错误时保留手动字段，并由配置区显示具体错误。
      }
    }
    final prefix = _text('prefix').replaceAll(RegExp(r'^/+|/+$'), '');
    final ossPath = bucket.isEmpty
        ? '请先填写授权码或 Bucket'
        : 'oss://$bucket${prefix.isEmpty ? '/' : '/$prefix/'}';
    return Card(
      elevation: 0,
      child: Padding(
        padding: const EdgeInsets.all(14),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Row(
              children: [
                const Icon(Icons.account_tree_outlined, size: 20),
                const SizedBox(width: 8),
                const Expanded(
                  child: Text(
                    '路径浏览',
                    style: TextStyle(fontWeight: FontWeight.w600),
                  ),
                ),
                IconButton(
                  tooltip: '刷新路径信息',
                  onPressed: _refreshOssListing,
                  icon: const Icon(Icons.refresh, size: 18),
                ),
              ],
            ),
            const Divider(),
            const Text('本地源目录'),
            const SizedBox(height: 6),
            ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: const Icon(Icons.sd_storage_outlined),
              title: Text(
                source.isEmpty ? '未选择' : source,
                maxLines: 3,
                overflow: TextOverflow.ellipsis,
              ),
              subtitle: const Text('读卡器 / 外接磁盘'),
              onTap: () => _pickDirectory('sourcePath', '选择读卡器源目录'),
            ),
            const SizedBox(height: 8),
            const Text('本地工作区'),
            ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: const Icon(Icons.folder_copy_outlined),
              title: Text(
                workspace.isEmpty ? '未选择' : workspace,
                maxLines: 3,
                overflow: TextOverflow.ellipsis,
              ),
              subtitle: Text(_config.transcodeEnabled ? '转码临时目录' : '直传模式下不会使用'),
              onTap: () => _pickDirectory('workspacePath', '选择本地工作区'),
            ),
            const Divider(height: 26),
            const Text('OSS 目标目录'),
            const SizedBox(height: 6),
            ListTile(
              dense: true,
              contentPadding: EdgeInsets.zero,
              leading: const Icon(Icons.cloud_outlined),
              title: Text(
                ossPath,
                maxLines: 3,
                overflow: TextOverflow.ellipsis,
              ),
              subtitle: Text(
                endpoint.isEmpty ? '等待授权码解析' : endpoint,
                maxLines: 2,
                overflow: TextOverflow.ellipsis,
              ),
            ),
            if (prefix.isNotEmpty)
              Wrap(
                spacing: 4,
                runSpacing: 4,
                children: [
                  const Icon(Icons.home_work_outlined, size: 16),
                  for (final segment in prefix.split('/'))
                    Chip(
                      label: Text(segment),
                      visualDensity: VisualDensity.compact,
                      materialTapTargetSize: MaterialTapTargetSize.shrinkWrap,
                    ),
                ],
              ),
            OutlinedButton.icon(
              onPressed: (_manager.running || _starting)
                  ? null
                  : _selectOssPrefix,
              icon: const Icon(Icons.folder_open_outlined),
              label: const Text('选择 OSS 路径'),
            ),
            const SizedBox(height: 8),
            Row(
              children: [
                const Expanded(child: Text('OSS 原目录内容')),
                if (_ossListing)
                  const SizedBox(
                    width: 14,
                    height: 14,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
              ],
            ),
            const SizedBox(height: 4),
            if (_ossListingError != null)
              Text(
                _ossListingError!,
                maxLines: 3,
                overflow: TextOverflow.ellipsis,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              )
            else if (_ossEntries.isEmpty)
              const Text('点击右上角刷新读取 OSS 原目录', style: TextStyle(fontSize: 12))
            else
              Expanded(
                child: ListView.builder(
                  itemCount: _ossEntries.length,
                  itemBuilder: (context, index) => Padding(
                    padding: const EdgeInsets.symmetric(vertical: 2),
                    child: Text(
                      _ossEntries[index],
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(fontSize: 12),
                    ),
                  ),
                ),
              ),
            const SizedBox(height: 10),
            Text(
              _config.transcodeEnabled
                  ? '当前流程：源目录 → 本地工作区 → OSS'
                  : '当前流程：源目录 → OSS（不复制）',
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ],
        ),
      ),
    );
  }

  Widget _configPanel() {
    final busy = _manager.running || _starting;
    final visibleFields = _fields
        .where(
          (field) =>
              !widget.demoMode ||
              !{
                'ffmpegPath',
                'ossutilPath',
                'accessKeyId',
                'accessKeySecret',
                'securityToken',
              }.contains(field.$1),
        )
        .where((field) => _showAdvanced || !_advancedKeys.contains(field.$1))
        .toList(growable: false);
    final specialCount = 6;
    return Card(
      elevation: 0,
      child: Form(
        key: _formKey,
        child: Scrollbar(
          child: GridView.builder(
            padding: const EdgeInsets.all(16),
            itemCount: visibleFields.length + specialCount,
            gridDelegate: const SliverGridDelegateWithMaxCrossAxisExtent(
              maxCrossAxisExtent: 460,
              mainAxisExtent: 76,
              crossAxisSpacing: 14,
              mainAxisSpacing: 6,
            ),
            itemBuilder: (context, index) {
              final specialIndex = index - visibleFields.length;
              if (specialIndex == 0) {
                return Align(
                  alignment: Alignment.centerLeft,
                  child: Row(
                    children: [
                      const Icon(Icons.movie_outlined, size: 20),
                      const SizedBox(width: 8),
                      const Expanded(
                        child: Text('视频转码', overflow: TextOverflow.ellipsis),
                      ),
                      Tooltip(
                        message: '关闭后保留原始视频，只执行复制和上传',
                        child: Switch(
                          value: _config.transcodeEnabled,
                          onChanged: busy
                              ? null
                              : (value) => setState(
                                  () => _config = _config.copyWith(
                                    transcodeEnabled: value,
                                  ),
                                ),
                        ),
                      ),
                    ],
                  ),
                );
              }
              if (specialIndex == 1) {
                return Align(
                  alignment: Alignment.centerLeft,
                  child: Row(
                    children: [
                      const Icon(Icons.cloud_upload_outlined, size: 20),
                      const SizedBox(width: 8),
                      const Expanded(
                        child: Text('高吞吐批量上传', overflow: TextOverflow.ellipsis),
                      ),
                      const SizedBox(width: 8),
                      Tooltip(
                        message: '推荐开启：由 ossutil 统一管理大量文件的并发上传',
                        child: Switch(
                          value: _config.bulkUpload,
                          onChanged: busy
                              ? null
                              : (value) => setState(
                                  () => _config = _config.copyWith(
                                    bulkUpload: value,
                                  ),
                                ),
                        ),
                      ),
                    ],
                  ),
                );
              }
              if (specialIndex == 2) {
                return Align(
                  alignment: Alignment.centerLeft,
                  child: Row(
                    children: [
                      const Icon(Icons.delete_sweep_outlined, size: 20),
                      const SizedBox(width: 8),
                      const Expanded(
                        child: Text(
                          '上传成功后清理临时文件',
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                      const SizedBox(width: 8),
                      Tooltip(
                        message: '只删除本次任务的临时工作区，不会删除源盘文件',
                        child: Switch(
                          value: _config.deleteAfterUpload,
                          onChanged: busy
                              ? null
                              : (value) => setState(
                                  () => _config = _config.copyWith(
                                    deleteAfterUpload: value,
                                  ),
                                ),
                        ),
                      ),
                    ],
                  ),
                );
              }
              if (specialIndex == 3) {
                final selectedPreset = _presetFor(_text('ffmpegArgs'));
                return DropdownButtonFormField<String>(
                  initialValue: selectedPreset,
                  isExpanded: true,
                  decoration: const InputDecoration(
                    labelText: '视频编码方案',
                    helperText: '普通用户直接选择方案即可，也可在高级设置中自定义参数',
                  ),
                  items: _transcodePresets.keys
                      .map(
                        (label) => DropdownMenuItem<String>(
                          value: label,
                          child: Text(label),
                        ),
                      )
                      .toList(growable: false),
                  onChanged: busy || !_config.transcodeEnabled
                      ? null
                      : (label) {
                          final args = label == null
                              ? null
                              : _transcodePresets[label];
                          if (args == null) return;
                          _controllers['ffmpegArgs']?.text = args;
                          setState(() {});
                        },
                );
              }
              if (specialIndex == 4) {
                return Align(
                  alignment: Alignment.centerLeft,
                  child: TextButton.icon(
                    onPressed: busy
                        ? null
                        : () => setState(() => _showAdvanced = !_showAdvanced),
                    icon: Icon(
                      _showAdvanced ? Icons.expand_less : Icons.tune_outlined,
                    ),
                    label: Text(_showAdvanced ? '收起高级设置' : '显示高级设置'),
                  ),
                );
              }
              if (specialIndex == 5) {
                final hasCode = _text('authorizationCode').isNotEmpty;
                return Align(
                  alignment: Alignment.centerLeft,
                  child: Text(
                    _authorizationPreview(),
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                    style: Theme.of(context).textTheme.bodySmall?.copyWith(
                      color:
                          hasCode && _authorizationPreview().startsWith('授权码无效')
                          ? Theme.of(context).colorScheme.error
                          : Theme.of(context).colorScheme.primary,
                    ),
                  ),
                );
              }
              final (key, label, secret) = visibleFields[index];
              final numeric =
                  key == 'uploadConcurrency' ||
                  key == 'multipartParallel' ||
                  key == 'bigFileThresholdMb' ||
                  key == 'partSizeMb' ||
                  key == 'uploadRetries' ||
                  key == 'transcodeConcurrency' ||
                  key == 'copyConcurrency' ||
                  key == 'cleanupConcurrency';
              return TextFormField(
                controller: _controllers[key],
                obscureText: secret,
                // Flutter 的 obscureText 字段必须保持单行；授权码可直接粘贴，
                // 内部换行会由解析器忽略。
                maxLines: 1,
                keyboardType: numeric
                    ? TextInputType.number
                    : TextInputType.text,
                enabled:
                    !busy &&
                    !(_text('authorizationCode').isNotEmpty &&
                        const {
                          'bucket',
                          'endpoint',
                          'accessKeyId',
                          'accessKeySecret',
                          'securityToken',
                        }.contains(key)),
                onChanged: key == 'authorizationCode'
                    ? (_) => setState(() {})
                    : null,
                validator: (value) {
                  if (key == 'authorizationCode' &&
                      (value?.trim().isNotEmpty ?? false)) {
                    try {
                      OssAuthorization.parse(value!);
                    } on FormatException catch (error) {
                      return error.message;
                    }
                  }
                  if (key == 'outputExtension') {
                    final extension = value?.trim() ?? '';
                    final normalized = extension.startsWith('.')
                        ? extension
                        : '.$extension';
                    if (!RegExp(
                      r'^\.[a-zA-Z0-9]{1,10}$',
                    ).hasMatch(normalized)) {
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
                        : key == 'multipartParallel'
                        ? 128
                        : key == 'bigFileThresholdMb'
                        ? 10240
                        : key == 'partSizeMb'
                        ? 1024
                        : key == 'uploadConcurrency'
                        ? 128
                        : 32;
                    if (parsed == null || parsed < min || parsed > max) {
                      return '请输入 $min 到 $max 之间的整数';
                    }
                  }
                  return null;
                },
                decoration: InputDecoration(
                  labelText: label,
                  helperText: switch (key) {
                    'sourcePath' => '读卡器或外接磁盘中的照片/视频目录',
                    'workspacePath' => '本机临时空间，需要足够的磁盘容量',
                    'authorizationCode' => '粘贴服务方提供的一整段授权码即可',
                    'bucket' => '授权码模式下会自动填充',
                    'prefix' => '可填写 OSS 子目录，例如：照片/2026/10；留空则使用授权码中的路径',
                    'uploadConcurrency' => '默认 10；网络和磁盘较快时可提高',
                    _ => null,
                  },
                  suffixIcon: switch (key) {
                    'sourcePath' => IconButton(
                      tooltip: '选择源目录',
                      onPressed: busy
                          ? null
                          : () => _pickDirectory(key, '选择读卡器源目录'),
                      icon: const Icon(Icons.folder_open_outlined),
                    ),
                    'workspacePath' => IconButton(
                      tooltip: '选择工作区目录',
                      onPressed: busy
                          ? null
                          : () => _pickDirectory(key, '选择本地工作区'),
                      icon: const Icon(Icons.folder_open_outlined),
                    ),
                    'ffmpegPath' when !widget.demoMode => IconButton(
                      tooltip: '选择 ffmpeg.exe',
                      onPressed: busy
                          ? null
                          : () => _pickExecutable(key, '选择 ffmpeg.exe'),
                      icon: const Icon(Icons.video_file_outlined),
                    ),
                    'ossutilPath' when !widget.demoMode => IconButton(
                      tooltip: '选择 ossutil.exe',
                      onPressed: busy
                          ? null
                          : () => _pickExecutable(key, '选择 ossutil.exe'),
                      icon: const Icon(Icons.cloud_upload_outlined),
                    ),
                    _ => null,
                  },
                ),
              );
            },
          ),
        ),
      ),
    );
  }

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
    final busy = running || _starting;
    final scanning =
        _state.stage == PipelineStage.copying &&
        _state.message.startsWith('扫描中');
    // 批量模式没有逐文件进度，但 ossutil 仍可能报告整体百分比。
    // 只有外部工具没有给出百分比时才使用不确定进度条，避免隐藏已有进度。
    final overallIndeterminate =
        scanning ||
        (_state.stage == PipelineStage.uploading &&
            _state.message.startsWith('批量上传') &&
            _state.fileProgress == null);
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
                if (_state.paused) ...[
                  const SizedBox(width: 10),
                  const Chip(
                    avatar: Icon(Icons.pause_circle_outline, size: 18),
                    label: Text('已暂停'),
                    visualDensity: VisualDensity.compact,
                  ),
                ],
                const Spacer(),
                if (running && !_state.paused)
                  IconButton(
                    tooltip: _manager.bulkUploadActive ? '批量上传中不可暂停' : '暂停',
                    onPressed: _manager.bulkUploadActive
                        ? null
                        : _manager.pause,
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
                  onPressed: busy ? null : _start,
                  icon: const Icon(Icons.play_arrow),
                  label: Text(_starting ? '准备中…' : '开始任务'),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Row(
              children: [
                const SizedBox(width: 92, child: Text('总体进度')),
                Expanded(
                  child: LinearProgressIndicator(
                    value: overallIndeterminate ? null : _state.overall,
                  ),
                ),
                const SizedBox(width: 12),
                Text(
                  overallIndeterminate
                      ? (scanning ? '扫描中' : 'ossutil 上传中')
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
        if (_renderedLogError != null)
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 4),
            child: Text(
              '日志诊断：$_renderedLogError',
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
