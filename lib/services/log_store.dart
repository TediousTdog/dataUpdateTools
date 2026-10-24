import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:path/path.dart' as p;
import 'app_paths.dart';

/// 每次应用运行独立日志文件，按序异步写入；定期 flush，退出时完整关闭。
class LogStore {
  LogStore({this.rootDirectory});

  /// 可注入临时目录以便测试；生产环境固定写入用户应用数据目录。
  final Directory? rootDirectory;
  IOSink? _sink;
  Timer? _flushTimer;
  Future<void> _queue = Future<void>.value();
  Future<void> _lifecycleQueue = Future<void>.value();
  String? _filePath;
  Object? lastError;
  int _pendingLines = 0;
  int _lifecycleGeneration = 0;
  bool _closing = true;

  String? get filePath => _filePath;
  String? get directoryPath => _filePath == null ? null : p.dirname(_filePath!);

  Future<void> startSession() {
    final generation = ++_lifecycleGeneration;
    _closing = true;
    _flushTimer?.cancel();
    _flushTimer = null;
    return _enqueueLifecycle(() => _startSessionNow(generation));
  }

  Future<void> _startSessionNow(int generation) async {
    await _closeSink();
    _pendingLines = 0;
    lastError = null;
    _filePath = null;
    final dir =
        rootDirectory ??
        Directory(p.join(appDataDirectory(), 'MediaIngest', 'logs'));
    await dir.create(recursive: true);
    final now = DateTime.now();
    final stamp =
        '${now.year}${_two(now.month)}${_two(now.day)}_'
        '${_two(now.hour)}${_two(now.minute)}${_two(now.second)}_${now.microsecond}';
    _filePath = p.join(dir.path, 'run_$stamp.log');
    _sink = File(
      _filePath!,
    ).openWrite(mode: FileMode.writeOnlyAppend, encoding: utf8);
    _flushTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      _enqueueFlush();
    });
    // close()/startSession() 可能在本次打开期间排到 lifecycle 队列后面。
    // 只有仍是最新请求的启动才能重新接收日志。
    _closing = generation != _lifecycleGeneration;
  }

  Future<void> append(String line) {
    final sink = _sink;
    if (_closing || sink == null) {
      return Future<void>.error(StateError('日志会话尚未启动或正在关闭'));
    }
    final write = _queue.then((_) async {
      sink.writeln(line);
      _pendingLines++;
      if (_pendingLines >= 32) await _flushSink();
    });
    _queue = write.catchError((Object error) {
      lastError = error;
    });
    return write;
  }

  void _enqueueFlush() {
    if (_closing) return;
    final queued = _queue.then((_) => _flushSink());
    _queue = queued.catchError((Object error) {
      lastError = error;
    });
  }

  Future<void> _flushSink() async {
    final sink = _sink;
    if (sink == null || _pendingLines == 0) return;
    try {
      await sink.flush();
      _pendingLines = 0;
    } catch (error) {
      lastError = error;
    }
  }

  Future<void> close() {
    ++_lifecycleGeneration;
    _closing = true;
    _flushTimer?.cancel();
    _flushTimer = null;
    return _enqueueLifecycle(_closeSink);
  }

  Future<void> _closeSink() async {
    await _queue;
    final sink = _sink;
    _sink = null;
    if (sink != null) {
      try {
        await sink.flush();
      } catch (error) {
        lastError = error;
        rethrow;
      } finally {
        _pendingLines = 0;
        await sink.close();
      }
    }
  }

  Future<void> _enqueueLifecycle(Future<void> Function() operation) {
    final result = _lifecycleQueue.then((_) => operation());
    // 即使一次 lifecycle 操作失敗，也允許應用後續再次開啟或關閉會話。
    _lifecycleQueue = result.catchError((Object error) {
      lastError = error;
    });
    return result;
  }

  String _two(int value) => value.toString().padLeft(2, '0');
}
