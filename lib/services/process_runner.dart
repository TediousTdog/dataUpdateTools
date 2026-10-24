import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'pipeline_types.dart';

/// 持有当前子进程，取消流水线时可立即结束正在运行的工具。
class ProcessRunner {
  final Set<Process> _active = {};
  int _cancelGeneration = 0;

  Future<int> run({
    required String executable,
    required List<String> arguments,
    required LogCallback log,
    void Function(String line)? onStdout,
    void Function(String line)? onStderr,
  }) async {
    final generation = _cancelGeneration;
    // 不经 shell，arguments 中的每个元素都是独立参数，因此空格路径无需手工加引号。
    // Windows Dart runtime 会以非交互子进程方式创建进程；这里不使用 runInShell。
    final process = await Process.start(
      executable,
      arguments,
      runInShell: false,
      mode: ProcessStartMode.normal,
    );
    _active.add(process);
    // 取消可能发生在 CreateProcess 尚未返回的窗口期；generation 检查防止进程漏杀。
    if (generation != _cancelGeneration) process.kill();
    Object? outputFailure;
    StackTrace? outputFailureStack;
    void recordFailure(Object error, StackTrace stack) {
      outputFailure ??= error;
      outputFailureStack ??= stack;
    }

    void consumeLine(String line, void Function(String)? callback) {
      try {
        callback?.call(line);
      } catch (error, stack) {
        recordFailure(error, stack);
      }
      try {
        log(line);
      } catch (error, stack) {
        recordFailure(error, stack);
      }
    }

    final outDone = Completer<void>();
    final errDone = Completer<void>();
    process.stdout
        .transform(const Utf8Decoder(allowMalformed: true))
        .transform(const LineSplitter())
        .listen(
          (line) {
            consumeLine(line, onStdout);
          },
          onError: (Object error, StackTrace stack) {
            recordFailure(error, stack);
          },
          onDone: outDone.complete,
        );
    process.stderr
        .transform(const Utf8Decoder(allowMalformed: true))
        .transform(const LineSplitter())
        .listen(
          (line) {
            consumeLine(line, onStderr);
          },
          onError: (Object error, StackTrace stack) {
            recordFailure(error, stack);
          },
          onDone: errDone.complete,
        );
    try {
      final code = await process.exitCode;
      await Future.wait([outDone.future, errDone.future]);
      final failure = outputFailure;
      if (failure != null) {
        Error.throwWithStackTrace(failure, outputFailureStack!);
      }
      return code;
    } finally {
      _active.remove(process);
    }
  }

  Future<void> terminate() async {
    _cancelGeneration++;
    for (final process in _active.toList()) {
      process.kill();
    }
    _active.clear();
  }
}
