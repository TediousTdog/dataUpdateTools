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
        // ossutil/ffmpeg 的 Windows 版本可能分别输出 UTF-8 或当前代码页
        // （中文系统通常是 GBK）。按“每行”自适应解码，既保留实时进度，
        // 又不会因一种编码失败而丢失后续日志。
        .transform(_adaptiveLineDecoder())
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
        .transform(_adaptiveLineDecoder())
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

StreamTransformer<List<int>, String> _adaptiveLineDecoder() {
  final bytes = <int>[];
  String decode(List<int> line) {
    try {
      return utf8.decode(line, allowMalformed: false);
    } on FormatException {
      // systemEncoding 在 Windows 使用当前活动代码页；在 macOS/Linux
      // 仍为 UTF-8。这里只在严格 UTF-8 失败时走回退路径。
      try {
        return systemEncoding.decode(line);
      } on Object {
        // 开发环境若没有可用的系统代码页，也要保留日志行而不是让
        // stdout stream 结束并丢掉后续进度。
        return utf8.decode(line, allowMalformed: true);
      }
    }
  }

  return StreamTransformer<List<int>, String>.fromHandlers(
    handleData: (chunk, sink) {
      bytes.addAll(chunk);
      var start = 0;
      for (var index = 0; index < bytes.length; index++) {
        final delimiter = bytes[index];
        if (delimiter != 0x0a && delimiter != 0x0d) continue;
        // A CR at the very end of a chunk may be the first half of CRLF;
        // leave it buffered until the next chunk arrives.
        if (delimiter == 0x0d && index == bytes.length - 1) break;
        var line = bytes.sublist(start, index);
        sink.add(decode(line));
        start = index + 1;
        if (delimiter == 0x0d && start < bytes.length && bytes[start] == 0x0a) {
          index++;
          start++;
        }
      }
      if (start > 0) bytes.removeRange(0, start);
    },
    handleDone: (sink) {
      if (bytes.isNotEmpty) {
        var line = List<int>.from(bytes);
        if (line.isNotEmpty && line.last == 0x0d) {
          line = line.sublist(0, line.length - 1);
        }
        sink.add(decode(line));
      }
      sink.close();
    },
  );
}
