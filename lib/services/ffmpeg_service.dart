import 'dart:io';
import 'package:path/path.dart' as p;
import 'process_runner.dart';
import 'pipeline_types.dart';

class FFmpegService {
  FFmpegService(this.runner);
  final ProcessRunner runner;

  Future<File> transcode(
    File input, {
    required String executable,
    required String argumentTemplate,
    required String outputExtension,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    final ext = outputExtension.startsWith('.')
        ? outputExtension
        : '.$outputExtension';
    if (!RegExp(r'^\.[a-zA-Z0-9]{1,10}$').hasMatch(ext)) {
      throw ArgumentError.value(
        outputExtension,
        'outputExtension',
        '必须是简单文件扩展名',
      );
    }
    final output = await uniqueTranscodedOutputPath(input, ext);
    await File(output).parent.create(recursive: true);
    final parser = FFmpegProgressParser(onProgress: progress);
    final args = <String>[
      '-hide_banner',
      '-n',
      '-nostats',
      '-progress',
      'pipe:1',
      '-i',
      input.path,
      ..._splitArgs(argumentTemplate),
      output,
    ];
    final code = await runner.run(
      executable: executable,
      arguments: args,
      log: log,
      onStdout: parser.addStdoutLine,
      onStderr: parser.addStderrLine,
    );
    if (code != 0) {
      throw ProcessException(executable, args, 'ffmpeg 退出码 $code', code);
    }
    if (!await File(output).exists()) {
      throw FileSystemException('转码未生成输出文件', output);
    }
    // 转码文件已在工作区内成功落盘，再移除同目录的原视频，避免重复上传。
    await input.delete();
    return File(output);
  }
}

Future<String> uniqueTranscodedOutputPath(
  File input,
  String outputExtension,
) async {
  final sourceExtension = p
      .extension(input.path)
      .replaceFirst('.', '')
      .replaceAll(RegExp(r'[^a-zA-Z0-9_-]'), '_');
  final stem = p.basenameWithoutExtension(input.path);
  final directory = p.dirname(input.path);
  final taggedStem = '${stem}_from_$sourceExtension';
  var candidate = p.join(directory, '$taggedStem$outputExtension');
  var index = 2;
  while (await File(candidate).exists()) {
    candidate = p.join(directory, '${taggedStem}_$index$outputExtension');
    index++;
  }
  return candidate;
}

/// 将 FFmpeg `-progress pipe:1` 的 key=value 行解析成可展示的 0..1 进度。
class FFmpegProgressParser {
  FFmpegProgressParser({required this.onProgress});
  final ProgressCallback onProgress;
  double? _durationSeconds;
  double _lastProgress = -1;

  void addStdoutLine(String line) {
    final split = line.indexOf('=');
    if (split < 0) return;
    final key = line.substring(0, split), value = line.substring(split + 1);
    if (key == 'out_time_us' || key == 'out_time_ms') {
      final microseconds = int.tryParse(value);
      final duration = _durationSeconds;
      if (microseconds != null && duration != null && duration > 0) {
        // FFmpeg 的 out_time_ms 历史字段单位也是微秒；新版同时提供 out_time_us。
        final fraction = (microseconds / 1000000 / duration)
            .clamp(0, 0.999)
            .toDouble();
        if (fraction > _lastProgress) {
          _lastProgress = fraction;
          onProgress(fraction, '转码 ${(fraction * 100).toStringAsFixed(1)}%');
        }
      }
    } else if (key == 'progress' && value == 'end') {
      if (_lastProgress < 1) {
        _lastProgress = 1;
        onProgress(1, '转码完成');
      }
    }
  }

  void addStderrLine(String line) {
    final match = RegExp(
      r'Duration: (\d+):(\d+):(\d+(?:\.\d+)?)',
    ).firstMatch(line);
    if (match != null) {
      _durationSeconds =
          int.parse(match[1]!) * 3600 +
          int.parse(match[2]!) * 60 +
          double.parse(match[3]!);
    }
  }
}

/// 解析常见的带双引号/单引号参数模板，不做 shell 展开；不会执行变量或命令替换。
List<String> _splitArgs(String value) {
  final result = <String>[];
  final buffer = StringBuffer();
  String? quote;
  var escaped = false;
  for (final char in value.runes.map(String.fromCharCode)) {
    if (escaped) {
      buffer.write(char);
      escaped = false;
      continue;
    }
    if (char == r'\' && quote != null) {
      escaped = true;
      continue;
    }
    if (quote != null) {
      if (char == quote) {
        quote = null;
      } else {
        buffer.write(char);
      }
    } else if (char == '"' || char == "'") {
      quote = char;
    } else if (char.trim().isEmpty) {
      if (buffer.isNotEmpty) {
        result.add(buffer.toString());
        buffer.clear();
      }
    } else {
      buffer.write(char);
    }
  }
  if (quote != null) throw FormatException('参数模板包含未闭合引号');
  if (buffer.isNotEmpty) result.add(buffer.toString());
  return result;
}
