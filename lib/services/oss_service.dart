import 'dart:io';
import 'package:path/path.dart' as p;
import '../models/app_config.dart';
import 'process_runner.dart';
import 'pipeline_types.dart';

class OssService {
  OssService(this.runner);
  final ProcessRunner runner;

  /// 读取 OSS 原目录的对象列表，供左侧路径浏览器使用。
  ///
  /// 采用独立 argv 调用 ossutil，不经过 shell；因此 Bucket 前缀中的中文、
  /// 空格和特殊字符不会被命令行重新编码。不同 ossutil 版本对列表输出格式
  /// 略有差异，UI 保留原始行以便排查。
  Future<List<String>> listPrefix({
    required AppConfig config,
    int maxLines = 200,
    void Function(String line)? onLog,
  }) async {
    final effective = config.resolveAuthorization();
    if (effective.bucket.trim().isEmpty) {
      throw ArgumentError('OSS Bucket 不能为空');
    }
    if (effective.endpoint.trim().isEmpty) {
      throw ArgumentError('OSS Endpoint 不能为空');
    }
    if (effective.accessKeyId.trim().isEmpty ||
        effective.accessKeySecret.trim().isEmpty) {
      throw ArgumentError('OSS AccessKey ID/Secret 不能为空');
    }
    final prefix = _cleanPrefix(effective.prefix);
    final destination =
        'oss://${effective.bucket}/${prefix.isEmpty ? '' : '$prefix/'}';
    final lines = <String>[];
    final args = <String>[
      '-e',
      effective.endpoint,
      '-i',
      effective.accessKeyId,
      '-k',
      effective.accessKeySecret,
      if (effective.securityToken.trim().isNotEmpty) ...[
        '-t',
        effective.securityToken,
      ],
      'ls',
      '-r',
      destination,
    ];
    final code = await runner.run(
      executable: effective.ossutilPath,
      arguments: args,
      log: onLog ?? (_) {},
      onStdout: (line) {
        if (lines.length < maxLines && line.trim().isNotEmpty) lines.add(line);
      },
      onStderr: (line) {
        onLog?.call(line);
      },
    );
    if (code != 0) throw StateError('读取 OSS 目录失败，退出码 $code');
    return lines;
  }

  /// 构造单文件上传参数。参数始终以独立 argv 传给 ossutil，不经过 cmd.exe，
  /// 所以 Windows 路径中的空格、中文和括号都不会被 shell 二次解析。
  List<String> buildUploadArguments(
    File file, {
    required AppConfig config,
    required String workspace,
  }) {
    final effective = config.resolveAuthorization();
    if (effective.bucket.trim().isEmpty) {
      throw ArgumentError('OSS Bucket 不能为空');
    }
    if (effective.endpoint.trim().isEmpty) {
      throw ArgumentError('OSS Endpoint 不能为空');
    }
    if (effective.accessKeyId.trim().isEmpty ||
        effective.accessKeySecret.trim().isEmpty) {
      throw ArgumentError('OSS AccessKey ID/Secret 不能为空');
    }
    final relative = p
        .relative(file.path, from: workspace)
        .replaceAll(r'\', '/');
    final prefix = _cleanPrefix(effective.prefix);
    final key = [if (prefix.isNotEmpty) prefix, relative].join('/');
    final destination = 'oss://${effective.bucket}/$key';
    final multipart = effective.multipartParallel.clamp(1, 128);
    final threshold = _mbToBytes(effective.bigFileThresholdMb, maxMb: 10240);
    final partSize = _mbToBytes(effective.partSizeMb, maxMb: 1024);
    return <String>[
      '-e',
      effective.endpoint,
      '-i',
      effective.accessKeyId,
      '-k',
      effective.accessKeySecret,
      if (effective.securityToken.trim().isNotEmpty) ...[
        '-t',
        effective.securityToken,
      ],
      'cp',
      '-f',
      '--jobs=1',
      '--parallel=$multipart',
      '--bigfile-threshold=$threshold',
      '--part-size=$partSize',
      file.path,
      destination,
      ..._splitExtra(effective.ossExtraArgs),
    ];
  }

  Future<void> upload(
    File file, {
    required AppConfig config,
    required String workspace,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    final effective = config.resolveAuthorization();
    final args = buildUploadArguments(
      file,
      config: effective,
      workspace: workspace,
    );
    final code = await runner.run(
      executable: effective.ossutilPath,
      arguments: args,
      log: log,
      onStdout: (line) => _report(line, progress),
      onStderr: (line) => _report(line, progress),
    );
    if (code != 0) throw StateError('ossutil 上传失败，退出码 $code');
    progress(1, '上传完成');
  }

  /// 使用 ossutil 自身的批量/分片并行能力上传整个 run 目录。
  ///
  /// 该入口对应批处理里的 `cp -rf --jobs=N --parallel=N`，比为每个文件
  /// 启动一个进程更适合大量小文件，同时仍通过独立 argv 规避路径编码问题。
  /// 返回值为 ossutil 退出码；非零由调用方转换成失败状态。
  Future<int> uploadTree({
    required String workspace,
    required AppConfig config,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    final effective = config.resolveAuthorization();
    if (effective.bucket.trim().isEmpty) {
      throw ArgumentError('OSS Bucket 不能为空');
    }
    if (effective.endpoint.trim().isEmpty) {
      throw ArgumentError('OSS Endpoint 不能为空');
    }
    if (effective.accessKeyId.trim().isEmpty ||
        effective.accessKeySecret.trim().isEmpty) {
      throw ArgumentError('OSS AccessKey ID/Secret 不能为空');
    }
    final prefix = _cleanPrefix(effective.prefix);
    final destination =
        'oss://${effective.bucket}/${prefix.isEmpty ? '' : '$prefix/'}';
    final jobs = effective.uploadConcurrency.clamp(1, 128);
    final multipart = effective.multipartParallel.clamp(1, 128);
    final threshold = _mbToBytes(effective.bigFileThresholdMb, maxMb: 10240);
    final partSize = _mbToBytes(effective.partSizeMb, maxMb: 1024);
    // 末尾的 `.` 让 ossutil 复制目录内容而不是再套一层目录名。
    final source = '$workspace${Platform.pathSeparator}.';
    final args = <String>[
      '-e',
      effective.endpoint,
      '-i',
      effective.accessKeyId,
      '-k',
      effective.accessKeySecret,
      if (effective.securityToken.trim().isNotEmpty) ...[
        '-t',
        effective.securityToken,
      ],
      'cp',
      '-rf',
      '--jobs=$jobs',
      '--parallel=$multipart',
      '--bigfile-threshold=$threshold',
      '--part-size=$partSize',
      source,
      destination,
      ..._splitExtra(effective.ossExtraArgs),
    ];
    final code = await runner.run(
      executable: effective.ossutilPath,
      arguments: args,
      log: log,
      onStdout: (line) => _report(line, progress),
      onStderr: (line) => _report(line, progress),
    );
    if (code == 0) progress(1, '批量上传完成');
    return code;
  }

  void _report(String line, ProgressCallback progress) {
    final match = RegExp(r'(\d{1,3}(?:\.\d+)?)\s*%').firstMatch(line);
    if (match != null) {
      progress((double.parse(match[1]!) / 100).clamp(0, 1).toDouble(), line);
    }
  }
}

String _cleanPrefix(String value) =>
    value.replaceAll(r'\', '/').replaceAll(RegExp(r'^/+|/+$'), '');

int _mbToBytes(int value, {required int maxMb}) =>
    value.clamp(1, maxMb).toInt() * 1024 * 1024;

List<String> _splitExtra(String value) =>
    value.trim().isEmpty ? <String>[] : value.trim().split(RegExp(r'\s+'));
