import 'dart:io';
import 'package:path/path.dart' as p;
import '../models/app_config.dart';
import 'process_runner.dart';
import 'pipeline_types.dart';

class OssService {
  OssService(this.runner);
  final ProcessRunner runner;

  Future<void> upload(
    File file, {
    required AppConfig config,
    required String workspace,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    if (config.bucket.trim().isEmpty) throw ArgumentError('OSS Bucket 不能为空');
    // 参数是独立 argv，不进行 shell 拼接。相对路径转换为对象 key，保留工作区目录结构。
    final relative = p
        .relative(file.path, from: workspace)
        .replaceAll(r'\', '/');
    final prefix = config.prefix.replaceAll(RegExp(r'^/+|/+$'), '');
    final key = [if (prefix.isNotEmpty) prefix, relative].join('/');
    final destination = 'oss://${config.bucket}/$key';
    final args = <String>[
      'cp',
      file.path,
      destination,
      '-e',
      config.endpoint,
      '-i',
      config.accessKeyId,
      '-k',
      config.accessKeySecret,
      '-t',
      config.securityToken,
      '-j',
      '1',
      ..._splitExtra(config.ossExtraArgs),
    ];
    final code = await runner.run(
      executable: config.ossutilPath,
      arguments: args,
      log: log,
      onStdout: (line) => _report(line, progress),
      onStderr: (line) => _report(line, progress),
    );
    if (code != 0) throw StateError('ossutil 上传失败，退出码 $code');
    progress(1, '上传完成');
  }

  void _report(String line, ProgressCallback progress) {
    final match = RegExp(r'(\d{1,3}(?:\.\d+)?)\s*%').firstMatch(line);
    if (match != null) {
      progress((double.parse(match[1]!) / 100).clamp(0, 1).toDouble(), line);
    }
  }
}

List<String> _splitExtra(String value) =>
    value.trim().isEmpty ? <String>[] : value.trim().split(RegExp(r'\s+'));
