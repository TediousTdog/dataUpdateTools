import 'dart:io';
import 'package:path/path.dart' as p;
import 'pipeline_types.dart';

class CopyService {
  CopyService({Future<File> Function(File source, String target)? copyFile})
    : _copyFile = copyFile ?? ((source, target) => source.copy(target));

  final Future<File> Function(File source, String target) _copyFile;

  /// 递归扫描后用异步文件 I/O 并行复制；Dart 的异步文件操作不会占用 Flutter UI isolate。
  Future<List<File>> copyTree({
    required String source,
    required String workspace,
    required int concurrency,
    required PauseGate gate,
    required LogCallback log,
    required CopyProgressCallback progress,
    required void Function(String name, double progress, String status) onTask,
    required void Function(String relativePath, int size) onFileScanned,
    required void Function(int discovered, bool complete, int totalBytes)
    onScan,
  }) async {
    final src = Directory(source);
    if (!await src.exists()) throw FileSystemException('源目录不存在', source);
    final root = Directory(workspace);
    await root.create(recursive: true);
    final files = <File>[];
    final fileSizes = <int>[];
    var totalBytes = 0;
    var nextScanReport = 100;
    await for (final entity in src.list(recursive: true, followLinks: false)) {
      await gate.checkpoint();
      if (entity is File) {
        final size = await entity.length();
        files.add(entity);
        fileSizes.add(size);
        totalBytes += size;
        onFileScanned(p.relative(entity.path, from: source), size);
      }
      if (files.length >= nextScanReport) {
        onScan(files.length, false, totalBytes);
        nextScanReport += 100;
      }
    }
    log('扫描完成：${files.length} 个文件');
    onScan(files.length, true, totalBytes);
    var next = 0, done = 0;
    var copiedBytes = 0;
    var stopScheduling = false;
    Object? copyFailure;
    final copied = <File>[];
    Future<void> worker() async {
      while (true) {
        if (stopScheduling) return;
        await gate.checkpoint();
        if (gate.cancelled || stopScheduling) return;
        final index = next++;
        if (index >= files.length) return;
        final sourceFile = files[index];
        final name = p.relative(sourceFile.path, from: source);
        onTask(name, 0, '复制中');
        final relative = p.relative(sourceFile.path, from: source);
        final target = File(p.join(workspace, relative));
        try {
          await target.parent.create(recursive: true);
          await _copyFile(sourceFile, target.path);
          copied.add(target);
          done++;
          copiedBytes += fileSizes[index];
          onTask(name, 1, '已复制');
          progress(
            totalBytes > 0
                ? copiedBytes / totalBytes
                : (files.isEmpty ? 1 : done / files.length),
            p.basename(sourceFile.path),
            done,
            files.length,
          );
        } catch (error) {
          onTask(name, 0, '失败');
          copyFailure ??= error;
          stopScheduling = true;
          gate.resume();
          return;
        }
      }
    }

    await Future.wait(
      List.generate(concurrency.clamp(1, 32).toInt(), (_) => worker()),
    );
    if (gate.cancelled) throw const PipelineCancelledException();
    if (copyFailure != null) throw copyFailure!;
    if (files.isEmpty) progress(1, '', 0, 0);
    return copied;
  }
}
