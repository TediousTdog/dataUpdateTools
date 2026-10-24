import 'dart:io';

import 'package:path/path.dart' as p;

import 'pipeline_types.dart';

typedef CleanupTaskCallback =
    void Function(File file, double progress, String status);
typedef CleanupProgressCallback =
    void Function(double progress, int completedFiles, int totalFiles);

/// 只清理本次任务自己的 run 目录，并以有界并发删除文件。
///
/// 每个文件删除完成后才报告完成，因此暂停/取消只会等待当前系统文件操作
/// 收尾，不会在工作池继续领取后续文件。目录树最后一次性删除，用来清掉空目录。
class CleanupService {
  CleanupService({
    Future<void> Function(File file)? deleteFile,
    Future<void> Function(Directory directory)? deleteDirectory,
  }) : _deleteFile = deleteFile ?? ((file) => file.delete()),
       _deleteDirectory =
           deleteDirectory ??
           ((directory) => directory.delete(recursive: true));

  final Future<void> Function(File file) _deleteFile;
  final Future<void> Function(Directory directory) _deleteDirectory;

  Future<void> cleanRun({
    required String rootPath,
    required List<File> files,
    required int concurrency,
    required PauseGate gate,
    required CleanupTaskCallback onTask,
    required CleanupProgressCallback onProgress,
  }) async {
    final root = p.normalize(p.absolute(rootPath));
    final safeFiles = files
        .map((file) {
          final path = p.normalize(p.absolute(file.path));
          if (!_isWithin(root, path)) {
            throw ArgumentError.value(
              file.path,
              'files',
              '清理文件必须位于本次 run 工作区内',
            );
          }
          return File(path);
        })
        .toList(growable: false);

    var next = 0;
    var completed = 0;
    var stopScheduling = false;
    Object? firstFailure;

    Future<void> worker() async {
      while (true) {
        if (stopScheduling) return;
        await gate.checkpoint();
        if (stopScheduling) return;
        final index = next++;
        if (index >= safeFiles.length) return;
        final file = safeFiles[index];
        onTask(file, 0, '清理中');
        try {
          await _deleteFile(file);
          completed++;
          onTask(file, 1, '已清理');
          // 预留最后 5% 给空目录和 run 根目录收尾，避免文件刚删完便提前显示 100%。
          final fileFraction = safeFiles.isEmpty
              ? 0.95
              : 0.95 * completed / safeFiles.length;
          onProgress(fileFraction, completed, safeFiles.length);
        } catch (error) {
          onTask(file, 0, gate.cancelled ? '已中止' : '失败');
          if (gate.cancelled) return;
          firstFailure ??= error;
          stopScheduling = true;
          // 若用户同时暂停，解除等待，让已领取的 worker 能收尾并汇总失败。
          gate.resume();
          return;
        }
      }
    }

    final workerCount = safeFiles.isEmpty
        ? 0
        : concurrency.clamp(1, 32).toInt().clamp(1, safeFiles.length);
    await Future.wait(List.generate(workerCount, (_) => worker()));
    if (gate.cancelled) throw const PipelineCancelledException();
    if (firstFailure != null) throw firstFailure!;

    await gate.checkpoint();
    final directory = Directory(root);
    if (await directory.exists()) await _deleteDirectory(directory);
    onProgress(1, completed, safeFiles.length);
  }

  bool _isWithin(String root, String file) {
    final normalizedRoot = Platform.isWindows ? root.toLowerCase() : root;
    final normalizedFile = Platform.isWindows ? file.toLowerCase() : file;
    return p.isWithin(normalizedRoot, normalizedFile);
  }
}
