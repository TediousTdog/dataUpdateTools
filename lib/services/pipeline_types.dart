import 'dart:async';

enum PipelineStage {
  idle,
  copying,
  transcoding,
  uploading,
  cleaning,
  completed,
  failed,
  cancelled,
}

class PipelineSnapshot {
  const PipelineSnapshot({
    this.stage = PipelineStage.idle,
    this.overall = 0,
    this.fileProgress = 0,
    this.currentFile = '',
    this.completedFiles = 0,
    this.totalFiles = 0,
    this.message = '',
    this.activeTasks = const [],
    this.activeTaskCount = 0,
    this.paused = false,
    this.originStage,
  });
  final PipelineStage stage;
  final double overall;
  final double? fileProgress;
  final String currentFile, message;
  final int completedFiles, totalFiles;
  final List<PipelineFileTask> activeTasks;
  final int activeTaskCount;
  final bool paused;
  final PipelineStage? originStage;

  PipelineSnapshot copyWith({bool? paused}) => PipelineSnapshot(
    stage: stage,
    overall: overall,
    fileProgress: fileProgress,
    currentFile: currentFile,
    completedFiles: completedFiles,
    totalFiles: totalFiles,
    message: message,
    activeTasks: activeTasks,
    activeTaskCount: activeTaskCount,
    paused: paused ?? this.paused,
    originStage: originStage,
  );
}

class PipelineFileTask {
  const PipelineFileTask({
    required this.name,
    required this.progress,
    required this.status,
  });
  final String name;

  /// null 表示外部工具尚未提供可解析的百分比，应展示不确定进度条。
  final double? progress;
  final String status;

  bool get isActive => const {'复制中', '转码中', '上传中', '重试上传中'}.contains(status);
}

typedef LogCallback = void Function(String message);
typedef ProgressCallback = void Function(double progress, String detail);
typedef CopyProgressCallback =
    void Function(
      double progress,
      String detail,
      int completedFiles,
      int totalFiles,
    );

/// 协作式暂停门：暂停时不启动下一个文件；运行中的文件由子服务完成后再暂停。
class PauseGate {
  bool _paused = false;
  bool _cancelled = false;
  Completer<void>? _resume;
  bool get cancelled => _cancelled;
  bool get paused => _paused;
  void pause() {
    _paused = true;
  }

  void resume() {
    _paused = false;
    _resume?.complete();
    _resume = null;
  }

  void cancel() {
    _cancelled = true;
    resume();
  }

  Future<void> checkpoint() async {
    if (_cancelled) throw const PipelineCancelledException();
    if (!_paused) return;
    _resume ??= Completer<void>();
    await _resume!.future;
    if (_cancelled) throw const PipelineCancelledException();
  }
}

class PipelineCancelledException implements Exception {
  const PipelineCancelledException();
}
