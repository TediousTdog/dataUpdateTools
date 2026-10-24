import 'dart:async';

import '../models/app_config.dart';
import 'pipeline_types.dart';

/// macOS 前端演示用的本地模拟流水线。
///
/// 演示模式故意不读取磁盘、不启动 ffmpeg/ossutil，也不连接 OSS；它只
/// 发送与真实 PipelineManager 相同的快照和日志，便于在没有 Windows 工具
/// 和测试凭证的 Mac 上完整演示页面交互。
class DemoPipelineManager {
  DemoPipelineManager();

  final _events = StreamController<PipelineSnapshot>.broadcast();
  PipelineSnapshot _snapshot = const PipelineSnapshot();
  Timer? _timer;
  bool _running = false;
  bool _paused = false;
  bool _cancelled = false;
  LogCallback? _log;

  Stream<PipelineSnapshot> get events => _events.stream;
  PipelineSnapshot get snapshot => _snapshot;
  bool get running => _running;
  bool get bulkUploadActive => false;
  String? get lastLogError => null;

  void _emit(PipelineSnapshot next) {
    _snapshot = next;
    if (!_events.isClosed) _events.add(next);
  }

  void pause() {
    if (!_running) return;
    _paused = true;
    _log?.call('演示任务已暂停');
    _emit(_snapshot.copyWith(paused: true));
  }

  void resume() {
    if (!_running) return;
    _paused = false;
    _log?.call('演示任务已继续');
    _emit(_snapshot.copyWith(paused: false));
  }

  Future<void> cancel() async {
    if (!_running) return;
    _cancelled = true;
    _timer?.cancel();
    _timer = null;
    _running = false;
    _log?.call('演示任务已取消');
    _emit(PipelineSnapshot(
      stage: PipelineStage.cancelled,
      overall: _snapshot.overall,
      fileProgress: _snapshot.fileProgress,
      currentFile: _snapshot.currentFile,
      completedFiles: _snapshot.completedFiles,
      totalFiles: _snapshot.totalFiles,
      message: '演示任务已取消',
      originStage: _snapshot.stage,
    ));
  }

  Future<void> run(AppConfig config, {required LogCallback log}) async {
    if (_running) throw StateError('演示任务正在运行');
    _running = true;
    _paused = false;
    _cancelled = false;
    _log = log;
    const files = <String>[
      'IMG_0001.JPG',
      'VID_0002.MOV',
      'IMG_0003.JPG',
      'VID_0004.MOV',
    ];
    log('演示模式：不会访问真实目录、OSS 或外部程序');
    log('已加载 ${config.transcodeEnabled ? '复制 → 转码 → 上传' : '复制 → 上传'} 示例任务');

    final stages = <PipelineStage>[
      PipelineStage.copying,
      if (config.transcodeEnabled) PipelineStage.transcoding,
      PipelineStage.uploading,
      if (config.deleteAfterUpload) PipelineStage.cleaning,
    ];
    final totalSteps = stages.length * files.length;
    var stageIndex = 0;
    var fileIndex = 0;
    var fileProgress = 0.0;
    _emit(
      PipelineSnapshot(
        stage: stages.first,
        totalFiles: files.length,
        message: '演示准备中',
      ),
    );

    final done = Completer<void>();
    _timer = Timer.periodic(const Duration(milliseconds: 120), (_) {
      if (_cancelled || done.isCompleted) return;
      if (_paused) return;
      fileProgress = (fileProgress + 0.08).clamp(0, 1).toDouble();
      final stage = stages[stageIndex];
      final name = files[fileIndex];
      final completed = stageIndex * files.length + fileIndex;
      final overall = ((completed + fileProgress) / totalSteps).clamp(0, 1).toDouble();
      final action = switch (stage) {
        PipelineStage.copying => '复制',
        PipelineStage.transcoding => '转码',
        PipelineStage.uploading => '上传',
        PipelineStage.cleaning => '清理',
        _ => '处理',
      };
      _emit(PipelineSnapshot(
        stage: stage,
        overall: overall,
        fileProgress: fileProgress,
        currentFile: name,
        completedFiles: completed,
        totalFiles: files.length,
        message: '$action ${fileIndex + 1}/${files.length}',
        activeTasks: [PipelineFileTask(name: name, progress: fileProgress, status: '${action}中')],
        activeTaskCount: 1,
      ));
      if (fileProgress < 1) return;
      log('[演示] $action 完成：$name');
      fileProgress = 0;
      fileIndex++;
      if (fileIndex < files.length) return;
      fileIndex = 0;
      stageIndex++;
      if (stageIndex < stages.length) {
        final nextName = switch (stages[stageIndex]) {
          PipelineStage.transcoding => '视频转码',
          PipelineStage.uploading => 'OSS 上传',
          PipelineStage.cleaning => '清理',
          _ => '下一阶段',
        };
        log('[演示] 开始$nextName');
        return;
      }
      _timer?.cancel();
      _timer = null;
      _running = false;
      log('演示任务完成：所有阶段均已模拟');
      _emit(PipelineSnapshot(
        stage: PipelineStage.completed,
        overall: 1,
        fileProgress: 1,
        completedFiles: files.length,
        totalFiles: files.length,
        message: '演示完成',
      ));
      done.complete();
    });
    await done.future;
  }

  Future<void> close() async {
    await cancel();
    await _events.close();
  }
}
