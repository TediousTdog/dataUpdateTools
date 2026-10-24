import 'dart:async';
import 'dart:io';
import 'package:path/path.dart' as p;
import '../models/app_config.dart';
import 'copy_service.dart';
import 'ffmpeg_service.dart';
import 'oss_service.dart';
import 'pipeline_types.dart';
import 'process_runner.dart';

class PipelineManager {
  PipelineManager({
    CopyService? copy,
    FFmpegService? ffmpeg,
    OssService? oss,
    ProcessRunner? runner,
  }) : _copy = copy ?? CopyService(),
       _runner = runner ?? ProcessRunner() {
    _ffmpeg = ffmpeg ?? FFmpegService(_runner);
    _oss = oss ?? OssService(_runner);
  }
  final CopyService _copy;
  final ProcessRunner _runner;
  late final FFmpegService _ffmpeg;
  late final OssService _oss;
  final _events = StreamController<PipelineSnapshot>.broadcast();
  Stream<PipelineSnapshot> get events => _events.stream;
  PauseGate? _gate;
  LogCallback? _activeLog;
  bool get running => _gate != null;
  PipelineSnapshot _snapshot = const PipelineSnapshot();
  PipelineSnapshot get snapshot => _snapshot;

  void pause() {
    final gate = _gate;
    if (gate == null || gate.paused) return;
    gate.pause();
    _activeLog?.call('收到暂停请求；当前文件完成后暂停派发');
    _emit(_snapshot.copyWith(paused: true));
  }

  void resume() {
    final gate = _gate;
    if (gate == null || !gate.paused) return;
    gate.resume();
    _activeLog?.call('任务已恢复，继续派发文件');
    _emit(_snapshot.copyWith(paused: false));
  }

  Future<void> cancel() async {
    if (_gate != null) _activeLog?.call('收到取消请求；正在终止活动进程');
    _gate?.cancel();
    await _runner.terminate();
  }

  /// 页面退出时释放广播流；调用方应先等待当前 run() 收尾。
  Future<void> close() async {
    if (_events.isClosed) return;
    await cancel();
    await _events.close();
  }

  void _emit(PipelineSnapshot value) {
    _snapshot = value;
    if (!_events.isClosed) _events.add(value);
  }

  Future<void> run(AppConfig config, {required LogCallback log}) async {
    if (running) throw StateError('任务正在运行');
    final gate = _gate = PauseGate();
    _activeLog = log;
    const copyWeight = 0.30;
    const transcodeWeight = 0.30;
    final uploadWeight = config.deleteAfterUpload ? 0.35 : 0.40;
    final cleanupWeight = config.deleteAfterUpload ? 0.05 : 0.0;
    const transcodeStart = copyWeight;
    const uploadStart = copyWeight + transcodeWeight;
    final cleanupStart = uploadStart + uploadWeight;
    final taskStates = <String, PipelineFileTask>{};
    void update(
      PipelineStage stage,
      double overall, {
      double? file = 0,
      String name = '',
      int done = 0,
      int total = 0,
      String message = '',
      List<PipelineFileTask>? tasks,
      PipelineStage? originStage,
    }) => _emit(
      PipelineSnapshot(
        stage: stage,
        overall: overall.clamp(0, 1).toDouble(),
        fileProgress: file?.clamp(0, 1).toDouble(),
        currentFile: name,
        completedFiles: done,
        totalFiles: total,
        message: message,
        activeTasks: tasks ?? taskStates.values.toList(growable: false),
        activeTaskCount: taskStates.values
            .where((task) => task.isActive)
            .length,
        paused: gate.paused,
        originStage: originStage,
      ),
    );
    final terminalTaskStatuses = {'已复制', '已转码', '已上传', '失败', '已中止'};
    void setTask(
      String key,
      String name,
      double? progress,
      String status, {
      bool emit = true,
    }) {
      taskStates[key] = PipelineFileTask(
        name: name,
        progress: progress?.clamp(0, 1).toDouble(),
        status: status,
      );
      while (taskStates.length > 12) {
        final finished = taskStates.entries.where(
          (e) => terminalTaskStatuses.contains(e.value.status),
        );
        if (finished.isEmpty) break;
        taskStates.remove(finished.first.key);
      }
      if (!emit) return;
      update(
        _snapshot.stage,
        _snapshot.overall,
        file: progress,
        name: name,
        done: _snapshot.completedFiles,
        total: _snapshot.totalFiles,
        message: _snapshot.message,
      );
    }

    void report(String line) => log(_redact(line, config));
    try {
      final source = p.normalize(p.absolute(config.sourcePath));
      final workspace = p.normalize(p.absolute(config.workspacePath));
      if (p.equals(source, workspace) ||
          p.isWithin(source, workspace) ||
          p.isWithin(workspace, source)) {
        throw ArgumentError('源目录与工作区不能相同或互相嵌套');
      }
      final runId = DateTime.now().toUtc().microsecondsSinceEpoch;
      final runWorkspace = p.join(config.workspacePath, 'run_$runId');
      report('本次临时工作区：$runWorkspace');
      update(PipelineStage.copying, 0, message: '扫描中，正在读取目录');
      var copyCount = 0;
      final sourceFileSizes = <String, int>{};
      final files = await _copy.copyTree(
        source: config.sourcePath,
        workspace: runWorkspace,
        concurrency: config.copyConcurrency,
        gate: gate,
        log: report,
        onTask: (name, value, status) => setTask(
          'copy:$name',
          name,
          status == '复制中' ? null : value,
          status,
          emit: status != '已复制',
        ),
        onFileScanned: (relativePath, size) =>
            sourceFileSizes[relativePath] = size,
        onScan: (discovered, complete, totalBytes) {
          if (complete) copyCount = discovered;
          update(
            PipelineStage.copying,
            0,
            total: complete ? discovered : 0,
            message: complete
                ? '已扫描 $discovered 个文件（${_formatBytes(totalBytes)}），准备复制'
                : '扫描中，已发现 $discovered 个文件（${_formatBytes(totalBytes)}）',
          );
        },
        progress: (value, detail, done, total) => update(
          PipelineStage.copying,
          value * copyWeight,
          file: null,
          name: detail.isEmpty ? _snapshot.currentFile : detail,
          done: done,
          total: total == 0 ? copyCount : total,
          message: '已复制 $done/$total 个文件',
        ),
      );
      final ext = config.videoExtensions.map((e) => e.toLowerCase()).toSet();
      final videos = files
          .where((f) => ext.contains(p.extension(f.path).toLowerCase()))
          .toList();
      update(
        PipelineStage.transcoding,
        transcodeStart,
        total: videos.length,
        message: videos.isEmpty ? '没有待转码视频' : '准备转码 ${videos.length} 个视频',
      );
      var nextVideo = 0, transcoded = 0;
      var stopTranscoding = false;
      Object? transcodeFailure;
      final videoProgress = List<double>.filled(videos.length, 0);
      final videoWeights = videos
          .map(
            (video) =>
                (sourceFileSizes[p.relative(video.path, from: runWorkspace)] ??
                        0)
                    .toDouble(),
          )
          .map((size) => size > 0 ? size : 1.0)
          .toList(growable: false);
      final totalVideoWeight = videoWeights.fold<double>(
        0,
        (sum, weight) => sum + weight,
      );
      var totalVideoProgress = 0.0;
      Future<void> transcodeWorker() async {
        while (true) {
          if (stopTranscoding) return;
          await gate.checkpoint();
          if (stopTranscoding) return;
          final index = nextVideo++;
          if (index >= videos.length) return;
          final video = videos[index];
          final name = p.relative(video.path, from: runWorkspace);
          final taskKey = 'transcode:${video.path}';
          setTask(taskKey, name, null, '转码中');
          report('开始转码：$name');
          try {
            await _ffmpeg.transcode(
              video,
              executable: _resolveTool(config.ffmpegPath),
              argumentTemplate: config.ffmpegArgs,
              outputExtension: config.outputExtension,
              log: (line) => report('[$name] $line'),
              progress: (value, detail) {
                final stableValue = value
                    .clamp(videoProgress[index], 1)
                    .toDouble();
                totalVideoProgress +=
                    (stableValue - videoProgress[index]) * videoWeights[index];
                videoProgress[index] = stableValue;
                setTask(taskKey, name, stableValue, '转码中', emit: false);
                final fraction = totalVideoWeight == 0
                    ? 1.0
                    : totalVideoProgress / totalVideoWeight;
                update(
                  PipelineStage.transcoding,
                  transcodeStart + fraction * transcodeWeight,
                  file: stableValue,
                  name: name,
                  done: transcoded,
                  total: videos.length,
                  message: detail,
                );
              },
            );
            if (gate.cancelled) {
              setTask(taskKey, name, videoProgress[index], '已中止');
              return;
            }
            if (stopTranscoding && transcodeFailure != null) {
              setTask(taskKey, name, videoProgress[index], '已中止');
              return;
            }
            totalVideoProgress +=
                (1 - videoProgress[index]) * videoWeights[index];
            videoProgress[index] = 1;
            transcoded++;
            setTask(taskKey, name, 1, '已转码', emit: false);
            update(
              PipelineStage.transcoding,
              transcodeStart +
                  (totalVideoWeight == 0
                          ? 1
                          : totalVideoProgress / totalVideoWeight) *
                      transcodeWeight,
              file: 1,
              name: name,
              done: transcoded,
              total: videos.length,
              message: '转码完成',
            );
          } catch (error) {
            setTask(
              taskKey,
              name,
              videoProgress[index],
              gate.cancelled || transcodeFailure != null ? '已中止' : '失败',
            );
            if (gate.cancelled) return;
            if (transcodeFailure == null) {
              transcodeFailure = error;
              stopTranscoding = true;
              gate.resume();
              await _runner.terminate();
            }
            return;
          }
        }
      }

      await Future.wait(
        List.generate(
          config.transcodeConcurrency.clamp(1, 8).toInt(),
          (_) => transcodeWorker(),
        ),
      );
      if (gate.cancelled) throw const PipelineCancelledException();
      if (transcodeFailure != null) throw transcodeFailure!;
      if (videos.isEmpty) {
        update(
          PipelineStage.transcoding,
          transcodeStart + transcodeWeight,
          total: 0,
          message: '没有待转码视频',
        );
      }

      update(PipelineStage.uploading, uploadStart, message: '枚举本次工作区文件');
      final workspaceFiles = await _listFiles(
        runWorkspace,
        gate: gate,
        onProgress: (discovered) => update(
          PipelineStage.uploading,
          uploadStart,
          message: '枚举中，已发现 $discovered 个文件',
        ),
      );
      final count = workspaceFiles.length;
      update(
        PipelineStage.uploading,
        uploadStart,
        total: count,
        message: '统计 $count 个文件大小',
      );
      final uploadFileSizes = await _measureFileSizes(
        workspaceFiles,
        gate,
        onProgress: (measured, total) => update(
          PipelineStage.uploading,
          uploadStart,
          total: count,
          done: measured,
          message: '统计文件大小 $measured/$count（${_formatBytes(total)}）',
        ),
      );
      final totalUploadWorkUnits = uploadFileSizes.fold<int>(
        0,
        (sum, size) => sum + (size > 0 ? size : 1),
      );
      update(
        PipelineStage.uploading,
        uploadStart + (count == 0 ? uploadWeight : 0),
        total: count,
        message: count == 0 ? '没有需要上传的文件' : 'OSS 上传',
      );
      var next = 0, uploaded = 0;
      var stopScheduling = false;
      Object? uploadFailure;
      final uploadProgress = List<double>.filled(count, 0);
      final effectiveUploadProgress = List<double>.filled(count, 0);
      var totalUploadProgress = 0.0;
      double uploadFraction() {
        if (count == 0) return 1;
        return (totalUploadProgress / totalUploadWorkUnits)
            .clamp(0, 1)
            .toDouble();
      }

      double progressWeight(int index) =>
          (uploadFileSizes[index] > 0 ? uploadFileSizes[index] : 1).toDouble();

      final retryLimit = config.uploadRetries.clamp(0, 5).toInt();
      Future<void> uploadWorker() async {
        while (true) {
          if (stopScheduling) return;
          await gate.checkpoint();
          if (stopScheduling) return;
          final index = next++;
          if (index >= count) return;
          final file = workspaceFiles[index];
          final name = p.relative(file.path, from: runWorkspace);
          Object? fileError;
          for (var attempt = 0; attempt <= retryLimit; attempt++) {
            if (attempt > 0) {
              if (stopScheduling) {
                setTask('upload:$index', name, uploadProgress[index], '已中止');
                return;
              }
              // 新尝试的单文件条从 0 重新显示；总体任务保持最高有效进度，避免回退。
              uploadProgress[index] = 0;
              setTask('upload:$index', name, null, '重试 $attempt/$retryLimit');
              report(
                '[$name] 上传失败：${_redact(fileError.toString(), config)}；准备第 $attempt/$retryLimit 次重试',
              );
              update(
                PipelineStage.uploading,
                uploadStart + uploadFraction() * uploadWeight,
                file: 0,
                name: name,
                done: uploaded,
                total: count,
                message: '重试 $attempt/$retryLimit',
              );
              final delayMs = (300 * (1 << (attempt - 1)))
                  .clamp(300, 5000)
                  .toInt();
              try {
                await _waitForRetry(
                  gate,
                  Duration(milliseconds: delayMs),
                  () => stopScheduling,
                );
              } on PipelineCancelledException {
                setTask('upload:$index', name, null, '已中止');
                return;
              }
              if (gate.cancelled) {
                setTask('upload:$index', name, null, '已中止');
                return;
              }
              if (stopScheduling) {
                setTask('upload:$index', name, null, '已中止');
                return;
              }
              setTask('upload:$index', name, null, '重试上传中');
            } else {
              setTask('upload:$index', name, null, '上传中');
            }
            try {
              await _oss.upload(
                file,
                config: config.copyWith(
                  ossutilPath: _resolveTool(config.ossutilPath),
                ),
                workspace: runWorkspace,
                log: (line) => report('[$name] $line'),
                progress: (value, detail) {
                  final stableValue = value
                      .clamp(uploadProgress[index], 1)
                      .toDouble();
                  final effectiveValue =
                      stableValue > effectiveUploadProgress[index]
                      ? stableValue
                      : effectiveUploadProgress[index];
                  totalUploadProgress +=
                      (effectiveValue - effectiveUploadProgress[index]) *
                      progressWeight(index);
                  uploadProgress[index] = stableValue;
                  effectiveUploadProgress[index] = effectiveValue;
                  final totalProgress = uploadFraction();
                  setTask(
                    'upload:$index',
                    name,
                    stableValue,
                    '上传中',
                    emit: false,
                  );
                  update(
                    PipelineStage.uploading,
                    uploadStart + totalProgress * uploadWeight,
                    file: stableValue,
                    name: name,
                    done: uploaded,
                    total: count,
                    message: _redact(detail, config),
                  );
                },
              );
              if (gate.cancelled || uploadFailure != null) {
                setTask('upload:$index', name, uploadProgress[index], '已中止');
                return;
              }
              if (stopScheduling && uploadFailure != null) {
                setTask('upload:$index', name, uploadProgress[index], '已中止');
                return;
              }
              fileError = null;
              break;
            } catch (error) {
              fileError = error;
              if (gate.cancelled) {
                setTask('upload:$index', name, uploadProgress[index], '已中止');
                return;
              }
              if (attempt >= retryLimit || stopScheduling) break;
            }
          }
          if (fileError != null) {
            setTask(
              'upload:$index',
              name,
              uploadProgress[index],
              gate.cancelled || uploadFailure != null ? '已中止' : '失败',
            );
            if (gate.cancelled) return;
            if (uploadFailure == null) {
              uploadFailure = fileError;
              stopScheduling = true;
              gate.resume();
              await _runner.terminate();
            }
            return;
          }
          if (fileError == null) {
            totalUploadProgress +=
                (1 - effectiveUploadProgress[index]) * progressWeight(index);
            uploadProgress[index] = 1;
            effectiveUploadProgress[index] = 1;
            uploaded++;
            setTask('upload:$index', name, 1, '已上传', emit: false);
            update(
              PipelineStage.uploading,
              uploadStart + uploadFraction() * uploadWeight,
              file: 1,
              name: name,
              done: uploaded,
              total: count,
              message: '上传完成',
            );
          }
        }
      }

      await Future.wait(
        List.generate(
          config.uploadConcurrency.clamp(1, 32).toInt(),
          (_) => uploadWorker(),
        ),
      );
      if (gate.cancelled) throw const PipelineCancelledException();
      if (uploadFailure != null) throw uploadFailure!;
      if (config.deleteAfterUpload) {
        await gate.checkpoint();
        update(PipelineStage.cleaning, cleanupStart, message: '清理工作区');
        await Directory(runWorkspace).delete(recursive: true);
        update(
          PipelineStage.cleaning,
          cleanupStart + cleanupWeight,
          message: '清理完成',
        );
      }
      update(PipelineStage.completed, 1, message: '任务完成');
      report('任务完成：已上传 $uploaded 个文件');
    } on PipelineCancelledException {
      update(
        PipelineStage.cancelled,
        _snapshot.overall,
        message: '已取消',
        originStage: _snapshot.stage,
      );
      report('任务已取消');
    } catch (error, stack) {
      if (gate.cancelled) {
        update(
          PipelineStage.cancelled,
          _snapshot.overall,
          message: '已取消',
          originStage: _snapshot.stage,
        );
        report('任务已取消');
        return;
      }
      update(
        PipelineStage.failed,
        _snapshot.overall,
        message: error.toString(),
        originStage: _snapshot.stage,
      );
      report('任务失败：$error');
      report(stack.toString());
      rethrow;
    } finally {
      _gate = null;
      _activeLog = null;
    }
  }

  Future<List<File>> _listFiles(
    String rootPath, {
    required PauseGate gate,
    void Function(int discovered)? onProgress,
  }) async {
    final result = <File>[];
    final root = Directory(rootPath);
    if (!await root.exists()) return result;
    await for (final entity in root.list(recursive: true, followLinks: false)) {
      await gate.checkpoint();
      if (entity is File) {
        result.add(entity);
        if (result.length % 100 == 0) onProgress?.call(result.length);
      }
    }
    return result;
  }

  Future<List<int>> _measureFileSizes(
    List<File> files,
    PauseGate gate, {
    void Function(int measured, int totalBytes)? onProgress,
  }) async {
    final sizes = List<int>.filled(files.length, 0);
    var next = 0;
    var measured = 0;
    var totalBytes = 0;
    Future<void> worker() async {
      while (true) {
        await gate.checkpoint();
        final index = next++;
        if (index >= files.length) return;
        sizes[index] = await files[index].length();
        measured++;
        totalBytes += sizes[index];
        if (measured % 100 == 0 || measured == files.length) {
          onProgress?.call(measured, totalBytes);
        }
      }
    }

    final concurrency = files.length.clamp(1, 32).toInt();
    await Future.wait(List.generate(concurrency, (_) => worker()));
    return sizes;
  }

  String _redact(String value, AppConfig config) {
    var safe = value;
    for (final secret in [config.accessKeySecret, config.securityToken]) {
      if (secret.isNotEmpty) safe = safe.replaceAll(secret, '***');
    }
    return safe;
  }

  Future<void> _waitForRetry(
    PauseGate gate,
    Duration duration,
    bool Function() shouldStop,
  ) async {
    var remaining = duration.inMilliseconds;
    while (remaining > 0) {
      await gate.checkpoint();
      if (shouldStop()) return;
      final interval = remaining < 100 ? remaining : 100;
      await Future<void>.delayed(Duration(milliseconds: interval));
      remaining -= interval;
    }
    await gate.checkpoint();
  }

  String _resolveTool(String path) => p.isAbsolute(path)
      ? path
      : p.join(p.dirname(Platform.resolvedExecutable), path);

  String _formatBytes(int bytes) {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var value = bytes.toDouble();
    var unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit++;
    }
    return '${value.toStringAsFixed(unit == 0 ? 0 : 1)} ${units[unit]}';
  }
}
