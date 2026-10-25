import 'dart:async';
import 'dart:io';
import 'package:path/path.dart' as p;
import '../models/app_config.dart';
import 'copy_service.dart';
import 'cleanup_service.dart';
import 'ffmpeg_service.dart';
import 'oss_service.dart';
import 'pipeline_types.dart';
import 'process_runner.dart';

class PipelineManager {
  PipelineManager({
    CopyService? copy,
    FFmpegService? ffmpeg,
    OssService? oss,
    CleanupService? cleanup,
    ProcessRunner? runner,
  }) : _copy = copy ?? CopyService(),
       _runner = runner ?? ProcessRunner(),
       _useBulkUpload = oss == null {
    _ffmpeg = ffmpeg ?? FFmpegService(_runner);
    _oss = oss ?? OssService(_runner);
    _cleanup = cleanup ?? CleanupService();
  }
  final CopyService _copy;
  final ProcessRunner _runner;
  // 默认 OssService 可以使用一次进程内的 --jobs/--parallel 批量上传。
  // 注入的测试/自定义服务保持逐文件回调语义，避免替换服务时丢失控制能力。
  final bool _useBulkUpload;
  late final FFmpegService _ffmpeg;
  late final OssService _oss;
  late final CleanupService _cleanup;
  final _events = StreamController<PipelineSnapshot>.broadcast();
  Stream<PipelineSnapshot> get events => _events.stream;
  PauseGate? _gate;
  LogCallback? _activeLog;
  Completer<void>? _runCompletion;
  Future<void>? _closeFuture;
  AppConfig? _activeConfig;
  bool _bulkUploadActive = false;
  bool get bulkUploadActive => _bulkUploadActive;
  bool get running => _gate != null;
  String? _lastLogError;
  String? get lastLogError => _lastLogError;
  PipelineSnapshot _snapshot = const PipelineSnapshot();
  PipelineSnapshot get snapshot => _snapshot;

  void pause() {
    final gate = _gate;
    if (gate == null || gate.paused) return;
    if (_bulkUploadActive) {
      _writeLog(_activeLog, '当前 ossutil 批量上传不支持运行中暂停；可取消任务后切换为逐文件模式');
      return;
    }
    gate.pause();
    _writeLog(_activeLog, '收到暂停请求；当前文件完成后暂停派发');
    _emit(_snapshot.copyWith(paused: true));
  }

  void resume() {
    final gate = _gate;
    if (gate == null || !gate.paused) return;
    gate.resume();
    _writeLog(_activeLog, '任务已恢复，继续派发文件');
    _emit(_snapshot.copyWith(paused: false));
  }

  Future<void> cancel() async {
    if (_gate != null) {
      _writeLog(_activeLog, '收到取消请求；正在终止活动进程');
    }
    _gate?.cancel();
    await _runner.terminate();
  }

  /// 取消活动任务并等待 worker 收尾后关闭事件流；可与 run() 并发调用。
  Future<void> close() {
    final existing = _closeFuture;
    if (existing != null) return existing;
    final operation = _closeNow();
    _closeFuture = operation;
    return operation;
  }

  Future<void> _closeNow() async {
    if (_events.isClosed) return;
    await cancel();
    final completion = _runCompletion;
    if (completion != null) await completion.future;
    await _events.close();
  }

  void _emit(PipelineSnapshot value) {
    _snapshot = value;
    if (!_events.isClosed) _events.add(value);
  }

  Future<void> run(AppConfig config, {required LogCallback log}) async {
    if (_closeFuture != null || _events.isClosed) {
      throw StateError('PipelineManager 已关闭');
    }
    if (running) throw StateError('任务正在运行');
    final gate = _gate = PauseGate();
    final completion = _runCompletion = Completer<void>();
    _activeLog = log;
    _activeConfig = config;
    _lastLogError = null;
    final runTimer = Stopwatch()..start();
    const copyWeight = 0.30;
    const transcodeWeight = 0.30;
    // 关闭转码时直接从源目录上传，不创建工作区；源盘内容绝不能被清理。
    final directUpload = !config.transcodeEnabled;
    final cleanupEnabled = config.deleteAfterUpload && !directUpload;
    final uploadWeight = cleanupEnabled ? 0.35 : 0.40;
    final cleanupWeight = cleanupEnabled ? 0.05 : 0.0;
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
    final terminalTaskStatuses = {'已复制', '已转码', '已上传', '已清理', '失败', '已中止'};
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

    void report(String line) => _writeLog(log, _redact(line, config));
    try {
      // 授权码在流水线入口统一解析，后续各阶段只使用同一份有效配置。
      // 解析失败会进入统一 failed 状态，且不会启动任何外部进程。
      config = config.resolveAuthorization();
      _activeConfig = config;
      // 所有路径在一次 run 开始时固定为绝对规范路径，避免工作目录变化或
      // 相对路径导致 run 目录、相对对象 key 和清理目标不一致。
      final source = p.normalize(p.absolute(config.sourcePath));
      final workspace = p.normalize(p.absolute(config.workspacePath));
      if (!directUpload && _sameOrNestedPath(source, workspace)) {
        throw ArgumentError('源目录与工作区不能相同或互相嵌套');
      }
      final runId = DateTime.now().toUtc().microsecondsSinceEpoch;
      final runWorkspace = directUpload
          ? source
          : p.join(workspace, 'run_$runId');
      report(
        '任务配置：源=$source，工作区=$workspace，目标=oss://${config.bucket}/${config.prefix}',
      );
      report(
        '并行参数：复制=${config.copyConcurrency}，转码=${config.transcodeConcurrency}，'
        '上传 jobs=${config.uploadConcurrency}，分片 parallel=${config.multipartParallel}，'
        '模式=${config.bulkUpload ? '批量' : '逐文件'}，'
        '数据来源=${directUpload ? '源目录直传（跳过复制）' : '本地工作区'}，'
        '清理=${cleanupEnabled ? '开启' : '关闭'}',
      );
      report(directUpload ? '直传源目录：$source' : '本次临时工作区：$runWorkspace');
      var copyCount = 0;
      final sourceFileSizes = <String, int>{};
      final files = directUpload
          ? await _listFiles(
              source,
              gate: gate,
              onProgress: (discovered) => update(
                PipelineStage.copying,
                copyWeight,
                message: '直传模式扫描中，已发现 $discovered 个文件',
              ),
            )
          : await _copy.copyTree(
              source: source,
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
      if (directUpload) {
        copyCount = files.length;
        update(
          PipelineStage.copying,
          copyWeight,
          total: copyCount,
          done: copyCount,
          message: '已关闭转码，跳过本地复制，准备直接上传',
        );
      }
      final ext = config.videoExtensions.map((e) => e.toLowerCase()).toSet();
      // 转码是可选步骤。关闭时保留原始视频，后续仍会随工作区文件一起上传。
      final videos = config.transcodeEnabled
          ? files
                .where((f) => ext.contains(p.extension(f.path).toLowerCase()))
                .toList()
          : <File>[];
      final copiedFileIndex = {
        for (var index = 0; index < files.length; index++)
          files[index].path: index,
      };
      final videoFileIndexes = videos
          .map((video) => copiedFileIndex[video.path]!)
          .toList(growable: false);
      taskStates.clear();
      update(
        PipelineStage.transcoding,
        transcodeStart,
        total: videos.length,
        message: !config.transcodeEnabled
            ? '已跳过视频转码'
            : videos.isEmpty
            ? '没有待转码视频'
            : '准备转码 ${videos.length} 个视频',
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
            final output = await _ffmpeg.transcode(
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
            // 转码服务会删除原视频并返回新文件；更新扫描清单，后续直接复用
            // 这份清单上传和清理，避免再对超大工作区做一次完整目录遍历。
            files[videoFileIndexes[index]] = output;
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
      if (!config.transcodeEnabled) {
        update(
          PipelineStage.transcoding,
          transcodeStart + transcodeWeight,
          total: 0,
          message: '视频转码已关闭，保留原始视频',
        );
      } else if (videos.isEmpty) {
        update(
          PipelineStage.transcoding,
          transcodeStart + transcodeWeight,
          total: 0,
          message: '没有待转码视频',
        );
      }

      taskStates.clear();
      final preferBulkUpload = _useBulkUpload && config.bulkUpload;
      final workspaceFiles = preferBulkUpload
          ? files
          : await _listFiles(
              runWorkspace,
              gate: gate,
              onProgress: (discovered) => update(
                PipelineStage.uploading,
                uploadStart,
                message: '枚举中，已发现 $discovered 个文件',
              ),
            );
      final count = workspaceFiles.length;
      final useBulkUpload = preferBulkUpload && count > 0;
      update(
        PipelineStage.uploading,
        uploadStart,
        total: count,
        message: useBulkUpload
            ? '批量上传准备中（jobs=${config.uploadConcurrency}，分片并行=${config.multipartParallel}）'
            : '统计 $count 个文件大小',
      );
      // 批量模式由 ossutil 自己扫描目录，不再对每个文件额外调用 length()；
      // 逐文件模式仍统计字节数，以准确加权 UI 总体进度。
      final uploadFileSizes = useBulkUpload
          ? List<int>.filled(count, 0)
          : await _measureFileSizes(
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
        file: null,
        message: count == 0
            ? '没有需要上传的文件'
            : useBulkUpload
            ? '批量上传中：jobs=${config.uploadConcurrency} × 分片并行=${config.multipartParallel}'
            : 'OSS 上传',
      );
      var uploaded = 0;
      if (useBulkUpload) {
        final taskKey = 'upload:batch';
        setTask(taskKey, '批量 OSS 上传（$count 个文件）', null, '上传中');
        report(
          '开始批量上传：$count 个文件，jobs=${config.uploadConcurrency}，parallel=${config.multipartParallel}',
        );
        var batchProgress = 0.0;
        Object? batchFailure;
        final retryLimit = config.uploadRetries.clamp(0, 5).toInt();
        for (var attempt = 0; attempt <= retryLimit; attempt++) {
          if (gate.cancelled) throw const PipelineCancelledException();
          try {
            _bulkUploadActive = true;
            update(
              PipelineStage.uploading,
              uploadStart + batchProgress * uploadWeight,
              file: null,
              name: '批量 OSS 上传',
              total: count,
              message:
                  '批量上传中：jobs=${config.uploadConcurrency} × 分片并行=${config.multipartParallel}',
            );
            late final int code;
            try {
              code = await _oss.uploadTree(
                workspace: runWorkspace,
                config: config.copyWith(
                  ossutilPath: _resolveTool(config.ossutilPath),
                ),
                log: (line) => report('[批量上传] $line'),
                progress: (value, detail) {
                  final observed = value.clamp(0, 1).toDouble();
                  if (observed > batchProgress) batchProgress = observed;
                  setTask(
                    taskKey,
                    '批量 OSS 上传（$count 个文件）',
                    observed,
                    '上传中',
                    emit: false,
                  );
                  update(
                    PipelineStage.uploading,
                    uploadStart + batchProgress * uploadWeight,
                    file: observed,
                    name: '批量 OSS 上传',
                    done: 0,
                    total: count,
                    message: '批量上传中：${_redact(detail, config)}',
                  );
                },
              );
            } finally {
              _bulkUploadActive = false;
            }
            if (code != 0) {
              throw StateError('ossutil 批量上传失败，退出码 $code');
            }
            batchFailure = null;
            break;
          } catch (error) {
            if (gate.cancelled) {
              setTask(taskKey, '批量 OSS 上传（$count 个文件）', null, '已中止');
              throw const PipelineCancelledException();
            }
            batchFailure = error;
            if (attempt >= retryLimit) break;
            final retry = attempt + 1;
            setTask(
              taskKey,
              '批量 OSS 上传（$count 个文件）',
              null,
              '重试 $retry/$retryLimit',
            );
            report(
              '批量上传失败：${_redact(error.toString(), config)}；准备第 $retry/$retryLimit 次重试',
            );
            update(
              PipelineStage.uploading,
              uploadStart + batchProgress * uploadWeight,
              file: null,
              name: '批量 OSS 上传',
              done: 0,
              total: count,
              message: '重试 $retry/$retryLimit',
            );
            final delayMs = (300 * (1 << attempt)).clamp(300, 5000).toInt();
            await _waitForRetry(
              gate,
              Duration(milliseconds: delayMs),
              () => false,
            );
          }
        }
        if (batchFailure != null) {
          setTask(taskKey, '批量 OSS 上传（$count 个文件）', null, '失败');
          throw batchFailure;
        }
        uploaded = count;
        setTask(taskKey, '批量 OSS 上传（$count 个文件）', 1, '已上传', emit: false);
        update(
          PipelineStage.uploading,
          uploadStart + uploadWeight,
          file: 1,
          name: '批量 OSS 上传',
          done: uploaded,
          total: count,
          message: '上传完成',
        );
      } else {
        var next = 0;
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
            (uploadFileSizes[index] > 0 ? uploadFileSizes[index] : 1)
                .toDouble();

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
      }
      if (cleanupEnabled) {
        await gate.checkpoint();
        taskStates.clear();
        update(PipelineStage.cleaning, cleanupStart, message: '清理工作区');
        final cleanupFiles = workspaceFiles;
        update(
          PipelineStage.cleaning,
          cleanupStart,
          total: cleanupFiles.length,
          message: '准备清理 ${cleanupFiles.length} 个文件',
        );
        await _cleanup.cleanRun(
          rootPath: runWorkspace,
          files: cleanupFiles,
          concurrency: config.cleanupConcurrency,
          gate: gate,
          onTask: (file, progress, status) => setTask(
            'cleanup:${file.path}',
            p.relative(file.path, from: runWorkspace),
            status == '清理中' ? null : progress,
            status,
          ),
          onProgress: (progress, done, total) => update(
            PipelineStage.cleaning,
            cleanupStart + progress * cleanupWeight,
            // 文件删除是原子操作，没有可信的单文件百分比；总体阶段进度
            // 则由 CleanupService 的完成文件数实时推进。
            file: null,
            name: _snapshot.currentFile,
            done: done,
            total: total,
            message: '已清理 $done/$total 个文件',
          ),
        );
        update(
          PipelineStage.cleaning,
          cleanupStart + cleanupWeight,
          message: '清理完成',
        );
      }
      update(PipelineStage.completed, 1, message: '任务完成');
      report('任务完成：已上传 $uploaded 个文件，耗时 ${_formatDuration(runTimer.elapsed)}');
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
      final safeError = _redact(error.toString(), config);
      update(
        PipelineStage.failed,
        _snapshot.overall,
        message: safeError,
        originStage: _snapshot.stage,
      );
      report('任务失败（耗时 ${_formatDuration(runTimer.elapsed)}）：$safeError');
      report(_redact(stack.toString(), config));
      rethrow;
    } finally {
      _gate = null;
      _bulkUploadActive = false;
      _activeLog = null;
      _activeConfig = null;
      if (!completion.isCompleted) completion.complete();
      if (identical(_runCompletion, completion)) _runCompletion = null;
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
    final secrets = [
      config.accessKeyId,
      config.accessKeySecret,
      config.securityToken,
      config.authorizationCode,
    ]..sort((left, right) => right.length.compareTo(left.length));
    for (final secret in secrets) {
      if (secret.isNotEmpty) safe = safe.replaceAll(secret, '***');
    }
    return safe;
  }

  void _writeLog(LogCallback? callback, String message) {
    if (callback == null) return;
    try {
      callback(message);
    } catch (error) {
      // 日志 sink 故障不能阻止 pause/resume/cancel，也不能把成功的文件
      // 传输误判成失败；把同步回调故障单独暴露给 UI 诊断区。
      _lastLogError ??= _redact(
        error.toString(),
        _activeConfig ?? const AppConfig(),
      );
    }
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

  String _resolveTool(String path) {
    if (p.isAbsolute(path)) return p.normalize(path);
    // 发布包优先从 exe 同级目录查找；开发模式下再回退到当前工作目录，
    // 这样把 tools/ 放在项目根目录即可直接 `flutter run -d windows` 调试。
    final besideExecutable = p.normalize(
      p.join(p.dirname(Platform.resolvedExecutable), path),
    );
    if (File(besideExecutable).existsSync()) return besideExecutable;
    return p.normalize(p.absolute(path));
  }

  bool _sameOrNestedPath(String first, String second) {
    // Windows 驱动器路径不区分大小写；path 包本身按字符串比较，若只用
    // p.equals/isWithin，`D:\Media` 与 `d:\media\run` 可能被误判为安全。
    final left = Platform.isWindows ? first.toLowerCase() : first;
    final right = Platform.isWindows ? second.toLowerCase() : second;
    return left == right || p.isWithin(left, right) || p.isWithin(right, left);
  }

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

  String _formatDuration(Duration duration) {
    final seconds = duration.inSeconds;
    if (seconds < 60) return '${seconds}s';
    final minutes = seconds ~/ 60;
    final remainingSeconds = seconds % 60;
    if (minutes < 60) return '${minutes}m ${remainingSeconds}s';
    final hours = minutes ~/ 60;
    return '${hours}h ${minutes % 60}m ${remainingSeconds}s';
  }
}
