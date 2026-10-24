import 'dart:async';
import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:untitled/models/app_config.dart';
import 'package:untitled/services/copy_service.dart';
import 'package:untitled/services/cleanup_service.dart';
import 'package:untitled/services/config_repository.dart';
import 'package:untitled/services/ffmpeg_service.dart';
import 'package:untitled/services/log_store.dart';
import 'package:untitled/services/log_severity.dart';
import 'package:untitled/services/oss_service.dart';
import 'package:untitled/services/pipeline_manager.dart';
import 'package:untitled/services/pipeline_types.dart';
import 'package:untitled/services/process_runner.dart';

Future<void> main() async {
  final root = await Directory.systemTemp.createTemp('pipeline_smoke_');
  try {
    await _verifyConfigPersistence(root);
    await _verifyLogPersistence(root);
    await _verifyIncrementalScanning(root);
    await _verifyZeroLengthFileWorkUnit(root);
    await _verifyCopyPoolPauseAndLimit(root);
    await _verifyCopyFailureStopsScheduling(root);
    _verifyFfmpegProgressParser();
    _verifyFfmpegArgumentTemplate();
    _verifyLogSeverityClassification();
    await _verifyDistinctTranscodeOutputs(root);
    await _verifyProcessRunner();
    await _verifyParallelPauseResume(root);
    await _verifyTranscodeFailureStopsScheduling(root);
    await _verifyTranscodeProgressIsByteWeighted(root);
    await _verifyUnknownUploadProgress(root);
    await _verifyUploadProgressIsByteWeighted(root);
    await _verifyUploadPreparationProgress(root);
    await _verifyUploadRetry(root);
    await _verifyRetryDoesNotRegressOverallProgress(root);
    await _verifyCancellationStopsDispatch(root);
    await _verifyCancellationMarksActiveTasksAborted(root);
    await _verifyUploadFailureStopsScheduling(root);
    await _verifyEmptyPipelineProgress(root);
    await _verifyCleanupPoolPauseAndLimit(root);
    await _verifyCleanupCancellationDrainsCurrentFiles(root);
    await _verifyCleanupIsScopedToCurrentRun(root);
    stdout.writeln('Pipeline smoke checks passed.');
  } finally {
    await root.delete(recursive: true);
  }
}

Future<void> _verifyConfigPersistence(Directory root) async {
  final repository = ConfigRepository(
    rootDirectory: Directory(p.join(root.path, 'config')),
  );
  const expected = AppConfig(
    sourcePath: r'F:\Camera Card',
    workspacePath: r'D:\Media Work',
    bucket: 'archive-bucket',
    prefix: 'ingest/2026',
    endpoint: 'https://oss-cn-hangzhou.aliyuncs.com',
    accessKeyId: 'test-id',
    accessKeySecret: 'test-secret',
    securityToken: 'test-token',
    videoExtensions: ['.mov', '.avi', '.mxf'],
    uploadConcurrency: 7,
    transcodeConcurrency: 3,
    copyConcurrency: 5,
    cleanupConcurrency: 3,
    uploadRetries: 4,
    deleteAfterUpload: true,
  );
  await repository.save(expected);
  final loaded = await repository.load();
  _check(
    loaded.toPrettyJson() == expected.toPrettyJson(),
    'config JSON round-trip did not preserve user settings',
  );
  await Future.wait([
    repository.save(expected.copyWith(prefix: 'parallel/one')),
    repository.save(expected.copyWith(prefix: 'parallel/two')),
  ]);
  final afterConcurrentSave = await repository.load();
  _check(
    afterConcurrentSave.prefix == 'parallel/two',
    'concurrent config saves were not serialized in call order',
  );
  final leftovers = await repository.file.then(
    (target) => target.parent
        .list()
        .where((entity) => entity.path.contains('.tmp.'))
        .toList(),
  );
  _check(leftovers.isEmpty, 'config save left a temporary file behind');
}

Future<void> _verifyIncrementalScanning(Directory root) async {
  final source = Directory(p.join(root.path, 'scan-source'))..createSync();
  final workspace = p.join(root.path, 'scan-workspace');
  final fileSizes = <String, int>{};
  for (var index = 0; index < 205; index++) {
    final size = index + 1;
    final name = 'file_$index.jpg';
    fileSizes[name] = size;
    await File(p.join(source.path, name)).writeAsString('x' * size);
  }
  final totalBytes = fileSizes.values.fold<int>(0, (sum, value) => sum + value);

  final scanUpdates = <(int, bool, int)>[];
  final copiedProgress = <(String, double, int, int)>[];
  final copied = await CopyService().copyTree(
    source: source.path,
    workspace: workspace,
    concurrency: 4,
    gate: PauseGate(),
    log: (_) {},
    progress: (value, name, done, total) =>
        copiedProgress.add((name, value, done, total)),
    onTask: (_, _, _) {},
    onFileScanned: (_, _) {},
    onScan: (count, complete, totalBytes) =>
        scanUpdates.add((count, complete, totalBytes)),
  );
  _check(copied.length == 205, 'scan/copy missed source files');
  _check(
    scanUpdates.length == 3 &&
        scanUpdates[0].$1 == 100 &&
        !scanUpdates[0].$2 &&
        scanUpdates[0].$3 > 0 &&
        scanUpdates[1].$1 == 200 &&
        !scanUpdates[1].$2 &&
        scanUpdates[1].$3 > scanUpdates[0].$3 &&
        scanUpdates[2] == (205, true, totalBytes),
    'incremental scan progress was not reported as expected',
  );
  var accumulatedBytes = 0;
  _check(copiedProgress.length == 205, 'copy progress missed file updates');
  for (var index = 0; index < copiedProgress.length; index++) {
    final (name, value, done, total) = copiedProgress[index];
    accumulatedBytes += fileSizes[name]!;
    _check(
      (value - accumulatedBytes / totalBytes).abs() < 0.000001 &&
          done == index + 1 &&
          total == 205,
      'copy progress was not weighted by bytes while preserving file counts',
    );
  }

  final cancelSource = Directory(p.join(root.path, 'scan-cancel-source'))
    ..createSync();
  for (var index = 0; index < 205; index++) {
    await File(p.join(cancelSource.path, 'file_$index.jpg')).writeAsString('x');
  }
  final gate = PauseGate();
  var cancelled = false;
  try {
    await CopyService().copyTree(
      source: cancelSource.path,
      workspace: p.join(root.path, 'scan-cancel-workspace'),
      concurrency: 2,
      gate: gate,
      log: (_) {},
      progress: (_, _, _, _) {},
      onTask: (_, _, _) {},
      onFileScanned: (_, _) {},
      onScan: (count, complete, _) {
        if (count == 100 && !complete) gate.cancel();
      },
    );
  } on PipelineCancelledException {
    cancelled = true;
  }
  _check(cancelled, 'cancellation was ignored during directory scanning');
}

Future<void> _verifyCopyPoolPauseAndLimit(Directory root) async {
  final source = Directory(p.join(root.path, 'copy-pool-source'))..createSync();
  for (var index = 0; index < 12; index++) {
    await File(p.join(source.path, 'file_$index.bin')).writeAsString('payload');
  }
  final releaseInitialBatch = Completer<void>();
  final initialBatchStarted = Completer<void>();
  final initialBatchCopied = Completer<void>();
  final gate = PauseGate();
  var copyCalls = 0;
  var activeCopies = 0;
  var maxActiveCopies = 0;
  final service = CopyService(
    copyFile: (sourceFile, targetPath) async {
      copyCalls++;
      activeCopies++;
      if (activeCopies > maxActiveCopies) maxActiveCopies = activeCopies;
      if (copyCalls == 3) initialBatchStarted.complete();
      try {
        if (copyCalls <= 3) await releaseInitialBatch.future;
        return await sourceFile.copy(targetPath);
      } finally {
        activeCopies--;
      }
    },
  );

  final run = service.copyTree(
    source: source.path,
    workspace: p.join(root.path, 'copy-pool-workspace'),
    concurrency: 3,
    gate: gate,
    log: (_) {},
    progress: (_, _, done, _) {
      if (done == 3 && !initialBatchCopied.isCompleted) {
        initialBatchCopied.complete();
      }
    },
    onTask: (_, _, _) {},
    onFileScanned: (_, _) {},
    onScan: (_, _, _) {},
  );

  try {
    await initialBatchStarted.future.timeout(const Duration(seconds: 5));
    gate.pause();
    releaseInitialBatch.complete();
    await initialBatchCopied.future.timeout(const Duration(seconds: 5));
    _check(copyCalls == 3, 'copy pool dispatched work while paused');
  } finally {
    if (!releaseInitialBatch.isCompleted) releaseInitialBatch.complete();
    gate.resume();
  }
  final copied = await run.timeout(const Duration(seconds: 5));
  _check(copied.length == 12, 'copy pool did not finish all files');
  _check(maxActiveCopies == 3, 'copy pool did not respect concurrency 3');
  _check(copyCalls == 12, 'copy pool dispatched an unexpected file count');
}

Future<void> _verifyCopyFailureStopsScheduling(Directory root) async {
  final source = Directory(p.join(root.path, 'copy-failure-source'))
    ..createSync();
  for (var index = 0; index < 8; index++) {
    await File(p.join(source.path, 'file_$index.bin')).writeAsString('payload');
  }
  final releaseSibling = Completer<void>();
  final bothWorkersStarted = Completer<void>();
  final failureRaised = Completer<void>();
  var calls = 0;
  var active = 0;
  var maxActive = 0;
  final service = CopyService(
    copyFile: (sourceFile, targetPath) async {
      final call = ++calls;
      active++;
      if (active > maxActive) maxActive = active;
      if (call == 2) bothWorkersStarted.complete();
      try {
        if (call == 1) {
          await Future<void>.delayed(const Duration(milliseconds: 5));
          failureRaised.complete();
          throw StateError('simulated copy failure');
        }
        if (call == 2) await releaseSibling.future;
        return await sourceFile.copy(targetPath);
      } finally {
        active--;
      }
    },
  );
  var finished = false;
  final run = service
      .copyTree(
        source: source.path,
        workspace: p.join(root.path, 'copy-failure-workspace'),
        concurrency: 2,
        gate: PauseGate(),
        log: (_) {},
        progress: (_, _, _, _) {},
        onTask: (_, _, _) {},
        onFileScanned: (_, _) {},
        onScan: (_, _, _) {},
      )
      .whenComplete(() => finished = true);

  await bothWorkersStarted.future.timeout(const Duration(seconds: 5));
  await failureRaised.future.timeout(const Duration(seconds: 5));
  await Future<void>.delayed(Duration.zero);
  _check(!finished, 'copy stage returned before sibling worker drained');
  _check(calls == 2, 'copy pool dispatched more work after a copy failure');
  _check(maxActive == 2, 'copy failure test did not exercise two workers');
  releaseSibling.complete();
  var failed = false;
  try {
    await run.timeout(const Duration(seconds: 5));
  } on StateError catch (error) {
    failed = error.message == 'simulated copy failure';
  }
  _check(failed, 'copy failure was not propagated after workers drained');
  _check(active == 0, 'copy worker remained active after failure completion');
}

Future<void> _verifyZeroLengthFileWorkUnit(Directory root) async {
  final source = Directory(p.join(root.path, 'zero-length-source'))
    ..createSync();
  await File(p.join(source.path, 'empty.bin')).writeAsBytes(const []);
  await File(p.join(source.path, 'full.bin')).writeAsString('123456789');
  final progress = <double>[];
  await CopyService().copyTree(
    source: source.path,
    workspace: p.join(root.path, 'zero-length-workspace'),
    concurrency: 1,
    gate: PauseGate(),
    log: (_) {},
    progress: (value, _, _, _) => progress.add(value),
    onTask: (_, _, _) {},
    onFileScanned: (_, _) {},
    onScan: (_, _, _) {},
  );
  _check(
    progress.isNotEmpty && progress.last == 1,
    'zero-length copy did not finish at 100%',
  );
  _check(
    progress.every((value) => value > 0 && value <= 1),
    'zero-length file produced a zero progress weight',
  );
}

Future<void> _verifyProcessRunner() async {
  final fixture = p.absolute('tool/process_fixture.dart');
  final runner = ProcessRunner();
  final echoOutput = <String>[];
  final echoExit = await runner.run(
    executable: Platform.resolvedExecutable,
    arguments: [fixture, 'echo', 'path with spaces'],
    log: echoOutput.add,
  );
  _check(echoExit == 0, 'ProcessRunner echo child failed');
  _check(
    echoOutput.contains('ARG:path with spaces'),
    'argv spacing was not preserved',
  );
  var callbackFailed = false;
  try {
    await runner.run(
      executable: Platform.resolvedExecutable,
      arguments: [fixture, 'echo', 'callback failure'],
      log: (_) => throw StateError('simulated log callback failure'),
    );
  } on StateError catch (error) {
    callbackFailed = error.message == 'simulated log callback failure';
  }
  _check(callbackFailed, 'ProcessRunner did not surface log callback failure');
  final recoveryExit = await runner.run(
    executable: Platform.resolvedExecutable,
    arguments: [fixture, 'echo', 'runner recovery'],
    log: (_) {},
  );
  _check(
    recoveryExit == 0,
    'ProcessRunner did not recover after callback failure',
  );

  final readySignals = List.generate(3, (_) => Completer<void>());
  final output = List.generate(3, (_) => <String>[]);
  final children = List.generate(
    3,
    (index) => runner.run(
      executable: Platform.resolvedExecutable,
      arguments: [fixture, 'wait'],
      log: (line) {
        output[index].add(line);
        if (line == 'READY' && !readySignals[index].isCompleted) {
          readySignals[index].complete();
        }
      },
    ),
  );
  try {
    await Future.wait(
      readySignals.map((signal) => signal.future),
    ).timeout(const Duration(seconds: 10));
  } finally {
    await runner.terminate();
  }
  final exitCodes = await Future.wait(
    children,
  ).timeout(const Duration(seconds: 10));
  _check(exitCodes.length == 3, 'terminate did not await every child process');
  _check(
    output.every((lines) => lines.contains('READY')),
    'not all child processes started',
  );
}

Future<void> _verifyCleanupIsScopedToCurrentRun(Directory root) async {
  final source = Directory(p.join(root.path, 'cleanup-source'))..createSync();
  final workspace = Directory(p.join(root.path, 'cleanup-workspace'))
    ..createSync();
  final staleFile = File(p.join(workspace.path, 'prior-run.txt'));
  await staleFile.writeAsString('preserve');
  await File(p.join(source.path, 'photo.jpg')).writeAsString('image');
  final manager = PipelineManager(
    oss: _FakeOss(expectedFirstBatch: 1),
    runner: ProcessRunner(),
  );
  final eventsClosed = Completer<void>();
  manager.events.listen((_) {}, onDone: eventsClosed.complete);
  await manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: workspace.path,
      videoExtensions: const ['.mov'],
      uploadConcurrency: 1,
      uploadRetries: 0,
      deleteAfterUpload: true,
    ),
    log: (_) {},
  );
  await manager.close();
  await eventsClosed.future.timeout(const Duration(seconds: 2));
  _check(!manager.running, 'manager remained active after close');
  final remaining = await workspace.list().toList();
  _check(
    remaining.length == 1 && remaining.single.path == staleFile.path,
    'cleanup removed files outside the current run directory',
  );
  _check(
    await staleFile.readAsString() == 'preserve',
    'cleanup altered prior workspace data',
  );
}

Future<void> _verifyCleanupPoolPauseAndLimit(Directory root) async {
  final source = Directory(p.join(root.path, 'cleanup-pool-source'))
    ..createSync();
  final workspace = Directory(p.join(root.path, 'cleanup-pool-workspace'))
    ..createSync();
  final staleFile = File(p.join(workspace.path, 'preserve.txt'));
  await staleFile.writeAsString('prior run');
  for (var index = 0; index < 8; index++) {
    await File(p.join(source.path, 'photo_$index.jpg')).writeAsString('image');
  }

  const limit = 3;
  final initialBatchStarted = Completer<void>();
  final initialBatchDeleted = Completer<void>();
  final releaseInitialBatch = Completer<void>();
  var deleteCalls = 0;
  var active = 0;
  var maxActive = 0;
  final cleanup = CleanupService(
    deleteFile: (file) async {
      final call = ++deleteCalls;
      active++;
      if (active > maxActive) maxActive = active;
      if (call == limit) initialBatchStarted.complete();
      try {
        if (call <= limit) await releaseInitialBatch.future;
        await file.delete();
      } finally {
        active--;
      }
    },
  );
  final manager = PipelineManager(
    oss: _FastOss(),
    cleanup: cleanup,
    runner: ProcessRunner(),
  );
  final snapshots = <PipelineSnapshot>[];
  final batchDeleted = Completer<void>();
  final subscription = manager.events.listen((snapshot) {
    snapshots.add(snapshot);
    if (snapshot.stage == PipelineStage.cleaning &&
        snapshot.completedFiles == limit &&
        !batchDeleted.isCompleted) {
      batchDeleted.complete();
    }
  });
  final run = manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: workspace.path,
      videoExtensions: const ['.mov'],
      uploadConcurrency: 2,
      cleanupConcurrency: limit,
      deleteAfterUpload: true,
    ),
    log: (_) {},
  );

  try {
    await initialBatchStarted.future.timeout(const Duration(seconds: 5));
    _check(
      manager.snapshot.stage == PipelineStage.cleaning &&
          manager.snapshot.activeTaskCount == limit,
      'cleanup workers were not visible in the pipeline snapshot',
    );
    _check(
      manager.snapshot.activeTasks.any((task) => task.status == '清理中'),
      'active cleanup work was not represented in the task list',
    );
    manager.pause();
    if (!releaseInitialBatch.isCompleted) releaseInitialBatch.complete();
    await batchDeleted.future.timeout(const Duration(seconds: 5));
    await Future<void>.delayed(Duration.zero);
    _check(
      deleteCalls == limit,
      'cleanup pool dispatched new files while paused',
    );
    _check(manager.snapshot.paused, 'cleanup pause state was not visible');
  } finally {
    if (!releaseInitialBatch.isCompleted) releaseInitialBatch.complete();
    manager.resume();
  }

  await run.timeout(const Duration(seconds: 5));
  await Future<void>.delayed(Duration.zero);
  await subscription.cancel();
  _check(
    manager.snapshot.stage == PipelineStage.completed &&
        manager.snapshot.overall == 1,
    'parallel cleanup did not complete the pipeline at 100%',
  );
  _check(deleteCalls == 8, 'cleanup did not delete every run file');
  _check(maxActive == limit, 'cleanup did not respect concurrency $limit');
  _check(await staleFile.exists(), 'cleanup deleted a prior-run file');
  _check(
    (await workspace.list().toList()).length == 1,
    'cleanup did not remove exactly the current run directory',
  );
  final cleaningProgress = snapshots
      .where((snapshot) => snapshot.stage == PipelineStage.cleaning)
      .map((snapshot) => snapshot.overall)
      .toList();
  for (var index = 1; index < cleaningProgress.length; index++) {
    _check(
      cleaningProgress[index] + 0.000001 >= cleaningProgress[index - 1],
      'cleanup stage progress moved backwards',
    );
  }
}

Future<void> _verifyCleanupCancellationDrainsCurrentFiles(
  Directory root,
) async {
  final source = Directory(p.join(root.path, 'cleanup-cancel-source'))
    ..createSync();
  final workspace = Directory(p.join(root.path, 'cleanup-cancel-workspace'))
    ..createSync();
  for (var index = 0; index < 6; index++) {
    await File(p.join(source.path, 'photo_$index.jpg')).writeAsString('image');
  }
  final releaseCurrentBatch = Completer<void>();
  final currentBatchStarted = Completer<void>();
  var deleteCalls = 0;
  var active = 0;
  final cleanup = CleanupService(
    deleteFile: (file) async {
      deleteCalls++;
      active++;
      if (active == 2) currentBatchStarted.complete();
      try {
        await releaseCurrentBatch.future;
        await file.delete();
      } finally {
        active--;
      }
    },
  );
  final manager = PipelineManager(
    oss: _FastOss(),
    cleanup: cleanup,
    runner: ProcessRunner(),
  );
  final run = manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: workspace.path,
      videoExtensions: const ['.mov'],
      cleanupConcurrency: 2,
      deleteAfterUpload: true,
    ),
    log: (_) {},
  );

  try {
    await currentBatchStarted.future.timeout(const Duration(seconds: 5));
    final cancel = manager.cancel();
    await cancel.timeout(const Duration(seconds: 5));
  } finally {
    if (!releaseCurrentBatch.isCompleted) releaseCurrentBatch.complete();
  }
  await run.timeout(const Duration(seconds: 5));
  _check(
    manager.snapshot.stage == PipelineStage.cancelled,
    'cancel during cleanup did not reach the cancelled stage',
  );
  _check(
    deleteCalls == 2 && active == 0,
    'cleanup cancellation scheduled new work or returned before active files drained',
  );
  _check(
    manager.snapshot.activeTaskCount == 0 &&
        manager.snapshot.activeTasks.every((task) => !task.isActive),
    'cancelled cleanup retained an active task in the UI snapshot',
  );
  final remainingRuns = await workspace.list().toList();
  _check(
    remainingRuns.length == 1 &&
        remainingRuns.single is Directory &&
        (await (remainingRuns.single as Directory).list().length) > 0,
    'cancelled cleanup did not leave remaining work in its run directory',
  );
}

Future<void> _verifyEmptyPipelineProgress(Directory root) async {
  for (final deleteAfterUpload in [false, true]) {
    final source = Directory(
      p.join(root.path, 'empty-source-$deleteAfterUpload'),
    )..createSync();
    final manager = PipelineManager(
      oss: _FakeOss(expectedFirstBatch: 1),
      runner: ProcessRunner(),
    );
    final observed = <PipelineSnapshot>[];
    final subscription = manager.events.listen(observed.add);
    await manager.run(
      AppConfig(
        sourcePath: source.path,
        workspacePath: p.join(root.path, 'empty-workspace-$deleteAfterUpload'),
        videoExtensions: const ['.mov'],
        deleteAfterUpload: deleteAfterUpload,
      ),
      log: (_) {},
    );
    await Future<void>.delayed(Duration.zero);
    await subscription.cancel();
    _check(
      manager.snapshot.stage == PipelineStage.completed &&
          manager.snapshot.overall == 1,
      'empty pipeline did not complete at 100%',
    );
    for (var index = 1; index < observed.length; index++) {
      _check(
        observed[index].overall + 0.000001 >= observed[index - 1].overall,
        'empty pipeline progress moved backwards',
      );
    }
    final stages = observed.map((snapshot) => snapshot.stage).toSet();
    _check(
      stages.containsAll({
        PipelineStage.copying,
        PipelineStage.transcoding,
        PipelineStage.uploading,
        PipelineStage.completed,
      }),
      'empty pipeline skipped one or more visible stages',
    );
    if (deleteAfterUpload) {
      _check(
        stages.contains(PipelineStage.cleaning),
        'empty cleanup stage was not represented in progress',
      );
    }
  }
}

Future<void> _verifyDistinctTranscodeOutputs(Directory root) async {
  final directory = Directory(p.join(root.path, 'output-collision'))
    ..createSync();
  final mov = File(p.join(directory.path, 'clip.mov'))..createSync();
  final avi = File(p.join(directory.path, 'clip.avi'))..createSync();
  await File(p.join(directory.path, 'clip_from_mov.mp4')).writeAsString('keep');
  final movOutput = await uniqueTranscodedOutputPath(mov, '.mp4');
  final aviOutput = await uniqueTranscodedOutputPath(avi, '.mp4');
  _check(
    movOutput != aviOutput,
    'same-stem video formats selected the same output',
  );
  _check(
    p.basename(movOutput) == 'clip_from_mov_2.mp4',
    'existing output was not preserved when selecting a conversion target',
  );
  _check(
    await File(p.join(directory.path, 'clip_from_mov.mp4')).readAsString() ==
        'keep',
    'output selection modified an existing file',
  );
}

void _verifyFfmpegProgressParser() {
  final values = <double>[];
  final parser = FFmpegProgressParser(
    onProgress: (value, _) => values.add(value),
  );
  parser.addStdoutLine('out_time_us=2500000');
  _check(values.isEmpty, 'progress emitted before duration was known');
  parser.addStderrLine(
    'Input #0, mov, from \'clip.mov\': Duration: 00:00:10.00, start: 0.0',
  );
  _check(
    values.length == 1 && (values.single - 0.25).abs() < 0.0001,
    'buffered FFmpeg progress was not emitted after duration arrived',
  );
  parser.addStdoutLine('out_time_ms=5000000');
  parser.addStdoutLine('out_time_us=4000000');
  parser.addStdoutLine('progress=end');
  _check(
    values.length == 3,
    'FFmpeg progress parser emitted an unexpected number of updates',
  );
  _check((values[0] - 0.25).abs() < 0.0001, 'out_time_us parsed incorrectly');
  _check((values[1] - 0.5).abs() < 0.0001, 'out_time_ms parsed incorrectly');
  _check(values[2] == 1, 'FFmpeg end marker did not complete progress');
}

void _verifyFfmpegArgumentTemplate() {
  final arguments = parseArgumentTemplate(
    r'''-metadata "title=C:\Users\Media Work\clip.mov" -metadata "note=a \"quoted\" value"''',
  );
  _check(
    arguments.length == 4 &&
        arguments[0] == '-metadata' &&
        arguments[1] == r'title=C:\Users\Media Work\clip.mov' &&
        arguments[2] == '-metadata' &&
        arguments[3] == 'note=a "quoted" value',
    'FFmpeg argv parsing corrupted Windows backslashes or quoted spaces',
  );
  var rejectedUnclosedQuote = false;
  try {
    parseArgumentTemplate(' -metadata "unclosed');
  } on FormatException {
    rejectedUnclosedQuote = true;
  }
  _check(rejectedUnclosedQuote, 'unclosed FFmpeg argument quote was accepted');
}

void _verifyLogSeverityClassification() {
  _check(
    classifyLogSeverity('[ERROR] connection refused') == LogSeverity.error,
    'ERROR prefix was not classified as error',
  );
  _check(
    classifyLogSeverity('WARN: retrying upload') == LogSeverity.warning,
    'WARN prefix was not classified as warning',
  );
  _check(
    classifyLogSeverity('request complete') == LogSeverity.info,
    'ordinary console output was not classified as info',
  );
  _check(
    const PipelineFileTask(
      name: 'photo.jpg',
      progress: null,
      status: '重试 1/2',
    ).isActive,
    'retry wait was not counted as an active worker',
  );
}

Future<void> _verifyUploadRetry(Directory root) async {
  final source = Directory(p.join(root.path, 'retry-source'))..createSync();
  final workspace = p.join(root.path, 'retry-workspace');
  await File(p.join(source.path, 'photo.jpg')).writeAsString('image');
  final oss = _FakeOss(expectedFirstBatch: 1, failFirst: true);
  final manager = PipelineManager(oss: oss, runner: ProcessRunner());
  final logs = <String>[];
  await manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: workspace,
      videoExtensions: const ['.mov'],
      uploadConcurrency: 1,
      uploadRetries: 1,
    ),
    log: logs.add,
  );
  _check(
    manager.snapshot.stage == PipelineStage.completed,
    'retry did not recover the upload',
  );
  _check(oss.started == 2, 'upload retry did not reattempt the same file');
  _check(
    logs.any((line) => line.contains('1/1 次重试')),
    'retry was missing from pipeline logs',
  );
}

Future<void> _verifyRetryDoesNotRegressOverallProgress(Directory root) async {
  final source = Directory(p.join(root.path, 'retry-progress-source'))
    ..createSync();
  await File(p.join(source.path, 'large.jpg')).writeAsString('x' * 100);
  final oss = _FakeOss(
    expectedFirstBatch: 1,
    failFirst: true,
    failFirstAfterProgress: true,
  );
  final manager = PipelineManager(oss: oss, runner: ProcessRunner());
  final snapshots = <PipelineSnapshot>[];
  final subscription = manager.events.listen(snapshots.add);
  await manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: p.join(root.path, 'retry-progress-workspace'),
      videoExtensions: const ['.mov'],
      uploadConcurrency: 1,
      uploadRetries: 1,
    ),
    log: (_) {},
  );
  await Future<void>.delayed(Duration.zero);
  await subscription.cancel();

  _check(oss.started == 2, 'failed upload attempt was not retried');
  final uploading = snapshots
      .where((snapshot) => snapshot.stage == PipelineStage.uploading)
      .toList();
  for (var index = 1; index < uploading.length; index++) {
    _check(
      uploading[index].overall + 0.000001 >= uploading[index - 1].overall,
      'overall progress regressed when an upload was retried',
    );
  }
  _check(
    manager.snapshot.stage == PipelineStage.completed &&
        manager.snapshot.overall == 1,
    'retried upload did not complete the pipeline',
  );
}

Future<void> _verifyUnknownUploadProgress(Directory root) async {
  final source = Directory(p.join(root.path, 'unknown-progress-source'))
    ..createSync();
  await File(p.join(source.path, 'photo.jpg')).writeAsString('image');
  final releaseUpload = Completer<void>();
  final oss = _FakeOss(expectedFirstBatch: 1, holdUntil: releaseUpload.future);
  final manager = PipelineManager(oss: oss, runner: ProcessRunner());
  final run = manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: p.join(root.path, 'unknown-progress-workspace'),
      videoExtensions: const ['.mov'],
      uploadConcurrency: 1,
      uploadRetries: 0,
    ),
    log: (_) {},
  );

  try {
    await oss.firstBatchStarted.future.timeout(const Duration(seconds: 5));
    final uploadingTask = manager.snapshot.activeTasks.singleWhere(
      (task) => task.status == '上传中',
    );
    _check(
      uploadingTask.progress == null,
      'upload without a percentage was presented as determinate 0%',
    );
    _check(
      manager.snapshot.activeTaskCount == 1,
      'active worker count was not reported in the pipeline snapshot',
    );
  } finally {
    if (!releaseUpload.isCompleted) releaseUpload.complete();
  }
  await run.timeout(const Duration(seconds: 5));
  _check(
    manager.snapshot.stage == PipelineStage.completed,
    'upload did not complete after unknown progress was released',
  );
  _check(
    manager.snapshot.activeTaskCount == 0,
    'active worker count did not clear after completion',
  );
}

Future<void> _verifyTranscodeProgressIsByteWeighted(Directory root) async {
  final source = Directory(p.join(root.path, 'weighted-transcode-source'))
    ..createSync();
  await File(p.join(source.path, 'small.mov')).writeAsString('x');
  await File(p.join(source.path, 'large.mov')).writeAsString('x' * 99);
  await File(p.join(source.path, 'empty.mov')).writeAsBytes(const []);
  final manager = PipelineManager(
    ffmpeg: _FakeFfmpeg(),
    oss: _FakeOss(expectedFirstBatch: 1),
    runner: ProcessRunner(),
  );
  final twentyPercent = <PipelineSnapshot>[];
  final subscription = manager.events.listen((snapshot) {
    if (snapshot.stage == PipelineStage.transcoding &&
        snapshot.message == 'transcode 20%') {
      twentyPercent.add(snapshot);
    }
  });

  await manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: p.join(root.path, 'weighted-transcode-workspace'),
      videoExtensions: const ['.mov'],
      transcodeConcurrency: 1,
      uploadConcurrency: 1,
      uploadRetries: 0,
    ),
    log: (_) {},
  );
  await Future<void>.delayed(Duration.zero);
  await subscription.cancel();

  _check(twentyPercent.length == 3, 'did not receive progress per video');
  var completedWeight = 0;
  for (final snapshot in twentyPercent) {
    final basename = p.basename(snapshot.currentFile);
    final weight = switch (basename) {
      'small.mov' => 1,
      'large.mov' => 99,
      _ => 1,
    };
    final expected = 0.30 + ((completedWeight + weight * 0.20) / 101 * 0.30);
    _check(
      (snapshot.overall - expected).abs() < 0.000001,
      'transcode overall progress was not weighted by source file size: ${snapshot.currentFile} ${snapshot.overall} != $expected',
    );
    completedWeight += weight;
  }
}

Future<void> _verifyUploadProgressIsByteWeighted(Directory root) async {
  final source = Directory(p.join(root.path, 'weighted-upload-source'))
    ..createSync();
  await File(p.join(source.path, 'small.jpg')).writeAsString('x');
  await File(p.join(source.path, 'large.jpg')).writeAsString('x' * 99);
  await File(p.join(source.path, 'empty.jpg')).writeAsBytes(const []);
  final manager = PipelineManager(
    oss: _FakeOss(expectedFirstBatch: 1),
    runner: ProcessRunner(),
  );
  final quarterProgress = <PipelineSnapshot>[];
  final subscription = manager.events.listen((snapshot) {
    if (snapshot.stage == PipelineStage.uploading &&
        snapshot.message == 'upload 25%') {
      quarterProgress.add(snapshot);
    }
  });

  await manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: p.join(root.path, 'weighted-upload-workspace'),
      videoExtensions: const ['.mov'],
      uploadConcurrency: 1,
      uploadRetries: 0,
    ),
    log: (_) {},
  );
  await Future<void>.delayed(Duration.zero);
  await subscription.cancel();

  _check(quarterProgress.length == 3, 'did not receive progress per upload');
  var completedWeight = 0;
  for (final snapshot in quarterProgress) {
    final basename = p.basename(snapshot.currentFile);
    final weight = switch (basename) {
      'small.jpg' => 1,
      'large.jpg' => 99,
      _ => 1,
    };
    final expected = 0.60 + ((completedWeight + weight * 0.25) / 101 * 0.40);
    _check(
      (snapshot.overall - expected).abs() < 0.000001,
      'upload overall progress was not weighted by the current file size: $basename ${snapshot.overall} != $expected',
    );
    completedWeight += weight;
  }
}

Future<void> _verifyUploadPreparationProgress(Directory root) async {
  final source = Directory(p.join(root.path, 'upload-prepare-source'))
    ..createSync();
  for (var index = 0; index < 205; index++) {
    await File(p.join(source.path, 'photo_$index.jpg')).writeAsString('x');
  }
  final manager = PipelineManager(oss: _FastOss(), runner: ProcessRunner());
  final messages = <String>[];
  final subscription = manager.events.listen((snapshot) {
    if (snapshot.stage == PipelineStage.uploading) {
      messages.add(snapshot.message);
    }
  });
  await manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: p.join(root.path, 'upload-prepare-workspace'),
      videoExtensions: const ['.mov'],
      uploadConcurrency: 8,
      uploadRetries: 0,
    ),
    log: (_) {},
  );
  await Future<void>.delayed(Duration.zero);
  await subscription.cancel();

  _check(
    messages.contains('枚举中，已发现 100 个文件') &&
        messages.contains('枚举中，已发现 200 个文件') &&
        messages.contains('统计文件大小 100/205（100 B）') &&
        messages.contains('统计文件大小 200/205（200 B）'),
    'upload file enumeration or size preparation progress was not surfaced',
  );
  _check(
    manager.snapshot.stage == PipelineStage.completed,
    'pipeline did not complete after upload preparation',
  );
}

Future<void> _verifyLogPersistence(Directory root) async {
  final directory = Directory(p.join(root.path, 'logs'));
  final store = LogStore(rootDirectory: directory);
  await store.startSession();
  final expected = [
    'first',
    '日志 UTF-8 验证',
    ...List.generate(256, (index) => 'concurrent-$index'),
    'last',
  ];
  await Future.wait(expected.map(store.append));
  final file = File(store.filePath!);
  await store.close();
  final lines = await file.readAsLines();
  _check(
    lines.join('|') == expected.join('|'),
    'persisted log contents are incorrect',
  );
  await store.startSession();
  final nextSessionPath = store.filePath!;
  await store.append('next session');
  await store.close();
  _check(nextSessionPath != file.path, 'new log session reused its prior file');
  _check(
    (await File(nextSessionPath).readAsLines()).single == 'next session',
    'new log session inherited pending lines from the previous session',
  );
}

Future<void> _verifyParallelPauseResume(Directory root) async {
  final source = Directory(p.join(root.path, 'success-source'))..createSync();
  final workspace = p.join(root.path, 'success-workspace');
  await Directory(workspace).create(recursive: true);
  final staleFile = File(p.join(workspace, 'stale-from-previous-run.jpg'));
  await staleFile.writeAsString('leave this prior-run file alone');
  for (var i = 0; i < 5; i++) {
    await File(p.join(source.path, 'video_$i.mov')).writeAsString('video');
  }
  for (var i = 0; i < 7; i++) {
    await File(p.join(source.path, 'photo_$i.jpg')).writeAsString('image');
  }

  final ffmpeg = _FakeFfmpeg();
  final oss = _FakeOss(expectedFirstBatch: 3);
  final manager = PipelineManager(
    ffmpeg: ffmpeg,
    oss: oss,
    runner: ProcessRunner(),
  );
  final observedProgress = <double>[];
  final controlLogs = <String>[];
  final subscription = manager.events.listen(
    (snapshot) => observedProgress.add(snapshot.overall),
  );
  final run = manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: workspace,
      videoExtensions: const ['.mov'],
      transcodeConcurrency: 2,
      uploadConcurrency: 3,
    ),
    log: controlLogs.add,
  );

  await oss.firstBatchStarted.future.timeout(const Duration(seconds: 5));
  _check(ffmpeg.maxActive == 2, 'transcode worker pool did not reach limit 2');
  manager.pause();
  await Future<void>.delayed(const Duration(milliseconds: 60));
  _check(manager.snapshot.paused, 'pipeline did not report paused state');
  _check(oss.started == 3, 'upload workers dispatched new files while paused');
  manager.resume();
  await run.timeout(const Duration(seconds: 10));
  await Future<void>.delayed(Duration.zero);
  await subscription.cancel();

  _check(
    manager.snapshot.stage == PipelineStage.completed,
    'pipeline did not complete',
  );
  _check(manager.snapshot.overall == 1, 'overall progress did not reach 100%');
  _check(
    ffmpeg.maxActive == 2,
    'configured transcode parallelism was not respected',
  );
  _check(oss.maxActive == 3, 'configured upload parallelism was not respected');
  _check(oss.started == 12, 'expected all 12 workspace files to upload');
  _check(
    controlLogs.any((line) => line.contains('收到暂停请求')) &&
        controlLogs.any((line) => line.contains('任务已恢复')),
    'pause/resume actions were not written to the global log',
  );
  _check(
    await staleFile.exists(),
    'new pipeline run modified a stale workspace file',
  );
  for (var i = 1; i < observedProgress.length; i++) {
    _check(
      observedProgress[i] + 0.000001 >= observedProgress[i - 1],
      'overall progress moved backwards in a successful run',
    );
  }
}

Future<void> _verifyUploadFailureStopsScheduling(Directory root) async {
  final source = Directory(p.join(root.path, 'failure-source'))..createSync();
  final workspace = p.join(root.path, 'failure-workspace');
  for (var i = 0; i < 12; i++) {
    await File(p.join(source.path, 'photo_$i.jpg')).writeAsString('image');
  }
  final oss = _FakeOss(expectedFirstBatch: 3, failFirst: true);
  final manager = PipelineManager(oss: oss, runner: ProcessRunner());
  try {
    await manager.run(
      AppConfig(
        sourcePath: source.path,
        workspacePath: workspace,
        videoExtensions: const ['.mov'],
        uploadConcurrency: 3,
        uploadRetries: 0,
      ),
      log: (_) {},
    );
    throw StateError(
      'pipeline unexpectedly succeeded despite an upload failure',
    );
  } catch (error) {
    if (error is StateError &&
        error.message ==
            'pipeline unexpectedly succeeded despite an upload failure') {
      rethrow;
    }
  }
  _check(
    manager.snapshot.stage == PipelineStage.failed,
    'upload failure was not surfaced',
  );
  _check(
    oss.started == 3,
    'pipeline scheduled more files after first upload failure',
  );
}

Future<void> _verifyTranscodeFailureStopsScheduling(Directory root) async {
  final source = Directory(p.join(root.path, 'transcode-failure-source'))
    ..createSync();
  for (var index = 0; index < 8; index++) {
    await File(p.join(source.path, 'video_$index.mov')).writeAsString('video');
  }
  final ffmpeg = _FailingFfmpeg();
  final manager = PipelineManager(ffmpeg: ffmpeg, runner: ProcessRunner());
  final run = manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: p.join(root.path, 'transcode-failure-workspace'),
      videoExtensions: const ['.mov'],
      transcodeConcurrency: 2,
      uploadConcurrency: 1,
      uploadRetries: 0,
    ),
    log: (_) {},
  );

  await ffmpeg.bothWorkersStarted.future.timeout(const Duration(seconds: 5));
  await ffmpeg.failureRaised.future.timeout(const Duration(seconds: 5));
  await Future<void>.delayed(Duration.zero);
  _check(ffmpeg.started == 2, 'transcode pool dispatched work after failure');
  _check(manager.running, 'pipeline stopped before sibling transcode drained');
  ffmpeg.releaseSibling.complete();
  var failed = false;
  try {
    await run.timeout(const Duration(seconds: 5));
  } on StateError catch (error) {
    failed = error.message == 'simulated transcode failure';
  }
  _check(failed, 'transcode failure was not propagated');
  _check(ffmpeg.active == 0, 'transcode worker remained active after failure');
  _check(
    manager.snapshot.stage == PipelineStage.failed &&
        manager.snapshot.activeTaskCount == 0,
    'transcode failure did not reach a terminal UI state',
  );
  _check(
    manager.snapshot.activeTasks.any((task) => task.status == '失败') &&
        manager.snapshot.activeTasks.any((task) => task.status == '已中止'),
    'transcode failure and sibling abort were not both shown in task list',
  );
}

Future<void> _verifyCancellationStopsDispatch(Directory root) async {
  final source = Directory(p.join(root.path, 'cancel-source'))..createSync();
  final workspace = p.join(root.path, 'cancel-workspace');
  for (var i = 0; i < 8; i++) {
    await File(p.join(source.path, 'photo_$i.jpg')).writeAsString('image');
  }
  final oss = _FakeOss(expectedFirstBatch: 3);
  final manager = PipelineManager(oss: oss, runner: ProcessRunner());
  final cancelLogs = <String>[];
  final run = manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: workspace,
      videoExtensions: const ['.mov'],
      uploadConcurrency: 3,
      uploadRetries: 0,
    ),
    log: cancelLogs.add,
  );
  await oss.firstBatchStarted.future.timeout(const Duration(seconds: 5));
  await manager.cancel();
  await run.timeout(const Duration(seconds: 5));
  _check(
    manager.snapshot.stage == PipelineStage.cancelled,
    'cancel did not reach terminal state',
  );
  _check(
    oss.started == 3,
    'cancel dispatched additional files after the active batch',
  );
  _check(
    cancelLogs.any((line) => line.contains('收到取消请求')) &&
        cancelLogs.any((line) => line == '任务已取消'),
    'cancel request and terminal result were not both logged',
  );
}

Future<void> _verifyCancellationMarksActiveTasksAborted(Directory root) async {
  final source = Directory(p.join(root.path, 'cancel-task-source'))
    ..createSync();
  await File(p.join(source.path, 'photo.jpg')).writeAsString('image');
  final releaseUpload = Completer<void>();
  final oss = _FakeOss(
    expectedFirstBatch: 1,
    holdUntil: releaseUpload.future,
    failAfterHold: true,
  );
  final manager = PipelineManager(oss: oss, runner: ProcessRunner());
  final run = manager.run(
    AppConfig(
      sourcePath: source.path,
      workspacePath: p.join(root.path, 'cancel-task-workspace'),
      videoExtensions: const ['.mov'],
      uploadConcurrency: 1,
      uploadRetries: 0,
    ),
    log: (_) {},
  );
  await oss.firstBatchStarted.future.timeout(const Duration(seconds: 5));
  await manager.cancel();
  if (!releaseUpload.isCompleted) releaseUpload.complete();
  await run.timeout(const Duration(seconds: 5));
  _check(
    manager.snapshot.stage == PipelineStage.cancelled,
    'active task cancellation did not reach cancelled stage',
  );
  _check(
    manager.snapshot.activeTasks.every(
      (task) => !task.isActive || task.status == '已中止',
    ),
    'cancelled pipeline retained an active task status',
  );
  _check(
    manager.snapshot.activeTasks.any((task) => task.status == '已中止'),
    'cancelled external task was not labeled aborted',
  );
}

void _check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

class _FakeFfmpeg extends FFmpegService {
  _FakeFfmpeg() : super(ProcessRunner());
  var active = 0;
  var maxActive = 0;

  @override
  Future<File> transcode(
    File input, {
    required String executable,
    required String argumentTemplate,
    required String outputExtension,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    active++;
    if (active > maxActive) maxActive = active;
    try {
      progress(0.2, 'transcode 20%');
      await Future<void>.delayed(const Duration(milliseconds: 20));
      progress(0.75, 'transcode 75%');
      progress(0.4, 'transcode 40% (late log)');
      final extension = outputExtension.startsWith('.')
          ? outputExtension
          : '.$outputExtension';
      final output = File(p.setExtension(input.path, extension));
      await output.writeAsString('converted');
      await input.delete();
      progress(1, 'transcode complete');
      return output;
    } finally {
      active--;
    }
  }
}

class _FailingFfmpeg extends FFmpegService {
  _FailingFfmpeg() : super(ProcessRunner());
  final bothWorkersStarted = Completer<void>();
  final failureRaised = Completer<void>();
  final releaseSibling = Completer<void>();
  var started = 0;
  var active = 0;

  @override
  Future<File> transcode(
    File input, {
    required String executable,
    required String argumentTemplate,
    required String outputExtension,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    final call = ++started;
    active++;
    if (call == 2) bothWorkersStarted.complete();
    try {
      if (call == 1) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
        failureRaised.complete();
        throw StateError('simulated transcode failure');
      }
      await releaseSibling.future;
      final output = File(p.setExtension(input.path, '.mp4'));
      await output.writeAsString('converted');
      await input.delete();
      return output;
    } finally {
      active--;
    }
  }
}

class _FakeOss extends OssService {
  _FakeOss({
    required this.expectedFirstBatch,
    this.failFirst = false,
    this.failFirstAfterProgress = false,
    this.failAfterHold = false,
    this.holdUntil,
  }) : super(ProcessRunner());
  final int expectedFirstBatch;
  final bool failFirst;
  final bool failFirstAfterProgress;
  final bool failAfterHold;
  final Future<void>? holdUntil;
  final firstBatchStarted = Completer<void>();
  var started = 0;
  var active = 0;
  var maxActive = 0;

  @override
  Future<void> upload(
    File file, {
    required AppConfig config,
    required String workspace,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    final call = ++started;
    active++;
    if (active > maxActive) maxActive = active;
    if (started >= expectedFirstBatch && !firstBatchStarted.isCompleted) {
      firstBatchStarted.complete();
    }
    try {
      await holdUntil;
      if (failAfterHold) throw StateError('simulated process termination');
      if (failFirst && call == 1 && !failFirstAfterProgress) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
        throw StateError('simulated upload failure');
      }
      progress(0.25, 'upload 25%');
      await Future<void>.delayed(const Duration(milliseconds: 35));
      progress(0.75, 'upload 75%');
      if (failFirst && call == 1 && failFirstAfterProgress) {
        throw StateError('simulated late upload failure');
      }
      progress(0.4, 'upload 40% (late log)');
      await Future<void>.delayed(const Duration(milliseconds: 15));
      progress(1, 'upload complete');
    } finally {
      active--;
    }
  }
}

class _FastOss extends OssService {
  _FastOss() : super(ProcessRunner());

  @override
  Future<void> upload(
    File file, {
    required AppConfig config,
    required String workspace,
    required LogCallback log,
    required ProgressCallback progress,
  }) async {
    progress(1, 'upload complete');
  }
}
