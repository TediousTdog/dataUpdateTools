import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:untitled/main.dart';
import 'package:untitled/services/config_repository.dart';
import 'package:untitled/services/log_store.dart';
import 'package:untitled/services/pipeline_manager.dart';
import 'package:untitled/services/pipeline_types.dart';

void main() {
  testWidgets('dashboard shows pipeline controls and log viewer', (
    tester,
  ) async {
    final temp = await Directory.systemTemp.createTemp('media_ingest_widget_');
    await tester.pumpWidget(
      PhotoIngestApp(
        configRepository: ConfigRepository(rootDirectory: temp),
        logStore: LogStore(rootDirectory: temp),
        pipelineManager: PipelineManager(),
      ),
    );
    await tester.pumpAndSettle();

    expect(find.text('媒体归档工作台'), findsOneWidget);
    expect(find.text('开始任务'), findsOneWidget);
    expect(find.text('总体进度'), findsOneWidget);
    expect(find.textContaining('运行日志'), findsOneWidget);
    expect(find.text('搜索当前日志'), findsOneWidget);
    expect(find.text('警告'), findsOneWidget);
    expect(find.text('错误'), findsOneWidget);
    await tester.pumpWidget(const SizedBox.shrink());
    await tester.runAsync(
      () => Future<void>.delayed(const Duration(milliseconds: 100)),
    );
    await temp.delete(recursive: true);
  });

  test('LogStore persists ordered lines in the session file', () async {
    final temp = await Directory.systemTemp.createTemp('media_ingest_logs_');
    final store = LogStore(rootDirectory: temp);
    await store.startSession();
    await store.append('first line');
    await store.append('second line');
    final path = store.filePath!;
    await store.close();

    final lines = await File(path).readAsLines();
    expect(lines, ['first line', 'second line']);
    await temp.delete(recursive: true);
  });

  test('LogStore serializes concurrent appends and flushes on close', () async {
    final temp = await Directory.systemTemp.createTemp('media_ingest_logs_');
    final store = LogStore(rootDirectory: temp);
    await store.startSession();
    final expected = List.generate(256, (index) => 'line-$index');
    await Future.wait(expected.map(store.append));
    final path = store.filePath!;
    await store.close();

    final lines = await File(path).readAsLines();
    expect(lines, expected);
    await temp.delete(recursive: true);
  });

  test('PauseGate blocks new work and cancellation releases waiters', () async {
    final gate = PauseGate();
    gate.pause();
    var passed = false;
    final waiting = gate.checkpoint().then((_) => passed = true);
    await Future<void>.delayed(Duration.zero);
    expect(passed, isFalse);
    gate.cancel();
    await expectLater(waiting, throwsA(isA<PipelineCancelledException>()));
  });
}
