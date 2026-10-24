import 'dart:io';
import 'package:flutter/material.dart';
import 'services/config_repository.dart';
import 'services/log_store.dart';
import 'services/pipeline_manager.dart';
import 'services/demo_pipeline_manager.dart';
import 'ui/pipeline_page.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const PhotoIngestApp());
}

class PhotoIngestApp extends StatelessWidget {
  const PhotoIngestApp({
    super.key,
    this.configRepository,
    this.logStore,
    this.pipelineManager,
    this.demoMode,
  });

  final ConfigRepository? configRepository;
  final LogStore? logStore;
  final PipelineManager? pipelineManager;
  /// macOS 演示默认开启；传入值可在测试或正式构建中覆盖。
  final bool? demoMode;

  @override
  Widget build(BuildContext context) => MaterialApp(
    title: '媒体归档工作台',
    debugShowCheckedModeBanner: false,
    theme: ThemeData(
      colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xff147d73)),
      scaffoldBackgroundColor: const Color(0xfff4f6f6),
      useMaterial3: true,
      inputDecorationTheme: const InputDecorationTheme(
        border: OutlineInputBorder(),
        isDense: true,
      ),
    ),
    home: PipelinePage(
      configRepository: configRepository,
      logStore: logStore,
      pipelineManager: pipelineManager,
      demoMode: demoMode ?? Platform.isMacOS,
      demoPipelineManager: demoMode == false ? null : DemoPipelineManager(),
    ),
  );
}
