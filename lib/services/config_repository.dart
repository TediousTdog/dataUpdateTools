import 'dart:convert';
import 'dart:io';
import 'package:path/path.dart' as p;
import '../models/app_config.dart';
import 'app_paths.dart';

class ConfigRepository {
  ConfigRepository({this.rootDirectory});

  /// 可注入配置目录以隔离测试；生产环境使用用户应用数据目录。
  final Directory? rootDirectory;
  Future<void> _saveQueue = Future<void>.value();

  Future<File> get file async {
    final dir =
        rootDirectory ?? Directory(p.join(appDataDirectory(), 'MediaIngest'));
    await dir.create(recursive: true);
    return File(p.join(dir.path, 'config.json'));
  }

  Future<AppConfig> load() async {
    final f = await file;
    if (!await f.exists()) return const AppConfig();
    try {
      final contents = await f.readAsString();
      return contents.isEmpty
          ? const AppConfig()
          : AppConfig.fromJson(Map<String, dynamic>.from(jsonDecode(contents)));
    } catch (_) {
      // 配置损坏时保留原文件供排查，应用仍可用默认值启动。
      return const AppConfig();
    }
  }

  Future<void> save(AppConfig config) {
    final operation = _saveQueue.then((_) => _saveNow(config));
    // 一个保存失败不应阻塞后续用户再次保存。
    _saveQueue = operation.catchError((Object _) {});
    return operation;
  }

  Future<void> _saveNow(AppConfig config) async {
    final target = await file;
    final temporary = File(
      '${target.path}.tmp.${DateTime.now().microsecondsSinceEpoch}',
    );
    try {
      await temporary.writeAsString(
        config.toPrettyJson(),
        encoding: utf8,
        flush: true,
      );
      await temporary.rename(target.path);
    } finally {
      if (await temporary.exists()) {
        await temporary.delete();
      }
    }
  }
}
