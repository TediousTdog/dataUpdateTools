import 'dart:io';
import 'package:path/path.dart' as p;

/// Windows uses APPDATA; other platforms use their conventional config directory for development/tests.
String appDataDirectory() {
  if (Platform.isWindows) {
    final appData =
        Platform.environment['APPDATA'] ?? Platform.environment['LOCALAPPDATA'];
    if (appData != null && appData.isNotEmpty) return appData;
  } else if (Platform.isMacOS) {
    final home = Platform.environment['HOME'];
    if (home != null && home.isNotEmpty) {
      return p.join(home, 'Library', 'Application Support');
    }
  } else {
    final xdg = Platform.environment['XDG_CONFIG_HOME'];
    if (xdg != null && xdg.isNotEmpty) return xdg;
    final home = Platform.environment['HOME'];
    if (home != null && home.isNotEmpty) return p.join(home, '.config');
  }
  throw StateError('找不到应用数据目录环境变量');
}
