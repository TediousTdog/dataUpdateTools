enum LogSeverity { info, warning, error }

/// 将进程原始日志和流水线提示归类，供日志面板的级别筛选复用。
LogSeverity classifyLogSeverity(String line) {
  final lower = line.toLowerCase();
  if (line.contains('失败') ||
      line.contains('异常') ||
      RegExp(r'\b(error|fatal|exception)\b').hasMatch(lower)) {
    return LogSeverity.error;
  }
  if (line.contains('重试') ||
      line.contains('已中止') ||
      RegExp(r'\b(warn|warning)\b').hasMatch(lower)) {
    return LogSeverity.warning;
  }
  return LogSeverity.info;
}
