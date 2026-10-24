import 'dart:convert';

/// 可编辑的运行配置。密码类字段只保存在本机 config.json，请勿将该文件提交到版本库。
class AppConfig {
  const AppConfig({
    this.sourcePath = r'F:\',
    this.workspacePath = r'D:\PhotoWork',
    this.bucket = '',
    this.prefix = '',
    this.endpoint = '',
    this.accessKeyId = '',
    this.accessKeySecret = '',
    this.securityToken = '',
    this.ffmpegPath = r'tools\ffmpeg.exe',
    this.ossutilPath = r'tools\ossutil.exe',
    this.ffmpegArgs = '-c:v h264_nvenc -preset fast -c:a copy',
    this.videoExtensions = const ['.mov', '.avi'],
    this.uploadConcurrency = 10,
    this.uploadRetries = 2,
    this.transcodeConcurrency = 2,
    this.outputExtension = '.mp4',
    this.deleteAfterUpload = false,
    this.copyConcurrency = 4,
    this.ossExtraArgs = '',
  });

  final String sourcePath, workspacePath, bucket, prefix, endpoint;
  final String accessKeyId, accessKeySecret, securityToken;
  final String ffmpegPath, ossutilPath, ffmpegArgs, outputExtension;
  final List<String> videoExtensions;
  final int uploadConcurrency, copyConcurrency;
  final int transcodeConcurrency;
  final int uploadRetries;
  final bool deleteAfterUpload;

  /// 额外 ossutil 参数，按空白分隔；有复杂引号需求时请扩展配置为 JSON 数组。
  final String ossExtraArgs;

  AppConfig copyWith({
    String? sourcePath,
    String? workspacePath,
    String? bucket,
    String? prefix,
    String? endpoint,
    String? accessKeyId,
    String? accessKeySecret,
    String? securityToken,
    String? ffmpegPath,
    String? ossutilPath,
    String? ffmpegArgs,
    List<String>? videoExtensions,
    int? uploadConcurrency,
    int? uploadRetries,
    int? transcodeConcurrency,
    String? outputExtension,
    bool? deleteAfterUpload,
    int? copyConcurrency,
    String? ossExtraArgs,
  }) => AppConfig(
    sourcePath: sourcePath ?? this.sourcePath,
    workspacePath: workspacePath ?? this.workspacePath,
    bucket: bucket ?? this.bucket,
    prefix: prefix ?? this.prefix,
    endpoint: endpoint ?? this.endpoint,
    accessKeyId: accessKeyId ?? this.accessKeyId,
    accessKeySecret: accessKeySecret ?? this.accessKeySecret,
    securityToken: securityToken ?? this.securityToken,
    ffmpegPath: ffmpegPath ?? this.ffmpegPath,
    ossutilPath: ossutilPath ?? this.ossutilPath,
    ffmpegArgs: ffmpegArgs ?? this.ffmpegArgs,
    videoExtensions: videoExtensions ?? this.videoExtensions,
    uploadConcurrency: uploadConcurrency ?? this.uploadConcurrency,
    uploadRetries: uploadRetries ?? this.uploadRetries,
    transcodeConcurrency: transcodeConcurrency ?? this.transcodeConcurrency,
    outputExtension: outputExtension ?? this.outputExtension,
    deleteAfterUpload: deleteAfterUpload ?? this.deleteAfterUpload,
    copyConcurrency: copyConcurrency ?? this.copyConcurrency,
    ossExtraArgs: ossExtraArgs ?? this.ossExtraArgs,
  );

  Map<String, dynamic> toJson() => {
    'sourcePath': sourcePath,
    'workspacePath': workspacePath,
    'bucket': bucket,
    'prefix': prefix,
    'endpoint': endpoint,
    'accessKeyId': accessKeyId,
    'accessKeySecret': accessKeySecret,
    'securityToken': securityToken,
    'ffmpegPath': ffmpegPath,
    'ossutilPath': ossutilPath,
    'ffmpegArgs': ffmpegArgs,
    'videoExtensions': videoExtensions,
    'uploadConcurrency': uploadConcurrency,
    'uploadRetries': uploadRetries,
    'transcodeConcurrency': transcodeConcurrency,
    'outputExtension': outputExtension,
    'deleteAfterUpload': deleteAfterUpload,
    'copyConcurrency': copyConcurrency,
    'ossExtraArgs': ossExtraArgs,
  };

  factory AppConfig.fromJson(Map<String, dynamic> j) => AppConfig(
    sourcePath: j['sourcePath'] ?? r'F:\',
    workspacePath: j['workspacePath'] ?? r'D:\PhotoWork',
    bucket: j['bucket'] ?? '',
    prefix: j['prefix'] ?? '',
    endpoint: j['endpoint'] ?? '',
    accessKeyId: j['accessKeyId'] ?? '',
    accessKeySecret: j['accessKeySecret'] ?? '',
    securityToken: j['securityToken'] ?? '',
    ffmpegPath: j['ffmpegPath'] ?? r'tools\ffmpeg.exe',
    ossutilPath: j['ossutilPath'] ?? r'tools\ossutil.exe',
    ffmpegArgs: j['ffmpegArgs'] ?? '-c:v h264_nvenc -preset fast -c:a copy',
    videoExtensions: List<String>.from(
      j['videoExtensions'] ?? ['.mov', '.avi'],
    ),
    uploadConcurrency: j['uploadConcurrency'] ?? 10,
    uploadRetries: j['uploadRetries'] ?? 2,
    transcodeConcurrency: j['transcodeConcurrency'] ?? 2,
    outputExtension: j['outputExtension'] ?? '.mp4',
    deleteAfterUpload: j['deleteAfterUpload'] ?? false,
    copyConcurrency: j['copyConcurrency'] ?? 4,
    ossExtraArgs: j['ossExtraArgs'] ?? '',
  );

  String toPrettyJson() => const JsonEncoder.withIndent('  ').convert(toJson());
}
