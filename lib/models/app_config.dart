import 'dart:convert';
import 'oss_authorization.dart';

/// 可编辑的运行配置。密码类字段只保存在本机 config.json，请勿将该文件提交到版本库。
class AppConfig {
  const AppConfig({
    this.sourcePath = r'F:\',
    this.workspacePath = r'D:\PhotoWork',
    this.bucket = '',
    this.prefix = '',
    this.endpoint = '',
    this.authorizationCode = '',
    this.accessKeyId = '',
    this.accessKeySecret = '',
    this.securityToken = '',
    this.ffmpegPath = r'tools\ffmpeg.exe',
    this.ossutilPath = r'tools\ossutil.exe',
    this.ffmpegArgs = '-c:v h264_nvenc -preset fast -c:a copy',
    this.videoExtensions = const ['.mov', '.avi'],
    this.transcodeEnabled = true,
    // 与 batch_upload_oss.bat 的默认值一致；可按带宽/CPU 调高或下调。
    this.uploadConcurrency = 10,
    this.bulkUpload = true,
    this.multipartParallel = 8,
    this.bigFileThresholdMb = 100,
    this.partSizeMb = 8,
    this.uploadRetries = 2,
    this.transcodeConcurrency = 2,
    this.outputExtension = '.mp4',
    this.deleteAfterUpload = false,
    this.copyConcurrency = 4,
    this.cleanupConcurrency = 4,
    this.ossExtraArgs = '',
  });

  final String sourcePath, workspacePath, bucket, prefix, endpoint;
  final String accessKeyId, accessKeySecret, securityToken, authorizationCode;
  final String ffmpegPath, ossutilPath, ffmpegArgs, outputExtension;
  final List<String> videoExtensions;
  final bool transcodeEnabled;
  final int uploadConcurrency, copyConcurrency;
  final int multipartParallel, bigFileThresholdMb, partSizeMb;
  final bool bulkUpload;
  final int cleanupConcurrency;
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
    String? authorizationCode,
    String? accessKeyId,
    String? accessKeySecret,
    String? securityToken,
    String? ffmpegPath,
    String? ossutilPath,
    String? ffmpegArgs,
    List<String>? videoExtensions,
    bool? transcodeEnabled,
    int? uploadConcurrency,
    bool? bulkUpload,
    int? multipartParallel,
    int? bigFileThresholdMb,
    int? partSizeMb,
    int? uploadRetries,
    int? transcodeConcurrency,
    String? outputExtension,
    bool? deleteAfterUpload,
    int? copyConcurrency,
    int? cleanupConcurrency,
    String? ossExtraArgs,
  }) => AppConfig(
    sourcePath: sourcePath ?? this.sourcePath,
    workspacePath: workspacePath ?? this.workspacePath,
    bucket: bucket ?? this.bucket,
    prefix: prefix ?? this.prefix,
    endpoint: endpoint ?? this.endpoint,
    authorizationCode: authorizationCode ?? this.authorizationCode,
    accessKeyId: accessKeyId ?? this.accessKeyId,
    accessKeySecret: accessKeySecret ?? this.accessKeySecret,
    securityToken: securityToken ?? this.securityToken,
    ffmpegPath: ffmpegPath ?? this.ffmpegPath,
    ossutilPath: ossutilPath ?? this.ossutilPath,
    ffmpegArgs: ffmpegArgs ?? this.ffmpegArgs,
    videoExtensions: videoExtensions ?? this.videoExtensions,
    transcodeEnabled: transcodeEnabled ?? this.transcodeEnabled,
    uploadConcurrency: uploadConcurrency ?? this.uploadConcurrency,
    bulkUpload: bulkUpload ?? this.bulkUpload,
    multipartParallel: multipartParallel ?? this.multipartParallel,
    bigFileThresholdMb: bigFileThresholdMb ?? this.bigFileThresholdMb,
    partSizeMb: partSizeMb ?? this.partSizeMb,
    uploadRetries: uploadRetries ?? this.uploadRetries,
    transcodeConcurrency: transcodeConcurrency ?? this.transcodeConcurrency,
    outputExtension: outputExtension ?? this.outputExtension,
    deleteAfterUpload: deleteAfterUpload ?? this.deleteAfterUpload,
    copyConcurrency: copyConcurrency ?? this.copyConcurrency,
    cleanupConcurrency: cleanupConcurrency ?? this.cleanupConcurrency,
    ossExtraArgs: ossExtraArgs ?? this.ossExtraArgs,
  );

  Map<String, dynamic> toJson() => {
    'sourcePath': sourcePath,
    'workspacePath': workspacePath,
    'bucket': bucket,
    'prefix': prefix,
    'endpoint': endpoint,
    'authorizationCode': authorizationCode,
    'accessKeyId': accessKeyId,
    'accessKeySecret': accessKeySecret,
    'securityToken': securityToken,
    'ffmpegPath': ffmpegPath,
    'ossutilPath': ossutilPath,
    'ffmpegArgs': ffmpegArgs,
    'videoExtensions': videoExtensions,
    'transcodeEnabled': transcodeEnabled,
    'uploadConcurrency': uploadConcurrency,
    'bulkUpload': bulkUpload,
    'multipartParallel': multipartParallel,
    'bigFileThresholdMb': bigFileThresholdMb,
    'partSizeMb': partSizeMb,
    'uploadRetries': uploadRetries,
    'transcodeConcurrency': transcodeConcurrency,
    'outputExtension': outputExtension,
    'deleteAfterUpload': deleteAfterUpload,
    'copyConcurrency': copyConcurrency,
    'cleanupConcurrency': cleanupConcurrency,
    'ossExtraArgs': ossExtraArgs,
  };

  factory AppConfig.fromJson(Map<String, dynamic> j) => AppConfig(
    sourcePath: j['sourcePath'] ?? r'F:\',
    workspacePath: j['workspacePath'] ?? r'D:\PhotoWork',
    bucket: j['bucket'] ?? '',
    prefix: j['prefix'] ?? '',
    endpoint: j['endpoint'] ?? '',
    authorizationCode: j['authorizationCode'] ?? '',
    accessKeyId: j['accessKeyId'] ?? '',
    accessKeySecret: j['accessKeySecret'] ?? '',
    securityToken: j['securityToken'] ?? '',
    ffmpegPath: j['ffmpegPath'] ?? r'tools\ffmpeg.exe',
    ossutilPath: j['ossutilPath'] ?? r'tools\ossutil.exe',
    ffmpegArgs: j['ffmpegArgs'] ?? '-c:v h264_nvenc -preset fast -c:a copy',
    videoExtensions: List<String>.from(
      j['videoExtensions'] ?? ['.mov', '.avi'],
    ),
    transcodeEnabled: j['transcodeEnabled'] ?? true,
    uploadConcurrency: j['uploadConcurrency'] ?? 10,
    bulkUpload: j['bulkUpload'] ?? true,
    multipartParallel: j['multipartParallel'] ?? 8,
    bigFileThresholdMb: j['bigFileThresholdMb'] ?? 100,
    partSizeMb: j['partSizeMb'] ?? 8,
    uploadRetries: j['uploadRetries'] ?? 2,
    transcodeConcurrency: j['transcodeConcurrency'] ?? 2,
    outputExtension: j['outputExtension'] ?? '.mp4',
    deleteAfterUpload: j['deleteAfterUpload'] ?? false,
    copyConcurrency: j['copyConcurrency'] ?? 4,
    cleanupConcurrency: j['cleanupConcurrency'] ?? 4,
    ossExtraArgs: j['ossExtraArgs'] ?? '',
  );

  String toPrettyJson() => const JsonEncoder.withIndent('  ').convert(toJson());

  /// 新格式授权码提供连接默认值；用户填写的 OSS 前缀始终优先。
  AppConfig resolveAuthorization() {
    if (authorizationCode.trim().isEmpty) return this;
    final authorization = OssAuthorization.parse(authorizationCode);
    return copyWith(
      bucket: authorization.bucket,
      prefix: prefix.trim().isNotEmpty ? prefix.trim() : authorization.prefix,
      endpoint: authorization.endpoint,
      accessKeyId: authorization.accessKeyId,
      accessKeySecret: authorization.accessKeySecret,
      // 授权码若不含 STS SecurityToken，应清空旧配置，避免意外混用凭证。
      securityToken: authorization.securityToken,
    );
  }
}
