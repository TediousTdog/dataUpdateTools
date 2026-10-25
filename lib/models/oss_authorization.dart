import 'dart:convert';

/// 从网页给出的 Base64(JSON) 授权码中解析 OSS 连接信息。
///
/// 常见字段为 id、secret、region、osspath、eptpl；也兼容直接提供
/// accessKeyId/accessKeySecret/endpoint/securityToken 的命名。授权码
/// 经常从聊天窗口复制，可能夹带换行、空格或使用 URL-safe Base64。
class OssAuthorization {
  const OssAuthorization({
    required this.accessKeyId,
    required this.accessKeySecret,
    required this.region,
    required this.endpoint,
    required this.bucket,
    required this.prefix,
    this.securityToken = '',
    this.description = '',
  });

  final String accessKeyId;
  final String accessKeySecret;
  final String region;
  final String endpoint;
  final String bucket;
  final String prefix;
  final String securityToken;
  final String description;

  /// 只从授权码的 OSS URI 解析 Bucket/前缀；不把整段 URI 当成本地路径，
  /// 因此中文、空格和 Windows 反斜杠不会被 shell 或本地路径规则改写。
  factory OssAuthorization.parse(String input) {
    late final dynamic value;
    final trimmed = input.trim();
    if (trimmed.isEmpty) throw const FormatException('授权码不能为空');
    if (trimmed.startsWith('{')) {
      try {
        value = jsonDecode(trimmed);
      } on FormatException {
        throw const FormatException('授权码看起来是 JSON，但 JSON 格式不正确');
      }
    } else {
      // 复制时偶尔会把整段授权码用引号括住，或在末尾混入网页百分号标记。
      // 只剥离两端包裹符，不从中间任意删除字符，避免悄悄改变凭证。
      var candidate = trimmed;
      const wrappers = ['"', "'", '`', '“', '”', '‘', '’'];
      for (final wrapper in wrappers) {
        if (candidate.startsWith(wrapper) && candidate.endsWith(wrapper)) {
          candidate = candidate.substring(
            wrapper.length,
            candidate.length - wrapper.length,
          );
          break;
        }
      }
      final compact = candidate
          .replaceFirst(RegExp(r'^data:.*?;base64,'), '')
          .replaceAll(RegExp(r'\s+'), '');
      // 有些复制源会在授权码末尾附加一个百分号作为界面标记；它不是
      // Base64 数据。仅容忍尾部的百分号，其他非法字符仍明确报错。
      final base64Text = compact
          .replaceFirst(RegExp(r'%+$'), '')
          .replaceAll('-', '+')
          .replaceAll('_', '/');
      if (base64Text.length % 4 == 1 ||
          !RegExp(r'^[A-Za-z0-9+/]*={0,2}$').hasMatch(base64Text) ||
          (base64Text.contains('=') && base64Text.length % 4 != 0)) {
        throw const FormatException('授权码不是有效的 Base64。请粘贴完整授权码，不要包含引号或说明文字');
      }
      final padded = base64Text.padRight(
        base64Text.length + ((4 - base64Text.length % 4) % 4),
        '=',
      );
      late final String decoded;
      try {
        decoded = utf8
            .decode(base64.decode(padded), allowMalformed: false)
            .replaceFirst('\ufeff', '');
      } on FormatException {
        throw const FormatException('Base64 内容不是有效的 UTF-8 文本；请确认授权码复制完整');
      }
      try {
        value = jsonDecode(decoded);
      } on FormatException {
        // 不展示解码文本，因为其中可能包含 AccessKey Secret。
        throw const FormatException(
          'Base64 已解码，但内容不是合法 JSON。请确认授权码由完整 JSON 按 UTF-8 编码后生成',
        );
      }
    }
    if (value is! Map) {
      throw const FormatException('授权码内容必须是 JSON 对象');
    }
    final json = Map<String, dynamic>.from(value);

    // 兼容网页授权码 JSON 与从 batch 配置整理出来的 JSON 字段名。
    String fieldAnyCase(List<String> names) {
      final wanted = names.map((name) => name.toLowerCase()).toSet();
      for (final entry in json.entries) {
        if (wanted.contains(entry.key.toString().toLowerCase()) &&
            entry.value is String &&
            (entry.value as String).trim().isNotEmpty) {
          return (entry.value as String).trim();
        }
      }
      return '';
    }

    final accessKeyId = fieldAnyCase([
      'accessKeyId',
      'access_key_id',
      'id',
      'OSS_ACCESS_KEY_ID',
    ]);
    final accessKeySecret = fieldAnyCase([
      'accessKeySecret',
      'access_key_secret',
      'secret',
      'OSS_ACCESS_KEY_SECRET',
    ]);
    final region = fieldAnyCase(['region', 'OSS_REGION']);
    final endpoint = fieldAnyCase([
      'endpoint',
      'eptpl', // 部分 OSS 浏览器授权码使用此字段名。
      'endpointTemplate',
      'OSS_ENDPOINT',
    ]).replaceAll(RegExp(r'\s+'), '');
    final ossPath = fieldAnyCase([
      'osspath',
      'ossPath',
      'path',
      'bucketPath',
      'OSS_ROOT',
    ]);
    final uri = _parseOssPath(ossPath);
    if (accessKeyId.isEmpty || accessKeySecret.isEmpty) {
      throw const FormatException('授权码缺少 AccessKey ID 或 Secret');
    }
    if (uri.$1.isEmpty) {
      throw const FormatException('授权码缺少有效的 oss://Bucket/前缀');
    }

    final resolvedEndpoint = endpoint.isNotEmpty
        ? endpoint
        : _endpointFromRegion(region);
    if (resolvedEndpoint.isEmpty) {
      throw const FormatException('授权码缺少 Endpoint，且无法从 Region 推导');
    }
    return OssAuthorization(
      accessKeyId: accessKeyId,
      accessKeySecret: accessKeySecret,
      region: region,
      endpoint: resolvedEndpoint,
      bucket: uri.$1,
      prefix: uri.$2,
      securityToken: fieldAnyCase(['securityToken', 'security_token', 'token']),
      description: fieldAnyCase(['desc', 'description']),
    );
  }

  static (String, String) _parseOssPath(String value) {
    final match = RegExp(
      r'^oss://([^/?#]+)(?:/([^?#]*))?/?$',
    ).firstMatch(value);
    if (match == null) return ('', '');
    final bucket = match[1]!.trim();
    var prefix = (match[2] ?? '').replaceAll(RegExp(r'^/+|/+$'), '');
    try {
      prefix = Uri.decodeComponent(prefix);
    } on ArgumentError {
      // 未编码的中文/空格以及不完整的百分号序列都应保留原文，不能让
      // 一个对象前缀阻断整批上传。
    }
    if (!RegExp(r'^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$').hasMatch(bucket)) {
      return ('', '');
    }
    return (bucket, prefix);
  }

  static String _endpointFromRegion(String region) {
    if (region.isEmpty) return '';
    final normalized = region.startsWith('oss-') ? region : 'oss-$region';
    return 'https://$normalized.aliyuncs.com';
  }
}
