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
    try {
      final trimmed = input.trim();
      if (trimmed.startsWith('{')) {
        value = jsonDecode(trimmed);
      } else {
        final compact = trimmed
            .replaceFirst(RegExp(r'^data:.*?;base64,'), '')
            .replaceAll(RegExp(r'\s+'), '');
        if (compact.isEmpty) throw const FormatException('授权码不能为空');
        final base64Text = compact.replaceAll('-', '+').replaceAll('_', '/');
        if (base64Text.length % 4 == 1) {
          throw const FormatException('授权码不是有效的 Base64 内容');
        }
        final padded = base64Text.padRight(
          base64Text.length + ((4 - base64Text.length % 4) % 4),
          '=',
        );
        final decoded = utf8
            .decode(base64.decode(padded), allowMalformed: false)
            .replaceFirst('\ufeff', '');
        value = jsonDecode(decoded);
      }
    } on Object {
      // 不把原文或解码后的内容放进异常，避免凭证被日志意外记录。
      throw const FormatException('授权码无法解码，请确认复制完整且为 UTF-8 JSON');
    }
    if (value is! Map) {
      throw const FormatException('授权码内容必须是 JSON 对象');
    }
    final json = Map<String, dynamic>.from(value);

    String field(List<String> names) {
      for (final name in names) {
        final value = json[name];
        if (value is String && value.trim().isNotEmpty) return value.trim();
      }
      return '';
    }

    final accessKeyId = field(['accessKeyId', 'access_key_id', 'id']);
    final accessKeySecret = field([
      'accessKeySecret',
      'access_key_secret',
      'secret',
    ]);
    final region = field(['region']);
    final endpoint = field([
      'endpoint',
      'eptpl', // 部分 OSS 浏览器授权码使用此字段名。
      'endpointTemplate',
    ]);
    final ossPath = field(['osspath', 'ossPath', 'path', 'bucketPath']);
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
      securityToken: field(['securityToken', 'security_token', 'token']),
      description: field(['desc', 'description']),
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
