import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:untitled/models/app_config.dart';

void main() {
  test('manual OSS prefix overrides the authorization code prefix', () {
    final authorization = base64.encode(
      utf8.encode(
        jsonEncode({
          'id': 'example-id',
          'secret': 'example-secret',
          'region': 'oss-cn-shanghai',
          'osspath': 'oss://example-bucket/default/path',
        }),
      ),
    );

    final resolved = AppConfig(
      authorizationCode: authorization,
      prefix: '照片/2026/10',
    ).resolveAuthorization();

    expect(resolved.bucket, 'example-bucket');
    expect(resolved.prefix, '照片/2026/10');
    expect(resolved.accessKeyId, 'example-id');
  });

  test('authorization prefix is used when manual OSS prefix is empty', () {
    final authorization = base64.encode(
      utf8.encode(
        jsonEncode({
          'id': 'example-id',
          'secret': 'example-secret',
          'region': 'oss-cn-shanghai',
          'osspath': 'oss://example-bucket/default/path',
        }),
      ),
    );

    final resolved = AppConfig(
      authorizationCode: authorization,
    ).resolveAuthorization();

    expect(resolved.prefix, 'default/path');
  });
}
