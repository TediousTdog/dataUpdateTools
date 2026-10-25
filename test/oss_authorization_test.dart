import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:untitled/models/oss_authorization.dart';

void main() {
  const json =
      '{"id":"example-id","secret":"example-secret",'
      '"region":"oss-cn-shanghai",'
      '"osspath":"oss://example-bucket/照片/2026",'
      '"eptpl":"https://oss-cn-shanghai.aliyuncs.com"}';

  test('parses UTF-8 base64 and tolerates copy-wrapped whitespace', () {
    final encoded = base64.encode(utf8.encode(json));
    final wrapped = '${encoded.substring(0, 20)}\n ${encoded.substring(20)}';

    final authorization = OssAuthorization.parse(wrapped);

    expect(authorization.accessKeyId, 'example-id');
    expect(authorization.bucket, 'example-bucket');
    expect(authorization.prefix, '照片/2026');
  });

  test('parses direct JSON and unpadded URL-safe base64', () {
    final urlSafe = base64Url.encode(utf8.encode(json)).replaceAll('=', '');

    expect(OssAuthorization.parse(json).bucket, 'example-bucket');
    expect(OssAuthorization.parse(urlSafe).prefix, '照片/2026');
  });

  test('accepts batch-style OSS setting names in a Base64 JSON code', () {
    final encoded = base64.encode(
      utf8.encode(
        jsonEncode({
          'OSS_ENDPOINT': 'oss-cn-beijing.aliyuncs.com',
          'OSS_ROOT': 'oss://example-bucket/archive/subfolder',
          'OSS_ACCESS_KEY_ID': 'example-id',
          'OSS_ACCESS_KEY_SECRET': 'example-secret',
        }),
      ),
    );

    final authorization = OssAuthorization.parse(encoded);

    expect(authorization.endpoint, 'oss-cn-beijing.aliyuncs.com');
    expect(authorization.bucket, 'example-bucket');
    expect(authorization.prefix, 'archive/subfolder');
  });

  test('tolerates a copied trailing percent and wrapped endpoint whitespace', () {
    final encoded = base64.encode(utf8.encode(jsonEncode({
      'id': 'example-id',
      'secret': 'example-secret',
      'region': 'oss-cn-beijing',
      'osspath': 'oss://example-bucket/',
      'eptpl': 'https://oss-cn-\n  beijing.aliyuncs.com',
    })));

    final authorization = OssAuthorization.parse('"$encoded%"');

    expect(authorization.endpoint, 'https://oss-cn-beijing.aliyuncs.com');
    expect(authorization.bucket, 'example-bucket');
  });

  test('reports decoding stage without exposing credential contents', () {
    expect(
      () => OssAuthorization.parse('not base64!'),
      throwsA(
        isA<FormatException>().having(
          (error) => error.message,
          'message',
          contains('Base64'),
        ),
      ),
    );
    expect(
      () => OssAuthorization.parse(base64.encode(utf8.encode('not json'))),
      throwsA(
        isA<FormatException>().having(
          (error) => error.message,
          'message',
          contains('不是合法 JSON'),
        ),
      ),
    );
  });
}
