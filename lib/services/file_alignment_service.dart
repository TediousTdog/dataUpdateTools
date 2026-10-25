import 'dart:io';

import 'package:excel/excel.dart';
import 'package:path/path.dart' as p;

class AlignmentRow {
  const AlignmentRow({required this.expectedName, this.group = ''});
  final String expectedName;
  final String group;
}

class AlignmentPair {
  const AlignmentPair({
    required this.source,
    required this.expectedName,
    this.group = '',
  });
  final File source;
  final String expectedName;
  final String group;
}

/// Excel 导入、模板生成和最终移动/重命名逻辑。
/// 所有操作都使用 rename（移动），不会删除源文件；目标文件存在时直接报错。
class FileAlignmentService {
  List<AlignmentRow> readXlsx(File file) {
    final book = Excel.decodeBytes(file.readAsBytesSync());
    if (book.tables.isEmpty) throw const FormatException('Excel 中没有工作表');
    final sheet = book.tables.values.first;
    if (sheet.rows.isEmpty) throw const FormatException('Excel 中没有数据');
    final rows = sheet.rows.map((row) => row.map(_cellText).toList()).toList();
    var nameColumn = 0;
    var groupColumn = -1;
    final header = rows.first.map((e) => e.toLowerCase()).toList();
    for (var i = 0; i < header.length; i++) {
      if (const {
        '预期名称',
        '文件名',
        '名称',
        'expectedname',
        'filename',
      }.contains(header[i]))
        nameColumn = i;
      if (const {'分组', '组名', 'group', 'folder'}.contains(header[i]))
        groupColumn = i;
    }
    final start =
        header.any(
          (e) =>
              e == '预期名称' ||
              e == '文件名' ||
              e == '名称' ||
              e == 'expectedname' ||
              e == 'filename',
        )
        ? 1
        : 0;
    return [
      for (final row in rows.skip(start))
        if (row.length > nameColumn && row[nameColumn].trim().isNotEmpty)
          AlignmentRow(
            expectedName: row[nameColumn].trim(),
            group: groupColumn >= 0 && row.length > groupColumn
                ? row[groupColumn].trim()
                : '',
          ),
    ];
  }

  Future<void> writeTemplate(String path) async {
    final book = Excel.createExcel();
    final sheet = book[book.getDefaultSheet() ?? 'Sheet1'];
    sheet.appendRow([TextCellValue('预期名称'), TextCellValue('分组')]);
    sheet.appendRow([TextCellValue('示例文件名'), TextCellValue('第一组')]);
    final bytes = book.encode();
    if (bytes == null) throw StateError('无法生成 Excel 模板');
    await File(path).writeAsBytes(bytes, flush: true);
  }

  Future<List<File>> listFiles(String directory) async =>
      (await Directory(
            directory,
          ).list(followLinks: false).where((e) => e is File).toList())
          .whereType<File>()
          .toList();

  Future<List<File>> apply({
    required String root,
    required List<AlignmentPair> pairs,
  }) async {
    final moved = <File>[];
    final staged = <({File temp, File destination})>[];
    final sourcePaths = pairs
        .map((pair) => p.normalize(pair.source.path))
        .toSet();
    final destinationPaths = <String>{};
    for (final pair in pairs) {
      final group = _cleanSegment(pair.group);
      final folder = group.isEmpty
          ? Directory(root)
          : Directory(p.join(root, group));
      await folder.create(recursive: true);
      final extension = p.extension(pair.source.path);
      final base = p.basenameWithoutExtension(pair.expectedName);
      if (base.trim().isEmpty) throw ArgumentError('预期名称不能为空');
      final destination = File(p.join(folder.path, '$base$extension'));
      if (!destinationPaths.add(p.normalize(destination.path))) {
        throw StateError('多个预期名称会生成同一个目标文件：${destination.path}');
      }
      if (p.equals(pair.source.path, destination.path)) {
        moved.add(destination);
        continue;
      }
      if (await destination.exists() &&
          !sourcePaths.contains(p.normalize(destination.path)))
        throw StateError('目标文件已存在：${destination.path}');
      final temporary = File(
        p.join(
          p.dirname(pair.source.path),
          '.media_ingest_${DateTime.now().microsecondsSinceEpoch}_${staged.length}.tmp',
        ),
      );
      await pair.source.rename(temporary.path);
      staged.add((temp: temporary, destination: destination));
    }
    for (final item in staged) {
      moved.add(await item.temp.rename(item.destination.path));
    }
    return moved;
  }

  String _cellText(Data? cell) => switch (cell?.value) {
    TextCellValue value => value.value.text ?? value.toString(),
    IntCellValue value => '${value.value}',
    DoubleCellValue value => '${value.value}',
    BoolCellValue value => '${value.value}',
    FormulaCellValue value => value.formula,
    _ => '',
  };

  String _cleanSegment(String value) {
    final cleaned = value.trim().replaceAll(RegExp(r'[\\/:*?"<>|]'), '_');
    if (cleaned == '.' || cleaned == '..') return '_';
    return cleaned;
  }
}
