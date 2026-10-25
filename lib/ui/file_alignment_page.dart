import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:path/path.dart' as p;

import '../services/file_alignment_service.dart';

/// 本地文件与 Excel 预期名称的拖动对齐、分组和安全移动页面。
class FileAlignmentPage extends StatefulWidget {
  const FileAlignmentPage({super.key});

  @override
  State<FileAlignmentPage> createState() => _FileAlignmentPageState();
}

class _FileAlignmentPageState extends State<FileAlignmentPage> {
  final _service = FileAlignmentService();
  final _groupController = TextEditingController();
  String? _rootPath;
  List<File> _files = const [];
  List<AlignmentRow> _rows = const [];
  final Map<int, File> _matches = {};
  final Map<int, String> _groups = {};
  final List<String> _groupNames = [];
  bool _busy = false;
  String? _error;
  String _phase = 'align';

  @override
  void dispose() {
    _groupController.dispose();
    super.dispose();
  }

  Future<void> _chooseFolder() async {
    final path = await FilePicker.platform.getDirectoryPath(
      dialogTitle: '选择待整理的本地文件夹',
    );
    if (path == null || path.isEmpty) return;
    setState(() {
      _rootPath = path;
      _error = null;
      _busy = true;
    });
    try {
      final files = await _service.listFiles(path);
      if (mounted) setState(() => _files = files);
    } catch (e) {
      if (mounted) setState(() => _error = '读取文件夹失败：$e');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _importExcel() async {
    final result = await FilePicker.platform.pickFiles(
      dialogTitle: '导入 Excel 对齐表',
      type: FileType.custom,
      allowedExtensions: const ['xlsx'],
    );
    final path = result?.files.single.path;
    if (path == null) return;
    try {
      final rows = _service.readXlsx(File(path));
      setState(() {
        _rows = rows;
        _matches.clear();
        _groups
          ..clear()
          ..addEntries([
            for (var i = 0; i < rows.length; i++) MapEntry(i, rows[i].group),
          ]);
        _groupNames
          ..clear()
          ..addAll(rows.map((e) => e.group).where((e) => e.isNotEmpty).toSet());
        _error = rows.isEmpty ? 'Excel 中没有可用的预期名称' : null;
      });
    } catch (e) {
      setState(() => _error = 'Excel 解析失败：$e');
    }
  }

  Future<void> _downloadTemplate() async {
    final path = await FilePicker.platform.saveFile(
      dialogTitle: '保存 Excel 模板',
      fileName: '文件对齐模板.xlsx',
      type: FileType.custom,
      allowedExtensions: const ['xlsx'],
    );
    if (path == null) return;
    await _service.writeTemplate(path);
    if (mounted)
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('模板已保存')));
  }

  void _nextToGroups() {
    if (_rootPath == null || _files.isEmpty) {
      setState(() => _error = '请先选择包含文件的本地文件夹');
      return;
    }
    if (_rows.isEmpty) {
      setState(() => _error = '请先导入 Excel');
      return;
    }
    if (_matches.length != _rows.length) {
      setState(
        () => _error = '还有 ${_rows.length - _matches.length} 个预期名称未完成对齐',
      );
      return;
    }
    setState(() {
      _phase = 'groups';
      _error = null;
    });
  }

  Future<void> _apply() async {
    final root = _rootPath;
    if (root == null) return;
    setState(() {
      _busy = true;
      _error = null;
    });
    try {
      final pairs = [
        for (var i = 0; i < _rows.length; i++)
          AlignmentPair(
            source: _matches[i]!,
            expectedName: _rows[i].expectedName,
            group: _groups[i] ?? '',
          ),
      ];
      final moved = await _service.apply(root: root, pairs: pairs);
      if (!mounted) return;
      await showDialog<void>(
        context: context,
        builder: (context) => AlertDialog(
          title: const Text('整理完成'),
          content: Text('已移动并重命名 ${moved.length} 个文件。\n原文件未删除，扩展名保持不变。'),
          actions: [
            FilledButton(
              onPressed: () => Navigator.pop(context),
              child: const Text('完成'),
            ),
          ],
        ),
      );
      if (mounted) Navigator.pop(context);
    } catch (e) {
      if (mounted) setState(() => _error = '操作未完成：$e');
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  void _addGroup() {
    final name = _groupController.text.trim();
    if (name.isEmpty || _groupNames.contains(name)) return;
    setState(() {
      _groupNames.add(name);
      _groupController.clear();
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('文件对齐与分组整理'),
        actions: [
          TextButton.icon(
            onPressed: _downloadTemplate,
            icon: const Icon(Icons.download_outlined),
            label: const Text('下载 Excel 模板'),
          ),
        ],
      ),
      body: Padding(
        padding: const EdgeInsets.all(20),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            _header(),
            if (_error != null) ...[
              const SizedBox(height: 10),
              Text(
                _error!,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              ),
            ],
            const SizedBox(height: 12),
            Expanded(
              child: _phase == 'align' ? _alignmentView() : _groupView(),
            ),
          ],
        ),
      ),
    );
  }

  Widget _header() => Wrap(
    spacing: 10,
    runSpacing: 8,
    crossAxisAlignment: WrapCrossAlignment.center,
    children: [
      OutlinedButton.icon(
        onPressed: _busy ? null : _chooseFolder,
        icon: const Icon(Icons.folder_open_outlined),
        label: Text(_rootPath == null ? '选择本地文件夹' : p.basename(_rootPath!)),
      ),
      OutlinedButton.icon(
        onPressed: _busy ? null : _importExcel,
        icon: const Icon(Icons.table_view_outlined),
        label: const Text('导入 Excel'),
      ),
      Text(_files.isEmpty ? '未读取文件' : '${_files.length} 个本地文件'),
      Text(_rows.isEmpty ? '未导入预期名称' : '${_rows.length} 个预期名称'),
    ],
  );

  Widget _alignmentView() => Column(
    children: [
      Expanded(
        child: Row(
          children: [
            Expanded(child: _fileList()),
            const SizedBox(width: 16),
            Expanded(child: _expectedList()),
          ],
        ),
      ),
      const SizedBox(height: 12),
      Align(
        alignment: Alignment.centerRight,
        child: FilledButton.icon(
          onPressed: _busy ? null : _nextToGroups,
          icon: const Icon(Icons.arrow_forward),
          label: const Text('确认对齐，进入分组'),
        ),
      ),
    ],
  );

  Widget _fileList() => Card(
    child: Column(
      children: [
        const ListTile(title: Text('本地文件'), subtitle: Text('拖动文件到右侧预期名称')),
        const Divider(height: 1),
        Expanded(
          child: ListView.builder(
            itemCount: _files.length,
            itemBuilder: (context, index) {
              final file = _files[index];
              final used = _matches.values.contains(file);
              return Draggable<File>(
                data: file,
                feedback: Material(
                  child: Chip(label: Text(p.basename(file.path))),
                ),
                child: ListTile(
                  dense: true,
                  leading: Icon(
                    used
                        ? Icons.check_circle
                        : Icons.insert_drive_file_outlined,
                    color: used ? Colors.green : null,
                  ),
                  title: Text(
                    p.basename(file.path),
                    overflow: TextOverflow.ellipsis,
                  ),
                  subtitle: Text(used ? '已对齐' : '可拖动'),
                ),
              );
            },
          ),
        ),
      ],
    ),
  );

  Widget _expectedList() => Card(
    child: Column(
      children: [
        const ListTile(
          title: Text('Excel 预期名称'),
          subtitle: Text('拖入文件后点击确认对齐'),
        ),
        const Divider(height: 1),
        Expanded(
          child: ListView.builder(
            itemCount: _rows.length,
            itemBuilder: (context, index) {
              final row = _rows[index];
              final file = _matches[index];
              return DragTarget<File>(
                onAcceptWithDetails: (details) =>
                    setState(() => _matches[index] = details.data),
                builder: (context, candidates, rejected) => ListTile(
                  dense: true,
                  tileColor: candidates.isNotEmpty
                      ? Theme.of(context).colorScheme.primaryContainer
                      : null,
                  leading: const Icon(Icons.drive_file_rename_outline),
                  title: Text(row.expectedName),
                  subtitle: Text(
                    file == null ? '拖动文件到这里' : '← ${p.basename(file.path)}',
                  ),
                  trailing: row.group.isEmpty
                      ? null
                      : Chip(label: Text(row.group)),
                ),
              );
            },
          ),
        ),
      ],
    ),
  );

  Widget _groupView() => Card(
    child: Padding(
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          const Text(
            '设计分组',
            style: TextStyle(fontSize: 18, fontWeight: FontWeight.w600),
          ),
          const SizedBox(height: 4),
          const Text('确认后将在当前本地文件夹内新建分组目录，并移动文件；不会删除文件。'),
          const SizedBox(height: 12),
          Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _groupController,
                  decoration: const InputDecoration(
                    labelText: '新增分组名称',
                    hintText: '例如：第一组',
                  ),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton(onPressed: _addGroup, child: const Text('添加')),
            ],
          ),
          const SizedBox(height: 12),
          Expanded(
            child: ListView.builder(
              itemCount: _rows.length,
              itemBuilder: (context, index) => ListTile(
                title: Text(_rows[index].expectedName),
                subtitle: Text(p.basename(_matches[index]!.path)),
                trailing: DropdownButton<String>(
                  value: _groups[index]?.isEmpty ?? true
                      ? null
                      : _groups[index],
                  hint: const Text('不分组'),
                  items: [
                    const DropdownMenuItem(value: '', child: Text('不分组')),
                    ..._groupNames.map(
                      (e) => DropdownMenuItem(value: e, child: Text(e)),
                    ),
                  ],
                  onChanged: (value) =>
                      setState(() => _groups[index] = value ?? ''),
                ),
              ),
            ),
          ),
          FilledButton.icon(
            onPressed: _busy ? null : _apply,
            icon: const Icon(Icons.folder_copy_outlined),
            label: const Text('确认分组并整理文件'),
          ),
        ],
      ),
    ),
  );
}
