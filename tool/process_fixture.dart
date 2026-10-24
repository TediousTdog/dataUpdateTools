import 'dart:async';
import 'dart:io';

Future<void> main(List<String> args) async {
  if (args.isEmpty) exitCode = 2;
  switch (args.first) {
    case 'echo':
      stdout.writeln('ARG:${args.skip(1).join('|')}');
      return;
    case 'wait':
      stdout.writeln('READY');
      await Completer<void>().future;
      return;
    default:
      exitCode = 2;
      return;
  }
}
