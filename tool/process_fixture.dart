import 'dart:async';
import 'dart:io';

Future<void> main(List<String> args) async {
  if (args.isEmpty) exitCode = 2;
  switch (args.first) {
    case 'echo':
      stdout.writeln('ARG:${args.skip(1).join('|')}');
      return;
    case 'utf8-lines':
      stdout.add([0xe4, 0xb8, 0xad, 0xe6, 0x96, 0x87, 0x0d]);
      await Future<void>.delayed(Duration.zero);
      stdout.add([0x0a, 0xe5, 0xae, 0x8c, 0xe6, 0x88, 0x90]);
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
