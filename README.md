# Dart port
Port agenda 0.6.16 .
Light weight job scheduler for Dart
Original https://github.com/rschmukler/agenda

# Agenda
Agenda is a light-weight job scheduling library for Node.js.

It offers:

- Minimal overhead. Agenda aims to keep its code base small.
- Mongo backed persistance layer.
- Scheduling with configurable priority, concurrency, and repeating
- Event backed job queue that you can hook into.
- Optional standalone web-interface (see [agenda-ui](https://github.com/moudy/agenda-ui))

# ToDo
- Scheduling via cron or human readable syntax.

# Example Usage

```dart
var connStr = 'mongodb://localhost/agenda-test';
var agenda = new Agenda();
agenda.database(connStr).then((_) {
  agenda.define('5 minutes job', {}, (job) {
    print("Running 5 minutes job");
    return new Future.value(null);
  });

  agenda.define('10 minutes job', {}, (job) {
    print("Running 10 minutes job");
    return new Future.value(null);
  });

  agenda.define('once a day job', {}, (job) {
    print('ONCE A DAY RUNNING');
    return new Future.value(null);
  });

  agenda.every(new Duration(minutes: 5)/*'5 minutes'*/, '5 minutes job');
  agenda.every(new Duration(minutes: 10)/*'10 minutes'*/, '10 minutes job');
  agenda.every(new Duration(days: 1)/* '0 5 * * 1-5'*/, 'once a day job');

  agenda.start();
});
```
