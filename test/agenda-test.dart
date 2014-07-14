import '../lib/agenda.dart';
import 'dart:async';

main(){
var connStr = 'mongodb://localhost/agenda-test';
var agenda = new Agenda();
agenda.database(connStr).then((_){
agenda.define('5 minutes job', {},(job) {
  print("Running 5 minutes job");
  return new Future.value(null);
});

agenda.define('10 minutes job',{}, (job) {
  print("Running 10 minutes job");
  return new Future.value(null);
});

agenda.define('once a day job',{}, (job) {
  print('ONCE A DAY RUNNING');
  return new Future.value(null);
});

agenda.every(new Duration(minutes: 5)/*'5 minutes'*/, '5 minutes job');
agenda.every(new Duration(minutes:10)/*'10 minutes'*/, '10 minutes job');
agenda.every(new Duration(days: 1)/* '0 5 * * 1-5'*/, 'once a day job');

agenda.start();
});
}