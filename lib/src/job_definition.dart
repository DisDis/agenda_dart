part of agenda;

class JobDefinition {
  final JobProcessor fn;
  final int concurrency;
  final int priority;
  final Duration lockLifetime;
  int running;
  JobDefinition({this.fn, this.concurrency, this.priority, this.lockLifetime, this.running});
}
