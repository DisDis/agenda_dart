part of agenda;

class Job {
  //var humanInterval = require('human-interval'),
  //  CronTime = require('cron').CronTime,
  // date = require('date.js');
  final Agenda agenda;

  final String name;
  String _type;
  int _id;
  final data;
  DateTime _nextRunAt;
  DateTime _lastRunAt;
  DateTime _lastFinishedAt;
  DateTime _failedAt;
  DateTime _lockedAt;
  String _lastModifiedBy;
  Duration repeatInterval = new Duration();
  int _priority;
  var _failReason;
  bool _scheduled;

  Job({this.agenda, int priority: 0, String this.name, this.data, String type: 'once', DateTime nextRunAt}) {
    // Process args
    _priority = parsePriority(priority);
    if (_priority == null) {
      _priority = 0;
    }
    if (nextRunAt == null) {
      _nextRunAt = new DateTime.now();
    }
    _type = type;
  }
  
  static Job parseJSON(Map json){
    var job = new Job(agenda: json["agenda"],name: json['name'],priority: json["priority"],type:json["type"],nextRunAt:json["nextRunAt"],data:json["data"]); 
    job._lastRunAt = json['lastRunAt'];
    job._failedAt = json['failedAt'];
    job._lastFinishedAt = json['lastFinishedAt'];
    job._lockedAt = json['lockedAt'];
    job._priority = json['priority'];
    job._lastModifiedBy = json['lastModifiedBy'];
    job._failReason = json['failReason'];
    job.repeatInterval = new Duration(milliseconds: json['repeatInterval']);
    return job;
  }

  Map toJSON() { // create a persistable Mongo object -RR
    var result = {
      '_id': _id,
      'name': name,
      'priority': _priority,
      'lastRunAt': _lastRunAt,
      'lastFinishedAt': _lastFinishedAt,
      'nextRunAt': _nextRunAt,
      'failedAt': _failedAt,
      'lockedAt': _lockedAt,
      'failReason': _failReason,
        "data" : data,
        "lastModifiedBy" : _lastModifiedBy,
        "repeatInterval" : repeatInterval.inMilliseconds,
        "type" : _type
    };

    return result;
  }

  computeNextRunAt() {
    Duration interval = repeatInterval;
    _nextRunAt = null;

    if (interval != null) {
      // Check if its a cron string
      DateTime lastRun = _lastRunAt;
      if (_lastRunAt == null) {
        lastRun = new DateTime.now();
      }
      try {
        _nextRunAt = lastRun.add(interval);
      } catch (e) {}
    } else {
      _nextRunAt = null;
    }
    return this;
  }

  repeatEvery(Duration interval) {
    repeatInterval = interval;
  }

  schedule(DateTime time) {
    _scheduled = true;
    _nextRunAt = time;
  }

  priority(priority) {
    _priority = parsePriority(priority);
  }

  fail(reason) {
    if (reason is Exception) {
      reason = reason.message;
    }
    _failReason = reason;
    _failedAt = new DateTime.now();
  }

  Future<Job> run() {
    var definition = agenda._definitions[name];
    //var setImmediate = setImmediate || process.nextTick;
    Completer<Job> completer = new Completer<Job>();
    jobCallback(result) {
      agenda._success.add(this);
      _lastFinishedAt = new DateTime.now();
      _lockedAt = null;
      return save().then((_) {
        completer.complete(this);
        //return result;
      });
    }

    jobError(err) {
      fail(err);
      agenda._fail.add(this);
      _lastFinishedAt = new DateTime.now();
      _lockedAt = null;
      return save().then((_) {
        completer.complete(this);
      });
    }

    scheduleMicrotask(() {
      _lastRunAt = new DateTime.now();
      computeNextRunAt();

      try {
        agenda._start.add(this);
        if (definition == null) {
          throw new Exception('Undefined job');
        }
        definition.fn(this).then(jobCallback).catchError(jobError);
      } catch (e) {
        jobError(e);
      } finally {
        agenda._complete.add(this);
      }
    });
    return completer.future;
  }

  Future save() {
    return agenda.saveJob(this);
  }

  Future remove() {
    return agenda._db.remove({
      "_id": _id
    });
  }

  Future touch() {
    _lockedAt = new DateTime.now();
    return save();
  }

  static final Map<String, int> priorityMap = {
    "lowest": -20,
    "low": -10,
    "normal": 0,
    "high": 10,
    "highest": 20
  };

  static int parsePriority(priority) {
    if (priority is String) {
      int i = priorityMap[priority];
      if (i == null) {
        i = 0;
      }
      return i;
    } else {
      return priority;
    }
  }
}
