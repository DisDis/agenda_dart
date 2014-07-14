library agenda;

import "package:mongo_dart/mongo_dart.dart" as mongo;
import 'dart:async';
part 'src/job.dart';
part 'src/job_definition.dart';

typedef Future JobProcessor(Job job);


class Agenda {
  String _name;
  set name(name) {
    _name = name;
  }
  String get name => _name;
  StreamController<Job> _fail = new StreamController.broadcast();
  StreamController<Job> _success = new StreamController.broadcast();
  StreamController<Job> _start = new StreamController.broadcast();
  StreamController<Job> _complete = new StreamController.broadcast();

  DateTime _nextScanAt;
  Duration _processEvery;
  Timer _processInterval;
  int _defaultConcurrency;
  int _maxConcurrency;
  final Map<String, JobDefinition> _definitions = {};
  final List<Job> _runningJobs = [];
  final List<Job> _jobQueue = [];
  Duration _defaultLockLifetime;

  mongo.DbCollection _db;
  Agenda({name, processEvery, defaultConcurrency: 5, maxConcurrency: 20, //this._definitions : {},
  //this._runningJobs : [],
  //this._jobQueue : [],
  defaultLockLifetime
}) {
    _maxConcurrency = maxConcurrency;
    _defaultConcurrency = defaultConcurrency;
    if (processEvery == null) {
      _processEvery = new Duration(seconds: 5);
    }
    if (defaultLockLifetime == null) {
      defaultLockLifetime = new Duration(minutes: 10);
    }
    _defaultLockLifetime = defaultLockLifetime;
  }
//utils.inherits(Agenda, Emitter);

// Configuration Methods

/*mongo (mongo.DbCollection db) {
  _db = db;
}*/

 Future database(String url,[ String collection]) {
    if (collection == null) {
      collection = 'agendaJobs';
    }
    if (url.indexOf('mongodb://') == -1) { //!url.match(/^mongodb:\/\/.*/)
      url = 'mongodb://' + url;
    }
    var db = new mongo.Db(url/*, {w: 0}*/);
    _db = db.collection(collection);
    return db.open(writeConcern: mongo.WriteConcern.UNACKNOWLEDGED);
  }


  set processEvery(Duration time) {
    _processEvery = time;
  }

  set maxConcurrency(int num) {
    _maxConcurrency = num;
  }

  set defaultConcurrency(int num) {
    _defaultConcurrency = num;
  }

  set defaultLockLifetime(Duration duration) {
    _defaultLockLifetime = duration;
  }

// Job Methods
  Job create(String name, data) {
    var priority = _definitions[name] != null ? _definitions[name].priority : 0;
    var job = new Job(name: name, data: data, type: 'normal', priority: priority, agenda: this);
    return job;
  }

  jobs() {
    throw new Exception("not implement");
    /*var args = Array.prototype.slice.call(arguments);//Clone args

  if(typeof args[args.length - 1] == 'function') {
    args.push(findJobsResultWrapper(this, args.pop()));
  }

  return _db.findItems.apply(this._db, args);*/
  }

  Future purge() {
    var definedNames = _definitions.keys;
    return _db.remove({
      "name": {
        r"$not": {
          r"$in": definedNames
        }
      }
    });
  }

  define(String name, Map options, JobProcessor processor) {
    if (options["concurrency"] == null) {
      options["concurrency"] = _defaultConcurrency;
    }
    if (options["priority"] == null) {
      options["priority"] = 0;
    }
    if (options["lockLifetime"] == null) {
      options["lockLifetime"] = _defaultLockLifetime;
    }
    _definitions[name] = new JobDefinition(fn: processor, concurrency: options["concurrency"], priority: options["priority"], lockLifetime: options["lockLifetime"], running: 0);
  }

  every(Duration interval, names, [data]) {

    createJob(Duration interval, String name, data) {
      var job;
      job = create(name, data);
      job._type = 'single';
      job.repeatEvery(interval);
      job.save();
      return job;
    }

    createJobs(Duration interval, List names, data) {
      return names.map((name) {
        return createJob(interval, name, data);
      });
    }

    if (names is String) {
      return createJob(interval, names, data);
    } else if (names is List) {
      return createJobs(interval, names, data);
    }


  }

  schedule(when, names, data) {

    createJob(when, name, data) {
      var job = create(name, data);
      job.schedule(when);
      job.save();
      return job;
    }

    createJobs(when, names, data) {
      return names.map((name) {
        return createJob(when, name, data);
      });
    }

    if (names is String) {
      return createJob(when, names, data);
    } else if (names is List) {
      return createJobs(when, names, data);
    }
  }

  Job now(name, data) {
    var job = this.create(name, data);
    job.schedule(new DateTime.now());
    job.save();
    return job;
  }

  Future cancel(query) {
    return _db.remove(query);
  }

  Future saveJob(Job job) {
    job._lastModifiedBy =_name;
    var props = job.toJSON();
    var id = job._id;

    props.remove("_id");//delete props._id;

    //props["lastModifiedBy"] = _name;

    var now = new DateTime.now();
    var protect = {};
    var update = {
      r"$set": props
    };

    Job processDbResult(res) {
      if (res is List) {
        res = res[0];
      }

      job._id = res["_id"];
      job._nextRunAt = res["nextRunAt"];

      if (job._nextRunAt != null && job._nextRunAt.millisecondsSinceEpoch < _nextScanAt.millisecondsSinceEpoch) {
        processJobs(job);
      }
      return job;
    }

    if (id != null) {
      return _findAndModify(query: {
        "_id": id
      }, sort: {}, update: update, newFlag: true).then(processDbResult);
    } else if (props["type"] == 'single') {
      if (props["nextRunAt"] != null && (props["nextRunAt"] as DateTime).compareTo(now) <= 0) { //props.nextRunAt <= now
        protect["nextRunAt"] = props["nextRunAt"];
        props.remove("nextRunAt"); //delete props.nextRunAt;
      }
      if (protect.keys.length > 0) {
        update[r"$setOnInsert"] = protect;
      }
      // Try an upsert.
      return _findAndModify(query: {
        "name": props["name"],
        "type": 'single'
      }, sort: {}, update: update, upsert: true, newFlag: true).then(processDbResult);
    } else {
      return _db.insert(props).then(processDbResult);
    }


  }

// Job Flow Methods

  start() {
    if (_processInterval == null) {
      _processInterval = new Timer.periodic(_processEvery, (_) {
        processJobs();
      });
      scheduleMicrotask(() {
        processJobs();
      });// process.nextTick(processJobs.bind(this));
    }
  }

  stop(cb) {
    if (_processInterval != null) {
      _processInterval.cancel();
    }
    _processInterval = null;
    unlockJobs(cb);
  }


  Future _findAndModify({Map query, Map sort, Map update, Map fields, bool upsert: false, bool newFlag: false, bool remove: false}) {
    var msg = {};
    msg["findAndModify"] = _db.collectionName;
    msg["query"] = query;
    msg["sort"] = sort;
    msg["update"] = update;
    msg["fields"] = fields;
    msg["upsert"] = upsert;
    msg["new"] = newFlag;
    msg["remove"] = remove;
    return _db.db.executeDbCommand(mongo.DbCommand.createQueryDbCommand(_db.db, msg));
  }

/**
 * Find and lock jobs
 * @param {String} jobName
 * @param {Function} cb
 * @protected
 */
  Future _findAndLockNextJob(String jobName, JobDefinition definition) {
    var now = new DateTime.now();
    var lockDeadline = new DateTime.now().subtract((definition.lockLifetime));


    return _findAndModify(query: {
      "nextRunAt": {
        r"$lte": _nextScanAt
      },
      r"$or": [{
          "lockedAt": null
        }, {
          "lockedAt": {
            r"$exists": false
          }
        }, {
          "lockedAt": {
            r"$lte": lockDeadline
          }
        }],
      "name": jobName
    }, sort: {
      'priority': -1
    }, update: {
      r"$set": {
        r"lockedAt": now
      }
    }, newFlag: true).then((value)=>findJobsResultWrapper(value["value"]));
  }


/**
 * Create Job object from data
 * @param {Object} agenda
 * @param {Object} jobData
 * @return {Job}
 * @private
 */
  Job createJob(Map jobData) {
    jobData["agenda"] = this;
    return Job.parseJSON(jobData);
  }

  Future unlockJobs(done) {
    int getJobId(j) => j._id;
    var jobIds = _jobQueue.map(getJobId).toList(growable: true);
    jobIds.addAll(_runningJobs.map(getJobId));
    return _db.update({
      "_id": {
        r"$in": jobIds
      }
    }, {
      r"$set": {
        "lockedAt": null
      }
    }, multiUpdate: true);
  }

  processJobs([Job extraJob]) {
    var jobName;

    jobProcessing() {
      if (_jobQueue.length == 0) {
        return;
      }
      var now = new DateTime.now();
      var job = _jobQueue.removeLast();//_jobQueue.pop();
      var name = job.name;
      var jobDefinition = _definitions[name];

      runOrRetry() {
        if (_processInterval != null) {
          if (jobDefinition.concurrency > jobDefinition.running && _runningJobs.length < _maxConcurrency) {

            _runningJobs.add(job);
            jobDefinition.running++;

            job.run().then((Job job) {
              var name = job.name;

              _runningJobs.remove(job);
              _definitions[name].running--;

              jobProcessing();
            });
            jobProcessing();
          } else {
            // Put on top to run ASAP
            _jobQueue.add(job);
          }
        }
      }



      if (job._nextRunAt == null || job._nextRunAt.compareTo(now) < 0) {
        runOrRetry();
      } else {
        new Timer(now.difference(job._nextRunAt), runOrRetry);//setTimeout(runOrRetry, job._nextRunAt - now);
      }


    }

    jobQueueFilling(String name) {
      var now = new DateTime.now();
      _nextScanAt = now.add(_processEvery);
      _findAndLockNextJob(name, _definitions[name]).then((Job job) {
        if (job != null) {
          _jobQueue.insert(0, job); //_jobQueue.unshift(job);
          jobQueueFilling(name);
          jobProcessing();
        }
      });
    }




    if (extraJob == null) {
      _definitions.keys.forEach((jobName) {
        jobQueueFilling(jobName);
      });
    } else {
      // On the fly lock a job
      var now = new DateTime.now();
      _findAndModify(query: {
        "_id": extraJob._id,
        "lockedAt": null
      }, sort: {}, update: {
        r"$set": {
          "lockedAt": now
        }
      }).then((resp) {
        if (resp != null) {
          _jobQueue.insert(0, extraJob);//_jobQueue.unshift(extraJob);
          jobProcessing();
        }
      });
    }


  }

/**
 *
 * @param agenda
 * @return {Function}
 * @private
 */
  findJobsResultWrapper(jobs) {
    if (jobs!=null) {
      //query result can be array or one record
      if (jobs is List) {
        jobs = jobs.map(createJob);
      } else {
        jobs = createJob(jobs);
      }
    }
    return jobs;
  }
}


