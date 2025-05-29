 EventScheduler {
	var <server;
	var <nodes;
	var <buses;
	var <events;
	var <isPlaying;
	var <startTime;
	var <startLag;
	var <>debug;
	var nodeWatcher;
	var <maxEvents;
	var <batchOverlapRatio;
	var nextEventIndex;
	var lastScheduledTime;
	var nextBatchTask;

	*new { |server, maxEvents=500, batchOverlapRatio=0.7, startLag=0.5, debug=false|
		^super.new.init(server, maxEvents, batchOverlapRatio, startLag, debug)
	}

	init { |serverArg, maxEventsArg, overlapRatioArg, lag, debugFlag|
		server = serverArg;
		maxEvents = maxEventsArg;
		batchOverlapRatio = overlapRatioArg;
		startLag = lag;
		debug = debugFlag;

		events = Array.new;
		nodes = Dictionary.new;
		buses = Dictionary.new;
		isPlaying = false;
		nextEventIndex = 0;

		nodeWatcher = NodeWatcher.new(server);

		this.registerGroup("default", server.defaultGroup);
	}

	log { |message|
		if(debug) { message.postln };
	}

	registerBus { |name, bus, synth|
		buses.put(name, bus);
		if(synth.notNil) {
			nodes.put(name, synth);
		};
		^bus;
	}

	registerGroup { |name, group|
		nodes.put(name, group);
		^group;
	}

	loadFile { |path|
		var file, jsonString, jsonData;

		file = File(path, "r");
		if(file.isOpen.not) {
			"ERROR: Could not open file %".format(path).postln;
			^false;
		};

		jsonString = file.readAllString;
		file.close;

		try {
			jsonData = jsonString.parseJSON;
		} { |error|
			try {
				jsonData = jsonString.parseYAML;
			} { |yamlError|
				"ERROR: Could not parse JSON data".postln;
				^false;
			};
		};

		if(jsonData.isNil || jsonData.isKindOf(Array).not) {
			"ERROR: Invalid JSON data format".postln;
			^false;
		};

		events = jsonData;
		nextEventIndex = 0;

		^true;
	}

	processPfields { |pfields|
		var processedArgs = List.new;

		pfields.keysValuesDo { |key, value|
			var processedKey = key.asSymbol;
			var processedValue;

			case
			{ value.isString } {
				if(buses.includesKey(value)) {
					processedValue = buses.at(value);
				} {
					if(value.every { |c| "0123456789.-".includes(c) }) {
						processedValue = value.asFloat;
					} {
						processedValue = value;
					};
				};
			}
			{ value.isNumber } {
				processedValue = value;
			}
			{ value.isNil } {
				processedValue = 0;
			}
			{
				processedValue = value;
			};

			processedArgs.add(processedKey);
			processedArgs.add(processedValue);
		};

		^processedArgs.asArray;
	}

	scheduleBatch { |startIdx|
		var now = SystemClock.seconds;
		var endIdx = startIdx;
		var nextBatchTime;
		var currentEvent;
		var scheduledCount = 0;
		var eventTime, absTime, relativeTime, nextBatchDelay;

		while({
			(endIdx < events.size) && { endIdx - startIdx < maxEvents }
		}) {
			currentEvent = events[endIdx];

			eventTime = currentEvent["start"].asFloat;
			absTime = startTime + eventTime;
			relativeTime = absTime - now;

			if(relativeTime > 0) {
				this.scheduleEvent(currentEvent);
				scheduledCount = scheduledCount + 1;
				lastScheduledTime = eventTime;
			};

			endIdx = endIdx + 1;
		};

		if(endIdx < events.size) {
			nextBatchTime = startTime + (lastScheduledTime * batchOverlapRatio);
			nextBatchDelay = nextBatchTime - now;

			if(nextBatchDelay > 0) {
				nextBatchTask = SystemClock.sched(nextBatchDelay, {
					this.scheduleBatch(endIdx);
					nil;
				});
			} {
				this.scheduleBatch(endIdx);
			};
		};

		nextEventIndex = endIdx;
	}

	scheduleEvent { |event|
		var type, id, eventTime, pfields;

		type = event["type"];
		id = event["id"];
		eventTime = event["start"].asFloat;
		pfields = event["pfields"] ?? { Dictionary.new };

		this.log("Event: % id:% at % sec".format(type, id, eventTime));

		case
		{ type == "new" } {
			var synthName = event["synthName"];
			var groupName = event["group"] ?? "default";
			this.createSynth(id, synthName, eventTime, pfields, groupName);
		}
		{ type == "set" } {
			this.setNode(id, eventTime, pfields);
		}
		{
			this.log("WARNING: Unknown event type: %".format(type));
		};
	}

	createSynth { |synthId, synthName, eventTime, pfields, groupName|
		var absTime = startTime + eventTime;
		var args = this.processPfields(pfields);
		var group = nodes.at(groupName);

		this.log("Creating synth % (synthName: %) in group %".format(synthId, synthName, groupName));

		SystemClock.schedAbs(absTime, {
			if(server.serverRunning) {
				server.bind {
					var synth = Synth(synthName, args, group);
					nodes.put(synthId, synth);
					nodeWatcher.register(synth);

					this.log("Synth % created with server nodeID %".format(synthId, synth.nodeID));

					synth.onFree {
						this.log("Synth % freed".format(synthId));
						nodes.removeAt(synthId);
					};
				};
			};
			nil;
		});
	}

	setNode { |nodeId, eventTime, pfields|
		var absTime = startTime + eventTime;
		var args = this.processPfields(pfields);

		SystemClock.schedAbs(absTime, {
			if(server.serverRunning) {
				server.bind {
					var node = nodes.at(nodeId);

					if(node.isNil) {
						this.log("ERROR: Cannot set node % - node not found at execution time".format(nodeId));
					} {
						this.log("Setting node % with args: %".format(nodeId, args));
						node.set(*args);
					};
				};
			};
			nil;
		});
	}

	play {
		if(server.serverRunning.not) {
			"ERROR: Server not running. Boot the server first.".postln;
			^false;
		};

		if(isPlaying) {
			"Already playing.".postln;
			^false
		};

		if(events.size <= 0) {
			"No events loaded.".postln;
			^false
		};

		startTime = SystemClock.seconds + startLag;
		isPlaying = true;
		nextEventIndex = 0;
		lastScheduledTime = 0;

		"Starting playback at %".format(startTime).postln;

		this.scheduleBatch(0);

		^true;
	}

	stop {
		if(isPlaying.not) {
			"Not playing.".postln;
			^false
		};

		if(nextBatchTask.notNil) {
			nextBatchTask.stop;
			nextBatchTask = nil;
		};

		server.freeAll;
		nodes.clear;

		isPlaying = false;
		"Playback stopped".postln;
		^true;
	}

	hasNode { |nodeId|
		^nodes.includesKey(nodeId);
	}

	status {
		"EventScheduler Status".postln;
		"  Playing: %".format(isPlaying).postln;
		"  Events: %".format(events.size).postln;
		"  Active nodes: %".format(nodes.size).postln;
		"  Registered buses: %".format(buses.size).postln;
	}
}