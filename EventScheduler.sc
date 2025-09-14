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

	var <groupMap;     // \name -> (group, srcGroup, fxGroup, srcBus, fxBus, fxNodes)
	var <defaultName;
	var <monitorNodes;

	var <mainGroup;
	var <mainBus;

	*new { |server, maxEvents=500, batchOverlapRatio=0.8, startLag=0.5, debug=false|
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
		monitorNodes = IdentityDictionary.new;

		defaultName = \default;

		this.ensureMonitorDef;
	}

	initGroupsFromMeta { |meta|
		var names = ((meta ? Dictionary.new)["groups"] ? #[]).collect(_.asSymbol);
		groupMap = IdentityDictionary.new;

		names.do { |nm|
			var gParent = Group.tail(server.defaultGroup);
			var gSrc = Group.head(gParent);
			var gFx = Group.after(gSrc);
			var bSrc = Bus.audio(server, 2);
			var bFx = Bus.audio(server, 2);

			groupMap[nm] = (
				group: gParent,
				srcGroup: gSrc,
				fxGroup: gFx,
				srcBus: bSrc,
				fxBus: bFx,
				fxNodes: IdentityDictionary.new
			);

			nodes.put(nm, gParent);
			buses.put(nm, bFx);

			server.bind {
				var bypass = Synth.head(gFx, \__busToBus, [
					\inBus, bSrc.index,
					\outBus, bFx.index,
					\gain, 1.0
				]);
				groupMap[nm][\fxNodes].put(\__bypass, bypass);
			};
		};

		{
			var gParent = Group.tail(server.defaultGroup);
			var gSrc = Group.head(gParent);
			var gFx = Group.after(gSrc);
			var bSrc = Bus.audio(server, 2);
			var bFx = Bus.audio(server, 2);

			groupMap[defaultName] = (
				group: gParent,
				srcGroup: gSrc,
				fxGroup: gFx,
				srcBus: bSrc,
				fxBus: bFx,
				fxNodes: IdentityDictionary.new
			);

			nodes.put(defaultName, gParent);
			buses.put(defaultName, bFx);

			server.bind {
				var bypass = Synth.head(gFx, \__busToBus, [
					\inBus, bSrc.index,
					\outBus, bFx.index,
					\gain, 1.0
				]);
				groupMap[defaultName][\fxNodes].put(\__bypass, bypass);
			};
		}.value;

		// Create main AFTER tracks so it runs last
		mainGroup = Group.tail(server.defaultGroup);
		mainBus = Bus.audio(server, 2);

		server.bind {
			Synth.tail(mainGroup, \__busToMain, [\inBus, mainBus.index, \outBus, 0, \gain, 1.0]);
		};

		// Now wire each track's fx -> main
		groupMap.keysValuesDo { |nm, entry|
			var gFx = entry[\fxGroup];
			var bFx = entry[\fxBus];
			server.bind {
				Synth.tail(gFx, \__busToBus, [\inBus, bFx.index, \outBus, mainBus.index, \gain, 1.0]);
			};
		};
	}


	buildInsertsFromMeta { |meta|
		var ins = (meta ? Dictionary.new)["inserts"];
		var normalized;
		if(ins.isNil) { ^this };

		normalized = if(ins.isArray) { ins } { [ins] };

		normalized.do { |item|
			item.keysValuesDo { |trk, spec|
				var t, entry, gFx, bSrc, bFx, chain, labels, defs, bypass;
				t = trk.asSymbol;
				entry = groupMap[t];
				gFx = entry[\fxGroup];
				bSrc = entry[\srcBus];
				bFx = entry[\fxBus];
				chain = if(spec.isKindOf(Dictionary)) { spec } { spec.as(Dictionary) };
				labels = chain.keys;
				defs = labels.collect { |k| chain[k] };
				defs = defs.collect { |v| v.isArray.if({ v }, { [v] }) }.flat;

				bypass = entry[\fxNodes].removeAt(\__bypass);
				if(bypass.notNil) { bypass.free };

				if(defs.isEmpty) {
					server.bind {
						var newBypass;
						newBypass = Synth.head(gFx, \__busToBus, [
							\inBus, bSrc.index,
							\outBus, bFx.index,
							\gain, 1.0
						]);
						entry[\fxNodes].put(\__bypass, newBypass);
					};
					^this;
				};

				{
					var stageBuses = Array.newClear(defs.size - 1).collect { Bus.audio(server, 2) };
					var prevBus = bSrc;
					var nextBus;

					defs.do { |defName, i|
						nextBus = if(i < defs.size - 1) { stageBuses[i] } { bFx };
						server.bind {
							var fx;
							fx = Synth.tail(gFx, defName.asSymbol, [
								\inbus, prevBus.index,
								\outbus, nextBus.index
							]);
							nodes.put(labels.wrapAt(i), fx);
							entry[\fxNodes].put(labels.wrapAt(i), fx);
						};
						prevBus = nextBus;
					};
				}.value;
			};
		};
	}

	ensureMonitorDef {
		Routine({
			if(SynthDescLib.global.at(\__busToMain).isNil) {
				SynthDef(\__busToMain, { |inBus=0, outBus=0, gain=1.0|
					var sig = In.ar(inBus, 2);
					Out.ar(outBus, sig * gain);
				}).add;
			};
			if(SynthDescLib.global.at(\__busToBus).isNil) {
				SynthDef(\__busToBus, { |inBus=0, outBus=0, gain=1.0|
					var sig = In.ar(inBus, 2);
					Out.ar(outBus, sig * gain);
				}).add;
			};
			if(SynthDescLib.global.at(\__kl_diskout).isNil) {
				SynthDef(\__kl_diskout, { |bufnum=0, inbus=0|
					var sig = In.ar(inbus, 2);
					DiskOut.ar(bufnum, sig);
				}).add;
			};
			server.sync;
		}).play;
	}

	log { |message|
		if(debug) { message.postln };
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

		if(jsonData["events"].isNil || jsonData["events"].isKindOf(Array).not) {
			"ERROR: No 'events' key found or 'events' is not an array".postln;
			^false;
		};

		events = jsonData["events"];
		nextEventIndex = 0;

		this.initGroupsFromMeta(jsonData["meta"]);
		this.buildInsertsFromMeta(jsonData["meta"]);

		^true;
	}

	processPfields { |pfields, groupName|
		var processedArgs = List.new;
		var entry = groupMap[(groupName ? defaultName).asSymbol] ? { groupMap[defaultName] };
		var targetBusIndex = entry[\srcBus].index;
		var sawOut = false;

		pfields.keysValuesDo { |key, value|
			var k = key.asSymbol;
			var v = value;
			var isOutKey = (k == \out) or: { k == \outbus } or: { k == \outBus };

			if(isOutKey) {
				sawOut = true;
				v = targetBusIndex;
			} {
				if(value.isString) {
					if(buses.includesKey(value)) {
						v = (buses.at(value)).index;
					} {
						if(value.every { |c| "0123456789.-".includes(c) }) { v = value.asFloat };
					};
				} {
					if(value.isNil) { v = 0 };
				};
			};

			processedArgs.add(k);
			processedArgs.add(v);
		};

		if(sawOut.not) {
			processedArgs.add(\out);
			processedArgs.add(targetBusIndex);
		};

		^processedArgs.asArray
	}

	scheduleBatch { |startIdx|
		var now, endIdx, nextBatchTime, currentEvent, scheduledCount, skippedCount;
		var eventTime, absTime, relativeTime, nextBatchDelay, firstEventTime, batchStartTime;
		var currentBatchStart, batchDuration, overlapTime;

		now = SystemClock.seconds;
		endIdx = startIdx;
		scheduledCount = 0;
		skippedCount = 0;
		firstEventTime = nil;
		batchStartTime = now;

		this.log("=== BATCH DEBUG: Starting batch at index % ===".format(startIdx));
		this.log("Current time: %, Start time: %, Piece time: %".format(now, startTime, now - startTime));

		while({
			(endIdx < events.size) && { endIdx - startIdx < maxEvents }
		}) {
			currentEvent = events[endIdx];

			eventTime = currentEvent["start"].asFloat;
			absTime = startTime + eventTime;
			relativeTime = absTime - now;

			if(firstEventTime.isNil) { firstEventTime = eventTime };

			if(relativeTime > 0) {
				this.scheduleEvent(currentEvent);
				scheduledCount = scheduledCount + 1;
				lastScheduledTime = eventTime;
			} {
				skippedCount = skippedCount + 1;
			};

			endIdx = endIdx + 1;
		};

		this.log("Batch complete: processed % events, scheduled %, skipped %".format(
			endIdx - startIdx, scheduledCount, skippedCount));
		this.log("Event time range: %s to %s (span: %s)".format(
			firstEventTime, lastScheduledTime, lastScheduledTime - firstEventTime));

		if(endIdx < events.size) {
			currentBatchStart = events[startIdx]["start"].asFloat;
			batchDuration = lastScheduledTime - currentBatchStart;
			overlapTime = batchDuration * batchOverlapRatio;
			nextBatchTime = startTime + currentBatchStart + overlapTime;
			nextBatchDelay = nextBatchTime - now;

			this.log("Next batch timing: batchStart=%, duration=%, overlap=%".format(
				currentBatchStart, batchDuration, overlapTime));
			this.log("Next batch delay: %s (at piece time %s)".format(
				nextBatchDelay, nextBatchTime - startTime));

			if(nextBatchDelay > 0) {
				nextBatchTask = SystemClock.sched(nextBatchDelay, {
					this.scheduleBatch(endIdx);
					nil;
				});
				this.log("Next batch scheduled for %s".format(nextBatchDelay));
			} {
				this.log("Next batch delay is negative! Scheduling immediately.");
				this.scheduleBatch(endIdx);
			};
		} {
			this.log("All events processed.");
		};

		nextEventIndex = endIdx;
	}

	scheduleEvent { |event|
		var type, id, eventTime, pfields, groupKey;
		groupKey = (event["group"] ? defaultName).asSymbol;

		type = event["type"];
		id = event["id"];
		eventTime = event["start"].asFloat;
		pfields = event["pfields"] ?? { Dictionary.new };

		case
		{ type == "new" } {
			var synthName = event["synthName"];
			this.createSynth(id, synthName, eventTime, pfields, groupKey);
		}
		{ type == "set" } {
			this.setNode(id, eventTime, pfields, groupKey);
		}
		{ type == "release" }{
			this.releaseNode(id, eventTime);
		}
		{ type == "message" } {
		}
		{
			this.log("WARNING: Unknown event type: %".format(type));
		};
	}

	createSynth { |synthId, synthName, eventTime, pfields, groupName|
		var absTime = startTime + eventTime;
		var args = this.processPfields(pfields, groupName);
		var entry = groupMap[(groupName ? defaultName).asSymbol] ? groupMap[defaultName];
		var targetGroup = entry[\srcGroup];

		SystemClock.schedAbs(absTime, {
			if(server.serverRunning) {
				server.bind {
					var synth;
					synth = Synth(synthName, args, targetGroup);
					nodes.put(synthId, synth);
					nodeWatcher.register(synth);
					synth.onFree { nodes.removeAt(synthId) };
				};
			};
			nil;
		});
	}

	setNode { |nodeId, eventTime, pfields, groupName|
		var absTime = startTime + eventTime;
		var args = this.processPfields(pfields, groupName);

		SystemClock.schedAbs(absTime, {
			if(server.serverRunning) {
				server.bind {
					var node = nodes.at(nodeId);
					if(node.notNil) { node.set(*args) };
				};
			};
			nil;
		});
	}

	releaseNode { |nodeId, eventTime|
		var absTime = startTime + eventTime;

		SystemClock.schedAbs(absTime, {
			if(server.serverRunning) {
				server.bind {
					var node = nodes.at(nodeId);
					if(node.notNil) { node.release(nil) };
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

		"Starting playback...".postln;

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

	record { |path, stems=true, end_padding=5.0|
		var pn = PathName(path);
		var dir = pn.pathOnly;
		var base = pn.fileNameWithoutExtension;
		var ext  = pn.extension ? "wav";
		var pieceDur;

		pieceDur = if(events.size > 0) {
			events.collect({ |e| e["start"].asFloat }).maxItem
		} { 0.0 };

		Routine({
			var tStart, tStop;
			var recs, bufMap, diskNodes, ch, frames;

			if(stems.not) {
				server.prepareForRecord(dir +/+ (base ++ "." ++ ext), 2);
				server.sync;

				this.play;

				tStart = startTime;
				tStop  = tStart + pieceDur + end_padding;

				SystemClock.schedAbs(tStart, { server.record; nil });
				SystemClock.schedAbs(tStop,  { server.stopRecording; nil });
			} {
				bufMap = IdentityDictionary.new;
				ch = 2;
				frames = 131072;

				groupMap.keysValuesDo { |nm, entry|
					var stemPath = dir +/+ (base ++ "_" ++ nm.asString ++ "." ++ ext);
					var buf = Buffer.alloc(server, frames, ch);
					buf.write(
						path: stemPath,
						headerFormat: ext.asString,
						sampleFormat: "int24",
						numFrames: 0,
						startFrame: 0,
						leaveOpen: true
					);
					bufMap[nm] = buf;
				};
				server.sync;

				this.play;

				tStart = startTime;
				tStop  = tStart + pieceDur + end_padding;

				diskNodes = IdentityDictionary.new;
				SystemClock.schedAbs(tStart, {
					server.bind {
						groupMap.keysValuesDo { |nm, entry|
							var buf, inbus, node;
							buf = bufMap[nm];
							inbus = entry[\fxBus].index;
							node = Synth.tail(entry[\fxGroup], \__kl_diskout, [
								\bufnum, buf.bufnum,
								\inbus,  inbus
							]);
							diskNodes[nm] = node;
						};
					};
					nil
				});

				SystemClock.schedAbs(tStop, {
					diskNodes.keysValuesDo { |k, n| n.free };
					AppClock.sched(0.1, {
						bufMap.keysValuesDo { |k, b| b.close; b.free };
						nil
					});
					nil
				});
			};
		}).play(AppClock);

		^true
	}
}