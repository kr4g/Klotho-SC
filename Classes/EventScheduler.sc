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

	var <mainGroup;
	var <mainBus;

	var <groupOrder;
	var <busSynths;

	var <loadedFilePath;

	var <enableMonitoring;

	var <isReady;

	var <>recordingCompleteCallback;

	*new { |server, maxEvents=500, batchOverlapRatio=0.8, startLag=0.5, debug=false, enableMonitoring=false|
		^super.new.init(server, maxEvents, batchOverlapRatio, startLag, debug, enableMonitoring)
	}

	init { |serverArg, maxEventsArg, overlapRatioArg, lag, debugFlag, monitorFlag|
		server = serverArg;
		maxEvents = maxEventsArg;
		batchOverlapRatio = overlapRatioArg;
		startLag = lag;
		debug = debugFlag;
		enableMonitoring = monitorFlag;
		isReady = false;

		events = Array.new;
		nodes = Dictionary.new;
		buses = Dictionary.new;
		isPlaying = false;
		nextEventIndex = 0;

		nodeWatcher = NodeWatcher.new(server);

		groupOrder = Array.new;
		busSynths = IdentityDictionary.new;

		defaultName = \default;

		CmdPeriod.add({ isPlaying = false; nextEventIndex = 0 });

		this.loadSynthDefs;
	}

	initGroupsAndInserts { |meta|
		var names = ((meta ? Dictionary.new)["groups"] ? #[]).collect(_.asSymbol);
		var inserts = (meta ? Dictionary.new)["inserts"];
		var insertMap = IdentityDictionary.new;

		groupOrder = names.copy;
		groupMap = IdentityDictionary.new;
		busSynths.clear;

		if(inserts.notNil) {
			inserts.keysValuesDo { |trackName, spec|
				var effects = List.new;
				var specs = if(spec.isKindOf(Array)) { spec } { [spec] };

				specs.do { |item|
					var uid = item["uid"].asSymbol;
					var synthName = item["synthName"].asSymbol;
					var args = item["args"];
					effects.add((
						uid: uid,
						synthName: synthName,
						args: args
					));
				};

				insertMap[trackName.asSymbol] = effects;
			};
		};

		names.do { |nm|
			this.createGroupWithInserts(nm, insertMap[nm]);
		};

		this.createGroupWithInserts(\main, insertMap[\main]);

		mainBus = groupMap[\main][\fxBus];
		mainGroup = groupMap[\main][\group];

		server.bind {
			var synthName = if(enableMonitoring) { \__busRouterMonitor } { \__busRouter };
			var args = if(enableMonitoring) {
				[\inBus, mainBus.index, \outBus, 0, \gain, 1.0, \id, groupOrder.size]
			} {
				[\inBus, mainBus.index, \outBus, 0, \gain, 1.0]
			};
			var mainBusSynth = Synth.tail(groupMap[\main][\group], synthName, args);
			busSynths[\main] = mainBusSynth;
			nodes.put(\__main_bus_synth, mainBusSynth);
		};

		groupMap.keysValuesDo { |nm, entry|
			if(nm != \main) {
				var gParent = entry[\group];
				var bFx = entry[\fxBus];
				var mainSrcBus = groupMap[\main][\srcBus];
				var idx = groupOrder.indexOf(nm);
				server.bind {
					var synthName = if(enableMonitoring) { \__busRouterMonitor } { \__busRouter };
					var args = if(enableMonitoring) {
						[\inBus, bFx.index, \outBus, mainSrcBus.index, \gain, 1.0, \id, idx]
					} {
						[\inBus, bFx.index, \outBus, mainSrcBus.index, \gain, 1.0]
					};
					var groupBusSynth = Synth.tail(gParent, synthName, args);
					busSynths[nm] = groupBusSynth;
					nodes.put(("__" ++ nm.asString ++ "_bus_synth").asSymbol, groupBusSynth);
				};
			};
		};
	}

	createGroupWithInserts { |groupName, effectsList|
		var gParent = Group.tail(server.defaultGroup);
		var gSrc = Group.head(gParent);
		var gFx = Group.after(gSrc);
		var bSrc = Bus.audio(server, 2);
		var bFx = Bus.audio(server, 2);

		groupMap[groupName] = (
			group: gParent,
			srcGroup: gSrc,
			fxGroup: gFx,
			srcBus: bSrc,
			fxBus: bFx,
			fxNodes: IdentityDictionary.new
		);

		nodes.put(groupName.asString, gParent);
		buses.put(groupName, bFx);

		if(effectsList.isNil or: { effectsList.isEmpty }) {
			server.bind {
				var bypass = Synth.head(gFx, \__busRouter, [
					\inBus, bSrc.index,
					\outBus, bFx.index,
					\gain, 1.0
				]);
				groupMap[groupName][\fxNodes].put(\__bypass, bypass);
			};
		} {
			var stageBuses = Array.newClear(effectsList.size - 1).collect { Bus.audio(server, 2) };
			var prevBus = bSrc;
			var nextBus;

			effectsList.do { |effect, i|
				var uid = effect.uid;
				var synthName = effect.synthName;
				var initialArgs = effect.args;
				var desc = SynthDescLib.global.at(synthName);
				var inParam, outParam;
				var argList;

				nextBus = if(i < (effectsList.size - 1)) { stageBuses[i] } { bFx };

				if(desc.notNil) {
					var controlDict = desc.controlDict;
					inParam = case
					{ controlDict.includesKey(\in) } { \in }
					{ controlDict.includesKey(\inbus) } { \inbus }
					{ controlDict.includesKey(\inBus) } { \inBus }
					{ \in };
					outParam = case
					{ controlDict.includesKey(\out) } { \out }
					{ controlDict.includesKey(\outbus) } { \outbus }
					{ controlDict.includesKey(\outBus) } { \outBus }
					{ \out };
				} {
					inParam = \in;
					outParam = \out;
				};

				argList = List.new;
				argList.add(inParam).add(prevBus.index);
				argList.add(outParam).add(nextBus.index);

				if(initialArgs.notNil) {
					initialArgs.keysValuesDo { |key, value|
						var k = key.asSymbol;
						var v = value;

						if(value.isString) {
							if(value.every { |c| "0123456789.-".includes(c) }) {
								v = value.asFloat
							};
						};

						argList.add(k).add(v);
					};
				};

				server.bind {
					var fx = Synth.tail(gFx, synthName, argList.asArray);
					nodes.put(uid.asString, fx);
					groupMap[groupName][\fxNodes].put(uid, fx);
				};

				prevBus = nextBus;
			};
		};
	}

	loadSynthDefs {
		Routine({
			if(SynthDescLib.global.at(\__busRouter).isNil) {
				SynthDef(\__busRouter, { |inBus=0, outBus=0, gain=1.0|
					var sig = In.ar(inBus, 2);
					sig = sig * gain;
					ReplaceOut.ar(inBus, sig);
					Out.ar(outBus, sig);
				}).add;
			};
			if(SynthDescLib.global.at(\__busRouterMonitor).isNil) {
				SynthDef(\__busRouterMonitor, { |inBus=0, outBus=0, gain=1.0, id=0|
					var amp, peak;
					var sig = In.ar(inBus, 2);
					sig = sig * gain;
					amp = Amplitude.kr(sig, 0.01, 0.1);
					peak = amp.sum * 0.5;
					SendReply.kr(Impulse.kr(20), '/trackLevel', [peak, id]);
					ReplaceOut.ar(inBus, sig);
					Out.ar(outBus, sig);
				}).add;
			};
			if(SynthDescLib.global.at(\__kl_diskout).isNil) {
				SynthDef(\__kl_diskout, { |bufnum=0, inbus=0|
					var sig = In.ar(inbus, 2);
					DiskOut.ar(bufnum, sig);
				}).add;
			};
			server.sync;
			isReady = true;
		}).play;
	}

	log { |message|
		if(debug) { message.postln };
	}

	getBusSynth { |groupName|
		^busSynths[groupName.asSymbol];
	}

	getAllBusSynths {
		^busSynths;
	}

	loadFile { |path|
		Routine({
			var file, jsonString, jsonData;

			while({ isReady.not }) { 0.01.wait };

			server.freeAll;
			server.newBusAllocators;
			server.sync;

			loadedFilePath = path;

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

			this.initGroupsAndInserts(jsonData["meta"]);
		}).play;

		^true;
	}

	processPfields { |pfields, groupName|
		var processedArgs = List.new;
		var entry = groupMap[(groupName ? defaultName).asSymbol] ? groupMap[defaultName];
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
			this.message("TODO", eventTime);
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
		var args = List.new;

		// Process pfields without bus routing
		pfields.keysValuesDo { |key, value|
			var k = key.asSymbol;
			var v = value;

			if(value.isString) {
				if(value.every { |c| "0123456789.-".includes(c) }) {
					v = value.asFloat;
				};
			} {
				if(value.isNil) { v = 0 };
			};

			args.add(k);
			args.add(v);
		};

		SystemClock.schedAbs(absTime, {
			if(server.serverRunning) {
				server.bind {
					var node = nodes.at(nodeId);
					if(node.notNil) { node.set(*args.asArray) };
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

	message { |msg, eventTime|
		var absTime = startTime + eventTime;

		SystemClock.schedAbs(absTime, {
			if(server.serverRunning) {
				server.bind {
					msg.postln;
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
		"Playback stopped".postln;
		//
		// if(nextBatchTask.notNil) {
		// 	nextBatchTask.stop;
		// 	nextBatchTask = nil;
		// };
		//
		// server.freeAll;
		// nodes.clear;
		Routine({
			CmdPeriod.run;
			server.sync;
			isPlaying = false;
			if(loadedFilePath.notNil) {
				this.loadFile(loadedFilePath);  // Reload
				"File reloaded and ready for playback".postln;
			};
		}).play;

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
				// SystemClock.schedAbs(tStop,  { server.stopRecording; this.stop; nil });
				SystemClock.schedAbs(tStop,  {
					server.stopRecording;
					this.stop;
					if(recordingCompleteCallback.notNil) {
						AppClock.sched(0, { recordingCompleteCallback.value; nil });
					};
					nil
				});
			} {
				bufMap = IdentityDictionary.new;
				ch = 2;
				frames = 131072;

				groupMap.keysValuesDo { |nm, entry|
					var stemPath = dir +/+ (base ++ "_" ++ nm.asString ++ "." ++ ext);
					var buf = Buffer.alloc(server, frames, ch);
					server.sync;

					buf.write(
						path: stemPath,
						headerFormat: ext.asString,
						sampleFormat: "int24",
						numFrames: 0,
						startFrame: 0,
						leaveOpen: true
					);
					server.sync;

					bufMap[nm] = buf;
				};

				this.play;

				tStart = startTime;
				tStop  = tStart + pieceDur + end_padding;

				diskNodes = IdentityDictionary.new;
				SystemClock.schedAbs(tStart, {
					server.bind {
						groupMap.keysValuesDo { |nm, entry|
							var buf, inbus, node, busRouter;
							buf = bufMap[nm];
							inbus = entry[\fxBus].index;
							busRouter = busSynths[nm];
							node = Synth.after(busRouter, \__kl_diskout, [
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
					// AppClock.sched(0.1, {
					// 	bufMap.keysValuesDo { |k, b| b.close; b.free };
					// 	// isPlaying = false;
					// 	this.stop;
					// 	nil
					// });
					AppClock.sched(0.1, {
						bufMap.keysValuesDo { |k, b| b.close; b.free };
						this.stop;
						if(recordingCompleteCallback.notNil) {
							recordingCompleteCallback.value;
						};
						nil
					});
					nil
				});
			};
		}).play(AppClock);

		^true
	}
}