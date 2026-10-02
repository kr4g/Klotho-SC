// EventScheduler: plays a Klotho Score.write file on a native scsynth with the
// semantics of Klotho's SuperSonic engine (scheduler_core.js and
// scheduler_score.js), message for message:
//
//   * loadFile(path) decodes the file (KSPayload), checks every SynthDef it
//     needs is known, loads sample buffers, uploads the control-envelope
//     buffer (<stem>.wav) and the decoder geometry. Every refusal happens here,
//     before a node exists.
//   * play builds the topology (KSMixer) under one score group, sets up the
//     control envelopes (one control bus per descriptor, preset with /c_set),
//     then walks the send plan: events and envelope synths merged by start,
//     one time-tagged bundle per distinct instant, sent `latency` seconds
//     ahead. Nothing is ever dropped for being late or for being dense.
//   * new -> /s_new at the head of the track's src group with out = srcBus +
//     speakerLane; releaseAfter + dur + a gated def -> /n_set gate 0 at
//     start + dur; set -> /n_set (out re-pointed); release -> gate 0 on gated
//     defs; envelope targets -> /n_map at the event, or deferred to startTime.
//   * finish at pieceDur + tailPause (onFinish), the score group freed after
//     ringTime, onIdle 0.25 s later. stop frees the score group only: nothing
//     else on the server is touched and no callback fires.
//   * loop: true, or an integer number of cycles. Envelopes map and fire on
//     every cycle (the browser maps them on none while looping; see NOTES).
//
// oscTap sees every message: a KSTrace, or a Function(t, addr, args, sent,
// flags). Spatial files play on the hardware array (\array) or through the
// binaural fold (\fold); the default is \array when the server has enough
// outputs.

EventScheduler {
	var <server;
	var <assets, <payload, <mixer;
	var <isPlaying, <isReady, <isRecording;
	var <playStart, <>startupDelay, <>latency, <>ringTime, <>tailPause;
	var <>loop, <output, <>stemTaps;
	var <>onFinish, <>onIdle, <>onEvent, <>onLoad;
	var <>oscTap, <>debug, <>enableMonitoring, <>allowUnknownDefs, <>postWarnings;
	var <>maxBundleBytes;
	var <>recordingCompleteCallback;
	var <loadedFilePath, <lastError, <effectiveOutput, <pieceDur, <lastSentTag;
	var <plan;
	var stopToken, playRoutine, loadRoutine, recordRoutine, deferredRings;
	var nodeMap, defNameMap, warnedGroups, warnedBufs;
	var <controlBusMap, controlGroup, controlBuses, ctrlBufnum, geomBufnum;
	var recorders;
	var <sampleMap;

	*new { |server, startupDelay = 0.1, latency, ringTime = 5, enableMonitoring = false, debug = false|
		^super.new.init(server, startupDelay, latency, ringTime, enableMonitoring, debug)
	}

	init { |argServer, argStartupDelay, argLatency, argRingTime, argMonitoring, argDebug|
		server = argServer ? Server.default;
		startupDelay = argStartupDelay;
		latency = argLatency ? server.latency ? 0.2;
		ringTime = argRingTime;
		enableMonitoring = argMonitoring;
		debug = argDebug;
		tailPause = 0;
		loop = false;
		output = nil;
		stemTaps = false;
		postWarnings = true;
		allowUnknownDefs = false;
		maxBundleBytes = 8192;
		isPlaying = false;
		isReady = false;
		isRecording = false;
		stopToken = 0;
		deferredRings = List.new;
		nodeMap = Dictionary.new;
		defNameMap = Dictionary.new;
		warnedGroups = Set.new;
		warnedBufs = Set.new;
		controlBusMap = [];
		controlBuses = List.new;
		recorders = List.new;
		sampleMap = Dictionary.new;
		assets = KSAssets(server);
		mixer = KSMixer(this);
	}

	// ---- configuration -------------------------------------------------------

	// \array (hardware 0..N-1), \fold (binaural decode to 0/1) or nil (auto:
	// \array when the server has at least N outputs). Changing it after a load
	// reloads the file, because the fold needs the geometry buffer.
	output_ { |mode|
		var resolved;
		if(mode.notNil and: { #[\array, \fold].includes(mode).not }) {
			Error("EventScheduler: output must be \\array, \\fold or nil").throw
		};
		output = mode;
		if(payload.notNil and: { loadedFilePath.notNil }) {
			resolved = this.prResolveOutput;
			if(resolved != effectiveOutput) { this.loadFile(loadedFilePath) };
		};
	}

	loadSynthDefDir { |path|
		^assets.loadSynthDefDir(path)
	}

	loadKlothoDefs { |dir|
		^assets.loadKlothoDefs(dir)
	}

	// ---- messages out ----------------------------------------------------------

	nextNodeID {
		^server.nextNodeID
	}

	sendMsg { |addr, args|
		args = args.asArray;
		server.sendMsg(addr, *args);
		this.prTap(nil, addr, args, this.prSentNow, nil);
	}

	prSentNow {
		^if(playStart.notNil) { thisThread.seconds - playStart } { nil }
	}

	prTap { |t, addr, args, sent, flags|
		if(oscTap.isNil) { ^this };
		if(oscTap.isKindOf(KSTrace)) { oscTap.tap(t, addr, args, sent, flags) } { oscTap.value(t, addr, args, sent, flags) };
	}

	prLog { |addr, args|
		this.prTap(nil, addr, args ? [], this.prSentNow, nil);
	}

	warn { |text|
		if(postWarnings) { ("EventScheduler: " ++ text).warn };
		this.prLog('#warn', [text]);
	}

	prError { |text|
		("EventScheduler: " ++ text).error;
		this.prLog('#error', [text]);
	}

	log { |text|
		if(debug) { ("EventScheduler: " ++ text).postln };
		this.prLog('#debug', [text]);
	}

	prSync {
		if(server.serverRunning) {
			server.sync;
			this.prTap(nil, '/sync', [], this.prSentNow, nil);
		};
	}

	registerNode { |id, nodeID, defName|
		nodeMap[id] = nodeID;
		defNameMap[id] = defName;
	}

	// ---- loading ----------------------------------------------------------------

	// Load a Score.write file. Runs in its own Routine (buffers need /sync);
	// `action` is called with (success, scheduler) when it is done.
	loadFile { |path, action|
		if(loadRoutine.notNil) { loadRoutine.stop };
		loadRoutine = Routine({
			var ok = this.prLoad(path);
			loadRoutine = nil;
			action.value(ok, this);
		}).play(SystemClock);
		^this
	}

	// The same, from inside a Routine. Returns true on success.
	loadFileSync { |path|
		^this.prLoad(path)
	}

	// Auto: \array when the server has at least N outputs. A stereo file takes
	// the same path either way (a stereo router onto hardware 0/1), reported
	// as \fold, the browser's one and only mode.
	prResolveOutput {
		var mw = KSMixer.computeMainWidth(payload);
		^output ?? { if(mw > 2 and: { server.options.numOutputBusChannels >= mw }) { \array } { \fold } }
	}

	prLoad { |path|
		var p, needed, missing, missingSamples, widths, mw, err;
		this.prTeardownAll;
		playStart = nil;
		isReady = false;
		lastError = nil;
		payload = nil;
		plan = nil;
		p = try { KSPayload.read(path) } { |e| err = e; nil };
		if(p.isNil) {
			lastError = "could not load %: %".format(path, if(err.notNil) { err.errorString } { "unreadable" });
			this.prError(lastError);
			^false
		};
		payload = p;
		loadedFilePath = path;
		p.notes.do { |n| this.prLog('#note', [n]) };

		assets.ensureInfra;
		mw = KSMixer.computeMainWidth(p);
		effectiveOutput = this.prResolveOutput;
		if(p.spatial.notNil) {
			widths = Set.new;
			p.spatial.tracks.do { |t|
				if(t.width.notNil and: { t.width == t.width.round } and: { t.width >= 1 }) { widths.add(t.width.asInteger) };
			};
			widths.add(mw);
			widths.do { |w| assets.ensureWidthFamily(w) };
		};
		if(p.isBare.not) { assets.ensureWidthFamily(2) };

		plan = try { mixer.spatialPlan(p, effectiveOutput) } { |e| err = e; \refused };
		if(plan == \refused) {
			plan = nil;
			lastError = err.errorString;
			this.prError("playback could not start: " ++ lastError);
			^false
		};

		needed = p.instrumentDefNames;
		if(p.isBare.not) { needed = needed.add("__busRouter") };
		if(p.hasControlEnvelopes) { needed = needed.add("__klEnvCtrl") };
		needed = needed ++ (p.metaDefNames ? []);
		missing = assets.missingDefs(needed.asSet.asArray);
		missing.do { |n| this.prLog('#missingDef', [n]) };
		if(missing.size > 0) {
			if(allowUnknownDefs) {
				this.warn("SynthDef(s) not known to SynthDescLib: % -- played anyway and treated as ungated".format(missing.join(", ")));
			} {
				lastError = "missing SynthDef(s): % -- load them with loadSynthDefDir(path) or loadKlothoDefs, or set allowUnknownDefs = true".format(missing.join(", "));
				this.prError(lastError);
				^false
			};
		};

		missingSamples = assets.loadSamples(p, { |bufnum, name| this.prLog('#loadSample', [bufnum, name]) });
		missingSamples.do { |n|
			this.prLog('#note', ["sample % is not in the file's meta.samples, the user sample map or Klotho's bundled samples; events using it drop the pfield".format(n.quote)]);
		};
		sampleMap = assets.sampleMap;

		if(p.hasControlEnvelopes) { this.prUploadControlBuffer(p) };
		if(plan.notNil and: { plan.decoder.notNil }
			and: { (effectiveOutput == \fold) or: { plan.mainWidth <= 2 } }) {
			this.prUploadGeometry(plan);
		};
		this.prSync;
		isReady = true;
		onLoad.value(this, true);
		^true
	}

	prUploadControlBuffer { |p|
		ctrlBufnum = server.bufferAllocator.alloc(1);
		this.sendMsg('/b_alloc', [ctrlBufnum, p.numFrames, 1]);
		this.prSync;
		this.prSetnChunks(ctrlBufnum, p.controlFloats);
	}

	prUploadGeometry { |spatialPlan|
		geomBufnum = server.bufferAllocator.alloc(1);
		this.sendMsg('/b_alloc', [geomBufnum, spatialPlan.mainWidth, 6]);
		this.prSync;
		this.prSetnChunks(geomBufnum, spatialPlan.decoder.coefficients);
	}

	// /b_setn in runs of 200 values, as the browser does.
	prSetnChunks { |bufnum, floats|
		var off = 0, n = floats.size;
		while { off < n } {
			var end = (off + 200).min(n);
			var chunk = floats.copyRange(off, end - 1).asArray.collect(_.asFloat);
			this.sendMsg('/b_setn', [bufnum, off, chunk.size] ++ chunk);
			off = end;
		};
	}

	prFreeBuffers {
		if(ctrlBufnum.notNil) {
			this.sendMsg('/b_free', [ctrlBufnum]);
			server.bufferAllocator.free(ctrlBufnum);
			ctrlBufnum = nil;
		};
		if(geomBufnum.notNil) {
			this.sendMsg('/b_free', [geomBufnum]);
			server.bufferAllocator.free(geomBufnum);
			geomBufnum = nil;
		};
	}

	// ---- play -----------------------------------------------------------------

	play {
		var token;
		if(server.serverRunning.not) {
			this.prError("server not running; boot it first");
			^false
		};
		if(payload.isNil) {
			this.prError("no file loaded");
			^false
		};
		if(isReady.not) {
			this.prLog('#playRejected', [lastError ? "the loaded file was refused"]);
			this.prError("playback could not start: " ++ (lastError ? "the loaded file was refused"));
			^false
		};
		stopToken = stopToken + 1;
		token = stopToken;
		this.prCancelPlay;
		playStart = thisThread.seconds + startupDelay;
		this.prLog('#play', []);
		nodeMap = Dictionary.new;
		defNameMap = Dictionary.new;
		warnedGroups = Set.new;
		warnedBufs = Set.new;
		try {
			mixer.build(payload, effectiveOutput, geomBufnum, stemTaps, enableMonitoring);
			this.prSetupControlEnvelopes;
		} { |err|
			this.prAbortPlaySetup(err);
			^false
		};
		pieceDur = payload.pieceDur;
		isPlaying = true;
		playRoutine = Routine({ this.prRun(token) }).play(SystemClock);
		^true
	}

	// scheduler_core.js _abortPlaySetup: free what was built, say why once, run
	// the end-of-play callbacks so a transport re-arms.
	prAbortPlaySetup { |err|
		var msg = err.errorString;
		isPlaying = false;
		lastError = msg;
		this.prFreeLive;
		this.prError("playback could not start: " ++ msg);
		this.prLog('#onFinish', []);
		onFinish.value(this);
		this.prLog('#onIdle', []);
		onIdle.value(this);
	}

	// scheduler_score.js setupControlEnvelopes.
	prSetupControlEnvelopes {
		var p = payload, floats, blockSize;
		controlBusMap = [];
		if(p.hasControlEnvelopes.not or: { ctrlBufnum.isNil }) { ^this };
		floats = p.controlFloats;
		blockSize = p.blockSize;
		controlGroup = this.nextNodeID;
		this.sendMsg('/g_new', [controlGroup, 0, mixer.scoreGroup]);
		p.descriptors.do { |desc|
			var bus = Bus.control(server, 1), startFrame = desc.blockIndex * blockSize, firstValue, params;
			controlBuses.add(bus);
			firstValue = if(startFrame < floats.size) { floats[startFrame] } { 0.0 };
			this.sendMsg('/c_set', [bus.index, firstValue]);
			params = desc.pfields.reject(_.isNil);
			if(params.size == 0) { params = ["amp"] };
			controlBusMap = controlBusMap.add((
				bus: bus.index, params: params, targets: desc.targets,
				start: desc.start, dur: desc.dur, bufnum: ctrlBufnum,
				startFrame: startFrame, numFrames: blockSize, groupId: controlGroup
			));
		};
	}

	prFreeControlBuses {
		controlBuses.do { |b| b.free };
		controlBuses = List.new;
		controlBusMap = [];
		controlGroup = nil;
	}

	// scheduler_core.js _buildSendPlan: events plus control items, stable-sorted
	// by start, events before control items at an equal start.
	prBuildSendPlan {
		var items = payload.events.collect { |ev| (kind: \event, start: ev.start, ev: ev) };
		var ctrl = controlBusMap.collect { |cm| (kind: \ctrl, start: cm.start, cm: cm) };
		var merged;
		if(ctrl.size == 0) { ^items };
		merged = items ++ ctrl;
		merged.do { |it, i| it[\seq] = i };
		^merged.sort { |a, b| (a.start < b.start) or: { (a.start == b.start) and: { a.seq < b.seq } } }
	}

	prLoopState {
		var finite = 0;
		if(loop.isKindOf(Number) and: { loop > 1 }) { finite = loop.floor.asInteger };
		if(loop == true or: { finite > 0 }) {
			^(cycleIndex: 0, finite: finite)
		};
		^nil
	}

	prRun { |token|
		var loopState = this.prLoopState, cycle = 0, relOffset = 0.0, cycleDur, finishAt;
		var sendPlan = this.prBuildSendPlan, running = true;
		cycleDur = pieceDur + tailPause;
		while { running } {
			var buckets = this.prBuckets(sendPlan, cycle, relOffset, loopState.notNil);
			buckets.do { |b|
				var tt = playStart + b.time, delta;
				delta = (tt - latency) - thisThread.seconds;
				if(delta > 0) { delta.wait };
				if(token != stopToken) { ^nil };
				this.prSendTimed(tt, b.msgs);
			};
			if(loopState.isNil) {
				running = false;
			} {
				cycle = cycle + 1;
				if(loopState.finite > 0 and: { cycle >= loopState.finite }) {
					running = false;
				} {
					relOffset = relOffset + cycleDur;
				};
			};
		};
		finishAt = playStart + relOffset + pieceDur + tailPause;
		((finishAt - thisThread.seconds).max(0)).wait;
		if(token != stopToken) { ^nil };
		this.prFinishPlayback(token);
	}

	// One cycle's messages, grouped by time tag, in plan order. Carried items
	// (auto-releases, deferred maps) land in their own instant ahead of the
	// events that start there, which is the browser's send order too.
	prBuckets { |sendPlan, cycle, relOffset, looping|
		var buckets = Dictionary.new, idPrefix = if(looping) { "c" ++ cycle ++ ":" } { nil };
		var add = { |t, msg|
			var key = (t * 1e9).round / 1e9, b = buckets[key];
			if(b.isNil) { b = (time: t, msgs: List.new); buckets[key] = b };
			b.msgs.add(msg);
		};
		sendPlan.do { |item|
			var t = relOffset + item.start, cm, nid, ev, evId;
			if(item.kind == \ctrl) {
				cm = item.cm;
				nid = this.nextNodeID;
				add.value(t, ['/s_new', '__klEnvCtrl', nid, 0, cm.groupId,
					'bufnum', cm.bufnum, 'bus', cm.bus, 'dur', cm.dur,
					'startFrame', cm.startFrame, 'numFrames', cm.numFrames]);
				cm[\nodeId] = nid;
			} {
				ev = item.ev;
				evId = if(idPrefix.notNil) { idPrefix ++ ev.id } { ev.id };
				switch(ev.type,
					"new", { this.prBundleNew(ev, evId, t, relOffset, add) },
					"set", { this.prBundleSet(ev, evId, t, relOffset, add) },
					"release", { this.prBundleRelease(ev, evId, t, add) }
				);
				if(ev.type == "new" and: { ev.stepIndex.notNil } and: { onEvent.notNil }) {
					this.prScheduleOnEvent(playStart + t, ev.stepIndex);
				};
			};
		};
		^buckets.values.asArray.sort { |a, b| a.time < b.time }
	}

	prScheduleOnEvent { |absTime, stepIndex|
		var token = stopToken;
		SystemClock.schedAbs(absTime, {
			if(token == stopToken) { onEvent.value(stepIndex, this) };
			nil
		});
	}

	// scheduler_core.js _resolveDefPfields: null, objects and arrays are
	// dropped; a string is a sample name on a buf* key (mapped to its bufnum)
	// and dropped anywhere else. Returns a List of [key, value] pairs.
	prResolvePfields { |pfields|
		var out = List.new;
		pfields.keysValuesDo { |key, val|
			var mapped;
			if(val.notNil and: { val.isKindOf(Dictionary).not } and: { val.isKindOf(Array).not }) {
				if(val.isString) {
					if(key.beginsWith("buf")) {
						mapped = sampleMap[val];
						if(mapped.notNil) {
							out.add([key, mapped]);
						} {
							if(warnedBufs.includes(val).not) {
								warnedBufs.add(val);
								this.warn("[Klotho] sample % is not loaded; events using it will be silent or play the wrong buffer.".format(val.quote));
							};
						};
					};
				} {
					out.add([key, val]);
				};
			};
		};
		^out
	}

	prSetPair { |pairs, key, value|
		pairs.do { |p| if(p[0] == key) { p[1] = value; ^pairs } };
		pairs.add([key, value]);
		^pairs
	}

	prFlatten { |pairs|
		var out = Array.new(pairs.size * 2);
		pairs.do { |p| out = out.add(p[0].asSymbol).add(p[1]) };
		^out
	}

	// Track lookup with the browser's fallback: "default" is main; an unknown
	// name plays on main with one warning per name per play.
	prTrackInfo { |group|
		var g = group ? "default", info;
		if(mixer.trackMap.isNil) { ^nil };
		info = mixer.trackMap[g];
		if(info.isNil) {
			if(warnedGroups.includes(g).not) {
				warnedGroups.add(g);
				this.warn("[Klotho] no track named '%'; its events play on the main chain. Known tracks: %".format(
					g, (mixer.trackOrder ++ ["main", "default"]).join(", ")));
			};
			info = mixer.trackMap["default"];
		};
		^info
	}

	prBundleNew { |ev, evId, t, relOffset, add|
		var defName, nid, pairs, target, info, args, mappings;
		if(ev.defName == "__rest__") { ^this };
		defName = KSPayload.resolveDefName(ev.defName);
		nid = this.nextNodeID;
		pairs = this.prResolvePfields(ev.pfields);
		if(mixer.trackMap.notNil) {
			info = this.prTrackInfo(ev[\group]);
			target = if(info.notNil) { info.srcGroup } { mixer.scoreGroup };
			if(info.notNil) { this.prSetPair(pairs, "out", info.srcBus.index + (ev.speakerLane ? 0)) };
		} {
			target = mixer.scoreGroup;
		};
		args = ['/s_new', defName, nid, 0, target] ++ this.prFlatten(pairs);
		add.value(t, args);
		nodeMap[evId] = nid;
		defNameMap[evId] = defName;
		this.prMaybeAutoRelease(ev, nid, defName, t, add);
		mappings = this.prControlMappings(ev.id, ev.start);
		mappings.do { |mp|
			var mapT = if(mp.deferred) { relOffset + mp.startTime } { t };
			add.value(mapT, ['/n_map', nid, mp.param, mp.bus]);
		};
	}

	prBundleSet { |ev, evId, t, relOffset, add|
		var nid = nodeMap[evId] ?? { nodeMap[ev.id] }, defName, pairs, info, args, mappings;
		if(nid.isNil) { ^this };
		defName = defNameMap[evId] ?? { defNameMap[ev.id] };
		pairs = this.prResolvePfields(ev.pfields);
		if(mixer.trackMap.notNil) {
			info = this.prTrackInfo(ev[\group]);
			if(info.notNil) { this.prSetPair(pairs, "out", info.srcBus.index + (ev.speakerLane ? 0)) };
		};
		args = ['/n_set', nid] ++ this.prFlatten(pairs);
		add.value(t, args);
		this.prMaybeAutoRelease(ev, nid, defName, t, add);
		mappings = this.prControlMappings(ev.id, ev.start);
		mappings.do { |mp|
			if((mp.startTime - ev.start).abs <= 1e-6) {
				add.value(t, ['/n_map', nid, mp.param, mp.bus]);
			};
		};
	}

	prBundleRelease { |ev, evId, t, add|
		var nid = nodeMap[evId] ?? { nodeMap[ev.id] }, defName;
		if(nid.isNil) { ^this };
		defName = defNameMap[evId] ?? { defNameMap[ev.id] };
		if(assets.isGated(defName, payload.metaDefs).not) { ^this };
		add.value(t, ['/n_set', nid, 'gate', 0]);
	}

	// scheduler_core.js _maybeScheduleAutoRelease.
	prMaybeAutoRelease { |ev, nid, defName, t, add|
		if(ev.releaseAfter.not) { ^this };
		if(ev.dur.isNil or: { (ev.dur > 0).not }) { ^this };
		if(assets.isGated(defName, payload.metaDefs).not) { ^this };
		add.value(t + ev.dur, ['/n_set', nid, 'gate', 0]);
	}

	// scheduler_score.js _getControlMappingsForEvent: one mapping per declared
	// pfield of every descriptor that targets this event id.
	prControlMappings { |evId, evStart|
		var mappings = List.new;
		controlBusMap.do { |cm|
			var tgt = cm.targets.detect { |tg| tg.id == evId };
			if(tgt.notNil) {
				var deferred = evStart.notNil and: { tgt.startTime > (evStart + 1e-9) };
				cm.params.do { |param|
					mappings.add((param: param, bus: cm.bus, startTime: tgt.startTime, deferred: deferred));
				};
			};
		};
		^mappings
	}

	// One instant: as few bundles as the byte budget allows, every message
	// time-tagged at exactly `tt`, logged in send order.
	prSendTimed { |tt, msgs|
		var bundles = List.new, cur = List.new, curSize = 16, late, sent, flags;
		msgs.do { |m|
			var sz = m.msgSize + 4;
			if(cur.size > 0 and: { (curSize + sz) > maxBundleBytes }) {
				bundles.add(cur);
				cur = List.new;
				curSize = 16;
			};
			cur.add(m);
			curSize = curSize + sz;
		};
		if(cur.size > 0) { bundles.add(cur) };
		bundles.do { |b| server.sendBundle(tt - thisThread.seconds, *b.asArray) };
		late = Main.elapsedTime > tt;
		sent = thisThread.seconds - playStart;
		flags = if(late) { (late: true) } { nil };
		msgs.do { |m| this.prTap(tt - playStart, m[0], m.copyRange(1, m.size - 1), sent, flags) };
		if(lastSentTag.isNil or: { tt > lastSentTag }) { lastSentTag = tt };
	}

	// ---- finish, ring-out, stop ---------------------------------------------------

	prFinishPlayback { |token|
		var entry;
		isPlaying = false;
		this.prLog('#onFinish', []);
		onFinish.value(this);
		entry = mixer.detach;
		entry[\controlBuses] = controlBuses;
		controlBuses = List.new;
		controlBusMap = [];
		controlGroup = nil;
		deferredRings.add(entry);
		ringTime.wait;
		if(token != stopToken) { ^nil };
		this.prFreeRing(entry);
		0.25.wait;
		if(token != stopToken) { ^nil };
		this.prSync;
		if(token != stopToken) { ^nil };
		playRoutine = nil;
		this.prLog('#onIdle', []);
		onIdle.value(this);
	}

	prFreeRing { |entry|
		KSMixer.freeEntry(this, entry);
		(entry[\controlBuses] ? []).do { |b| b.free };
		deferredRings.remove(entry);
	}

	prCancelAllDeferredRings {
		deferredRings.copy.do { |entry| this.prFreeRing(entry) };
		deferredRings = List.new;
	}

	prFreeLive {
		if(mixer.isBuilt) { mixer.freeNow };
		this.prFreeControlBuses;
	}

	prStopRecorders {
		recorders.do { |r| r.stop };
		recorders = List.new;
		if(recordRoutine.notNil) { recordRoutine.stop; recordRoutine = nil };
		isRecording = false;
	}

	// The teardown a new play runs first (scheduler_core.js play's restart
	// section): the old routine stops, ringing groups are freed now.
	prCancelPlay {
		if(playRoutine.notNil) { playRoutine.stop; playRoutine = nil };
		this.prCancelAllDeferredRings;
		this.prFreeLive;
		isPlaying = false;
	}

	// Stop now. Frees this play's score group and nothing else; bundles already
	// queued in scsynth for later instants address freed nodes and do nothing.
	// No callback fires.
	stop {
		var purged = 0, now;
		stopToken = stopToken + 1;
		isPlaying = false;
		if(playRoutine.notNil) { playRoutine.stop; playRoutine = nil };
		this.prLog('#stop', []);
		this.prStopRecorders;
		this.prCancelAllDeferredRings;
		this.prFreeLive;
		if(playStart.notNil and: { oscTap.isKindOf(KSTrace) }) {
			now = thisThread.seconds - playStart;
			purged = oscTap.markPurgedAfter(now);
		};
		this.prLog('#purge', [purged]);
		nodeMap = Dictionary.new;
		defNameMap = Dictionary.new;
		if(server.serverRunning) { { server.sync }.fork };
		^true
	}

	prTeardownAll {
		stopToken = stopToken + 1;
		isPlaying = false;
		if(playRoutine.notNil) { playRoutine.stop; playRoutine = nil };
		this.prStopRecorders;
		this.prCancelAllDeferredRings;
		this.prFreeLive;
		this.prFreeBuffers;
		assets.freeSamples;
		sampleMap = Dictionary.new;
		nodeMap = Dictionary.new;
		defNameMap = Dictionary.new;
	}

	// Release everything this player holds on the server.
	free {
		this.prTeardownAll;
		payload = nil;
		isReady = false;
	}

	// ---- recording --------------------------------------------------------------

	// Record one pass of the loaded file: the main output (N channels in
	// \array mode, 2 in \fold) to `path`, plus one file per track when `stems`
	// is true (<base>_<track>.<ext>, each at the track's width). Never writes
	// the control-envelope .wav.
	record { |path, stems = false, onComplete|
		var dir, base, ext;
		if(payload.isNil or: { isReady.not }) {
			this.prError("record: no file loaded");
			^false
		};
		path = path.standardizePath;
		if(payload.wavPath.notNil and: { path == payload.wavPath.standardizePath }) {
			this.prError("record: % is the control-envelope buffer of the loaded file; choose another path".format(path));
			^false
		};
		if(isPlaying or: { isRecording }) { this.stop };
		dir = path.dirname;
		base = PathName(path).fileNameWithoutExtension;
		ext = PathName(path).extension;
		if(ext.isNil or: { ext.size == 0 }) { ext = "wav" };
		recordRoutine = Routine({
			var recs = List.new, mix, numCh, savedLoop, stopAt, mw, token;
			mw = KSMixer.computeMainWidth(payload);
			numCh = if(effectiveOutput == \array) { mw } { 2 };
			savedLoop = loop;
			loop = false;
			isRecording = true;
			mix = KSDiskRecorder(server, dir +/+ (base ++ "." ++ ext), numCh, 0, 0, \addToTail);
			mix.prepare(assets);
			recs.add(mix);
			recorders = recs;
			if(this.play.not) {
				this.prStopRecorders;
				loop = savedLoop;
				^nil
			};
			token = stopToken;
			if(stems and: { mixer.trackMap.notNil }) {
				(mixer.trackOrder ++ ["main"]).do { |nm|
					var t = mixer.trackInfo(nm), r;
					r = KSDiskRecorder(server, dir +/+ (base ++ "_" ++ nm ++ "." ++ ext), t.width, t.fxBus.index, 0, \addToTail);
					r.prepare(assets);
					recs.add(r);
				};
			};
			if(token != stopToken) { ^nil };
			recs.do { |r| this.prSendTimed(playStart, [r.startMessage(this.nextNodeID)]) };
			stopAt = playStart + pieceDur + tailPause + ringTime + 0.1;
			((stopAt - thisThread.seconds).max(0)).wait;
			if(token != stopToken) { ^nil };
			recs.do { |r| r.stop };
			recorders = List.new;
			isRecording = false;
			recordRoutine = nil;
			loop = savedLoop;
			onComplete.value(this);
			recordingCompleteCallback.value(this);
		}).play(SystemClock);
		^true
	}

	// ---- introspection and GUI hooks --------------------------------------------

	trackNames {
		if(payload.isNil) { ^[] };
		^payload.trackNames ++ ["main"]
	}

	insertNames { |track|
		var specs;
		if(payload.isNil) { ^[] };
		specs = payload.insertSpecs(track);
		if(specs.isNil) { ^[] };
		^specs.collect { |s| s.defName }
	}

	setTrackGain { |track, amp|
		mixer.gains[track.asString] = amp;
		mixer.applyGain(track.asString);
	}

	muteTrack { |track, flag = true|
		if(flag) { mixer.mutes.add(track.asString) } { mixer.mutes.remove(track.asString) };
		mixer.applyAllGains;
	}

	soloTrack { |track, flag = true|
		if(flag) { mixer.solos.add(track.asString) } { mixer.solos.remove(track.asString) };
		mixer.applyAllGains;
	}

	hasNode { |id|
		^nodeMap.includesKey(id.asString)
	}

	nodeID { |id|
		^nodeMap[id.asString]
	}

	status {
		"EventScheduler status".postln;
		"  file: %".format(loadedFilePath).postln;
		"  ready: %  playing: %  recording: %".format(isReady, isPlaying, isRecording).postln;
		if(payload.notNil) {
			"  events: %  pieceDur: %  tracks: %".format(payload.events.size, payload.pieceDur, this.trackNames).postln;
			"  output: %  loop: %  ringTime: %".format(effectiveOutput, loop, ringTime).postln;
		};
	}
}
