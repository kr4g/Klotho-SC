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
//     one time-tagged bundle per distinct instant (split by maxBundleBytes and
//     maxBundleMsgs, never inside one event's messages), sent `latency`
//     seconds ahead and built as it goes, so a long file costs nothing up
//     front. Nothing is ever dropped for being late or for being dense:
//     scsynth holds at most queueSlots (2048) time-tagged bundles and drops
//     the next one silently, so an occupancy gate (KSQueueLoad, the port of
//     scheduler_core.js's _schedLoad) keeps the bundles in flight under
//     safeQueueLimit and defers a send until slots come due (late delivery is
//     the degradation, as in the browser). A late /s_new, or a release within
//     one control block of its /s_new, would leave the voice hung (gate 0
//     applied before EnvGen's first calc), so a gate release is never tagged
//     before releaseGuard after its note, or lateReleaseGuard after the send
//     of a note that went out late (lateHorizon widens "late" to short leads).
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
	// scsynth's scheduler: PriorityQueueT<SC_ScheduledEvent, 2048>, one slot
	// per time-tagged bundle, freed when it comes due, the 2049th dropped with
	// no message (research-scsynth-queue.md). 75 % of it is the working
	// limit, the ratio scheduler_core.js uses (384 of SuperSonic's 512); the
	// rest absorbs overshoot and other clients of the same server.
	classvar <>queueSlots = 2048;
	classvar <>safeQueueLimit = 1536;
	classvar <>minBatchBundles = 25;      // defer when headroom is thinner than this
	classvar <>maxBundlesPerSend = 200;   // bundles handed over between two gate checks
	classvar <>deferMin = 0.02, <>deferMax = 0.25;
	var <server;
	var <assets, <payload, <mixer;
	var <isPlaying, <isReady, <isRecording;
	var <playStart, <>startupDelay, <>latency, <>ringTime, <>tailPause;
	var <>loop, <output, <>stemTaps, <>binauralStems, <travel;
	var <>onFinish, <>onIdle, <>onEvent, <>onLoad;
	var <>oscTap, <>debug, <>enableMonitoring, <>allowUnknownDefs, <>postWarnings;
	var <>maxBundleBytes, <>maxBundleMsgs;
	var <>releaseGuard, <>lateReleaseGuard, <>lateHorizon, <>lateSlotHold;
	var <deferCount, <guardCount, releaseNotBefore;
	var <>recordingCompleteCallback;
	var <loadedFilePath, <lastError, <effectiveOutput, <pieceDur, <lastSentTag;
	var <plan;
	var stopToken, playRoutine, loadRoutine, recordRoutine, deferredRings, firstCursor, firstBucket;
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
		binauralStems = false;
		travel = nil;
		postWarnings = true;
		allowUnknownDefs = false;
		maxBundleBytes = 8192;
		maxBundleMsgs = 250;
		releaseGuard = 0.003;
		lateReleaseGuard = 0.05;
		lateHorizon = 0.0;
		lateSlotHold = 0.03;
		deferCount = 0;
		guardCount = 0;
		releaseNotBefore = Dictionary.new;
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

	// The headphone fold's travel scale: nil plays the file's own table, a
	// number from 0 to 1 re-scales the propagation part of every delay
	// (1.0 the venue as built, 0.1 and 0.03 compact; the interaural cues do
	// not move). Needs a file whose decoder carries travelDelays, which
	// Score.write has written since the scale existed. Takes effect on the
	// next load, or at once when the geometry is already on the server.
	travel_ { |scale|
		if(scale.notNil) {
			if(scale.isNumber.not or: { scale.isNaN } or: { scale < 0 } or: { scale > 1 }) {
				Error("EventScheduler: travel must be nil or a number from 0 to 1").throw
			};
		};
		travel = scale;
		if(geomBufnum.notNil and: { plan.notNil } and: { plan.decoder.notNil }) {
			this.prSetnChunks(geomBufnum, this.prGeometryTable(plan));
		};
	}

	// The decoder table at this player's travel scale: the file's own, or
	// its delays moved by (travel - fileTravel) * travelDelays[lane].
	prGeometryTable { |spatialPlan|
		var dec = spatialPlan.decoder, table, shift, n;
		if(travel.isNil or: { travel == dec.travel }) { ^dec.coefficients };
		n = spatialPlan.mainWidth;
		if(dec.travelDelays.isNil or: { dec.travelDelays.size != n }) {
			Error("EventScheduler: travel = % asks for a re-scaled fold, but this file's decoder carries no travelDelays (it was written before the scale existed, or by hand). Write it again with Score.write, which records them, or set travel = nil to play the file's own table.".format(travel)).throw
		};
		shift = travel - dec.travel;
		table = dec.coefficients.copy;
		n.do { |lane|
			var base = lane * 6, d = shift * dec.travelDelays[lane];
			table[base] = table[base] + d;
			table[base + 1] = table[base + 1] + d;
			if(table[base] < 0 or: { table[base + 1] < 0 }) {
				Error("EventScheduler: re-scaling lane % to travel % gives a negative delay; the file's travelDelays do not match its table".format(lane, travel)).throw
			};
		};
		^table
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
		// Defs the GUI's meter taps need, defined now so the load's final sync
		// lands them before play sends the first /s_new (/d_recv is async).
		if(enableMonitoring) {
			assets.ensureMeter(2);
			widths !? { widths.do { |w| assets.ensureMeter(w) } };
		};

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
			try { this.prUploadGeometry(plan) } { |e|
				lastError = e.errorString;
				this.prError("playback could not start: " ++ lastError);
				^false
			};
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
		var table = this.prGeometryTable(spatialPlan);
		geomBufnum = server.bufferAllocator.alloc(1);
		this.sendMsg('/b_alloc', [geomBufnum, spatialPlan.mainWidth, 6]);
		this.prSync;
		this.prSetnChunks(geomBufnum, table);
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
		playStart = nil;
		nodeMap = Dictionary.new;
		defNameMap = Dictionary.new;
		releaseNotBefore = Dictionary.new;
		deferCount = 0;
		guardCount = 0;
		warnedGroups = Set.new;
		warnedBufs = Set.new;
		try {
			mixer.build(payload, effectiveOutput, geomBufnum, stemTaps, enableMonitoring, binauralStems);
			this.prSetupControlEnvelopes;
		} { |err|
			this.prAbortPlaySetup(err);
			^false
		};
		pieceDur = payload.pieceDur;
		// The first instant is built here, before playStart is fixed, so a
		// first instant of thousands of notes (100 ms of sclang work) still
		// goes out startupDelay ahead; every later instant is built when its
		// send time comes. playStart is taken from the wall clock, which the
		// build above has moved on while the logical clock stood still.
		firstCursor = this.prCursor(this.prBuildSendPlan, 0, 0.0, this.prLoopState.notNil);
		firstBucket = this.prNextBucket(firstCursor);
		playStart = thisThread.seconds.max(Main.elapsedTime) + startupDelay;
		this.prLog('#play', []);
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
	// by start, events before control items at an equal start. Always sorted:
	// the cursor walks the plan in time order, and a file need not be.
	prBuildSendPlan {
		var items = payload.events.collect { |ev| (kind: \event, start: ev.start, ev: ev) };
		var ctrl = controlBusMap.collect { |cm| (kind: \ctrl, start: cm.start, cm: cm) };
		var merged;
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
			var cursor, bucket;
			if(cycle == 0 and: { firstCursor.notNil }) {
				cursor = firstCursor;
				bucket = firstBucket;
				firstCursor = nil;
				firstBucket = nil;
			} {
				cursor = this.prCursor(sendPlan, cycle, relOffset, loopState.notNil);
				bucket = this.prNextBucket(cursor);
			};
			while { bucket.notNil } {
				var tt = playStart + bucket.time, delta;
				delta = (tt - latency) - thisThread.seconds;
				if(delta > 0) { delta.wait };
				if(token != stopToken) { ^nil };
				this.prSendTimed(tt, bucket.groups, token);
				if(token != stopToken) { ^nil };
				bucket.steps.do { |pair| this.prScheduleOnEvent(playStart + pair[0], pair[1]) };
				bucket = this.prNextBucket(cursor);
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

	// One cycle's send plan, walked one instant at a time (prNextBucket) so
	// the per-event work (pfields, node ids, mappings) happens at send time
	// and not in one block after playStart is fixed. Carried items
	// (auto-releases, deferred maps) land in their own instant ahead of the
	// events that start there, which is the browser's send order too.
	prCursor { |sendPlan, cycle, relOffset, looping|
		^(plan: sendPlan, idx: 0, relOffset: relOffset,
			idPrefix: if(looping) { "c" ++ cycle ++ ":" } { nil },
			carried: Dictionary.new, times: SortedList.new)
	}

	prKey { |t|
		^(t * 1e9).round / 1e9
	}

	// The next instant, or nil when the cycle is spent. A bucket is
	// (time:, groups:, steps:) where every group is one event's messages, kept
	// together in one bundle because two bundles with one time tag may be run
	// by scsynth in either order, and steps holds [time, stepIndex] pairs for
	// onEvent, scheduled once the instant has been sent.
	prNextBucket { |c|
		var planT, planKey, carriedKey, key, bucket, group;
		var add;
		planT = if(c.idx < c.plan.size) { c.relOffset + c.plan[c.idx].start } { nil };
		planKey = if(planT.notNil) { this.prKey(planT) } { nil };
		carriedKey = if(c.times.isEmpty) { nil } { c.times.first };
		if(planKey.isNil and: { carriedKey.isNil }) { ^nil };
		if(carriedKey.notNil and: { planKey.isNil or: { carriedKey <= planKey } }) {
			key = carriedKey;
			c.times.removeAt(0);
			bucket = c.carried.removeAt(key);
		} {
			key = planKey;
			bucket = (time: planT, groups: List.new, steps: List.new);
		};
		add = { |t, msg|
			var k = this.prKey(t), b;
			if(k <= key) {
				group.add(msg);
			} {
				b = c.carried[k];
				if(b.isNil) { b = (time: t, groups: List.new, steps: List.new); c.carried[k] = b; c.times.add(k) };
				b.groups.add(List[msg]);
			};
		};
		while { c.idx < c.plan.size and: { this.prKey(c.relOffset + c.plan[c.idx].start) == key } } {
			group = List.new;
			this.prAddItem(c, c.plan[c.idx], add, bucket);
			if(group.size > 0) { bucket.groups.add(group) };
			c.idx = c.idx + 1;
		};
		^bucket
	}

	prAddItem { |c, item, add, bucket|
		var t = c.relOffset + item.start, cm, nid, ev, evId;
		if(item.kind == \ctrl) {
			cm = item.cm;
			nid = this.nextNodeID;
			add.value(t, ['/s_new', '__klEnvCtrl', nid, 0, cm.groupId,
				'bufnum', cm.bufnum, 'bus', cm.bus, 'dur', cm.dur,
				'startFrame', cm.startFrame, 'numFrames', cm.numFrames]);
			cm[\nodeId] = nid;
		} {
			ev = item.ev;
			evId = if(c.idPrefix.notNil) { c.idPrefix ++ ev.id } { ev.id };
			switch(ev.type,
				"new", { this.prBundleNew(ev, evId, t, c.relOffset, add) },
				"set", { this.prBundleSet(ev, evId, t, c.relOffset, add) },
				"release", { this.prBundleRelease(ev, evId, t, add) }
			);
			if(ev.type == "new" and: { ev.stepIndex.notNil } and: { onEvent.notNil }) {
				bucket.steps.add([t, ev.stepIndex]);
			};
		};
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

	// One instant: its groups packed into as few bundles as maxBundleBytes and
	// maxBundleMsgs allow, every message time-tagged at exactly `tt`, logged in
	// send order. Before each hand-over the occupancy gate prunes the ledger of
	// bundles already due and, when fewer than minBatchBundles slots are free,
	// waits for the earliest due one (20 ms to 250 ms, as the browser does)
	// rather than sending into a full queue. A gate release whose node was
	// sent late, or that falls within releaseGuard of its /s_new, is moved to
	// its own bundle at the first safe tag.
	prSendTimed { |tt, groups, token|
		var ledger = KSQueueLoad.for(server), bundles, idx = 0, guarded = List.new, kept = List.new;
		var sent, lateAny = false, stopped = false;
		token = token ? stopToken;
		groups.do { |g|
			var keep = List.new;
			g.do { |m|
				var notBefore;
				if(m[0] == '/n_set' and: { m.size == 4 } and: { m[2] == 'gate' } and: { m[3] == 0 }) {
					notBefore = releaseNotBefore[m[1]];
					if(notBefore.notNil and: { notBefore > (tt + 1e-9) }) {
						guarded.add([notBefore, m]);
					} {
						keep.add(m);
					};
				} {
					keep.add(m);
				};
			};
			if(keep.size > 0) { kept.add(keep) };
		};
		bundles = this.prChunk(kept);
		while { idx < bundles.size and: { stopped.not } } {
			var now = Main.elapsedTime, load = ledger.prune(now), headroom = safeQueueLimit - load, n, earliest, waitTime;
			if(headroom < minBatchBundles) {
				earliest = ledger.peek;
				waitTime = if(earliest.notNil) { (earliest - now).max(deferMin).min(deferMax) } { 0.1 };
				deferCount = deferCount + 1;
				this.prLog('#defer', [load, waitTime]);
				waitTime.wait;
				if(token != stopToken) { stopped = true };
			} {
				n = (bundles.size - idx).min(maxBundlesPerSend).min(headroom);
				n.do {
					var b = bundles[idx], sentAt, late;
					server.sendBundle(tt - thisThread.seconds, *b);
					sentAt = Main.elapsedTime;
					// A bundle that is already due still takes a slot until the
					// next hardware callback performs it, so a burst of late
					// bundles must not be counted as free: hold its slot for
					// lateSlotHold (one callback at any buffer size up to 1024).
					ledger.push(tt.max(sentAt + lateSlotHold));
					late = sentAt > tt;
					lateAny = lateAny or: late;
					// A note sent after its tag is performed at scsynth's next
					// drain, and a release reaching that same drain is applied
					// before the voice's first calc, which hangs it. In a pair
					// sent together the window is wider (a lead under 25 ms with
					// the release inside the next 20 ms hangs, env_probe5.scd),
					// but the stream only pairs a note with its release when it
					// is catching up late; lateHorizon widens the trigger to
					// short leads for a server where that proves otherwise.
					b.do { |m|
						if(m[0] == '/s_new') {
							releaseNotBefore[m[2]] = if(sentAt > (tt - lateHorizon)) {
								(sentAt + lateReleaseGuard).max(tt + releaseGuard)
							} {
								tt + releaseGuard
							};
						};
					};
					bundles[idx] = [b, late];
					idx = idx + 1;
				};
			};
		};
		sent = thisThread.seconds - playStart;
		bundles.copyRange(0, idx - 1).do { |pair|
			var flags = if(pair[1]) { (late: true) } { nil };
			pair[0].do { |m| this.prTap(tt - playStart, m[0], m.copyRange(1, m.size - 1), sent, flags) };
		};
		if(idx > 0 and: { lastSentTag.isNil or: { tt > lastSentTag } }) { lastSentTag = tt };
		if(stopped) { ^this };
		if(guarded.size > 0) {
			// One bundle per 5 ms of guard tags, not one per release: a late
			// 6000-note instant would otherwise cost 6000 queue slots here.
			var byTag = Dictionary.new, tags = SortedList.new;
			guarded.do { |pair|
				var tag = (pair[0] / 0.005).ceil * 0.005, g = byTag[tag];
				guardCount = guardCount + 1;
				this.prLog('#releaseGuard', [pair[1][1], tt - playStart, tag - playStart]);
				if(g.isNil) { g = List.new; byTag[tag] = g; tags.add(tag) };
				g.add(List[pair[1]]);
			};
			tags.do { |tag| this.prSendTimed(tag, byTag[tag], token) };
		};
	}

	// Bundles of whole groups: a group never straddles two bundles, a bundle
	// never exceeds maxBundleBytes (one oversize group goes alone) or
	// maxBundleMsgs. Every bundle is one scsynth queue slot.
	prChunk { |groups|
		var bundles = List.new, cur = List.new, curSize = 16;
		groups.do { |g|
			var sz = g.sum { |m| m.msgSize + 4 };
			if(cur.size > 0 and: { ((curSize + sz) > maxBundleBytes) or: { (cur.size + g.size) > maxBundleMsgs } }) {
				bundles.add(cur.asArray);
				cur = List.new;
				curSize = 16;
			};
			g.do { |m| cur.add(m) };
			curSize = curSize + sz;
		};
		if(cur.size > 0) { bundles.add(cur.asArray) };
		^bundles
	}

	// Bundles this player (and every other on the server) has in scsynth's
	// queue right now, by the ledger.
	queueLoad {
		^KSQueueLoad.for(server).prune(Main.elapsedTime)
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
	//
	// binaural: the mix is the headphone fold (so the output must be \fold)
	// and each stem is decoded by its own __spatialDecodeW on the geometry
	// buffer, after the track's summing router, onto a private stereo bus:
	// <base>_<track>.<ext> is then a stereo file of the whole track as the
	// listener hears it, and the stems sum to the mix. No _main stem is
	// written in that mode: the mix file is the decoded main.
	record { |path, stems = false, binaural = false, onComplete|
		var dir, base, ext;
		if(payload.isNil or: { isReady.not }) {
			this.prError("record: no file loaded");
			^false
		};
		if(binaural) {
			if(plan.isNil or: { plan.decoder.isNil }) {
				this.prError("record: binaural needs a speaker array with positions, and this file has no geometry to fold with; declare the track with a SpeakerArray and write it again");
				^false
			};
			if(effectiveOutput != \fold) {
				this.prError("record: binaural records the headphone fold, and this player's output is \\" ++ effectiveOutput ++ "; set output = \\fold (which reloads the file) and record again");
				^false
			};
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
			var recs = List.new, stemRecs = List.new, numCh, savedLoop, stopAt, mw, token;
			mw = KSMixer.computeMainWidth(payload);
			numCh = if(effectiveOutput == \array) { mw } { 2 };
			savedLoop = loop;
			loop = false;
			isRecording = true;
			recorders = recs;
			// Open every file before play: the start messages are tagged at
			// playStart, only startupDelay ahead, and each file costs two syncs.
			// Stem buses exist only once play has built the tracks.
			recs.add(KSDiskRecorder(server, dir +/+ (base ++ "." ++ ext), numCh, 0, 0, \addToTail));
			if(stems and: { payload.isBare.not }) {
				var names = if(binaural) { payload.trackNames } { payload.trackNames ++ ["main"] };
				names.do { |nm|
					var w = if(binaural) { 2 } { this.prTrackWidth(nm) };
					var r = KSDiskRecorder(server, dir +/+ (base ++ "_" ++ nm ++ "." ++ ext), w, nil, 0, \addToTail);
					recs.add(r);
					stemRecs.add([nm, r]);
				};
			};
			recs.do { |r| r.prepare(assets) };
			if(this.play.not) {
				this.prStopRecorders;
				loop = savedLoop;
				^nil
			};
			token = stopToken;
			if(binaural and: { stemRecs.size > 0 }) {
				var decoded = mixer.setupStemDecoders(geomBufnum);
				stemRecs.do { |pair| pair[1].bus = decoded[pair[0]].index };
			} {
				stemRecs.do { |pair| pair[1].bus = mixer.trackInfo(pair[0]).fxBus.index };
			};
			recs.do { |r| this.prSendTimed(playStart, [List[r.startMessage(this.nextNodeID)]], token) };
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

	// The width KSMixer.build gives a track, read from the load's plan.
	prTrackWidth { |name|
		if(plan.isNil) { ^2 };
		if(name == "main") { ^plan.mainWidth };
		^plan.widths[name] ? 2
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
