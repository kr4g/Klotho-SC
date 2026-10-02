// KSPayload: a Score.write file, decoded by schema and validated.
//
// Reads {"meta": {...}, "events": [...]} as Klotho's Score.write writes it
// (klotho/thetos/composition/score.py) and recovers the types the browser
// scheduler sees: ids, group names, def names and speaker labels stay strings;
// starts, durations and pfield values become numbers; null stays nil.
//
// Also reads the companion control buffer (<stem>.wav, mono float32) and
// infers the control-envelope block size when meta.blockSize is absent:
// numFrames / descriptors, which must divide exactly.
//
// Every structural refusal throws an Error here, before the player has sent a
// single message. Soft problems are collected in `notes` and reported by the
// player as '#note' / '#warn' lines.

KSPayload {
	var <path, <dir, <wavPath;
	var <raw, <meta;
	var <groups, <inserts, <spatial, <descriptors;
	var <blockSize, <blockSizeSource;
	var <controlFloats, <numFrames;
	var <metaDefNames, <metaDefs, <metaSamples, <metaBlockSize;
	var <events, <skippedTypes;
	var <pieceDur, <isBare;
	var <notes;

	*read { |path|
		^super.new.init(path)
	}

	init { |argPath|
		var file, text;
		path = argPath;
		dir = path.dirname;
		notes = List.new;
		skippedTypes = Dictionary.new;
		file = File(path, "r");
		if(file.isOpen.not) {
			Error("KSPayload: could not open %".format(path)).throw
		};
		text = file.readAllString;
		file.close;
		raw = try { text.parseJSON } { nil };
		if(raw.isNil or: { raw.isKindOf(Dictionary).not }) {
			Error("KSPayload: % is not a JSON object".format(path)).throw
		};
		if(raw["events"].isNil or: { raw["events"].isKindOf(Array).not }) {
			Error("KSPayload: % has no \"events\" array".format(path)).throw
		};
		meta = raw["meta"];
		if(meta.notNil and: { meta.isKindOf(Dictionary).not }) {
			Error("KSPayload: \"meta\" must be an object").throw
		};
		meta = meta ?? { Dictionary.new };
		this.decodeMeta;
		this.decodeEvents(raw["events"]);
		this.readControlBuffer;
		this.inferBlockSize;
		pieceDur = this.computePieceDur;
	}

	// ---- meta ------------------------------------------------------------

	decodeMeta {
		var g = meta["groups"], ins = meta["inserts"], sp = meta["spatial"], ce = meta["controlEnvelopes"];

		groups = nil;
		if(g.notNil) {
			if(g.isKindOf(Array).not) { Error("KSPayload: meta.groups must be an array").throw };
			groups = g.collect { |x| KSJSON.str(x) ? "" };
		};

		inserts = nil;
		if(ins.notNil) {
			if(ins.isKindOf(Dictionary).not) { Error("KSPayload: meta.inserts must be an object").throw };
			inserts = Dictionary.new;
			ins.keysValuesDo { |track, specs|
				var list = if(specs.isKindOf(Array)) { specs } { [specs] };
				inserts[track.asString] = list.collect { |spec|
					var args = Dictionary.new;
					if(spec.isKindOf(Dictionary).not) {
						Error("KSPayload: insert on track % is not an object".format(track)).throw
					};
					(spec["args"] ?? { Dictionary.new }).keysValuesDo { |k, v|
						args[k.asString] = KSJSON.value(v);
					};
					(
						uid: KSJSON.str(spec["uid"]),
						defName: KSJSON.str(spec["defName"] ?? { spec["synthName"] }),
						args: args
					)
				};
			};
		};

		spatial = nil;
		if(sp.notNil) {
			if(sp.isKindOf(Dictionary).not) { Error("KSPayload: meta.spatial must be an object").throw };
			spatial = this.decodeSpatial(sp);
		};

		descriptors = [];
		if(ce.notNil) {
			if(ce.isKindOf(Array).not) { Error("KSPayload: meta.controlEnvelopes must be an array").throw };
			descriptors = ce.collect { |d, i|
				var targets;
				if(d.isKindOf(Dictionary).not) { Error("KSPayload: controlEnvelopes[%] is not an object".format(i)).throw };
				targets = (d["targets"] ?? { [] }).collect { |t|
					(id: KSJSON.str(t["id"]), startTime: KSJSON.num(t["startTime"]) ? 0.0)
				};
				(
					blockIndex: KSJSON.int(d["blockIndex"]) ? i,
					start: KSJSON.num(d["start"]) ? 0.0,
					dur: KSJSON.num(d["dur"]) ? 0.0,
					pfields: (d["pfields"] ?? { [] }).collect { |p| KSJSON.str(p) },
					targets: targets
				)
			};
		};

		metaBlockSize = KSJSON.int(meta["blockSize"]);
		metaDefNames = meta["defNames"];
		if(metaDefNames.notNil) { metaDefNames = metaDefNames.collect { |x| KSJSON.str(x) } };
		metaDefs = nil;
		if(meta["defs"].isKindOf(Dictionary)) {
			metaDefs = Dictionary.new;
			meta["defs"].keysValuesDo { |name, d|
				var gate = if(d.isKindOf(Dictionary)) { KSJSON.bool(d["gate"]) } { nil };
				metaDefs[name.asString] = (
					gate: gate,
					kind: if(d.isKindOf(Dictionary)) { KSJSON.str(d["kind"]) } { nil },
					ins: if(d.isKindOf(Dictionary)) { KSJSON.int(d["ins"]) } { nil },
					outs: if(d.isKindOf(Dictionary)) { KSJSON.int(d["outs"]) } { nil }
				);
			};
		};
		metaSamples = nil;
		if(meta["samples"].isKindOf(Dictionary)) {
			metaSamples = Dictionary.new;
			meta["samples"].keysValuesDo { |name, d|
				var file = if(d.isKindOf(Dictionary)) { KSJSON.str(d["file"]) } { KSJSON.str(d) };
				metaSamples[name.asString] = (file: file);
			};
		};

		// The JS takes the bare single-group path only when meta has none of
		// these keys at all; an empty array or object still counts as present.
		isBare = groups.isNil and: { inserts.isNil } and: { spatial.isNil };
	}

	decodeSpatial { |sp|
		var arrays = Dictionary.new, tracks = Dictionary.new;
		(sp["arrays"] ?? { Dictionary.new }).keysValuesDo { |id, a|
			var dec = nil, d = a["decoder"];
			if(d.isKindOf(Dictionary)) {
				dec = (
					kind: KSJSON.str(d["kind"]),
					stride: KSJSON.int(d["stride"]),
					coefficients: (d["coefficients"] ?? { [] }).collect { |x| KSJSON.num(x) ? 0.0 },
					maxDelay: KSJSON.num(d["maxDelay"]),
					fields: (d["fields"] ?? { [] }).collect { |x| KSJSON.str(x) },
					// The travel scale the table was written at, and the unscaled
					// propagation part of each lane's delay (seconds), so the
					// player can re-scale without the geometry. Absent in files
					// written before the scale existed.
					travel: KSJSON.num(d["travel"]) ? 1.0,
					shadowIldDb: KSJSON.num(d["shadowIldDb"]) ? 0.0,
					travelDelays: d["travelDelays"] !? { |td| td.collect { |x| KSJSON.num(x) ? 0.0 } }
				);
			};
			arrays[id.asString] = (
				name: KSJSON.str(a["name"]) ? id.asString,
				labels: (a["labels"] ?? { [] }).collect { |x| KSJSON.str(x) },
				width: KSJSON.int(a["width"]),
				positions: a["positions"],
				decoder: dec
			);
		};
		(sp["tracks"] ?? { Dictionary.new }).keysValuesDo { |name, t|
			tracks[name.asString] = (
				array: if(t.isKindOf(Dictionary)) { KSJSON.str(t["array"]) } { nil },
				widthRaw: if(t.isKindOf(Dictionary)) { t["width"] } { nil },
				width: if(t.isKindOf(Dictionary)) { KSJSON.num(t["width"]) } { nil }
			);
		};
		^(arrays: arrays, tracks: tracks)
	}

	// ---- events ----------------------------------------------------------

	decodeEvents { |list|
		var out = List.new;
		list.do { |e, i|
			var type, id, start, pf, ev;
			if(e.isKindOf(Dictionary).not) {
				Error("KSPayload: events[%] is not an object".format(i)).throw
			};
			type = KSJSON.str(e["type"]);
			if(["new", "set", "release"].includesEqual(type).not) {
				skippedTypes[type ? "nil"] = (skippedTypes[type ? "nil"] ? 0) + 1;
			} {
				id = KSJSON.str(e["id"]);
				if(id.isNil or: { id.size == 0 }) {
					Error("KSPayload: events[%] (%) has no id".format(i, type)).throw
				};
				start = KSJSON.num(e["start"]);
				if(start.isNil or: { start < 0 } or: { start.isNaN }) {
					Error("KSPayload: events[%] (id %) has an invalid start %".format(i, id, e["start"])).throw
				};
				pf = Dictionary.new;
				if(e["pfields"].notNil) {
					if(e["pfields"].isKindOf(Dictionary).not) {
						Error("KSPayload: events[%] pfields must be an object".format(i)).throw
					};
					e["pfields"].keysValuesDo { |k, v| pf[k.asString] = KSJSON.value(v) };
				};
				ev = (
					type: type,
					id: id,
					start: start,
					defName: KSJSON.str(e["defName"] ?? { e["synthName"] }),
					dur: KSJSON.num(e["dur"]),
					releaseAfter: KSJSON.bool(e["releaseAfter"]) == true,
					pfields: pf,
					group: KSJSON.str(e["group"]),
					speaker: e["speaker"],
					speakerLane: KSJSON.int(e["speakerLane"]),
					stepIndex: KSJSON.int(e["_stepIndex"]),
					index: i
				);
				out.add(ev);
			};
		};
		events = out.asArray;
		skippedTypes.keysValuesDo { |t, n|
			notes.add("% event(s) of type % skipped (only new, set and release play)".format(n, t));
		};
	}

	// ---- control buffer ---------------------------------------------------

	// Path(filepath).with_suffix('.wav'): replace the last extension only.
	*companionWavPath { |p|
		var base = p.basename, d = p.dirname, i = base.findBackwards(".");
		if(i.isNil or: { i == 0 }) { ^d +/+ (base ++ ".wav") };
		^d +/+ (base.copyRange(0, i - 1) ++ ".wav")
	}

	readControlBuffer {
		var sf, data;
		wavPath = this.class.companionWavPath(path);
		controlFloats = nil;
		numFrames = 0;
		if(File.exists(wavPath).not) {
			if(descriptors.size > 0) {
				notes.add("meta.controlEnvelopes present but no companion % : no envelope synth, no /c_set and no /n_map is sent".format(wavPath.basename));
			};
			wavPath = nil;
			^this
		};
		sf = SoundFile.openRead(wavPath);
		if(sf.isNil) {
			Error("KSPayload: could not read the control buffer %".format(wavPath)).throw
		};
		if(sf.numChannels != 1) {
			sf.close;
			Error("KSPayload: control buffer % must be mono, has % channels".format(wavPath, sf.numChannels)).throw
		};
		if(sf.sampleFormat != "float") {
			notes.add("control buffer % is % rather than float32".format(wavPath.basename, sf.sampleFormat));
		};
		data = FloatArray.newClear(sf.numFrames);
		sf.readData(data);
		sf.close;
		controlFloats = data;
		numFrames = data.size;
	}

	inferBlockSize {
		var q;
		blockSize = 512;
		blockSizeSource = "default";
		if(metaBlockSize.notNil and: { metaBlockSize > 0 }) {
			blockSize = metaBlockSize;
			blockSizeSource = "meta.blockSize";
			^this
		};
		if(controlFloats.notNil and: { descriptors.size > 0 }) {
			q = numFrames / descriptors.size;
			if(q != q.round or: { q < 1 }) {
				Error("KSPayload: cannot infer blockSize: % frames / % descriptors is not an integer".format(numFrames, descriptors.size)).throw
			};
			blockSize = q.asInteger;
			blockSizeSource = "inferred numFrames/descriptors";
		};
	}

	// Envelopes are active only with a buffer to drive them (the browser gets
	// bufferB64 = null without the .wav and sets nothing up).
	hasControlEnvelopes {
		^descriptors.size > 0 and: { controlFloats.notNil }
	}

	// ---- derived facts ----------------------------------------------------

	// scheduler_core.js _computePieceDur, over the playable events.
	computePieceDur {
		var dur = 0.0;
		events.do { |ev|
			var evEnd = ev.start;
			if((ev.type == "new" or: { ev.type == "set" }) and: { ev.dur.notNil }) {
				evEnd = evEnd + ev.dur;
			};
			if(evEnd > dur) { dur = evEnd };
		};
		^dur
	}

	// scheduler_core.js _resolveDefName.
	*resolveDefName { |name|
		if(name == "__rest__") { ^"__rest__" };
		if(name.isNil or: { name.size == 0 } or: { name == "sonic-pi-beep" }) { ^"kl_tri" };
		^name
	}

	// Every SynthDef name the events and inserts will ask for (not the infra
	// family, which depends on the mixer's plan).
	instrumentDefNames {
		var set = Set.new;
		events.do { |ev|
			var d;
			if(ev.type == "new") {
				d = this.class.resolveDefName(ev.defName);
				if(d != "__rest__") { set.add(d) };
			};
		};
		if(inserts.notNil) {
			inserts.do { |list| list.do { |spec| if(spec.defName.notNil) { set.add(spec.defName) } } };
		};
		^set.asArray.sort
	}

	// Sample names: every string value on a buf* pfield.
	sampleNames {
		var set = Set.new;
		events.do { |ev|
			ev.pfields.keysValuesDo { |k, v|
				if(v.isString and: { k.beginsWith("buf") }) { set.add(v) };
			};
		};
		^set.asArray.sort
	}

	trackNames {
		^(groups ? []).copy
	}

	insertSpecs { |track|
		if(inserts.isNil) { ^nil };
		^inserts[track]
	}
}
