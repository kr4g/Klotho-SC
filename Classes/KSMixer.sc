// KSMixer: the native setupTracks.
//
// Builds the group, bus and insert topology of scheduler_score.js setupTracks,
// message for message and in the same order, under one score group at the head
// of the root node so that stop and ring-out can free exactly this play and
// nothing else on the server:
//
//   score group (/g_new S 0 0)
//     per track in meta.groups order: parent (tail of S), src (head of parent),
//       fx (after src); srcBus and fxBus, one channel per speaker
//     main, the same way, last, at the widest declared width (at least 2)
//     per track: the insert chain (inBus/outBus, intermediate buses as wide
//       as the chain) at the tail of fx, or a bypass router at its head
//     per non-main track: a router at the tail of its parent, fxBus -> main src
//     main's output stage: the binaural decoder (fold), the array mirror
//       (array), or a stereo router when there is no geometry
//     "default" aliases main
//
// Bus numbers come from the server's own allocator, so this coexists with
// whatever else the host session has running; the parity comparison
// canonicalises them by first appearance.

KSMixer {
	var <scheduler, <server;
	var <scoreGroup, <trackMap, <trackOrder, <mainWidth, <plan, <outputMode, <isBare;
	var <buses, <isBuilt, <stemLayout, <meterNodes;
	var <>gains, <>mutes, <>solos;

	*new { |scheduler|
		^super.new.init(scheduler)
	}

	init { |argScheduler|
		scheduler = argScheduler;
		server = scheduler.server;
		buses = List.new;
		isBuilt = false;
		gains = Dictionary.new;
		mutes = Set.new;
		solos = Set.new;
		meterNodes = Dictionary.new;
	}

	// ---- the spatial plan (scheduler_score.js _spatialPlan) -----------------

	*computeMainWidth { |payload|
		var w = 2;
		if(payload.spatial.isNil) { ^w };
		payload.spatial.tracks.do { |t|
			if(t.width.notNil and: { t.width > w }) { w = t.width.asInteger };
		};
		^w
	}

	requireDef { |name, why|
		if(scheduler.assets.hasDef(name)) { ^this };
		Error("[Klotho] this player has no SynthDef named % which % needs. Klotho precompiles the spatial family at widths 1, 2, 4, 6, 8, 12, 16, 24 and 32; a speaker count outside that list has no compiled def to send, and scsynth does not refuse a missing def -- it creates nothing and the array plays SILENTLY. Declare an array at one of those widths.".format(name, why)).throw
	}

	// Validate meta.spatial and decide widths, the decoder's geometry and the
	// output def. Every refusal in here happens before any node or bus exists.
	spatialPlan { |payload, mode|
		var sp = payload.spatial, tracks, arrays, names, widths, mw = 2, order, widest, chosen;
		var arrayName, arrayMeta, decoder, distinct, seen, warnings = List.new, declaredMain;
		if(sp.isNil) { ^nil };
		tracks = sp.tracks;
		arrays = sp.arrays;
		names = tracks.keys.asArray.collect(_.asString).sort;
		if(names.size == 0) { ^nil };
		widths = Dictionary.new;
		names.do { |nm|
			var w = tracks[nm].width;
			if(w.isNil or: { w.isNaN } or: { w != w.round } or: { w < 1 }) {
				Error("[Klotho] spatial track % declares width %; a speaker count must be a whole number of at least 1.".format(nm.quote, tracks[nm].widthRaw)).throw
			};
			if(w > 32) {
				Error("[Klotho] spatial track % declares % speakers and the decoder family stops at 32. SpeakerArray refuses a wider array at construction, so this payload was not built by one. Refusing here rather than sending it: scsynth would skip the oversized SynthDef and the array would play SILENTLY.".format(nm.quote, w.asInteger)).throw
			};
			widths[nm] = w.asInteger;
			if(w > mw) { mw = w.asInteger };
		};
		order = (payload.groups ? []).copy.add("main");
		widest = List.new;
		order.do { |n| if(widths[n] == mw and: { widest.includesEqual(n).not }) { widest.add(n) } };
		names.do { |n| if(widths[n] == mw and: { widest.includesEqual(n).not }) { widest.add(n) } };
		chosen = if(widest.includesEqual("main")) { "main" } { widest[0] };
		arrayName = if(chosen.notNil) { tracks[chosen][\array] } { nil };
		arrayMeta = if(arrayName.notNil) { arrays[arrayName] } { nil };
		decoder = if(arrayMeta.notNil) { arrayMeta.decoder } { nil };

		distinct = List.new;
		widest.do { |n| var an = tracks[n][\array]; if(an.notNil and: { distinct.includesEqual(an).not }) { distinct.add(an) } };
		if(distinct.size > 1) {
			warnings.add("[Klotho] tracks % declare % different speaker arrays (%) at the same width. They sum lane for lane onto one set of speakers; the headphone fold uses %'s geometry.".format(widest.join(", "), distinct.size, distinct.join(", "), arrayName.quote));
		};

		if(decoder.notNil) {
			if(decoder.stride != 6) {
				Error("[Klotho] the geometry table for array % declares stride % and the compiled decoders index 6 fields per lane positionally. Loading it would misread every lane -- a silent geometry error, not a load failure.".format(arrayName.quote, decoder.stride)).throw
			};
			if(decoder.coefficients.size != (mw * 6)) {
				Error("[Klotho] the geometry table for array % has % floats; a %-lane decoder needs exactly %.".format(arrayName.quote, decoder.coefficients.size, mw, mw * 6)).throw
			};
			if(mode != \array or: { mw <= 2 }) {
				this.requireDef(KSAssets.decoderDefName(mw), "the %-speaker headphone fold".format(mw));
			};
		};
		seen = Set.new;
		names.do { |n| seen.add(widths[n]) };
		seen.add(mw);
		seen.asArray.sort.do { |w| this.requireDef(KSAssets.routerDefName(w), "a %-channel track chain".format(w)) };
		if(mode == \array and: { mw > 2 }) {
			this.requireDef(KSAssets.arrayOutDefName(mw), "the %-channel array output".format(mw));
		};
		declaredMain = widths["main"];
		^(
			widths: widths, mainWidth: mw, arrayName: arrayName, decoderTrack: chosen,
			decoder: decoder, geomKey: arrayName.asString ++ "|" ++ mw,
			warnings: warnings, declaredMain: declaredMain
		)
	}

	// ---- build -------------------------------------------------------------

	widthOf { |name|
		var w;
		if(plan.isNil) { ^2 };
		w = plan.widths[name];
		^if(w.notNil) { w } { 2 }
	}

	effectiveGain { |name|
		var g = gains[name] ? 1.0;
		if(mutes.includes(name)) { ^0.0 };
		if(solos.size > 0 and: { solos.includes(name).not } and: { name != "main" }) { ^0.0 };
		^g
	}

	// Sends the whole topology. `geomBufnum` is the preloaded geometry buffer
	// (fold mode) or nil. Throws on a refusal; whatever was created by then is
	// the caller's to free (freeNow).
	build { |payload, mode, geomBufnum, stemTaps = false, monitoring = false|
		var names, mainSrcBus, mainFxBus, allTracks, insertSpecs, narrow, outGain;
		isBare = payload.isBare;
		outputMode = mode;
		trackMap = nil;
		trackOrder = [];
		stemLayout = nil;
		meterNodes = Dictionary.new;

		scoreGroup = scheduler.nextNodeID;
		scheduler.sendMsg('/g_new', [scoreGroup, 0, 0]);
		isBuilt = true;
		if(isBare) { ^this };

		plan = this.spatialPlan(payload, mode);
		plan !? { plan.warnings.do { |w| scheduler.warn(w) } };
		mainWidth = if(plan.notNil) { plan.mainWidth } { 2 };
		names = payload.trackNames;
		trackMap = Dictionary.new;

		names.do { |nm|
			var w = this.widthOf(nm), parent, src, fx, srcBus, fxBus;
			parent = scheduler.nextNodeID;
			src = scheduler.nextNodeID;
			fx = scheduler.nextNodeID;
			srcBus = this.allocBus(w);
			fxBus = this.allocBus(w);
			scheduler.sendMsg('/g_new', [parent, 1, scoreGroup]);
			scheduler.sendMsg('/g_new', [src, 0, parent]);
			scheduler.sendMsg('/g_new', [fx, 3, src]);
			trackMap[nm] = (
				name: nm, parentGroup: parent, srcGroup: src, fxGroup: fx,
				srcBus: srcBus, fxBus: fxBus, width: w, insertNodes: Dictionary.new
			);
		};
		trackOrder = names.copy;

		block {
			var parent = scheduler.nextNodeID, src = scheduler.nextNodeID, fx = scheduler.nextNodeID;
			mainSrcBus = this.allocBus(mainWidth);
			mainFxBus = this.allocBus(mainWidth);
			scheduler.sendMsg('/g_new', [parent, 1, scoreGroup]);
			scheduler.sendMsg('/g_new', [src, 0, parent]);
			scheduler.sendMsg('/g_new', [fx, 3, src]);
			trackMap["main"] = (
				name: "main", parentGroup: parent, srcGroup: src, fxGroup: fx,
				srcBus: mainSrcBus, fxBus: mainFxBus, width: mainWidth, insertNodes: Dictionary.new
			);
		};

		insertSpecs = payload.inserts ?? { Dictionary.new };
		if(plan.notNil and: { mainWidth > 2 } and: { plan.widths["main"].isNil }
			and: { insertSpecs["main"].notNil } and: { insertSpecs["main"].size > 0 }) {
			scheduler.warn("[Klotho] the master chain has inserts and main was widened to % channels by a spatial track, but main declares no speakers of its own -- so those inserts were only ever checked as STEREO. A stereo insert on a %-channel chain reads and writes two lanes and leaves the other % unwritten: those speakers will be SILENT. Declare the master with its array too, or take the inserts off main.".format(mainWidth, mainWidth, mainWidth - 2));
		};
		if(plan.notNil and: { plan.declaredMain.notNil } and: { plan.declaredMain < mainWidth }) {
			scheduler.warn("[Klotho] main declares % speakers but the chain is built % channels wide (another track declares %, and every track sums into main); the array declared on main describes only lanes 0..% of it.".format(plan.declaredMain, mainWidth, mainWidth, plan.declaredMain - 1));
		};

		allTracks = names ++ ["main"];
		allTracks.do { |tName|
			var track = trackMap[tName], specs = insertSpecs[tName], chainWidth = track.width;
			var chainRouter = KSAssets.routerDefName(chainWidth);
			if(specs.isNil or: { specs.size == 0 }) {
				var bypass = scheduler.nextNodeID;
				scheduler.sendMsg('/s_new', [chainRouter, bypass, 0, track.fxGroup,
					'inBus', track.srcBus.index, 'outBus', track.fxBus.index, 'gain', 1.0]);
				track.insertNodes["__bypass"] = bypass;
			} {
				var prevBus = track.srcBus;
				specs.do { |spec, fi|
					var nextBus = if(fi < (specs.size - 1)) { this.allocBus(chainWidth) } { track.fxBus };
					var fxNode = scheduler.nextNodeID;
					var args = [spec.defName, fxNode, 1, track.fxGroup, 'inBus', prevBus.index, 'outBus', nextBus.index];
					spec.args.keysValuesDo { |k, v| args = args.add(k).add(v) };
					scheduler.sendMsg('/s_new', args);
					track.insertNodes[spec.uid] = fxNode;
					scheduler.registerNode(spec.uid, fxNode, spec.defName);
					prevBus = nextBus;
				};
			};
		};

		narrow = List.new;
		names.do { |rName|
			var rTrack = trackMap[rName], routerId = scheduler.nextNodeID;
			scheduler.sendMsg('/s_new', [KSAssets.routerDefName(rTrack.width), routerId, 1, rTrack.parentGroup,
				'inBus', rTrack.fxBus.index, 'outBus', mainSrcBus.index, 'gain', this.effectiveGain(rName)]);
			rTrack.routerNode = routerId;
			if(rTrack.width < mainWidth) { narrow.add(rName) };
		};
		if(narrow.size > 0) {
			scheduler.warn("[Klotho] no speakers are declared for: %. Those tracks play through the first 2 speakers of the array (lanes 0 and 1), because a stereo signal names no speaker and the room has nowhere else to put it. Declare the track with speakers= to place it anywhere else.".format(narrow.join(", ")));
		};

		outGain = this.effectiveGain("main");
		block {
			var main = trackMap["main"], outId;
			case
			{ mode == \array and: { mainWidth > 2 } } {
				outId = scheduler.nextNodeID;
				scheduler.sendMsg('/s_new', [KSAssets.arrayOutDefName(mainWidth), outId, 1, main.parentGroup,
					'inBus', mainFxBus.index, 'outBus', 0, 'gain', outGain]);
				main.routerNode = outId;
			}
			{ plan.notNil and: { plan.decoder.notNil } } {
				if(geomBufnum.isNil) {
					Error("[Klotho] the headphone fold needs the geometry buffer, which was not loaded; load the file again with output set to \\fold").throw
				};
				outId = scheduler.nextNodeID;
				scheduler.sendMsg('/s_new', [KSAssets.decoderDefName(mainWidth), outId, 1, main.parentGroup,
					'inBus', mainFxBus.index, 'outBus', 0, 'bufnum', geomBufnum, 'gain', outGain]);
				main.decoderNode = outId;
				main.routerNode = outId;
			}
			{
				outId = scheduler.nextNodeID;
				scheduler.sendMsg('/s_new', ['__busRouter', outId, 1, main.parentGroup,
					'inBus', mainFxBus.index, 'outBus', 0, 'gain', outGain]);
				main.routerNode = outId;
				if(plan.notNil) {
					scheduler.warn("[Klotho] the speaker array on this score carries labels but no positions, so there is no geometry to fold with. Only speakers 1 and 2 reach the output; the rest are routed but inaudible here.");
				};
			};
		};

		if(trackMap["default"].isNil) { trackMap["default"] = trackMap["main"] };

		if(stemTaps) { this.setupStemTaps };
		if(monitoring) { this.setupMeters };
		^this
	}

	// scheduler_score.js setupStemTaps: one stereo router per non-main track,
	// after its summing router, onto hardware outputs 2+2i.
	setupStemTaps {
		var names = trackOrder, layout = List.new, folded = List.new;
		if(trackMap.isNil) { ^nil };
		if(names.size > 15) {
			scheduler.warn("[Klotho] stems: only the first 15 of % tracks get separate stems (output-channel limit).".format(names.size));
			names = names.copyRange(0, 14);
		};
		names.do { |nm, i|
			var track = trackMap[nm], outCh = 2 + (2 * i), tapId;
			if(track.notNil and: { track.routerNode.notNil }) {
				if(track.width > 2) { folded.add(nm) };
				tapId = scheduler.nextNodeID;
				scheduler.sendMsg('/s_new', ['__busRouter', tapId, 3, track.routerNode,
					'inBus', track.fxBus.index, 'outBus', outCh, 'gain', 1.0]);
				layout.add((name: nm, ch: [outCh, outCh + 1]));
			};
		};
		if(folded.size > 0) {
			scheduler.warn("[Klotho] stems: % have more than two speakers, and a stem is a stereo pair -- those stems carry speakers 1 and 2 only.".format(folded.join(", ")));
		};
		stemLayout = layout.asArray;
		^stemLayout
	}

	// GUI meters: a tap after each router, reading the post-fader bus.
	setupMeters {
		(trackOrder ++ ["main"]).do { |nm, i|
			var track = trackMap[nm], def, id;
			if(track.notNil and: { track.routerNode.notNil }) {
				def = scheduler.assets.ensureMeter(track.width);
				id = scheduler.nextNodeID;
				scheduler.sendMsg('/s_new', [def, id, 3, track.routerNode, 'inBus', track.fxBus.index, 'id', i]);
				meterNodes[nm] = id;
			};
		};
	}

	allocBus { |width|
		var b = Bus.audio(server, width);
		if(b.isNil) {
			Error("[Klotho] out of private audio buses: a %-channel run could not be allocated".format(width)).throw
		};
		buses.add(b);
		^b
	}

	trackInfo { |name|
		if(trackMap.isNil) { ^nil };
		^trackMap[name]
	}

	routerNode { |name|
		var t = this.trackInfo(name);
		^if(t.notNil) { t.routerNode } { nil }
	}

	// The live router nodes, for the GUI's faders.
	applyGain { |name|
		var node = this.routerNode(name);
		if(node.notNil and: { isBuilt }) {
			scheduler.sendMsg('/n_set', [node, 'gain', this.effectiveGain(name)]);
		};
	}

	applyAllGains {
		if(trackMap.isNil) { ^this };
		(trackOrder ++ ["main"]).do { |nm| this.applyGain(nm) };
	}

	// Take the group and buses away from the mixer so a ring-out can free them
	// later while a new play builds a fresh topology.
	detach {
		var entry = (scoreGroup: scoreGroup, buses: buses);
		scoreGroup = nil;
		buses = List.new;
		trackMap = nil;
		isBuilt = false;
		stemLayout = nil;
		meterNodes = Dictionary.new;
		^entry
	}

	// Free this play's nodes now: /g_freeAll then /n_free on the score group,
	// and give the buses back.
	freeNow {
		var entry = this.detach;
		this.class.freeEntry(scheduler, entry);
	}

	*freeEntry { |scheduler, entry|
		if(entry.scoreGroup.notNil) {
			scheduler.sendMsg('/g_freeAll', [entry.scoreGroup]);
			scheduler.sendMsg('/n_free', [entry.scoreGroup]);
		};
		entry.buses.do { |b| b.free };
	}
}
