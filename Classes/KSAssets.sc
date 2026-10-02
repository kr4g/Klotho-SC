// KSAssets: SynthDefs, gate knowledge and sample buffers for one server.
//
// The browser engine ships Klotho's precompiled .scsyndef bytes and consults a
// manifest to decide whether a def is gated. Natively the same bytes are loaded
// with /d_loadDir (loadSynthDefDir / loadKlothoDefs) and the gate question is
// answered from the SynthDesc that SynthDescLib reads out of the same file.
//
// When the Klotho assets are not loaded, the infrastructure defs the player
// itself needs (__busRouter, __klEnvCtrl, the width families) are built here
// in sclang from the same source text Klotho compiles them from, at the same
// widths Klotho precompiles (1 2 4 6 8 12 16 24 32) and no others, so a width
// that the browser refuses is refused here too.

KSAssets {
	classvar <>klothoAssetsDir;
	classvar <precompiledWidths;

	var <server;
	var <loadedDirs, <fallbackDefs;
	var <sampleMap, <sampleBuffers;
	var <>samplePaths;
	var bundledSamples;

	*initClass {
		precompiledWidths = #[1, 2, 4, 6, 8, 12, 16, 24, 32];
		klothoAssetsDir = "~/Klotho/klotho/utils/playback/supersonic/assets".standardizePath;
	}

	*new { |server|
		^super.new.init(server)
	}

	init { |argServer|
		server = argServer;
		loadedDirs = List.new;
		fallbackDefs = IdentityDictionary.new;
		sampleMap = Dictionary.new;
		sampleBuffers = Dictionary.new;
		samplePaths = Dictionary.new;
		SynthDescLib.global.addServer(server);
		ServerBoot.add(this, server);
	}

	// Re-send what we loaded or defined when the server comes back.
	doOnServerBoot {
		loadedDirs.do { |dir| server.sendMsg("/d_loadDir", dir) };
		fallbackDefs.do { |def| def.send(server) };
	}

	// ---- SynthDef directories ---------------------------------------------

	// Load every .scsyndef under `path` into the server (/d_loadDir) and into
	// SynthDescLib (so the controls, including `gate`, are known). Returns the
	// number of SynthDescs read. Syncs when called inside a Routine.
	loadSynthDefDir { |path, sync = true|
		var files, n = 0;
		path = path.standardizePath;
		if(File.exists(path).not) {
			Error("KSAssets: no such directory %".format(path)).throw
		};
		files = (path +/+ "*.scsyndef").pathMatch;
		server.sendMsg("/d_loadDir", path);
		if(loadedDirs.includesEqual(path).not) { loadedDirs.add(path) };
		SynthDescLib.global.read(path +/+ "*.scsyndef");
		n = files.size;
		if(sync and: { thisThread.isKindOf(Routine) } and: { server.serverRunning }) { server.sync };
		^n
	}

	// Klotho's own assets: synthdefs/{infra,instruments,effects} under `dir`
	// (default: the Klotho checkout's assets folder).
	loadKlothoDefs { |dir, sync = true|
		var n = 0, base = (dir ? klothoAssetsDir).standardizePath +/+ "synthdefs";
		#["infra", "instruments", "effects"].do { |sub|
			var p = base +/+ sub;
			if(File.exists(p)) { n = n + this.loadSynthDefDir(p, false) };
		};
		if(sync and: { thisThread.isKindOf(Routine) } and: { server.serverRunning }) { server.sync };
		^n
	}

	hasDef { |name|
		^SynthDescLib.global.at(name.asSymbol).notNil
	}

	missingDefs { |names|
		^names.reject { |n| this.hasDef(n) }.asArray.sort
	}

	controlNames { |name|
		var desc = SynthDescLib.global.at(name.asSymbol);
		if(desc.isNil) { ^nil };
		^desc.controlNames
	}

	// Gate knowledge, in the order the browser would have it: the compiled
	// def's own controls; failing that, the file's meta.defs[name].gate; failing
	// that, ungated (no auto-release).
	isGated { |name, metaDefs|
		var desc = SynthDescLib.global.at(name.asSymbol), d;
		if(desc.notNil) { ^desc.controlDict.includesKey(\gate) };
		if(metaDefs.notNil) {
			d = metaDefs[name.asString];
			if(d.notNil and: { d.gate.notNil }) { ^d.gate };
		};
		^false
	}

	// ---- sclang fallbacks (same source as Klotho's .scd files) ---------------

	prDefine { |def|
		fallbackDefs[def.name.asSymbol] = def;
		def.add;
		if(server.serverRunning and: { SynthDescLib.global.servers.includes(server).not }) {
			def.send(server);
		};
	}

	ensureInfra {
		if(this.hasDef(\__busRouter).not) {
			this.prDefine(SynthDef(\__busRouter, { |inBus=0, outBus=0, gain=1.0|
				var sig = In.ar(inBus, 2);
				sig = sig * gain;
				ReplaceOut.ar(inBus, sig);
				Out.ar(outBus, sig);
			}));
		};
		if(this.hasDef(\__busRouterMonitor).not) {
			this.prDefine(SynthDef(\__busRouterMonitor, { |inBus=0, outBus=0, gain=1.0, id=0|
				var amp, peak;
				var sig = In.ar(inBus, 2);
				sig = sig * gain;
				amp = Amplitude.kr(sig, 0.01, 0.1);
				peak = amp.sum * 0.5;
				SendReply.kr(Impulse.kr(20), '/trackLevel', [peak, id]);
				ReplaceOut.ar(inBus, sig);
				Out.ar(outBus, sig);
			}));
		};
		if(this.hasDef(\__chainLimiter).not) {
			this.prDefine(SynthDef(\__chainLimiter, { |inBus=0, outBus=0, gain=1.0, preGain=1.0, limitLevel=0.95, lookAhead=0.01|
				var sig = In.ar(inBus, 2);
				sig = sig * preGain.max(0.0);
				sig = Limiter.ar(sig, limitLevel.clip(0.01, 1.0), lookAhead.max(0.001));
				sig = sig * gain.max(0.0);
				sig = LeakDC.ar(sig);
				ReplaceOut.ar(inBus, sig);
				Out.ar(outBus, sig);
			}));
		};
		if(this.hasDef(\__klEnvCtrl).not) {
			// Klotho's shipped def: sample-and-hold in ~30 ms steps so a
			// Changed()-retriggered glide (VarLag warp: \exp) re-arms.
			this.prDefine(SynthDef(\__klEnvCtrl, { |bufnum=0, bus=0, dur=1, startFrame=0, numFrames=512|
				var phase = Line.kr(startFrame, startFrame + numFrames - 1, dur, doneAction: 2);
				var stepFrames = ((numFrames / dur) * 0.03).max(1.0);
				var stepped = startFrame + (((phase - startFrame) / stepFrames).floor * stepFrames);
				Out.kr(bus, BufRd.kr(1, bufnum, stepped, interpolation: 2));
			}));
		};
	}

	*routerDefName { |width|
		^if(width == 2) { "__busRouter" } { "__busRouter" ++ width }
	}

	*decoderDefName { |width|
		^"__spatialDecode" ++ width
	}

	*arrayOutDefName { |width|
		^"__spatialArrayOut" ++ width
	}

	// The width family, built only at the widths Klotho precompiles.
	ensureWidthFamily { |n|
		var maxDelay = 0.33;
		if(precompiledWidths.includes(n).not) { ^false };
		if(n == 2) { this.ensureInfra };
		if(this.hasDef(this.class.routerDefName(n)).not) {
			this.prDefine(SynthDef(this.class.routerDefName(n).asSymbol, { |inBus=0, outBus=0, gain=1.0|
				var sig = In.ar(inBus, n);
				sig = sig * gain;
				ReplaceOut.ar(inBus, sig);
				Out.ar(outBus, sig);
			}));
		};
		if(this.hasDef(this.class.arrayOutDefName(n)).not) {
			this.prDefine(SynthDef(this.class.arrayOutDefName(n).asSymbol, { |inBus=0, outBus=2, gain=1.0|
				var sig = In.ar(inBus, n);
				sig = sig * gain;
				Out.ar(outBus, sig);
			}));
		};
		if(this.hasDef(this.class.decoderDefName(n)).not) {
			this.prDefine(SynthDef(this.class.decoderDefName(n).asSymbol, { |inBus=0, outBus=0, bufnum=0, gain=1.0|
				var lanes = In.ar(inBus, n).asArray;
				var negTau = -2pi * SampleDur.ir;
				var ls = Array.newClear(n), rs = Array.newClear(n);
				n.do { |k|
					var c = BufRd.kr(6, bufnum, k, loop: 0, interpolation: 1);
					var lane = lanes.at(k);
					var wetL = DelayN.ar(lane, maxDelay, c.at(0)) * c.at(2);
					var wetR = DelayN.ar(lane, maxDelay, c.at(1)) * c.at(3);
					ls[k] = OnePole.ar(wetL, exp(c.at(4) * negTau));
					rs[k] = OnePole.ar(wetR, exp(c.at(5) * negTau));
				};
				Out.ar(outBus, [Mix(ls), Mix(rs)] * gain);
			}));
		};
		^true
	}

	// A level tap for the GUI meters: reads a bus, touches nothing.
	ensureMeter { |n|
		var name = ("__ksMeter" ++ n).asSymbol;
		if(this.hasDef(name).not) {
			this.prDefine(SynthDef(name, { |inBus=0, id=0|
				var sig = In.ar(inBus, n);
				var amp = Amplitude.kr(sig, 0.01, 0.1);
				var peak = if(n > 1) { amp.reduce(\max) } { amp };
				SendReply.kr(Impulse.kr(20), '/trackLevel', [peak, id]);
			}));
		};
		^name
	}

	ensureDiskOut { |n|
		var name = ("__ksDiskOut" ++ n).asSymbol;
		if(this.hasDef(name).not) {
			this.prDefine(SynthDef(name, { |bufnum=0, inBus=0|
				DiskOut.ar(bufnum, In.ar(inBus, n));
			}));
		};
		^name
	}

	// ---- samples ----------------------------------------------------------

	bundledSampleManifest {
		var p, text, d;
		if(bundledSamples.notNil) { ^bundledSamples };
		bundledSamples = Dictionary.new;
		p = klothoAssetsDir +/+ "samples" +/+ "samples.json";
		if(File.exists(p)) {
			text = File.readAllString(p);
			d = try { text.parseJSON } { nil };
			if(d.isKindOf(Dictionary)) {
				d.keysValuesDo { |name, info|
					var file = if(info.isKindOf(Dictionary)) { info["file"] } { nil };
					if(file.notNil) { bundledSamples[name.asString] = klothoAssetsDir +/+ "samples" +/+ file };
				};
			};
		};
		^bundledSamples
	}

	// Where a sample name's file is: the file's own meta.samples, then the
	// user map, then Klotho's bundled samples.json.
	resolveSamplePath { |name, payload|
		var entry, p;
		if(payload.notNil and: { payload.metaSamples.notNil }) {
			entry = payload.metaSamples[name];
			if(entry.notNil and: { entry.file.notNil }) {
				p = entry.file;
				if(p.beginsWith("/").not and: { p.beginsWith("~").not }) { p = payload.dir +/+ p };
				^p.standardizePath
			};
		};
		if(samplePaths[name].notNil) { ^samplePaths[name].standardizePath };
		^this.bundledSampleManifest[name]
	}

	// Load every sample the payload names, once each. Returns the names it
	// could not resolve. Call server.sync afterwards.
	loadSamples { |payload, onLoad|
		var missing = List.new;
		payload.sampleNames.do { |name|
			var p, buf;
			if(sampleMap[name].isNil) {
				p = this.resolveSamplePath(name, payload);
				if(p.isNil or: { File.exists(p).not }) {
					missing.add(name);
				} {
					buf = Buffer.read(server, p);
					sampleBuffers[name] = buf;
					sampleMap[name] = buf.bufnum;
					onLoad.value(buf.bufnum, name, p);
				};
			};
		};
		^missing.asArray
	}

	freeSamples {
		sampleBuffers.do { |buf| buf.free };
		sampleBuffers = Dictionary.new;
		sampleMap = Dictionary.new;
	}
}
