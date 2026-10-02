// KSTrace: the oscTap sink that writes a klotho-supersonic-trace/1 file.
//
// One line per OSC message the player hands to scsynth (one line per message of
// a bundle), in send order, plus '#' lifecycle lines. The format is specified in
// the parity folder's README (agents/overnight-20261001/parity/README.md):
//
//   {"t": <tag - playStart | null>, "addr": "...", "args": [...], "sent": <s>}
//
// 't' is rounded to 6 decimals; 'sent' is omitted when it is unknown (lines sent
// before any play, i.e. at load). stop() marks every bundle whose tag lies after
// the stop instant as "purged": the score group is gone, so scsynth never runs
// it. Records stay in memory until write(), which is what lets stop() mark them.

KSTrace {
	var <records, <>header, <>path;

	*new { |path, header|
		^super.new.init(path, header)
	}

	init { |argPath, argHeader|
		path = argPath;
		header = argHeader ?? { Dictionary.new };
		records = List.new;
	}

	// The oscTap entry point: EventScheduler calls this with
	// (t, addr, args, sent, flags) where flags is nil or an Event with any of
	// \late, \dropped, \purged set true.
	tap { |t, addr, args, sent, flags|
		var rec = (t: t, addr: addr.asString, args: args.asArray.copy, sent: sent);
		if(flags.notNil) {
			if(flags[\late] == true) { rec[\late] = true };
			if(flags[\dropped] == true) { rec[\dropped] = true };
			if(flags[\purged] == true) { rec[\purged] = true };
		};
		records.add(rec);
		^rec
	}

	// A Function usable directly as EventScheduler.oscTap.
	asTapFunction {
		^{ |t, addr, args, sent, flags| this.tap(t, addr, args, sent, flags) }
	}

	// Bundles already handed over whose tag lies after `time` (seconds from
	// playStart) are marked purged: the group they address has been freed.
	markPurgedAfter { |time|
		var n = 0;
		records.do { |rec|
			if(rec[\t].notNil and: { rec[\purged] != true } and: { rec[\t] > (time + 1e-9) }) {
				rec[\purged] = true;
				n = n + 1;
			};
		};
		^n
	}

	count { |addr|
		^records.count { |r| r[\addr] == addr }
	}

	lines {
		var out = List.new;
		out.add(KSJSON.encode((header: header)));
		records.do { |rec|
			var d = Dictionary.new;
			d["t"] = if(rec[\t].isNil) { nil } { this.round6(rec[\t]) };
			d["addr"] = rec[\addr];
			d["args"] = rec[\args].collect { |a| this.cleanArg(a) };
			if(rec[\sent].notNil) { d["sent"] = this.round6(rec[\sent]) };
			if(rec[\late] == true) { d["late"] = true };
			if(rec[\dropped] == true) { d["dropped"] = true };
			if(rec[\purged] == true) { d["purged"] = true };
			out.add(this.encodeOrdered(d, ["t", "addr", "args", "sent", "late", "dropped", "purged"]));
		};
		^out
	}

	// Keys in the README's order, so a trace reads like the oracle's.
	encodeOrdered { |dict, order|
		var parts = List.new;
		order.do { |k|
			if(dict.includesKey(k)) {
				parts.add(KSJSON.quote(k) ++ ":" ++ KSJSON.encode(dict[k]));
			};
		};
		^"{" ++ parts.join(",") ++ "}"
	}

	round6 { |x|
		var v = (x * 1e6).round / 1e6;
		if(v == 0) { ^0.0 };
		^v
	}

	cleanArg { |a|
		if(a.isKindOf(Symbol)) { ^a.asString };
		if(a.isKindOf(Float) and: { a.isNaN or: { a.abs == inf } }) { ^a.asString };
		^a
	}

	write { |argPath|
		var file, p = argPath ? path;
		if(p.isNil) { Error("KSTrace: no path to write to").throw };
		file = File(p, "w");
		if(file.isOpen.not) { Error("KSTrace: could not open % for writing".format(p)).throw };
		this.lines.do { |l| file.write(l); file.write("\n") };
		file.close;
		^p
	}
}
