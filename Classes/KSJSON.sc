// JSON helpers for Klotho-SC.
//
// Decoding: String:parseJSON (yaml-cpp underneath) returns every scalar as a
// String and JSON null as nil, so the typed shape of a Score.write file is
// recovered by schema (KSPayload), not by guessing from the text. The helpers
// here turn one scalar into a number / boolean / string on request.
//
// Encoding: a small writer for the trace lines (KSTrace). Floats print with the
// fewest digits that round-trip, so a float32 buffer value such as
// 0.05000000074505806 is written exactly and 0.1 stays "0.1".

KSJSON {
	classvar numberRegexp = "^[-+]?([0-9]+\\.?[0-9]*|\\.[0-9]+)([eE][-+]?[0-9]+)?$";

	*isNumberString { |v|
		^v.isString and: { v.size > 0 } and: { numberRegexp.matchRegexp(v) }
	}

	// Scalar -> Float, or nil when it is not a number.
	*num { |v|
		if(v.isNumber) { ^v.asFloat };
		if(this.isNumberString(v)) { ^v.asFloat };
		^nil
	}

	// Scalar -> Integer, or nil when it is not a whole number.
	*int { |v|
		var f = this.num(v);
		if(f.isNil) { ^nil };
		if(f != f.round) { ^nil };
		^f.asInteger
	}

	// Scalar -> Boolean, or nil.
	*bool { |v|
		if(v === true or: { v === false }) { ^v };
		if(v == "true") { ^true };
		if(v == "false") { ^false };
		^nil
	}

	// Scalar -> String, or nil.
	*str { |v|
		if(v.isNil) { ^nil };
		if(v.isString) { ^v };
		if(v.isKindOf(Symbol)) { ^v.asString };
		if(v.isNumber) { ^v.asString };
		^nil
	}

	// A pfield / insert-arg value as the JS scheduler would see it after JSON
	// parsing: numbers, booleans, strings, nil, or a container (Dictionary /
	// Array) that the scheduler then drops.
	*value { |v|
		var b;
		if(v.isNil) { ^nil };
		if(v.isKindOf(Dictionary) or: { v.isKindOf(Array) }) { ^v };
		if(this.isNumberString(v)) { ^v.asFloat };
		b = this.bool(v);
		if(b.notNil) { ^b };
		^v
	}

	// ---- encoding --------------------------------------------------------

	*encode { |v|
		var keys;
		if(v.isNil) { ^"null" };
		if(v === true) { ^"true" };
		if(v === false) { ^"false" };
		if(v.isKindOf(Integer)) { ^v.asString };
		if(v.isKindOf(Float)) { ^this.floatString(v) };
		if(v.isString) { ^this.quote(v) };
		if(v.isKindOf(Symbol)) { ^this.quote(v.asString) };
		if(v.isKindOf(Dictionary)) {
			keys = v.keys.asArray.collect(_.asString).sort;
			^"{" ++ keys.collect({ |k|
				this.quote(k) ++ ":" ++ this.encode(v.at(k) ?? { v.at(k.asSymbol) })
			}).join(",") ++ "}"
		};
		if(v.isKindOf(SequenceableCollection)) {
			^"[" ++ v.collect({ |x| this.encode(x) }).join(",") ++ "]"
		};
		^this.quote(v.asString)
	}

	*floatString { |f|
		if(f.isNaN) { ^"\"nan\"" };
		if(f == inf) { ^"\"inf\"" };
		if(f == inf.neg) { ^"\"-inf\"" };
		if(f.abs < 1e9 and: { f == f.asInteger }) { ^f.asInteger.asString };
		[15, 16, 17].do { |p|
			var s = f.asStringPrec(p);
			if(s.asFloat == f) { ^s };
		};
		^f.asStringPrec(17)
	}

	*quote { |s|
		var out = String.new(s.size + 2);
		out = out.add($");
		s.do { |c|
			case
			{ c == $" } { out = out ++ "\\\"" }
			{ c == $\\ } { out = out ++ "\\\\" }
			{ c == $\n } { out = out ++ "\\n" }
			{ c == $\r } { out = out ++ "\\r" }
			{ c == $\t } { out = out ++ "\\t" }
			{ c.ascii < 32 } { out = out ++ "\\u" ++ c.ascii.asHexString(4).toLower }
			{ out = out.add(c) };
		};
		^out.add($")
	}
}
