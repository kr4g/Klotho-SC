// KSQueueLoad: the in-flight ledger behind EventScheduler's occupancy gate.
//
// scsynth keeps at most 2048 time-tagged bundles per server
// (SC_CoreAudio.h, PriorityQueueT<SC_ScheduledEvent, 2048>; one slot per
// bundle however many messages it holds, freed when the bundle comes due) and
// drops the 2049th on arrival with no message, no reply and no counter. The
// ledger is the only detector a sender has. One ledger per server address,
// shared by every player on it, like scheduler_core.js's page-global
// __klothoSchedLoad: a second player on the same server sees the first one's
// load. Entries are due times in SystemClock seconds; prune drops the ones
// already due. A server quit empties the ledger (the queue died with it).

KSQueueLoad {
	classvar registry;
	var <dues, <key;

	*for { |server|
		var k = server.addr.ip ++ ":" ++ server.addr.port, ledger;
		registry = registry ?? { Dictionary.new };
		ledger = registry[k];
		if(ledger.isNil) {
			ledger = super.new.init(k);
			registry[k] = ledger;
			ServerQuit.add({ ledger.clear }, server);
		};
		^ledger
	}

	init { |argKey|
		key = argKey;
		dues = SortedList.new;
	}

	push { |due|
		dues.add(due);
	}

	// Drop every entry due at or before `now`; returns what is still in flight.
	prune { |now|
		while { dues.notEmpty and: { dues.first <= now } } { dues.removeAt(0) };
		^dues.size
	}

	peek {
		^if(dues.isEmpty) { nil } { dues.first }
	}

	count {
		^dues.size
	}

	clear {
		dues = SortedList.new;
	}
}
