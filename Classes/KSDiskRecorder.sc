// KSDiskRecorder: one DiskOut writer, any channel count.
//
// prepare (inside a Routine: allocates and opens the file, syncs), then
// startMessage gives the /s_new the scheduler sends at playStart, and stop
// frees the node and closes the file.

KSDiskRecorder {
	var <server, <path, <numChannels, <bus, <target, <addAction;
	var <buffer, <node, <defName, <isOpen;

	*new { |server, path, numChannels, bus, target, addAction = \addToTail|
		^super.newCopyArgs(server, path, numChannels, bus, target, addAction).init
	}

	init {
		isOpen = false;
	}

	// Allocate the disk buffer and open the file. Call from a Routine.
	prepare { |assets, headerFormat = "wav", sampleFormat = "int24"|
		defName = assets.ensureDiskOut(numChannels);
		buffer = Buffer.alloc(server, 65536, numChannels);
		server.sync;
		buffer.write(path, headerFormat, sampleFormat, 0, 0, true);
		server.sync;
		isOpen = true;
	}

	addActionNumber {
		^switch(addAction, \addToHead, 0, \addToTail, 1, \addBefore, 2, \addAfter, 3, \addReplace, 4, 1)
	}

	// The message that starts writing; the scheduler sends it time-tagged.
	startMessage { |nodeID|
		node = nodeID;
		^['/s_new', defName, nodeID, this.addActionNumber, target, 'bufnum', buffer.bufnum, 'inBus', bus]
	}

	stop {
		if(node.notNil) { server.sendMsg('/n_free', node); node = nil };
		if(buffer.notNil) {
			buffer.close;
			buffer.free;
			buffer = nil;
		};
		isOpen = false;
	}
}
