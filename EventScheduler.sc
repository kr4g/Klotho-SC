EventScheduler {
	var <server;
	var <synths;
	var pythonAddr;
	var <scheduledEvents;
	var schedulingCapacity;
	var schedulingMinimum;
	var checkInterval;
	var <startTime;
	var <startLag;
	var <isPlaying;
	var totalEventsReceived;
	var totalEventsProcessed;
	var <allEventsReceived;
	var cleanupRoutine;
	var oscDef;

	*new { |server, capacity=500, minimum=150, interval=0.05, startLag=0.1|
		^super.new.init(server, capacity, minimum, interval, startLag)
	}

	init { |serverArg, capacity, minimum, interval, lag|
		server = serverArg;
		schedulingCapacity = capacity;
		schedulingMinimum = minimum;
		checkInterval = interval;
		startLag = lag;
		pythonAddr = NetAddr("127.0.0.1", 9000);
		this.reset;
		this.initOSCListener;
	}

	reset {
		scheduledEvents = Dictionary.new;
		synths = Dictionary.new;
		startTime = nil;
		isPlaying = false;
		allEventsReceived = false;
		totalEventsReceived = 0;
		totalEventsProcessed = 0;
		// "Scheduler ready.".postln;
	}

	initOSCListener {
		oscDef = OSCdef(\eventReceiver, { |msg, time, addr, recvPort|
			var eventType = msg[1];
			var eventArgs = msg[2..];

			if(eventType == \end_of_transmission, {
				allEventsReceived = true;
				// "All events received.".postln;
				// this.startCleanup;
			}, {
				this.handleIncomingEvent(eventType, eventArgs);
				totalEventsReceived = totalEventsReceived + 1;

				if(scheduledEvents.size >= schedulingCapacity, {
					pythonAddr.sendMsg("/pause");
					// this.startCleanup;
				});
			});
		}, '/storeEvent');
	}

	handleIncomingEvent { |type, args|
		var eventId = UniqueID.next;

		var synthId = args[0];
		var synthName = args[1];
		var startTime = args[2].asFloat;
		var synthArgs = args[3..];

		switch(type,
			\new, {
				this.scheduleSynth(eventId, synthId, synthName, startTime, synthArgs);
			},
			\set, {
				this.scheduleSet(eventId, synthId, startTime, synthArgs);
			}
		);
	}

	scheduleSynth { |eventId, synthId, synthName, start, args|
		if(isPlaying, {
			var schedTime = startTime + start;
			scheduledEvents.put(eventId, schedTime);

			SystemClock.schedAbs(schedTime, {
				server.bind {
					var synth = Synth(synthName, args);
					if(synthId.notNil, {
						synths.put(synthId, synth);
						synth.onFree({
							synths.removeAt(synthId);
						});
					});
				};
			});
		});
	}

	scheduleSet { |eventId, synthId, start, args|
		if(isPlaying, {
			var schedTime = startTime + start;
			scheduledEvents.put(eventId, schedTime);

			SystemClock.schedAbs(schedTime, {
				server.bind {
					var synth = synths.at(synthId);
					if(synth.notNil, {
						synth.set(*args);
					});
				};
			});
		});
	}

	startCleanup {
		cleanupRoutine = Routine({
			var currentTime;
			loop {
				currentTime = thisThread.seconds;
				scheduledEvents.keysValuesDo({ |eventId, schedTime|
					if(schedTime < currentTime, {
						scheduledEvents.removeAt(eventId);
						totalEventsProcessed = totalEventsProcessed + 1;
						pythonAddr.sendMsg("/event_processed");
					});
				});

				if(allEventsReceived and: { scheduledEvents.isEmpty }, {
					// "Composition done.".postln;
					this.stop;
					cleanupRoutine.stop;
				}, {
					if(scheduledEvents.size < schedulingMinimum and: { allEventsReceived.not }, {
						pythonAddr.sendMsg("/resume");
						// cleanupRoutine.stop;
					});
				});

				checkInterval.wait;
			}
		}).play;
	}

	play {
		if(isPlaying.not, {
			isPlaying = true;
			startTime = thisThread.seconds + startLag;
			// pythonAddr.sendMsg("/reset");
			pythonAddr.sendMsg("/start");
			this.startCleanup;
			"Starting playback...".postln;
		}, {
			"Already playing.".postln;
		});
	}

	stop {
		synths.clear;
		this.reset;
		"Stopping playback.".postln;
	}
}

// record { |outputDir, fileName, dur, waitTime=0.02, postDelay=5|
// 	if(isPlaying, {
// 		"Already playing. Stop first before recording.".postln;
// 		^this;
// 	});
//
// 	recordingPath = this.prGenerateUniqueFileName(outputDir, fileName);
// 	postRecordingDelay = postDelay;
//
// 	recorder = Routine({
// 		if(dur.notNil, {
// 			server.record(recordingPath, numChannels: 2, duration: dur);
// 			}, {
// 				server.record(recordingPath, numChannels: 2);
// 		});
// 		waitTime.wait;
// 		this.play;
// 	}).play;
// }

// prGenerateUniqueFileName { |outputDir, fileName|
// 	var path = outputDir +/+ fileName ++ ".wav";
// 	var i = 1;
// 	while ({File.exists(path)}) {
// 		path = outputDir +/+ fileName ++ "_" ++ i ++ ".wav";
// 		i = i + 1;
// 	};
// 	^path;
// }