EventSchedulerGUI {
	var <scheduler;
	var <window;
	var <fileMenu, <fileNameLabel;
	var <playButton, <recordButton, <stemsCheckbox;
	var <trackContainer;
	var <trackViews;
	var <levelUpdateTask;
	var <groupOrder;
	var <currentFilePath;
	var <server;

	*new { |server|
		^super.new.init(server)
	}

	init { |serverArg|
		server = serverArg ? Server.default;
		scheduler = EventScheduler.new(server, enableMonitoring: true);
		trackViews = List.new;
		groupOrder = List.new;
		this.createWindow;
		this.createFileMenu;
		this.createTransportControls;
		this.createTrackSection;
		this.layoutWindow;
		window.front;
	}

	createWindow {
		window = Window("EventScheduler", Rect(100, 100, 900, 650))
		.onClose_({ this.cleanup });

		fileNameLabel = StaticText()
		.string_("No file loaded")
		.align_(\center)
		.background_(Color.gray(0.95))
		.font_(Font.default.size_(14));
	}

	createFileMenu {
		var menuItems = ["Select Action...", "Load File...", "Recent Files", "New Session"];
		fileMenu = PopUpMenu()
		.items_(menuItems)
		.value_(0)
		.action_({ |menu|
			switch(menu.value,
				1, {
					this.loadFileDialog;
					AppClock.sched(0.1, { menu.value_(0); nil });
				},
				2, {
					this.showRecentFiles;
					AppClock.sched(0.1, { menu.value_(0); nil });
				},
				3, {
					this.newSession;
					AppClock.sched(0.1, { menu.value_(0); nil });
				}
			)
		});
	}

	createTransportControls {
		playButton = Button()
		.states_([
			["▶", Color.black, Color.green(0.8)],
			["■", Color.white, Color.red(0.8)]
		])
		.font_(Font.default.size_(16))
		.action_({ |btn|
			if(btn.value == 1) {
				this.play;
			} {
				this.stop;
			}
		});

		recordButton = Button()
		.states_([
			["●", Color.black, Color.white],
			["●", Color.white, Color.red]
		])
		.font_(Font.default.size_(16))
		.action_({ |btn|
			if(btn.value == 1) {
				this.startRecording;
			} {
				this.stopRecording;
			}
		});

		stemsCheckbox = CheckBox()
		.value_(true);
	}

	createTrackSection {
		trackContainer = ScrollView()
		.hasHorizontalScroller_(false)
		.hasVerticalScroller_(true);
	}

	updateTracks {
		var trackNames;

		this.stopLevelMonitoring;

		trackViews.clear;
		groupOrder.clear;

		if(trackContainer.canvas.notNil) {
			trackContainer.canvas.remove;
		};

		if(scheduler.groupMap.notNil) {
			// Use the scheduler's groupOrder directly
			if(scheduler.groupOrder.notNil and: { scheduler.groupOrder.size > 0 }) {
				groupOrder = scheduler.groupOrder.copy;
			} {
				// Fallback if groupOrder isn't set
				groupOrder = scheduler.groupMap.keys.asArray;
			};

			// Ensure main is always last
			if(groupOrder.includes(\main)) {
				groupOrder.remove(\main);
			};
			groupOrder = groupOrder.add(\main);

			trackNames = groupOrder;
		} {
			trackNames = [];
		};

		trackNames.do { |trackName|
			if(scheduler.groupMap.includesKey(trackName)) {
				this.createTrackView(trackName);
			};
		};

		this.relayoutTracks;
	}

	createTrackView { |trackName|
		var trackView, nameLabel, gainSlider, muteButton, soloButton;
		var insertContainer, insertLabel, meterView, gainContainer;
		var dbLabel;

		trackView = CompositeView()
		.background_(Color.gray(0.9))
		.fixedWidth_(120)
		.layout_(VLayout(
			nameLabel = StaticText()
			.string_(trackName.asString.toUpper)
			.align_(\center)
			.background_(Color.gray(0.7))
			.fixedHeight_(25),

			HLayout(
				muteButton = Button()
				.states_([
					["M", Color.black, Color.white],
					["M", Color.white, Color.red]
				])
				.fixedSize_(Size(25, 25))
				.action_({ |btn| this.muteTrack(trackName, btn.value == 1) }),

				soloButton = Button()
				.states_([
					["S", Color.black, Color.white],
					["S", Color.white, Color.yellow]
				])
				.fixedSize_(Size(25, 25))
				.action_({ |btn| this.soloTrack(trackName, btn.value == 1) })
			).margins_(2),

			dbLabel = StaticText()
			.string_("0 dB")
			.align_(\center)
			.font_(Font.default.size_(10))
			.fixedHeight_(20),

			gainContainer = CompositeView()
			.minHeight_(200)
			.background_(Color.black)
			.layout_(HLayout(
				meterView = LevelIndicator()
				.warning_(0.75)
				.critical_(0.9)
				.background_(Color.black)
				.numTicks_(10)
				.numMajorTicks_(5)
				.drawsPeak_(true)
				.fixedWidth_(25),

				gainSlider = Slider()
				.orientation_(\vertical)
				.value_(this.ampToSlider(1.0))
				.background_(Color.clear)
				.fixedWidth_(20)
				.action_({ |slider|
					var db = this.sliderToDb(slider.value);
					var amp = db.dbamp;
					this.setTrackGain(trackName, amp);
					dbLabel.string = "% dB".format(db.round(0.1));
				}),

				StaticText()
				// .string_("0\n\n-20\n\n-40\n\n-60\n\n-∞")
				.string_("+6\n\n0\n\n-20\n\n-40\n\n-60")
				.align_(\left)
				.font_(Font.default.size_(9))
				.fixedWidth_(25)
			)),

			insertLabel = StaticText()
			.string_("Inserts")
			.align_(\center)
			.fixedHeight_(20),

			insertContainer = CompositeView()
			.layout_(VLayout().margins_(2).spacing_(2))
			.background_(Color.gray(0.8))
			.minHeight_(100)
		).margins_(2).spacing_(2));

		this.createInserts(insertContainer, trackName);

		trackViews.add((
			name: trackName,
			view: trackView,
			gainSlider: gainSlider,
			meterView: meterView,
			muteButton: muteButton,
			soloButton: soloButton,
			insertContainer: insertContainer,
			dbLabel: dbLabel
		));
	}

	createInserts { |container, trackName|
		var groupEntry, fxNodes;

		if(scheduler.groupMap.notNil) {
			groupEntry = scheduler.groupMap[trackName];
			if(groupEntry.notNil) {
				fxNodes = groupEntry[\fxNodes];
				if(fxNodes.notNil) {
					fxNodes.keysValuesDo { |uid, synth|
						if(uid != \__bypass and: { synth.notNil }) {
							var synthName = synth.defName;
							if(synthName.notNil) {
								this.createInsertView(container, synthName.asString);
							};
						};
					};
				};
			};
		};
	}

	createInsertView { |container, insertName|
		var insertView;

		insertView = StaticText()
		.string_(insertName)
		.align_(\center)
		.background_(Color.gray(0.6))
		.fixedHeight_(20);

		container.layout.add(insertView);
	}

	layoutWindow {
		var transportLayout;

		transportLayout = HLayout(
			StaticText().string_("File:").fixedWidth_(40),
			fileMenu.maxWidth_(200),
			nil,
			HLayout(
				playButton.fixedWidth_(60),
				recordButton.fixedWidth_(60),
				stemsCheckbox,
				StaticText().string_("Stems").fixedWidth_(40)
			),
			nil
		);

		window.layout = VLayout(
			fileNameLabel.fixedHeight_(25),
			transportLayout,
			trackContainer
		);

		this.relayoutTracks;
	}

	relayoutTracks {
		var tracksLayout;

		tracksLayout = HLayout();
		trackViews.do { |trackData|
			tracksLayout.add(trackData.view);
		};
		tracksLayout.add(nil);

		trackContainer.canvas = View().layout_(tracksLayout);
	}

	loadFileDialog {
		FileDialog({ |paths|
			var path = if(paths.isArray) { paths[0] } { paths };
			this.loadFile(path);
		}, {}, 0, 0);
	}

	showRecentFiles {
		"Recent files menu".postln;
	}

	newSession {
		"New session".postln;
	}

	loadFile { |path|
		var pathName;

		this.resetGUI;
		currentFilePath = path;

		Routine({
			if(scheduler.loadFile(path)) {
				pathName = PathName(path);
				0.2.wait;

				AppClock.sched(0, {
					fileNameLabel.string_(pathName.fileNameWithoutExtension);
					("Loaded file: " ++ path).postln;
					this.updateTracks;
					this.startLevelMonitoring;
				});
			} {
				AppClock.sched(0, {
					fileNameLabel.string_("Load failed");
					currentFilePath = nil;
					("Failed to load file: " ++ path).postln;
				});
			};
		}).play;
	}

	resetGUI {
		if(scheduler.isPlaying) {
			scheduler.stop;
		};
		this.stopLevelMonitoring;
		trackViews.clear;
		if(trackContainer.canvas.notNil) {
			trackContainer.canvas.remove;
		};
		playButton.value = 0;
	}

	play {
		if(scheduler.play) {
			playButton.value = 1;
			this.startLevelMonitoring;
		};
	}

	stop {
		scheduler.stop;
		playButton.value = 0;
		recordButton.value = 0;

		this.resetMeters;

		Routine({
			0.3.wait;
			AppClock.sched(0, {
				if(currentFilePath.notNil) {
					this.loadFile(currentFilePath);
				};
			});
		}).play;
	}

	startRecording {
		if(currentFilePath.notNil) {
			var pathName = PathName(currentFilePath);
			var dir = pathName.pathOnly;
			var base = pathName.fileNameWithoutExtension;
			// var outputPath = dir +/+ base ++ "_render";
			var outputPath = dir +/+ base ++ ".wav";
			var stems = stemsCheckbox.value;

			recordButton.value = 1;

			scheduler.recordingCompleteCallback = {
				defer {
					playButton.value = 0;
					recordButton.value = 0;
					"Recording complete".postln;
				};
			};

			if(scheduler.isPlaying.not) {
				playButton.value = 1;
				scheduler.record(outputPath, stems);
			} {
				// If already playing, need to restart with recording
				this.stop;
				Routine({
					0.5.wait;
					scheduler.recordingCompleteCallback = {
						defer {
							playButton.value = 0;
							recordButton.value = 0;
							"Recording complete".postln;
						};
					};
					scheduler.record(outputPath, stems);
				}).play;
			};

			("Recording to: " ++ outputPath).postln;
		} {
			"No file loaded - cannot record".postln;
			recordButton.value = 0;
		};
	}

	stopRecording {
		recordButton.value = 0;
		playButton.value = 0;
		"Recording stopped".postln;
	}

	muteTrack { |trackName, isMuted|
		("Track " ++ trackName ++ " muted: " ++ isMuted).postln;
	}

	soloTrack { |trackName, isSoloed|
		("Track " ++ trackName ++ " soloed: " ++ isSoloed).postln;
	}

	setTrackGain { |trackName, gain|
		var busSynth = scheduler.getBusSynth(trackName);
		if(busSynth.notNil) {
			busSynth.set(\gain, gain);
		};
	}

	sliderToDb { |value|
		var amp;
		if(value <= 0) { ^(-inf) };
		amp = value.squared * 6.dbamp;
		^amp.ampdb;
	}

	ampToSlider { |amp|
		var normalized;
		if(amp <= 0) { ^0 };
		normalized = (amp / 6.dbamp).clip(0, 1);
		^normalized.sqrt;
	}

	resetMeters {
		trackViews.do { |trackData|
			defer {
				trackData.meterView.value = 0;
				trackData.meterView.peakLevel = 0;
			};
		};
	}

	startLevelMonitoring {
		var peakValues;

		this.stopLevelMonitoring;

		if(scheduler.groupMap.isNil or: { server.serverRunning.not }) {
			^nil;
		};

		peakValues = IdentityDictionary.new;

		scheduler.groupMap.keysValuesDo { |trackName, entry|
			peakValues[trackName] = 0;
		};

		OSCdef(\trackLevelMonitor, { |msg|
			var level = msg[3];
			var id = msg[4].asInteger;
			var trackName = groupOrder[id];
			if(trackName.notNil) {
				peakValues[trackName] = level;
			};
		}, '/trackLevel', server.addr);

		levelUpdateTask = Task({
			var decayRate = 0.96;
			loop {
				if(scheduler.isPlaying.not) {
					peakValues.keysValuesDo { |key, val|
						peakValues[key] = val * decayRate;
					};
				};

				defer {
					trackViews.do { |trackData|
						var level = peakValues[trackData.name] ? 0;
						var dbValue = level.ampdb.max(-60);
						trackData.meterView.value = dbValue.linlin(-60, 0, 0, 1);
						trackData.meterView.peakLevel = dbValue.linlin(-60, 0, 0, 1);
					};
				};

				0.05.wait;
			};
		}).play;
	}

	stopLevelMonitoring {
		if(levelUpdateTask.notNil) {
			levelUpdateTask.stop;
			levelUpdateTask = nil;
		};

		OSCdef(\trackLevelMonitor).free;

		this.resetMeters;
	}

	cleanup {
		this.stopLevelMonitoring;
		trackViews.clear;
		groupOrder.clear;
		window = nil;
	}

	close {
		window.close;
	}
}