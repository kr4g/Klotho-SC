// EventSchedulerGUI: a transport and mixer window for EventScheduler.
//
// Load, play/stop, loop, array/fold output, record (with stems), one strip per
// track (main last) with mute, solo, a fader on the track's router gain, a
// level meter and the insert chain in order. The strips come from the loaded
// file; the fader gains persist across plays and apply to the live routers.

EventSchedulerGUI {
	var <scheduler, <server;
	var <window;
	var <fileNameLabel, <statusLabel;
	var <loadButton, <playButton, <recordButton, <stemsCheckbox, <binauralCheckbox, <loopMenu, <outputMenu;
	var <trackContainer, <trackViews, <levelUpdateTask, <trackOrder;
	var <currentFilePath;
	var <dbHead;

	*new { |server|
		^super.new.init(server)
	}

	init { |serverArg|
		server = serverArg ? Server.default;
		scheduler = EventScheduler.new(server: server, enableMonitoring: true);
		scheduler.onFinish = { this.onSchedulerFinish };
		scheduler.onIdle = { this.onSchedulerIdle };
		trackViews = List.new;
		trackOrder = List.new;
		dbHead = 6;
		this.createWindow;
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

		statusLabel = StaticText()
		.string_("")
		.align_(\left)
		.font_(Font.default.size_(11));
	}

	createTransportControls {
		loadButton = Button()
		.states_([["Load..."]])
		.action_({ this.loadFileDialog });

		playButton = Button()
		.states_([
			["▶", Color.black, Color.green(0.8)],
			["■", Color.white, Color.red(0.8)]
		])
		.font_(Font.default.size_(16))
		.action_({ |btn|
			if(btn.value == 1) { this.play } { this.stop };
		});

		recordButton = Button()
		.states_([
			["●", Color.black, Color.white],
			["●", Color.white, Color.red]
		])
		.font_(Font.default.size_(16))
		.action_({ |btn|
			if(btn.value == 1) { this.startRecording } { this.stopRecording };
		});

		stemsCheckbox = CheckBox().value_(false);
		binauralCheckbox = CheckBox().value_(false);

		loopMenu = PopUpMenu()
		.items_(["Loop: off", "Loop: ∞", "Loop ×2", "Loop ×4", "Loop ×8"])
		.value_(0)
		.action_({ |menu|
			scheduler.loop = [false, true, 2, 4, 8][menu.value];
		});

		outputMenu = PopUpMenu()
		.items_(["Output: auto", "Output: array", "Output: fold"])
		.value_(0)
		.action_({ |menu|
			this.setStatus("output: " ++ menu.items[menu.value]);
			scheduler.output = [nil, \array, \fold][menu.value];
		});
	}

	createTrackSection {
		trackContainer = ScrollView()
		.hasHorizontalScroller_(true)
		.hasVerticalScroller_(true);
	}

	layoutWindow {
		var transportLayout = HLayout(
			loadButton.fixedWidth_(80),
			nil,
			playButton.fixedWidth_(60),
			recordButton.fixedWidth_(60),
			stemsCheckbox,
			StaticText().string_("Stems").fixedWidth_(40),
			binauralCheckbox,
			StaticText().string_("Binaural").fixedWidth_(55),
			loopMenu.fixedWidth_(110),
			outputMenu.fixedWidth_(130),
			nil
		);
		window.layout = VLayout(
			fileNameLabel.fixedHeight_(25),
			transportLayout,
			trackContainer,
			statusLabel.fixedHeight_(18)
		);
		this.relayoutTracks;
	}

	setStatus { |text|
		defer { if(window.notNil and: { window.isClosed.not }) { statusLabel.string = text } };
	}

	// ---- tracks ----------------------------------------------------------------

	updateTracks {
		this.stopLevelMonitoring;
		trackViews.clear;
		trackOrder.clear;
		if(trackContainer.canvas.notNil) { trackContainer.canvas.remove };
		scheduler.trackNames.do { |name| trackOrder.add(name) };
		trackOrder.do { |name| this.createTrackView(name) };
		this.relayoutTracks;
	}

	createTrackView { |trackName|
		var trackView, nameLabel, gainSlider, muteButton, soloButton;
		var insertContainer, insertLabel, meterView, gainContainer, dbLabel;
		var gain = scheduler.mixer.gains[trackName] ? 1.0;

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
				.states_([["M", Color.black, Color.white], ["M", Color.white, Color.red]])
				.fixedSize_(Size(25, 25))
				.value_(scheduler.mixer.mutes.includes(trackName).binaryValue)
				.action_({ |btn| scheduler.muteTrack(trackName, btn.value == 1) }),

				soloButton = Button()
				.states_([["S", Color.black, Color.white], ["S", Color.white, Color.yellow]])
				.fixedSize_(Size(25, 25))
				.value_(scheduler.mixer.solos.includes(trackName).binaryValue)
				.action_({ |btn| scheduler.soloTrack(trackName, btn.value == 1) })
			).margins_(2),

			dbLabel = StaticText()
			.string_("% dB".format(gain.ampdb.round(0.1)))
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
				.value_(this.ampToSlider(gain))
				.background_(Color.clear)
				.fixedWidth_(20)
				.action_({ |slider|
					var db = this.sliderToDb(slider.value);
					scheduler.setTrackGain(trackName, db.dbamp);
					dbLabel.string = "% dB".format(db.round(0.1));
				}),

				StaticText()
				.string_("+%\n\n0\n\n-20\n\n-40\n\n-60".format(dbHead))
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

		scheduler.insertNames(trackName).do { |name|
			insertContainer.layout.add(
				StaticText().string_(name.asString).align_(\center).background_(Color.gray(0.6)).fixedHeight_(20)
			);
		};

		trackViews.add((
			name: trackName, view: trackView, gainSlider: gainSlider, meterView: meterView,
			muteButton: muteButton, soloButton: soloButton, insertContainer: insertContainer, dbLabel: dbLabel
		));
	}

	relayoutTracks {
		var tracksLayout = HLayout();
		trackViews.do { |trackData| tracksLayout.add(trackData.view) };
		tracksLayout.add(nil);
		trackContainer.canvas = View().layout_(tracksLayout);
	}

	// ---- file ------------------------------------------------------------------

	loadFileDialog {
		FileDialog({ |paths|
			var path = if(paths.isArray) { paths[0] } { paths };
			this.loadFile(path);
		}, {}, 0, 0);
	}

	loadFile { |path|
		this.resetGUI;
		currentFilePath = path;
		fileNameLabel.string_("Loading...");
		scheduler.loadFile(path, { |ok|
			defer {
				if(window.isNil or: { window.isClosed }) { ^nil };
				if(ok) {
					fileNameLabel.string_(PathName(path).fileNameWithoutExtension);
					this.setStatus("loaded % events, % s, output %".format(
						scheduler.payload.events.size, scheduler.pieceDur ? scheduler.payload.pieceDur, scheduler.effectiveOutput));
					this.updateTracks;
					this.startLevelMonitoring;
				} {
					fileNameLabel.string_("Load failed");
					this.setStatus(scheduler.lastError ? "load failed");
					currentFilePath = nil;
				};
			};
		});
	}

	resetGUI {
		if(scheduler.isPlaying) { scheduler.stop };
		this.stopLevelMonitoring;
		trackViews.clear;
		if(trackContainer.canvas.notNil) { trackContainer.canvas.remove };
		playButton.value = 0;
		recordButton.value = 0;
	}

	// ---- transport ---------------------------------------------------------------

	play {
		if(scheduler.play) {
			playButton.value = 1;
			this.startLevelMonitoring;
			this.setStatus("playing");
		} {
			playButton.value = 0;
			this.setStatus(scheduler.lastError ? "could not play");
		};
	}

	stop {
		scheduler.stop;
		playButton.value = 0;
		recordButton.value = 0;
		this.resetMeters;
		this.setStatus("stopped");
	}

	onSchedulerFinish {
		this.setStatus("finished; ringing out");
	}

	onSchedulerIdle {
		defer {
			if(window.notNil and: { window.isClosed.not }) {
				playButton.value = 0;
				this.resetMeters;
				this.setStatus("idle");
			};
		};
	}

	startRecording {
		var pathName, dir, base, outputPath, stems, binaural;
		if(currentFilePath.isNil) {
			this.setStatus("No file loaded - cannot record");
			recordButton.value = 0;
			^this
		};
		pathName = PathName(currentFilePath);
		dir = pathName.pathOnly;
		base = pathName.fileNameWithoutExtension;
		// Never <stem>.wav: that is the file's control-envelope buffer.
		outputPath = dir +/+ (base ++ "_render.wav");
		stems = stemsCheckbox.value;
		binaural = binauralCheckbox.value;
		recordButton.value = 1;
		playButton.value = 1;
		this.startLevelMonitoring;
		scheduler.record(outputPath, stems, binaural, { |sched|
			defer {
				if(window.notNil and: { window.isClosed.not }) {
					playButton.value = 0;
					recordButton.value = 0;
					this.setStatus("recorded " ++ outputPath);
				};
			};
		});
		this.setStatus("recording to " ++ outputPath);
	}

	stopRecording {
		this.stop;
	}

	// ---- faders and meters ----------------------------------------------------------

	sliderToDb { |value|
		var amp;
		if(value <= 0) { ^(-inf) };
		amp = value.squared * dbHead.dbamp;
		^amp.ampdb;
	}

	ampToSlider { |amp|
		var normalized;
		if(amp <= 0) { ^0 };
		normalized = (amp / dbHead.dbamp).clip(0, 1);
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
		if(server.serverRunning.not or: { trackOrder.size == 0 }) { ^nil };
		peakValues = Dictionary.new;
		trackOrder.do { |trackName| peakValues[trackName] = 0 };

		OSCdef(\trackLevelMonitor, { |msg|
			var level = msg[3];
			var id = msg[4].asInteger;
			var trackName = trackOrder[id];
			if(trackName.notNil) { peakValues[trackName] = level };
		}, '/trackLevel', server.addr);

		levelUpdateTask = Task({
			var decayRate = 0.96;
			loop {
				if(scheduler.isPlaying.not) {
					peakValues.keysValuesDo { |key, val| peakValues[key] = val * decayRate };
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
		if(scheduler.isPlaying or: { scheduler.isRecording }) { scheduler.stop };
		trackViews.clear;
		trackOrder.clear;
		window = nil;
	}

	close {
		if(window.notNil) { window.close };
	}
}
