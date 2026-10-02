# Klotho-SC

`Klotho-SC` is an extension for SuperCollider that integrates with <a href="https://github.com/kr4g/Klotho" target="_blank">Klotho</a>, a Python package for computational music composition.

`EventScheduler` loads the JSON file that Klotho's `Score.write` produces and plays it on a native scsynth with the semantics of Klotho's browser engine (SuperSonic): the same groups, buses and insert chains, the same auto-release and control-envelope rules, the same spatial routing for speaker arrays (hardware array or binaural fold), loop cycles, and a ring-out that frees only the score group. `EventSchedulerGUI` adds a transport and a mixer with faders, meters, mute and solo.

```supercollider
s.boot;
e = EventScheduler(s);
{ e.loadKlothoDefs }.fork;                 // Klotho's bundled SynthDefs
e.loadFile("piece.json", { |ok| if(ok) { e.play } });
```

Classes: `EventScheduler` (the player), `EventSchedulerGUI`, `KSPayload` (file decoding and validation), `KSAssets` (SynthDefs, gate lookup, sample buffers), `KSMixer` (the topology), `KSTrace` (an `oscTap` that writes every message in the parity trace format), `KSDiskRecorder`.
