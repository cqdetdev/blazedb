package blazedb

// ActiveWorldOptions returns options tuned for long-running, mutation-heavy
// server worlds. It keeps the production-oriented Balanced durability profile,
// enables explicit delta records for event-layer block edits, and uses a
// larger cache for hot areas. This is the preset to start with for persistent
// worlds such as factions, warzones, and other active survival maps.
func ActiveWorldOptions() *Options {
	opts := BalancedOptions()
	opts.CacheSize = 256 * 1024 * 1024
	opts.EnableDeltaRecords = true
	return opts
}

// EphemeralWorldOptions returns options tuned for short-lived disposable
// worlds and overlays. It favors very low latency and assumes the overlay can
// be discarded when the session ends, which fits duel arenas, practice maps,
// minigame rounds, and generated temporary worlds.
func EphemeralWorldOptions() *Options {
	opts := TurboOptions()
	opts.CacheSize = 64 * 1024 * 1024
	opts.EnableDeltaRecords = true
	return opts
}

func cloneOptions(opts *Options) *Options {
	if opts == nil {
		return nil
	}
	clone := *opts
	return &clone
}
