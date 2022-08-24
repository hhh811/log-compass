package collect

type CollectorConf struct {
	triggerCacheSize    int // number of logs received to trigger persist
	triggerMilliSeconds int // number of milliseconds to trigger persist
}
