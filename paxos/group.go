package paxos

type Group struct {
	communicate *Communicate
	config      *Config
	instance    *Instance
}
