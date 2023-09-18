package paxos

type Group struct {
	communicate *Communicate
	config      *Config
	instance    *Instance
	initRet     error
}

func NewGroup(logStorage LogStorage,
	netWork NetWork,
	masterSM InsideSM,
	groupIdx int,
	options *Options) *Group {
	g := &Group{}

	g.config = NewConfig(logStorage,
		options.Sync,
		options.SyncInterval,
		options.UseMembership,
		options.MyNode,
		options.NodeInfoList,
		options.FollowerNodeInfos,
		groupIdx,
		options.GroupCount,
		options.MembershipChangeCallback)
	g.config.SetMasterSM(masterSM)
	g.communicate = NewCommunicate(g.config, netWork, options.MyNode.GetNodeId())
	g.instance = NewInstance(g.config, logStorage, g.communicate, options)
	g.initRet = ErrUnknown

	return g
}

func (g *Group) StartInit() {
	go g.Init()
}

func (g *Group) Init() {
	err := g.config.Init()
	if err != nil {
		return
	}

	// inside sm
	g.AddStateMachine(g.config.GetSystemVSM())
	g.AddStateMachine(g.config.GetMasterSM())

	g.initRet = g.instance.Init()
}

func (g *Group) GetInitRet() error {
	g.Join()
	return g.initRet
}

func (g *Group) AddStateMachine(sm StateMachine) {
	g.instance.AddStateMachine(sm)
}

func (g *Group) Join() {

}

func (g *Group) Start() {
	g.instance.Start()
}

func (g *Group) Stop() {
	g.instance.Stop()
}

func (g *Group) GetConfig() *Config {
	return g.config
}

func (g *Group) GetInstance() *Instance {
	return g.instance
}

func (g *Group) GetCommitter() *Committer {
	return g.instance.GetCommitter()
}

func (g *Group) GetCheckpointCleaner() *Cleaner {
	return g.instance.GetCheckpointCleaner()
}

func (g *Group) GetCheckpointReplayer() *Replayer {
	return g.instance.GetCheckpointReplayer()
}
