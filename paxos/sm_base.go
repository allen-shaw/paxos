package paxos

import (
	"encoding/binary"
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/golang/protobuf/proto"
	"math"
)

type BatchSMCtx struct {
	SMCtxs []*SMCtx
}

type SMFac struct {
	sms        []StateMachine
	myGroupIdx int
}

func NewSMFac(groupIdx int) *SMFac {
	return &SMFac{myGroupIdx: groupIdx}
}

func (f *SMFac) Execute(groupIdx int, instanceID uint64, paxosValue string, smCtx *SMCtx) bool {
	if len(paxosValue) < sizeOfInt {
		log.Error("value invalid", log.Uint64("instace_id", instanceID), log.String("value", paxosValue))
		//need do nothing, just skip
		return true
	}

	smID := int(binary.BigEndian.Uint64([]byte(paxosValue[:sizeOfInt])))
	if smID == 0 {
		log.Warn("value no need to do sm, just skil", log.Uint64("instance_id", instanceID))
		return true
	}

	value := paxosValue[sizeOfInt:]
	if smID == BatchProposeSMID {
		var batchSMCtx *BatchSMCtx
		if smCtx != nil && smCtx.Ctx != nil {
			batchSMCtx = smCtx.Ctx.(*BatchSMCtx)
		}
		return f.batchExecute(groupIdx, instanceID, value, batchSMCtx)
	}

	return f.execute(groupIdx, instanceID, value, smID, smCtx)
}

func (f *SMFac) ExecuteForCheckpoint(groupIdx int, instanceID uint64, paxosValue string) bool {
	if len(paxosValue) < sizeOfInt {
		log.Error("value invalid", log.Uint64("instace_id", instanceID), log.String("value", paxosValue))
		//need do nothing, just skip
		return true
	}

	smID := int(binary.BigEndian.Uint64([]byte(paxosValue[:sizeOfInt])))
	if smID == 0 {
		log.Warn("value no need to do sm, just skil", log.Uint64("instance_id", instanceID))
		return true
	}
	value := paxosValue[sizeOfInt:]
	if smID == BatchProposeSMID {
		return f.batchExecuteForCheckpoint(groupIdx, instanceID, value)
	}
	return f.executeForCheckpoint(groupIdx, instanceID, value, smID)
}

func (f *SMFac) PackPaxosValue(value string, smID int) string {
	if smID != 0 {
		return string(IntToBytes(smID)) + value
	}
	return value
}

func (f *SMFac) AddSM(sm StateMachine) {
	for _, s := range f.sms {
		if s.SMID() == sm.SMID() {
			return
		}
	}
	f.sms = append(f.sms, sm)
}

func (f *SMFac) BeforePropose(groupIdx int, value string) {
	smID := int(binary.BigEndian.Uint64([]byte(value[:sizeOfInt])))
	if smID == 0 {
		return
	}

	if smID == BatchProposeSMID {
		f.BeforeBatchPropose(groupIdx, value)
		return
	}

	bodyValue := value[sizeOfInt:]
	changed := f.BeforeProposeCall(groupIdx, smID, bodyValue)
	if changed {
		value = value[:sizeOfInt] + bodyValue
	}
}

func (f *SMFac) BeforeBatchPropose(groupIdx int, value string) {
	batchValues := &BatchPaxosValues{}
	err := proto.Unmarshal([]byte(value), batchValues)
	if err != nil {
		log.Error("body value unmarshal fail", log.String("value", value))
		return
	}

	changed := false
	smAlreadyCall := make(map[int32]struct{})

	for i := 0; i < len(batchValues.Values); i++ {
		value := batchValues.Values[i]
		if _, ok := smAlreadyCall[value.SMID]; ok {
			continue
		}
		smAlreadyCall[value.SMID] = struct{}{}
		changed = f.BeforeProposeCall(groupIdx, int(value.SMID), string(value.Value))
	}

	if changed {
		bodyValue, err := proto.Marshal(batchValues)
		if err != nil {
			// FIXME: 可能不应该panic?
			panic(err)
		}
		value = value[:sizeOfInt] + string(bodyValue)
	}
}

func (f *SMFac) BeforeProposeCall(groupIdx int, smID int, bodyValue string) bool {
	if smID == 0 {
		return false
	}
	if len(f.sms) == 0 {
		return false
	}

	for _, sm := range f.sms {
		if sm.SMID() == smID && sm.NeedCallBeforePropose() {
			sm.BeforePropose(groupIdx, bodyValue)
			return true
		}
	}
	return false
}

func (f *SMFac) GetCheckpointInstanceID(groupIdx int) uint64 {
	hasUseSM := false
	cpInstanceID := uint64(math.MaxUint64)
	cpInstanceIDInside := uint64(math.MaxUint64)
	for _, sm := range f.sms {
		checkpointInstanceID := sm.GetCheckpointInstanceID(groupIdx)
		if sm.SMID() == SystemVSMID || sm.SMID() == MasterVSMID {
			//system variables
			//master variables
			//if no user state machine, system and master's can use.
			//if have user state machine, use user'state machine's checkpointinstanceid.
			if checkpointInstanceID == math.MaxUint64 {
				continue
			}
			if checkpointInstanceID > cpInstanceIDInside || cpInstanceIDInside == math.MaxUint64 {
				cpInstanceIDInside = checkpointInstanceID
			}
			continue
		}

		hasUseSM = true
		if checkpointInstanceID == math.MaxUint64 {
			continue
		}

		if checkpointInstanceID > cpInstanceID || cpInstanceID == math.MaxUint64 {
			cpInstanceID = checkpointInstanceID
		}
	}

	if hasUseSM {
		return cpInstanceID
	}
	return cpInstanceIDInside
}

func (f *SMFac) GetSMList() []StateMachine {
	return f.sms
}

func (f *SMFac) batchExecute(groupIdx int, instanceID uint64, value string, batchSMCtx *BatchSMCtx) bool {
	batchValues := &BatchPaxosValues{}
	err := proto.Unmarshal([]byte(value), batchValues)
	if err != nil {
		log.Error("body value unmarshal fail", log.String("value", value))
		return false
	}

	if batchSMCtx != nil {
		if len(batchSMCtx.SMCtxs) != len(batchValues.Values) {
			log.Error("value size not equal to smctx size",
				log.Int("value_size", len(batchValues.Values)),
				log.Int("smctx_size", len(batchSMCtx.SMCtxs)))
			return false
		}
	}

	for i := 0; i < len(batchValues.Values); i++ {
		value := batchValues.Values[i]
		var smCtx *SMCtx
		if batchSMCtx != nil {
			smCtx = batchSMCtx.SMCtxs[i]
		}
		ok := f.execute(groupIdx, instanceID, string(value.Value), int(value.SMID), smCtx)
		if !ok {
			return false
		}
	}

	return true
}

func (f *SMFac) execute(groupIdx int, instanceID uint64, value string, smID int, smCtx *SMCtx) bool {
	if smID == 0 {
		log.Warn("value no need to do sm, just skip", log.Uint64("instance_id", instanceID))
		return true
	}

	if len(f.sms) == 0 {
		log.Error("no any sm, need wait sm", log.Uint64("instance_id", instanceID))
		return false
	}

	for _, sm := range f.sms {
		if sm.SMID() == smID {
			return sm.Execute(groupIdx, instanceID, value, smCtx)
		}
	}

	log.Error("execute fail", log.Int("unknown_sm_id", smID), log.Uint64("instance_id", instanceID))
	return false
}

func (f *SMFac) batchExecuteForCheckpoint(groupIdx int, instanceID uint64, value string) bool {
	batchValues := &BatchPaxosValues{}
	err := proto.Unmarshal([]byte(value), batchValues)
	if err != nil {
		log.Error("body value unmarshal fail", log.String("value", value))
		return false
	}

	for i := 0; i < len(batchValues.Values); i++ {
		value := batchValues.Values[i]
		ok := f.executeForCheckpoint(groupIdx, instanceID, string(value.Value), int(value.SMID))
		if !ok {
			return false
		}
	}
	return true
}

func (f *SMFac) executeForCheckpoint(groupIdx int, instanceID uint64, value string, smID int) bool {
	if smID == 0 {
		log.Warn("value no need to do sm, just skip", log.Uint64("instance_id", instanceID))
		return true
	}

	if len(f.sms) == 0 {
		log.Error("no any sm, need wait sm", log.Uint64("instance_id", instanceID))
		return false
	}

	for _, sm := range f.sms {
		if sm.SMID() == smID {
			return sm.ExecuteForCheckpoint(groupIdx, instanceID, value)
		}
	}
	log.Error("execute fail", log.Int("unknown_sm_id", smID), log.Uint64("instance_id", instanceID))
	return false
}
