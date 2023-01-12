package storage

import (
	"fmt"

	"github.com/alephium/wormhole-fork/node/pkg/vaa"
)

// TODO: read from config
var governanceChain = vaa.ChainIDUnset
var governanceAddr = vaa.Address{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4}

type emitterId struct {
	emitterChain   vaa.ChainID
	emitterAddress vaa.Address
	targetChain    vaa.ChainID

	// cached value
	idStr *string
}

func (e *emitterId) isGovernanceEmitter() bool {
	return e.emitterChain == governanceChain && e.emitterAddress == governanceAddr
}

func (e *emitterId) toString() string {
	if e.idStr != nil {
		return *e.idStr
	}
	var str string
	if e.isGovernanceEmitter() {
		str = fmt.Sprintf("%d/%s/%d", e.emitterChain, e.emitterAddress.String(), vaa.ChainIDUnset)
	} else {
		str = fmt.Sprintf("%d/%s/%d", e.emitterChain, e.emitterAddress.String(), e.targetChain)
	}
	e.idStr = &str
	return str
}

func (e *emitterId) toVaaId(seq uint64) string {
	return fmt.Sprintf("%s/%d", e.toString(), seq)
}

type sequencesCache struct {
	governanceSequence *uint64
	sequences          map[emitterId]uint64
}

func newSequencesCache() *sequencesCache {
	return &sequencesCache{
		governanceSequence: nil,
		sequences:          make(map[emitterId]uint64),
	}
}

func (s *sequencesCache) getNextSequence(emitterId *emitterId) *uint64 {
	if emitterId.isGovernanceEmitter() {
		return s.governanceSequence
	}
	seq, ok := s.sequences[*emitterId]
	if !ok {
		return nil
	}
	return &seq
}

func (s *sequencesCache) setNextSequence(emitterId *emitterId, seq uint64) {
	if emitterId.isGovernanceEmitter() {
		s.governanceSequence = &seq
	}
	s.sequences[*emitterId] = seq
}
