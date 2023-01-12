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
}

func (e *emitterId) isGovernanceEmitter() bool {
	return e.emitterChain == governanceChain && e.emitterAddress == governanceAddr
}

func (e *emitterId) toString() string {
	return fmt.Sprintf("%d/%s/%d", e.emitterChain, e.emitterAddress.String(), e.targetChain)
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
