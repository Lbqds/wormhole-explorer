package processor

import (
	"context"
	"errors"
	"fmt"

	"github.com/alephium/wormhole-fork/explorer/fly/deduplicator"

	"github.com/alephium/wormhole-fork/node/pkg/common"
	"github.com/alephium/wormhole-fork/node/pkg/processor"
	"github.com/alephium/wormhole-fork/node/pkg/vaa"
	ethCommon "github.com/ethereum/go-ethereum/common"
	"go.uber.org/zap"
)

type vaaGossipConsumer struct {
	gst          *common.GuardianSetState
	messageQueue chan<- *Message
	logger       *zap.Logger
	deduplicator *deduplicator.Deduplicator
}

// NewVAAGossipConsumer creates a new processor instances.
func NewVAAGossipConsumer(
	gst *common.GuardianSetState,
	deduplicator *deduplicator.Deduplicator,
	messageQueue chan<- *Message,
	logger *zap.Logger) *vaaGossipConsumer {
	return &vaaGossipConsumer{
		gst:          gst,
		deduplicator: deduplicator,
		messageQueue: messageQueue,
		logger:       logger,
	}
}

func verifyVAA(v *vaa.VAA, addresses []ethCommon.Address) error {
	if addresses == nil {
		return errors.New("No addresses were provided")
	}

	// Check if VAA doesn't have any signatures
	if len(v.Signatures) == 0 {
		return errors.New("VAA was not signed")
	}

	// Verify VAA has enough signatures for quorum
	quorum := processor.CalculateQuorum(len(addresses))
	if len(v.Signatures) < quorum {
		return errors.New("VAA did not have a quorum")
	}

	// Verify VAA signatures to prevent a DoS attack on our local store.
	if !v.VerifySignatures(addresses) {
		return errors.New("VAA had bad signatures")
	}

	return nil
}

// Push handles incoming VAAs depending on whether it is a pyth or non pyth.
func (p *vaaGossipConsumer) Push(ctx context.Context, v *vaa.VAA, serializedVaa []byte) error {
	if err := verifyVAA(v, p.gst.Get().Keys); err != nil {
		p.logger.Error("Received invalid vaa", zap.String("id", v.MessageID()))
		return err
	}

	err := p.deduplicator.Apply(ctx, v.MessageID(), func() error {
		message := &Message{
			vaa:        v,
			serialized: serializedVaa,
		}
		select {
		case p.messageQueue <- message:
			return nil
		default:
			return fmt.Errorf("message queue is full")
		}
	})

	if err != nil {
		p.logger.Error("Error consuming from Gossip network",
			zap.String("id", v.MessageID()),
			zap.Error(err))
		return err
	}

	return nil
}
