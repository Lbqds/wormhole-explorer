package processor

import (
	"context"

	"github.com/alephium/wormhole-fork/explorer/fly/storage"

	"go.uber.org/zap"
)

// VAAQueueConsumer represents a VAA queue consumer.
type VAAQueueConsumer struct {
	messageQueue <-chan *Message
	repository   *storage.Repository
	logger       *zap.Logger
}

// NewVAAQueueConsumer creates a new VAA queue consumer instances.
func NewVAAQueueConsumer(
	messageQueue <-chan *Message,
	repository *storage.Repository,
	logger *zap.Logger) *VAAQueueConsumer {
	return &VAAQueueConsumer{
		messageQueue: messageQueue,
		repository:   repository,
		logger:       logger,
	}
}

// Start consumes messages from VAA queue and store those messages in a repository.
func (c *VAAQueueConsumer) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-c.messageQueue:
				if err := c.repository.UpsertVaa(ctx, msg.vaa, msg.serialized); err != nil {
					c.logger.Error("Error inserting vaa in repository",
						zap.String("id", msg.vaa.MessageID()),
						zap.Error(err))
					continue
				}

				c.logger.Info("Vaa save in repository", zap.String("id", msg.vaa.MessageID()))
			}
		}
	}()
}
