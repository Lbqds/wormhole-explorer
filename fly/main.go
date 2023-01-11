package main

import (
	"context"
	"flag"

	"fmt"
	"os"

	"github.com/wormhole-foundation/wormhole-explorer/fly/deduplicator"
	"github.com/wormhole-foundation/wormhole-explorer/fly/guardiansets"
	"github.com/wormhole-foundation/wormhole-explorer/fly/migration"
	"github.com/wormhole-foundation/wormhole-explorer/fly/processor"
	"github.com/wormhole-foundation/wormhole-explorer/fly/server"
	"github.com/wormhole-foundation/wormhole-explorer/fly/storage"

	"github.com/certusone/wormhole/node/pkg/common"
	"github.com/certusone/wormhole/node/pkg/p2p"
	gossipv1 "github.com/certusone/wormhole/node/pkg/proto/gossip/v1"
	"github.com/certusone/wormhole/node/pkg/supervisor"
	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/v3/cache"
	"github.com/eko/gocache/v3/store"
	eth_common "github.com/ethereum/go-ethereum/common"
	crypto2 "github.com/ethereum/go-ethereum/crypto"
	ipfslog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
	"go.uber.org/zap"

	"github.com/joho/godotenv"
)

var (
	rootCtx       context.Context
	rootCtxCancel context.CancelFunc
)

// TODO refactor to another file/package
func newCache() (cache.CacheInterface[bool], error) {
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10000,          // Num keys to track frequency of (1000).
		MaxCost:     10 * (1 << 20), // Maximum cost of cache (10 MB).
		BufferItems: 64,             // Number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}
	store := store.NewRistretto(c)
	return cache.New[bool](store), nil
}

type mongodbConfig struct {
	mongodbUri   string
	databaseName string
}

func loadMongodbConfig(logger *zap.Logger) *mongodbConfig {
	if err := godotenv.Load(); err != nil {
		logger.Info("No .env file found")
	}
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		logger.Fatal("You must set your 'MONGODB_URI' environmental variable. See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}

	databaseName := os.Getenv("MONGODB_DATABASE")
	if databaseName == "" {
		logger.Fatal("You must set your 'MONGODB_DATABASE' environmental variable. See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}
	return &mongodbConfig{
		mongodbUri:   uri,
		databaseName: databaseName,
	}
}

func main() {
	// Node's main lifecycle context.
	p2pNetworkId := flag.String("id", "/wormhole/dev", "P2P network id")
	p2pPort := flag.Uint("port", 8999, "P2P UDP listener port")
	p2pBootstrap := flag.String("bootstrap", "", "P2P bootstrap peers (comma-separated)")
	nodeKeyPath := flag.String("nodeKey", "", "Path to node key (will be generated if it doesn't exist)")
	logLevel := flag.String("logLevel", "debug", "Log level")

	rootCtx, rootCtxCancel = context.WithCancel(context.Background())
	defer rootCtxCancel()
	common.SetRestrictiveUmask()

	lvl, err := ipfslog.LevelFromString(*logLevel)
	if err != nil {
		fmt.Println("Invalid log level")
		os.Exit(1)
	}

	logger := ipfslog.Logger("wormhole-fly").Desugar()

	ipfslog.SetAllLoggers(lvl)

	// Verify flags
	if *nodeKeyPath == "" {
		logger.Fatal("Please specify --nodeKey")
	}
	if *p2pBootstrap == "" {
		logger.Fatal("Please specify --bootstrap")
	}

	mongodbConfig := loadMongodbConfig(logger)

	db, err := storage.GetDB(rootCtx, logger, mongodbConfig.mongodbUri, mongodbConfig.databaseName)
	if err != nil {
		logger.Fatal("could not connect to DB", zap.Error(err))
	}

	// Run the database migration.
	err = migration.Run(db)
	if err != nil {
		logger.Fatal("error running migration", zap.Error(err))
	}

	repository := storage.NewRepository(db, logger)

	// Outbound gossip message queue
	sendC := make(chan []byte)

	// Inbound observations
	obsvC := make(chan *gossipv1.SignedObservation, 50)

	// Inbound observation requests
	obsvReqC := make(chan *gossipv1.ObservationRequest, 50)

	// Inbound signed VAAs
	signedInC := make(chan *gossipv1.SignedVAAWithQuorum, 50)

	// Heartbeat updates
	heartbeatC := make(chan *gossipv1.Heartbeat, 50)

	// Guardian set state managed by processor
	gst := common.NewGuardianSetState(heartbeatC)

	// Governor cfg
	govConfigC := make(chan *gossipv1.SignedChainGovernorConfig, 50)

	// Governor status
	govStatusC := make(chan *gossipv1.SignedChainGovernorStatus, 50)
	// Bootstrap guardian set, otherwise heartbeats would be skipped
	// TODO: fetch this and probably figure out how to update it live
	gs := guardiansets.GetLatest()
	gst.Set(&gs)

	// Ignore observation requests
	// Note: without this, the whole program hangs on observation requests
	discardMessages(rootCtx, obsvReqC)

	// Log observations
	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case o := <-obsvC:
				ok := verifyObservation(logger, o, gst.Get())
				if !ok {
					logger.Error("Could not verify observation", zap.String("id", o.MessageId))
					continue
				}
				err := repository.UpsertObservation(o)
				if err != nil {
					logger.Error("Error inserting observation", zap.Error(err))
				}
			}
		}
	}()

	// Log signed VAAs
	cache, err := newCache()
	if err != nil {
		logger.Fatal("could not create cache", zap.Error(err))
	}
	// Creates a deduplicator to discard VAA messages that were processed previously
	deduplicator := deduplicator.New(cache, logger)
	// TODO: configable buffer size
	messageQueue := make(chan *processor.Message, 256)
	// Creates a instance to consume VAA messages from Gossip network and handle the messages
	vaaGossipConsumer := processor.NewVAAGossipConsumer(gst, deduplicator, messageQueue, logger)
	// Creates a instance to consume VAA messages from a queue and store in a storage
	vaaQueueConsumer := processor.NewVAAQueueConsumer(messageQueue, repository, logger)
	vaaQueueConsumer.Start(rootCtx)

	// start fly http server.
	server := server.NewServer(logger, repository)
	server.Start()

	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case sVaa := <-signedInC:
				v, err := vaa.Unmarshal(sVaa.Vaa)
				if err != nil {
					logger.Error("Error unmarshalling vaa", zap.Error(err))
					continue
				}
				// Push an incoming VAA to be processed
				if err := vaaGossipConsumer.Push(rootCtx, v, sVaa.Vaa); err != nil {
					logger.Error("Error inserting vaa", zap.Error(err))
				}
			}
		}
	}()

	// Log heartbeats
	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case hb := <-heartbeatC:
				err := repository.UpsertHeartbeat(hb)
				if err != nil {
					logger.Error("Error inserting heartbeat", zap.Error(err))
				}
			}
		}
	}()

	// Log govConfigs
	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case govConfig := <-govConfigC:
				err := repository.UpsertGovernorConfig(govConfig)
				if err != nil {
					logger.Error("Error inserting gov config", zap.Error(err))
				}
			}
		}
	}()

	// Log govStatus
	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case govStatus := <-govStatusC:
				err := repository.UpsertGovernorStatus(govStatus)
				if err != nil {
					logger.Error("Error inserting gov status", zap.Error(err))
				}
			}
		}
	}()

	// Load p2p private key
	var priv crypto.PrivKey
	priv, err = common.GetOrCreateNodeKey(logger, *nodeKeyPath)
	if err != nil {
		logger.Fatal("Failed to load node key", zap.Error(err))
	}

	// Run supervisor.
	supervisor.New(rootCtx, logger, func(ctx context.Context) error {
		if err := supervisor.Run(ctx, "p2p", p2p.Run(obsvC, obsvReqC, nil, sendC, signedInC, priv, nil, gst, *p2pPort, *p2pNetworkId, *p2pBootstrap, "", false, rootCtxCancel, nil, govConfigC, govStatusC)); err != nil {
			return err
		}

		logger.Info("Started internal services")

		<-ctx.Done()
		return nil
	},
		// It's safer to crash and restart the process in case we encounter a panic,
		// rather than attempting to reschedule the runnable.
		supervisor.WithPropagatePanic)

	<-rootCtx.Done()
	server.Stop()
}

func verifyObservation(logger *zap.Logger, obs *gossipv1.SignedObservation, gs *common.GuardianSet) bool {
	pk, err := crypto2.Ecrecover(obs.GetHash(), obs.GetSignature())
	if err != nil {
		return false
	}

	theirAddr := eth_common.BytesToAddress(obs.GetAddr())
	signerAddr := eth_common.BytesToAddress(crypto2.Keccak256(pk[1:])[12:])
	if theirAddr != signerAddr {
		logger.Error("error validating observation, signer addr and addr don't match",
			zap.String("id", obs.MessageId),
			zap.String("obs_addr", theirAddr.Hex()),
			zap.String("signer_addr", signerAddr.Hex()),
		)
		return false
	}

	_, isFromGuardian := gs.KeyIndex(theirAddr)
	if !isFromGuardian {
		logger.Error("error validating observation, signer not in guardian set",
			zap.String("id", obs.MessageId),
			zap.String("obs_addr", theirAddr.Hex()),
		)
	}
	return isFromGuardian
}

func discardMessages[T any](ctx context.Context, obsvReqC chan T) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-obsvReqC:
			}
		}
	}()
}
