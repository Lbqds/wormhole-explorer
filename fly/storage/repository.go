package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	gossipv1 "github.com/alephium/wormhole-fork/node/pkg/proto/gossip/v1"
	"github.com/alephium/wormhole-fork/node/pkg/vaa"
	eth_common "github.com/ethereum/go-ethereum/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// TODO separate and maybe share between fly and web
type Repository struct {
	db          *mongo.Database
	log         *zap.Logger
	collections struct {
		vaas         *mongo.Collection
		missingVaas  *mongo.Collection
		heartbeats   *mongo.Collection
		observations *mongo.Collection
		vaaCounts    *mongo.Collection
	}
}

// TODO wrap repository with a service that filters using redis
func NewRepository(db *mongo.Database, log *zap.Logger) *Repository {
	return &Repository{db, log, struct {
		vaas         *mongo.Collection
		missingVaas  *mongo.Collection
		heartbeats   *mongo.Collection
		observations *mongo.Collection
		vaaCounts    *mongo.Collection
	}{
		vaas:         db.Collection("vaas"),
		missingVaas:  db.Collection("missingVaas"),
		heartbeats:   db.Collection("heartbeats"),
		observations: db.Collection("observations"),
		vaaCounts:    db.Collection("vaaCounts")}}
}

func (s *Repository) UpsertVaa(ctx context.Context, v *vaa.VAA, serializedVaa []byte) error {
	id := v.MessageID()
	now := time.Now()
	vaaDoc := VaaUpdate{
		ID:               id,
		Timestamp:        &v.Timestamp,
		Version:          v.Version,
		EmitterChain:     v.EmitterChain,
		EmitterAddr:      v.EmitterAddress.String(),
		TargetChain:      v.TargetChain,
		Sequence:         v.Sequence,
		GuardianSetIndex: v.GuardianSetIndex,
		Vaa:              serializedVaa,
		UpdatedAt:        &now,
	}

	update := bson.M{
		"$set":         vaaDoc,
		"$setOnInsert": indexedAt(now),
		"$inc":         bson.D{{Key: "revision", Value: 1}},
	}

	opts := options.Update().SetUpsert(true)
	result, err := s.collections.vaas.UpdateByID(ctx, id, update, opts)
	if err == nil && s.isNewRecord(result) {
		s.updateVAACount(v.EmitterChain)
	}
	return err
}

func (s *Repository) UpsertMissingVaa(ctx context.Context, vaaId string) error {
	now := time.Now()
	missingVaaDoc := MissingVaaUpdate{ID: vaaId}

	update := bson.M{
		"$set":         missingVaaDoc,
		"$setOnInsert": indexedAt(now),
	}

	opts := options.Update().SetUpsert(true)
	_, err := s.collections.missingVaas.UpdateByID(ctx, vaaId, update, opts)
	return err
}

func (s *Repository) UpsertObservation(o *gossipv1.SignedObservation) error {
	vaaID := strings.Split(o.MessageId, "/")
	if len(vaaID) != 4 {
		return fmt.Errorf("invalid vaa id: %s", o.MessageId)
	}
	emitterChainIdStr, emitter, targetChainIdStr, sequenceStr := vaaID[0], vaaID[1], vaaID[2], vaaID[3]
	id := fmt.Sprintf("%s/%s/%s", o.MessageId, hex.EncodeToString(o.Addr), hex.EncodeToString(o.Hash))
	now := time.Now()
	emitterChain, err := strconv.ParseUint(emitterChainIdStr, 10, 16)
	if err != nil {
		return err
	}
	targetChain, err := strconv.ParseUint(targetChainIdStr, 10, 16)
	if err != nil {
		return err
	}
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		return err
	}
	addr := eth_common.BytesToAddress(o.GetAddr())
	obs := ObservationUpdate{
		EmitterChain: vaa.ChainID(emitterChain),
		Emitter:      emitter,
		TargetChain:  vaa.ChainID(targetChain),
		Sequence:     sequence,
		MessageID:    o.GetMessageId(),
		Hash:         o.GetHash(),
		TxHash:       o.GetTxHash(),
		GuardianAddr: addr.String(),
		Signature:    o.GetSignature(),
		UpdatedAt:    &now,
	}

	update := bson.M{
		"$set":         obs,
		"$setOnInsert": indexedAt(now),
	}
	opts := options.Update().SetUpsert(true)
	_, err = s.collections.observations.UpdateByID(context.TODO(), id, update, opts)
	if err != nil {
		s.log.Error("Error inserting observation", zap.Error(err))
	}
	return err
}

func (s *Repository) UpsertHeartbeat(hb *gossipv1.Heartbeat) error {
	id := hb.GuardianAddr
	now := time.Now()
	update := bson.D{{Key: "$set", Value: hb}, {Key: "$set", Value: bson.D{{Key: "updatedAt", Value: now}}}, {Key: "$setOnInsert", Value: bson.D{{Key: "indexedAt", Value: now}}}}
	opts := options.Update().SetUpsert(true)
	_, err := s.collections.heartbeats.UpdateByID(context.TODO(), id, update, opts)
	return err
}

func (s *Repository) updateVAACount(chainID vaa.ChainID) {
	update := bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: uint64(1)}}}}
	opts := options.Update().SetUpsert(true)
	_, _ = s.collections.vaaCounts.UpdateByID(context.TODO(), chainID, update, opts)
}

func (s *Repository) isNewRecord(result *mongo.UpdateResult) bool {
	return result.MatchedCount == 0 && result.ModifiedCount == 0 && result.UpsertedCount == 1
}

// GetMongoStatus get mongo server status
func (r *Repository) GetMongoStatus(ctx context.Context) (*MongoStatus, error) {
	command := bson.D{{Key: "serverStatus", Value: 1}}
	result := r.db.RunCommand(ctx, command)
	if result.Err() != nil {
		return nil, result.Err()
	}

	var mongoStatus MongoStatus
	err := result.Decode(&mongoStatus)
	if err != nil {
		return nil, err
	}
	return &mongoStatus, nil
}
