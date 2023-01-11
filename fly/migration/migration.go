package migration

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// TODO: move this to migration tool that support mongodb.
func Run(db *mongo.Database) error {
	// Created heartbeats collection.
	err := db.CreateCollection(context.TODO(), "heartbeats")
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}

	// Created observations collection.
	err = db.CreateCollection(context.TODO(), "observations")
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}

	// Created vaaCounts collection.
	err = db.CreateCollection(context.TODO(), "vaaCounts")
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}

	// Create vaas collection.
	err = db.CreateCollection(context.TODO(), "vaas")
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}

	// create index in vaas collection by vaa key (emitterchain, emitterAddr, sequence)
	indexVaaByKey := mongo.IndexModel{
		Keys: bson.D{
			{Key: "timestamp", Value: -1},
			{Key: "emitterAddr", Value: 1},
			{Key: "emitterChain", Value: 1},
		}}
	_, err = db.Collection("vaas").Indexes().CreateOne(context.TODO(), indexVaaByKey)
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}

	indexVaaByTimestamp := mongo.IndexModel{
		Keys: bson.D{
			{Key: "emitterChain", Value: 1},
			{Key: "emitterAddr", Value: 1},
			{Key: "sequence", Value: 1},
		}}
	_, err = db.Collection("vaas").Indexes().CreateOne(context.TODO(), indexVaaByTimestamp)
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}

	// create index in observations collection by indexedAt.
	indexObservationsByIndexedAt := mongo.IndexModel{Keys: bson.D{{Key: "indexedAt", Value: 1}}}
	_, err = db.Collection("observations").Indexes().CreateOne(context.TODO(), indexObservationsByIndexedAt)
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}

	// create index in observations collect.
	indexObservationsByEmitterChainAndAddressAndSequence := mongo.IndexModel{
		Keys: bson.D{
			{Key: "emitterChain", Value: 1},
			{Key: "emitterAddr", Value: 1},
			{Key: "sequence", Value: 1}}}
	_, err = db.Collection("observations").Indexes().CreateOne(context.TODO(), indexObservationsByEmitterChainAndAddressAndSequence)
	if err != nil {
		target := &mongo.CommandError{}
		isCommandError := errors.As(err, target)
		if !isCommandError || err.(mongo.CommandError).Code != 48 {
			return err
		}
	}
	return nil
}
