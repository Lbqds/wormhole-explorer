// Package observations handle the request of VAA data from governor endpoint defined in the api.
package vaa

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/wormhole-foundation/wormhole-explorer/api/middleware"
	"github.com/wormhole-foundation/wormhole-explorer/api/response"
	"go.uber.org/zap"
)

// Controller definition.
type Controller struct {
	srv    *Service
	logger *zap.Logger
}

// NewController create a new controler.
func NewController(serv *Service, logger *zap.Logger) *Controller {
	return &Controller{srv: serv, logger: logger.With(zap.String("module", "VaaController"))}
}

// FindAll handler for the endpoint /vaas/.
func (c *Controller) FindAll(ctx *fiber.Ctx) error {
	p := middleware.GetPaginationFromContext(ctx)
	vaas, err := c.srv.FindAll(ctx.Context(), p)
	if err != nil {
		return err
	}
	return ctx.JSON(vaas)
}

// FindByChain handler for the endpoint /vaas/:chain.
func (c *Controller) FindByChain(ctx *fiber.Ctx) error {
	p := middleware.GetPaginationFromContext(ctx)
	chainID, err := middleware.ExtractChainID(ctx, c.logger)
	if err != nil {
		return err
	}
	vaas, err := c.srv.FindByChain(ctx.Context(), chainID, p)
	if err != nil {
		return err
	}
	return ctx.JSON(vaas)
}

// FindByEmitter handler for the endpoint /vaas/:chain/:emitter.
func (c *Controller) FindByEmitter(ctx *fiber.Ctx) error {
	p := middleware.GetPaginationFromContext(ctx)
	chainID, emitter, err := middleware.ExtractVAAChainIDEmitter(ctx, c.logger)
	if err != nil {
		return err
	}
	vaas, err := c.srv.FindByEmitter(ctx.Context(), chainID, *emitter, p)
	if err != nil {
		return err
	}
	return ctx.JSON(vaas)
}

// FindById handler for the endpoint /vaas/:chain/:emitter/:sequence/:signer/:hash.
func (c *Controller) FindById(ctx *fiber.Ctx) error {
	chainID, emitter, seq, err := middleware.ExtractVAAParams(ctx, c.logger)
	if err != nil {
		return err
	}

	vaa, err := c.srv.FindById(ctx.Context(), chainID, *emitter, strconv.FormatUint(seq, 10))
	if err != nil {
		return err
	}
	return ctx.JSON(vaa)
}

// FindSignedVAAByID get a VAA []byte from a chainID, emitter address and sequence.
// This endpoint has been migrated from the guardian grpc api.
func (c *Controller) FindSignedVAAByID(ctx *fiber.Ctx) error {
	chainID, emitter, seq, err := middleware.ExtractVAAParams(ctx, c.logger)
	if err != nil {
		return err
	}

	// check chainID is not Pyth. Pyth message are not stored with the other vaa.
	if ChainIDPythNet == chainID {
		return response.NewApiError(ctx, fiber.StatusBadRequest, response.InvalidParam,
			"not supported for PythNet", nil)
	}

	vaa, err := c.srv.FindById(ctx.Context(), chainID, *emitter, strconv.FormatUint(seq, 10))
	if err != nil {
		return err
	}
	response := struct {
		VaaBytes []byte `json:"vaaBytes"`
	}{
		VaaBytes: vaa.Data.Vaa,
	}
	return ctx.JSON(response)
}

// FindSignedBatchVAAByID get a Batch VAA from a chainID, emitter address and sequence.
// This endpoint has been migrated from the guardian grpc api.
func (c *Controller) FindSignedBatchVAAByID(ctx *fiber.Ctx) error {
	return response.NewApiError(ctx, fiber.StatusNotImplemented, response.Unimplemented, "not yet implemented", nil)
}

func (c *Controller) FindForPythnet(ctx *fiber.Ctx) error {
	return nil
}

// GetVaaCount handler for the endpoint /vaas/vaa-counts.
func (c *Controller) GetVaaCount(ctx *fiber.Ctx) error {
	p := middleware.GetPaginationFromContext(ctx)
	vaas, err := c.srv.GetVaaCount(ctx.Context(), p)
	if err != nil {
		return err
	}
	return ctx.JSON(vaas)
}
