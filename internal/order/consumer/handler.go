package consumer

import (
	"context"

	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
)

type Handler struct {
}

func (h *Handler) Process(ctx context.Context, msg kafka.Message) error {

	return nil
}
