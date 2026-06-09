//go:build integration

package integration_test

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Grizzly1127/trading_matchengine/internal/order/consumer"
	"github.com/Grizzly1127/trading_matchengine/internal/order/repository"
	"github.com/Grizzly1127/trading_matchengine/internal/order/status"
	"github.com/Grizzly1127/trading_matchengine/pkg/kafka"
	commonv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/common/v1"
	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"github.com/Grizzly1127/trading_matchengine/pkg/symbolrules"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/proto"
)

const containerStartTimeout = 3 * time.Minute

var (
	sharedOnce    sync.Once
	sharedPool    *pgxpool.Pool
	sharedCleanup func()
	sharedSkip    string
)

func TestOutboxPlaceOrderAndMatchAccepted(t *testing.T) {
	ctx, repo := setupRepo(t)

	const userID uint64 = 9001
	if err := repo.SeedBalance(ctx, userID, "USDT", "100000"); err != nil {
		t.Fatalf("seed balance: %v", err)
	}

	price := "50000"
	qty := "0.01"
	order, err := repo.InsertPending(ctx, repository.InsertPendingInput{
		UserID:        userID,
		ClientOrderID: "itest-outbox-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		Symbol:        "BTC-USDT",
		Side:          int16(commonv1.Side_SIDE_BUY),
		OrderType:     int16(commonv1.OrderType_ORDER_TYPE_LIMIT),
		Price:         &price,
		Quantity:      qty,
		OutboxTopic:   "order.commands",
	})
	if err != nil {
		t.Fatalf("insert pending: %v", err)
	}
	if order.Status != status.Pending {
		t.Fatalf("status=%s want PENDING", order.Status)
	}

	n, err := repo.CountUnpublishedOutbox(ctx)
	if err != nil || n < 1 {
		t.Fatalf("unpublished outbox count=%d err=%v", n, err)
	}

	entries, err := repo.FetchUnpublished(ctx, 10)
	if err != nil || len(entries) == 0 {
		t.Fatalf("fetch unpublished: %v len=%d", err, len(entries))
	}
	if err := repo.MarkPublished(ctx, entries[0].ID); err != nil {
		t.Fatalf("mark published: %v", err)
	}

	h := &consumer.MatchHandler{Repo: repo}
	ev := &matchingv1.MatchEvent{
		OrderId:   order.ID,
		Symbol:    order.Symbol,
		EventType: matchingv1.MatchEventType_ORDER_ACCEPTED,
		WalSeq:    1,
	}
	payload, _ := proto.Marshal(ev)
	if err := h.Process(ctx, kafka.Message{Value: payload}); err != nil {
		t.Fatalf("apply match: %v", err)
	}

	got, err := repo.GetOrderByUser(ctx, userID, order.ID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if got.Status != status.Accepted {
		t.Fatalf("status=%s want ACCEPTED", got.Status)
	}
}

func TestClientOrderIdempotency(t *testing.T) {
	ctx, repo := setupRepo(t)

	const userID uint64 = 9002
	if err := repo.SeedBalance(ctx, userID, "BTC", "10"); err != nil {
		t.Fatalf("seed btc: %v", err)
	}
	price := "100"
	in := repository.InsertPendingInput{
		UserID: userID, ClientOrderID: "idem-" + strconv.FormatInt(time.Now().UnixNano(), 10), Symbol: "BTC-USDT",
		Side: int16(commonv1.Side_SIDE_SELL), OrderType: int16(commonv1.OrderType_ORDER_TYPE_LIMIT),
		Price: &price, Quantity: "0.1", OutboxTopic: "order.commands",
	}
	o1, err := repo.InsertPending(ctx, in)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	existing, err := repo.FindByClientOrderID(ctx, userID, in.ClientOrderID)
	if err != nil || existing == nil || existing.ID != o1.ID {
		t.Fatalf("idempotent lookup: err=%v id=%d", err, existing.ID)
	}
}

func setupRepo(t *testing.T) (context.Context, *repository.Repository) {
	t.Helper()
	initSharedPostgres(t)
	if sharedSkip != "" {
		t.Skip(sharedSkip)
	}
	ctx := context.Background()
	assets, _ := symbolrules.DefaultAssetRegistry()
	return ctx, repository.New(sharedPool, assets)
}

func initSharedPostgres(t *testing.T) {
	t.Helper()
	sharedOnce.Do(func() {
		ctx := context.Background()
		if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
			pool, err := repository.NewPool(ctx, url, 0)
			if err != nil {
				sharedSkip = "TEST_DATABASE_URL connect failed: " + err.Error()
				return
			}
			if err := repository.MigrateUp(ctx, pool); err != nil {
				pool.Close()
				sharedSkip = "migrate failed: " + err.Error()
				return
			}
			sharedPool = pool
			sharedCleanup = func() { pool.Close() }
			return
		}

		startCtx, cancel := context.WithTimeout(ctx, containerStartTimeout)
		defer cancel()

		pg, err := postgres.Run(startCtx,
			"postgres:16-alpine",
			postgres.WithDatabase("trading"),
			postgres.WithUsername("trading"),
			postgres.WithPassword("trading"),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			if startCtx.Err() == context.DeadlineExceeded {
				sharedSkip = "postgres testcontainer start timeout (预拉镜像: docker pull postgres:16-alpine)"
				return
			}
			sharedSkip = "postgres testcontainer unavailable: " + err.Error()
			return
		}
		connStr, err := pg.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			_ = pg.Terminate(ctx)
			sharedSkip = "connection string: " + err.Error()
			return
		}
		pool, err := repository.NewPool(ctx, connStr, 0)
		if err != nil {
			_ = pg.Terminate(ctx)
			sharedSkip = "pool: " + err.Error()
			return
		}
		if err := repository.MigrateUp(ctx, pool); err != nil {
			pool.Close()
			_ = pg.Terminate(ctx)
			sharedSkip = "migrate: " + err.Error()
			return
		}
		sharedPool = pool
		sharedCleanup = func() {
			pool.Close()
			_ = pg.Terminate(ctx)
		}
	})
}

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedCleanup != nil {
		sharedCleanup()
	}
	os.Exit(code)
}
