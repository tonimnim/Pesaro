package inbox

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type queryKey struct{}
type crashTracer struct{ point string }

func (c *crashTracer) pause(point string) {
	if c.point == point {
		_, _ = os.Stdout.WriteString("checkpoint\n")
		select {}
	}
}
func (c *crashTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if data.SQL == "commit" {
		c.pause("before_commit")
	}
	return context.WithValue(ctx, queryKey{}, data.SQL)
}
func (c *crashTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if data.Err != nil {
		return
	}
	query, _ := ctx.Value(queryKey{}).(string)
	switch {
	case strings.HasPrefix(query, "INSERT INTO event_inbox"):
		c.pause("after_inbox")
	case strings.HasPrefix(query, "INSERT INTO ledger_operations"):
		c.pause("after_projection")
	case query == "commit":
		c.pause("after_commit")
	}
}

func TestInboxCrashChild(t *testing.T) {
	point := os.Getenv("PESAR_INBOX_CRASH_POINT")
	if point == "" {
		t.Skip("child entry point")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Open(ctx, os.Getenv("PESAR_RECONCILIATION_TEST_RUNTIME_URL"), false)
	if err != nil {
		t.Fatal(err)
	}
	config := s.pool.Config()
	s.pool.Close()
	config.ConnConfig.Tracer = &crashTracer{point: point}
	s.pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var data []byte
	if err = json.NewDecoder(os.Stdin).Decode(&data); err != nil {
		t.Fatal(err)
	}
	if err = s.Accept(ctx, data); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash point was not reached")
}

func TestInboxProcessCrashes(t *testing.T) {
	for _, point := range []string{"after_inbox", "after_projection", "before_commit", "after_commit"} {
		t.Run(point, func(t *testing.T) {
			s := testStore(t)
			event := testEvent(t)
			data, err := event.Encode()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInboxCrashChild$")
			for _, entry := range os.Environ() {
				name := strings.ToUpper(strings.SplitN(entry, "=", 2)[0])
				if !strings.HasPrefix(name, "PESAR_") && !strings.HasPrefix(name, "PESARO_") {
					child.Env = append(child.Env, entry)
				}
			}
			child.Env = append(child.Env, "PESAR_INBOX_CRASH_POINT="+point, "PESAR_RECONCILIATION_TEST_RUNTIME_URL="+os.Getenv("PESAR_RECONCILIATION_TEST_RUNTIME_URL"))
			input, err := child.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			if err = child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = child.Process.Kill() }()
			if err = json.NewEncoder(input).Encode(data); err != nil {
				t.Fatal(err)
			}
			line, err := bufio.NewReader(output).ReadString('\n')
			if err != nil || line != "checkpoint\n" {
				_ = child.Process.Kill()
				_ = child.Wait()
				t.Fatal("checkpoint missing", line, err)
			}
			if err = child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = child.Wait(); err == nil {
				t.Fatal("child was not killed")
			}
			want := 0
			if point == "after_commit" {
				want = 1
			}
			counts(t, s, event.BookID, want)
			for range 2 {
				if err = s.Accept(ctx, data); err != nil {
					t.Fatal(err)
				}
			}
			counts(t, s, event.BookID, 1)
			t.Log("process killed at", point, "; original event recovered; exactly one inbox and projection row")
		})
	}
}
