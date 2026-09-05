package merge

import (
	"context"
	"fmt"

	"github.com/iceisfun/golua/vm"
)

// Check the expanded tree before luaToGo allocates maps/slices or the encoder
// allocates a payload. Repeated aliases are charged on every occurrence.
type resultBudget struct {
	ctx       context.Context
	remaining int
	nodes     int
	active    map[*vm.Table]bool
}

func validateResultBudget(ctx context.Context, value vm.Value, maximum int) error {
	budget := resultBudget{
		ctx:       ctx,
		remaining: maximum,
		nodes:     max(64, min(maximum/8, 1_000_000)),
		active:    make(map[*vm.Table]bool),
	}
	return budget.visit(value, 0)
}

func (b *resultBudget) visit(value vm.Value, depth int) error {
	if err := b.ctx.Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrExecutionDeadline, err)
	}
	b.nodes--
	cost := 1
	if value.IsString() {
		cost += len(value.AsString()) + 2
	}
	if depth > 256 || b.nodes < 0 || cost > b.remaining {
		return fmt.Errorf("%w: expanded result exceeds the byte, node, or depth budget", ErrExecutionExhausted)
	}
	b.remaining -= cost
	if !value.IsTable() {
		return nil
	}
	table, ok := value.AsTable().(*vm.Table)
	if !ok {
		return fmt.Errorf("%w: virtual tables are not supported", ErrInvalidResult)
	}
	if b.active[table] {
		return fmt.Errorf("%w: result contains a table cycle", ErrInvalidResult)
	}
	b.active[table] = true
	defer delete(b.active, table)
	key := vm.Nil
	for {
		next, item, err := table.Next(key)
		if err != nil {
			return err
		}
		if next.IsNil() {
			return nil
		}
		if err := b.visit(next, depth+1); err != nil {
			return err
		}
		if err := b.visit(item, depth+1); err != nil {
			return err
		}
		key = next
	}
}
