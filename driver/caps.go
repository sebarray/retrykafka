package driver

import "context"

type CapsOverride struct {
	Driver
	Caps Capabilities
}

func (c CapsOverride) Capabilities() Capabilities { return c.Caps }

func (c CapsOverride) BeginTxn(ctx context.Context) (Txn, error) {
	td, ok := c.Driver.(TxnDriver)
	if !ok {
		return nil, ErrNoTxn
	}
	return td.BeginTxn(ctx)
}
