package cockroach_test

// This oracle deliberately imports no Ledger packages. It stores accepted
// instructions and hold lifecycles, and recomputes positions with math/big.
// It does not reuse posting templates, deltas, money values or state transitions.
import (
	"encoding/json"
	"math/big"
)

type referenceInstruction struct {
	Key, Payment, Hold, Kind, Bucket string
	Source                           int
	Principal, Fee, Version          int64
	Fenced                           bool
}
type referenceHold struct {
	Instruction referenceInstruction
	State       string
	Version     int64
}
type referenceBook struct {
	Cap       int64
	Receipts  map[string]string // Empty reason denotes an applied instruction.
	Claims    map[string]bool
	Transfers []referenceInstruction
	Holds     map[string]referenceHold
}
type referencePosition struct {
	Posted, Held [4]*big.Int // wallet A, wallet B, fee income, provider asset
	Reserved     [2]map[string]*big.Int
	Consumed     [2]map[string]*big.Int
	Journals     int
}

func newReferenceBook(cap int64) *referenceBook {
	return &referenceBook{Cap: cap, Receipts: map[string]string{}, Claims: map[string]bool{}, Holds: map[string]referenceHold{}}
}
func (b *referenceBook) clone() *referenceBook {
	data, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}
	var copy referenceBook
	if err = json.Unmarshal(data, &copy); err != nil {
		panic(err)
	}
	return &copy
}
func addExact(target *big.Int, units int64) { target.Add(target, big.NewInt(units)) }
func bucketValue(values map[string]*big.Int, bucket string) *big.Int {
	if values[bucket] == nil {
		values[bucket] = new(big.Int)
	}
	return values[bucket]
}
func (b *referenceBook) position() referencePosition {
	p := referencePosition{Journals: 1}
	for i := range p.Posted {
		p.Posted[i], p.Held[i] = new(big.Int), new(big.Int)
	}
	p.Posted[0].SetInt64(100000)
	p.Posted[3].SetInt64(100000)
	for i := range p.Reserved {
		p.Reserved[i], p.Consumed[i] = map[string]*big.Int{}, map[string]*big.Int{}
	}
	for _, transfer := range b.Transfers {
		addExact(p.Posted[transfer.Source], -transfer.Principal)
		addExact(p.Posted[transfer.Source], -transfer.Fee)
		addExact(p.Posted[1-transfer.Source], transfer.Principal)
		addExact(p.Posted[2], transfer.Fee)
		addExact(bucketValue(p.Consumed[transfer.Source], transfer.Bucket), transfer.Principal)
		p.Journals++
	}
	for _, hold := range b.Holds {
		i := hold.Instruction
		switch hold.State {
		case "RESERVED", "EXPOSED":
			addExact(p.Held[i.Source], i.Principal)
			addExact(p.Held[i.Source], i.Fee)
			addExact(p.Held[3], i.Principal)
			addExact(bucketValue(p.Reserved[i.Source], i.Bucket), i.Principal)
		case "CAPTURED":
			addExact(p.Posted[i.Source], -i.Principal)
			addExact(p.Posted[i.Source], -i.Fee)
			addExact(p.Posted[3], -i.Principal)
			addExact(p.Posted[2], i.Fee)
			addExact(bucketValue(p.Consumed[i.Source], i.Bucket), i.Principal)
			p.Journals++
		}
	}
	return p
}

func (b *referenceBook) apply(i referenceInstruction) string {
	if reason, exists := b.Receipts[i.Key]; exists {
		return reason
	}
	reason := b.decide(i)
	b.Receipts[i.Key] = reason
	return reason
}
func (b *referenceBook) decide(i referenceInstruction) string {
	if i.Kind == "transfer" || i.Kind == "reserve" {
		if b.Claims[i.Payment] {
			return "BUSINESS_ALREADY_DECIDED"
		}
		// An admitted decline permanently decides this payment too.
		b.Claims[i.Payment] = true
		p := b.position()
		available := new(big.Int).Sub(p.Posted[i.Source], p.Held[i.Source])
		total := new(big.Int).Add(big.NewInt(i.Principal), big.NewInt(i.Fee))
		if available.Cmp(total) < 0 {
			return "INSUFFICIENT_FUNDS"
		}
		usage := new(big.Int).Add(bucketValue(p.Reserved[i.Source], i.Bucket), bucketValue(p.Consumed[i.Source], i.Bucket))
		addExact(usage, i.Principal)
		if usage.Cmp(big.NewInt(b.Cap)) > 0 {
			return "LIMIT_EXCEEDED"
		}
		if i.Kind == "transfer" {
			b.Transfers = append(b.Transfers, i)
			return ""
		}
		pool := new(big.Int).Sub(p.Posted[3], p.Held[3])
		if pool.Cmp(big.NewInt(i.Principal)) < 0 {
			return "PROVIDER_CAPACITY_EXCEEDED"
		}
		b.Holds[i.Hold] = referenceHold{Instruction: i, State: "RESERVED", Version: 1}
		return ""
	}
	h := b.Holds[i.Hold]
	if h.Version != i.Version {
		return "STATE_CONFLICT"
	}
	switch i.Kind {
	case "expose":
		if h.State != "RESERVED" {
			return "STATE_CONFLICT"
		}
		h.State = "EXPOSED"
	case "capture":
		if h.State != "EXPOSED" {
			return "STATE_CONFLICT"
		}
		h.State = "CAPTURED"
	case "release":
		if i.Fenced {
			if h.State != "EXPOSED" {
				return "STATE_CONFLICT"
			}
		} else if h.State == "EXPOSED" {
			return "EXPOSED_REQUIRES_EVIDENCE"
		} else if h.State != "RESERVED" {
			return "STATE_CONFLICT"
		}
		h.State = "RELEASED"
	default:
		panic("reference model received an unsupported instruction")
	}
	h.Version++
	b.Holds[i.Hold] = h
	return ""
}
