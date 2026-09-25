package application

import (
	"errors"
	"time"

	"github.com/tonimnim/Pesaro/services/ledger/internal/domain"
)

func controlsFor(st State, a domain.Account) (Control, Control, error) {
	sub, ok := st.Controls[ControlKey{"SUBJECT", a.Owner}]
	if !ok {
		return Control{}, Control{}, ErrIntegrity
	}
	acct, ok := st.Controls[ControlKey{"ACCOUNT", a.ID}]
	if !ok {
		return Control{}, Control{}, ErrIntegrity
	}
	return sub, acct, nil
}
func (s *Service) evaluate(c Caller, cmd Command, st State, now time.Time, p *Plan) (string, error) {
	switch cmd.Kind {
	case CreateAccount:
		return create(c, cmd, st, p)
	case SetAccountControl:
		return setControl(c, cmd, st, p)
	case TransferInternal, ReservePayout:
		return s.admit(c, cmd, st, now, p)
	case MarkHoldExposed, CapturePayout, ReleasePayout:
		return s.resolve(c, cmd, st, now, p)
	}
	return "", ErrInvalid
}
func create(c Caller, cmd Command, st State, p *Plan) (string, error) {
	request := cmd.Create
	if !c.AllowsSubject(request.OwnerID) {
		return "", ErrPermission
	}
	p.Receipt.SubjectIDs = []domain.ID{request.OwnerID}
	if _, exists := st.Accounts[request.AccountID]; exists {
		return "ACCOUNT_EXISTS", nil
	}
	a := domain.Account{ID: request.AccountID, Book: st.Book.Book, Owner: request.OwnerID, Purpose: request.Purpose}
	if err := a.Validate(); err != nil {
		return "", ErrInvalid
	}
	p.NewAccount = &a
	p.Balances = []Change[Balance]{{After: Balance{AccountID: a.ID, Version: 1}}}
	for _, key := range []ControlKey{{"SUBJECT", a.Owner}, {"ACCOUNT", a.ID}} {
		if _, ok := st.Controls[key]; !ok {
			control := Control{Key: key, Version: 1, Epoch: 1}
			if key.Kind == "SUBJECT" {
				control.DailyCap = st.Book.DefaultCap
			}
			p.Controls = append(p.Controls, Change[Control]{After: control})
		}
	}
	return "", nil
}
func setControl(c Caller, cmd Command, st State, p *Plan) (string, error) {
	req := cmd.Control
	subject := req.Key.ID
	if req.Key.Kind == "ACCOUNT" {
		a, exists := st.Accounts[req.Key.ID]
		if !exists {
			return "", ErrNotFound
		}
		subject = a.Owner
		if !req.DailyCap.IsZero() {
			return "", ErrInvalid
		} // M1 caps are explicitly subject-wide.
	}
	if !c.AllowsSubject(subject) {
		return "", ErrPermission
	}
	p.Receipt.SubjectIDs = []domain.ID{subject}
	before, ok := st.Controls[req.Key]
	if !ok {
		return "", ErrNotFound
	}
	if before.Version != req.ExpectedVersion {
		return "STATE_CONFLICT", nil
	}
	after := before
	var err error
	after.Version, err = domain.NextVersion(before.Version)
	if err != nil {
		return "NUMERIC_LIMIT", nil
	}
	if req.RevokeGrants {
		after.Epoch, err = domain.NextVersion(before.Epoch)
		if err != nil {
			return "NUMERIC_LIMIT", nil
		}
	}
	after.DebitFrozen = req.DebitFrozen
	after.CreditFrozen = req.CreditFrozen
	after.DailyCap = req.DailyCap
	p.Controls = []Change[Control]{{Before: &before, After: after}}
	return "", nil
}
func (s *Service) admit(c Caller, cmd Command, st State, now time.Time, p *Plan) (string, error) {
	var terms Terms
	if cmd.Kind == TransferInternal {
		terms = cmd.Transfer.Terms
	} else {
		terms = cmd.Reserve.Terms
	}
	a, ok := st.Accounts[terms.SourceID]
	if !ok {
		return "", ErrNotFound
	}
	if !c.AllowsSubject(a.Owner) {
		return "", ErrPermission
	}
	p.Receipt.SubjectIDs = []domain.ID{a.Owner}
	// Authenticate the bound instruction before admitting even a rejection;
	// otherwise an invalid account purpose could poison a business claim.
	if !s.trust.grantBinding(cmd.BookID, terms, a.Owner) {
		return "", ErrPermission
	}
	if a.Purpose != domain.Wallet {
		return "INVALID_ACCOUNT_PURPOSE", nil
	}
	sub, acct, err := controlsFor(st, a)
	if err != nil {
		return "", err
	}
	if st.Claim != nil {
		return "BUSINESS_ALREADY_DECIDED", nil
	}
	g := terms.Grant.Claims
	if now.Before(g.NotBefore) || !now.Before(g.ExpiresAt) {
		return "GRANT_EXPIRED", nil
	}
	if g.SubjectEpoch != sub.Epoch || g.AccountEpoch != acct.Epoch {
		return "AUTHORIZATION_REVOKED", nil
	}
	if sub.DebitFrozen || acct.DebitFrozen {
		return "ACCOUNT_BLOCKED", nil
	}
	if st.Policy == nil || st.Policy.ID != terms.PolicyID || st.Policy.Fee != terms.Fee {
		return "INVALID_POLICY", nil
	}
	feeAccount, ok := st.Accounts[st.Policy.FeeAccountID]
	if !ok || feeAccount.Purpose != domain.FeeIncome {
		return "", ErrIntegrity
	}
	total, err := terms.Principal.Add(terms.Fee)
	if err != nil {
		return "NUMERIC_LIMIT", nil
	}
	bal, ok := st.Balances[a.ID]
	if !ok {
		return "", ErrIntegrity
	}
	available, err := bal.Totals.Available(a.Purpose)
	if err != nil {
		return "", ErrIntegrity
	}
	if available.Cmp(domain.WideAmount(total)) < 0 {
		return "INSUFFICIENT_FUNDS", nil
	}
	usage := Usage{OwnerID: a.Owner, Bucket: Bucket(now)}
	if st.Usage != nil {
		usage = *st.Usage
	}
	if usage.OwnerID != a.Owner || usage.Bucket != Bucket(now) {
		return "", ErrIntegrity
	}
	nextUsage := usage
	nextUsage.Values, err = usage.Values.Admit(sub.DailyCap, terms.Principal, cmd.Kind == ReservePayout)
	if errors.Is(err, domain.ErrLimit) {
		return "LIMIT_EXCEEDED", nil
	}
	if err != nil {
		return "NUMERIC_LIMIT", nil
	}
	nextUsage.Version, err = domain.NextVersion(usage.Version)
	if err != nil {
		return "NUMERIC_LIMIT", nil
	}
	p.Usage = &Change[Usage]{Before: st.Usage, After: nextUsage}
	if cmd.Kind == TransferInternal {
		if terms.SourceID == terms.BeneficiaryID {
			return "SELF_TRANSFER", nil
		}
		recipient, ok := st.Accounts[terms.BeneficiaryID]
		if !ok {
			return "UNKNOWN_BENEFICIARY", nil
		}
		if recipient.Purpose != domain.Wallet {
			return "INVALID_ACCOUNT_PURPOSE", nil
		}
		rs, ra, err := controlsFor(st, recipient)
		if err != nil {
			return "", err
		}
		if rs.CreditFrozen || ra.CreditFrozen {
			return "BENEFICIARY_BLOCKED", nil
		}
		lines := []domain.Line{{Account: a, Side: domain.Debit, Amount: total}, {Account: recipient, Side: domain.Credit, Amount: terms.Principal}}
		if !terms.Fee.IsZero() {
			lines = append(lines, domain.Line{Account: feeAccount, Side: domain.Credit, Amount: terms.Fee})
		}
		journal, err := domain.NewJournal(st.Book.Book, lines)
		if err != nil {
			return "", ErrIntegrity
		}
		p.Journal = &journal
		for _, line := range lines {
			before, ok := st.Balances[line.Account.ID]
			if !ok {
				return "", ErrIntegrity
			}
			after := before
			after.Totals, err = before.Totals.Post(line.Account.Purpose, line.Side, line.Amount)
			if err != nil {
				return "NUMERIC_LIMIT", nil
			}
			after.Version, err = domain.NextVersion(before.Version)
			if err != nil {
				return "NUMERIC_LIMIT", nil
			}
			p.Balances = append(p.Balances, Change[Balance]{Before: &before, After: after})
		}
		return "", nil
	}
	r := cmd.Reserve
	if st.Hold != nil {
		return "HOLD_EXISTS", nil
	}
	if r.PoolID != st.Policy.PoolID {
		return "INVALID_PROVIDER_POOL", nil
	}
	pool, ok := st.Accounts[r.PoolID]
	if !ok || pool.Purpose != domain.ProviderPool {
		return "", ErrIntegrity
	}
	ps, pa, err := controlsFor(st, pool)
	if err != nil {
		return "", err
	}
	if ps.DebitFrozen || pa.DebitFrozen {
		return "PROVIDER_POOL_BLOCKED", nil
	}
	if !now.Before(r.ExpiresAt) || r.ExpiresAt.After(now.Add(time.Duration(st.Policy.MaxHoldSeconds)*time.Second)) || r.ExpiresAt.After(g.ExpiresAt) {
		return "INVALID_EXPIRY", nil
	}
	for _, entry := range []struct {
		id     domain.ID
		amount domain.Amount
	}{{a.ID, total}, {pool.ID, terms.Principal}} {
		before, ok := st.Balances[entry.id]
		if !ok {
			return "", ErrIntegrity
		}
		after := before
		after.Totals, err = before.Totals.Reserve(st.Accounts[entry.id].Purpose, entry.amount)
		if errors.Is(err, domain.ErrUnderflow) {
			return "PROVIDER_CAPACITY_EXCEEDED", nil
		}
		if err != nil {
			return "NUMERIC_LIMIT", nil
		}
		after.Version, err = domain.NextVersion(before.Version)
		if err != nil {
			return "NUMERIC_LIMIT", nil
		}
		p.Balances = append(p.Balances, Change[Balance]{Before: &before, After: after})
	}
	terms.Grant.Signature = "" // Never persist the replayable proof/token itself.
	h := Hold{ID: r.HoldID, Terms: terms, OwnerID: a.Owner, AttemptID: r.AttemptID, PoolID: r.PoolID,
		FeeAccountID: st.Policy.FeeAccountID, ProviderAccountID: st.Policy.ProviderAccountID, CapabilityID: st.Policy.CapabilityID,
		Bucket: usage.Bucket, State: domain.HoldStateValue{State: domain.Reserved, Version: 1, ExpiresAt: r.ExpiresAt}}
	p.Hold = &Change[Hold]{After: h}
	return "", nil
}
func (s *Service) resolve(c Caller, cmd Command, st State, now time.Time, p *Plan) (string, error) {
	if st.Hold == nil {
		return "", ErrNotFound
	}
	before := *st.Hold
	if !c.AllowsSubject(before.OwnerID) {
		return "", ErrPermission
	}
	p.Receipt.SubjectIDs = []domain.ID{before.OwnerID}
	var ref HoldRef
	switch cmd.Kind {
	case MarkHoldExposed:
		ref = cmd.Expose.Ref
	case CapturePayout:
		ref = cmd.Capture.Ref
	case ReleasePayout:
		ref = cmd.Release.Ref
	}
	if ref.PaymentID != before.Terms.PaymentID || ref.AttemptID != before.AttemptID {
		return "HOLD_BINDING_CONFLICT", nil
	}
	if st.Claim == nil || st.Claim.HoldID != before.ID {
		return "", ErrIntegrity
	}
	after := before
	var err error
	switch cmd.Kind {
	case MarkHoldExposed:
		a, ok := st.Accounts[before.Terms.SourceID]
		if !ok {
			return "", ErrIntegrity
		}
		sub, acct, err := controlsFor(st, a)
		if err != nil {
			return "", err
		}
		g := before.Terms.Grant.Claims
		if cmd.Expose.GrantID != g.ID {
			return "GRANT_BINDING_CONFLICT", nil
		}
		if sub.DebitFrozen || acct.DebitFrozen {
			return "ACCOUNT_BLOCKED", nil
		}
		if g.SubjectEpoch != sub.Epoch || g.AccountEpoch != acct.Epoch {
			return "AUTHORIZATION_REVOKED", nil
		}
		if now.Before(g.NotBefore) || !now.Before(g.ExpiresAt) || len(s.trust.GrantKeys[g.Issuer]) != 32 {
			return "GRANT_EXPIRED", nil
		}
		pool, ok := st.Accounts[before.PoolID]
		if !ok {
			return "", ErrIntegrity
		}
		ps, pa, err := controlsFor(st, pool)
		if err != nil {
			return "", err
		}
		if ps.DebitFrozen || pa.DebitFrozen {
			return "PROVIDER_POOL_BLOCKED", nil
		}
		after.State, err = before.State.Expose(ref.ExpectedVersion, now)
		if errors.Is(err, domain.ErrExpired) {
			return "HOLD_EXPIRED", nil
		}
		if err != nil {
			return "STATE_CONFLICT", nil
		}
	case CapturePayout:
		ev := cmd.Capture.Evidence
		if !s.trust.evidence(cmd.BookID, before, ev, now) || ev.Claims.FinalState != "FINAL_SUCCESS" {
			return "INVALID_EVIDENCE", nil
		}
		p.Evidence = &ev.Claims
		after.State, err = before.State.Capture(ref.ExpectedVersion, true)
		if err != nil {
			return "STATE_CONFLICT", nil
		}
	case ReleasePayout:
		r := cmd.Release
		fenced := false
		if r.Reason == "FINAL_FAILURE" {
			if r.Evidence == nil || !s.trust.evidence(cmd.BookID, before, *r.Evidence, now) || r.Evidence.Claims.FinalState != "FINAL_FAILURE" {
				return "INVALID_EVIDENCE", nil
			}
			p.Evidence = &r.Evidence.Claims
			fenced = true
			if before.State.State != domain.Exposed {
				return "STATE_CONFLICT", nil
			}
		} else {
			if before.State.State == domain.Exposed {
				return "EXPOSED_REQUIRES_EVIDENCE", nil
			}
			if r.Reason == "EXPIRY" && now.Before(before.State.ExpiresAt) {
				return "NOT_EXPIRED", nil
			}
		}
		after.State, err = before.State.Release(ref.ExpectedVersion, fenced)
		if err != nil {
			return "STATE_CONFLICT", nil
		}
	}
	p.Hold = &Change[Hold]{Before: &before, After: after}
	claim := *st.Claim
	claim.State = string(after.State.State)
	claim.ResultOperationID = cmd.OperationID
	p.Claim = &Change[Claim]{Before: st.Claim, After: claim}
	if cmd.Kind == MarkHoldExposed {
		return "", nil
	}
	total, err := before.Terms.Principal.Add(before.Terms.Fee)
	if err != nil {
		return "", ErrIntegrity
	}
	capture := cmd.Kind == CapturePayout
	for _, entry := range []struct {
		id     domain.ID
		amount domain.Amount
		side   domain.Side
	}{{before.Terms.SourceID, total, domain.Debit}, {before.PoolID, before.Terms.Principal, domain.Credit}} {
		old, ok := st.Balances[entry.id]
		if !ok {
			return "", ErrIntegrity
		}
		next := old
		next.Totals, err = old.Totals.Release(entry.amount)
		if err != nil {
			return "", ErrIntegrity
		}
		if capture {
			next.Totals, err = next.Totals.Post(st.Accounts[entry.id].Purpose, entry.side, entry.amount)
			if err != nil {
				return "", ErrIntegrity
			}
		}
		next.Version, err = domain.NextVersion(old.Version)
		if err != nil {
			return "", ErrIntegrity
		}
		p.Balances = append(p.Balances, Change[Balance]{Before: &old, After: next})
	}
	if st.Usage == nil || st.Usage.OwnerID != before.OwnerID || st.Usage.Bucket != before.Bucket {
		return "", ErrIntegrity
	}
	u := *st.Usage
	u.Values, err = u.Values.Resolve(before.Terms.Principal, capture)
	if err != nil {
		return "", ErrIntegrity
	}
	u.Version, err = domain.NextVersion(u.Version)
	if err != nil {
		return "", ErrIntegrity
	}
	p.Usage = &Change[Usage]{Before: st.Usage, After: u}
	if capture {
		source, pool, fee := st.Accounts[before.Terms.SourceID], st.Accounts[before.PoolID], st.Accounts[before.FeeAccountID]
		lines := []domain.Line{{Account: source, Side: domain.Debit, Amount: total}, {Account: pool, Side: domain.Credit, Amount: before.Terms.Principal}}
		if !before.Terms.Fee.IsZero() {
			lines = append(lines, domain.Line{Account: fee, Side: domain.Credit, Amount: before.Terms.Fee})
			old, ok := st.Balances[fee.ID]
			if !ok {
				return "", ErrIntegrity
			}
			next := old
			next.Totals, err = old.Totals.Post(fee.Purpose, domain.Credit, before.Terms.Fee)
			if err != nil {
				return "", ErrIntegrity
			}
			next.Version, err = domain.NextVersion(old.Version)
			if err != nil {
				return "", ErrIntegrity
			}
			p.Balances = append(p.Balances, Change[Balance]{Before: &old, After: next})
		}
		j, err := domain.NewJournal(st.Book.Book, lines)
		if err != nil {
			return "", ErrIntegrity
		}
		p.Journal = &j
	}
	return "", nil
}
