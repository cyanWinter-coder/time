// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rate provides a rate limiter.
package rate

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
const Inf = Limit(math.MaxFloat64)

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

// A Limiter controls how frequently events are allowed to happen.
// It implements a "token bucket" of size b, initially full and refilled
// at rate r tokens per second.
// Informally, in any large enough time interval, the Limiter limits the
// rate to r tokens per second, with a maximum burst size of b events.
// As a special case, if r == Inf (the infinite rate), b is ignored.
// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
//
// The zero value is a valid Limiter, but it will reject all events.
// Use NewLimiter to create non-zero Limiters.
//
// Limiter has three main methods, Allow, Reserve, and Wait.
// Most callers should use Wait.
//
// Each of the three methods consumes a single token.
// They differ in their behavior when no token is available.
// If no token is available, Allow returns false.
// If no token is available, Reserve returns a reservation for a future token
// and the amount of time the caller must wait before using it.
// If no token is available, Wait blocks until one can be obtained
// or its associated context.Context is canceled.
//
// The methods AllowN, ReserveN, and WaitN consume n tokens.
//
// Limiter is safe for simultaneous use by multiple goroutines.
//
// A Reservation returned by ReserveN can be revoked before the reserved
// action happens. Revoke and RevokeAt report why a revocation did or did
// not return tokens; see RevocationStatus.
type Limiter struct {
	mu     sync.Mutex
	limit  Limit
	burst  int
	tokens float64
	// last is the last time the limiter's tokens field was updated
	last time.Time
	// lastEvent is the latest time of a rate-limited event (past or future)
	lastEvent time.Time
	// generation identifies the current configuration of the bucket. It is
	// captured by each Reservation and advanced by SetLimitAt and SetBurstAt
	// so that a reservation made against an old configuration cannot revoke
	// tokens from the current one.
	generation uint64
}

// Limit returns the maximum overall event rate.
func (lim *Limiter) Limit() Limit {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

// Burst returns the maximum burst size. Burst is the maximum number of tokens
// that can be consumed in a single call to Allow, Reserve, or Wait, so higher
// Burst values allow more events to happen at once.
// A zero Burst allows no events, unless limit == Inf.
func (lim *Limiter) Burst() int {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.burst
}

// Generation returns the limiter's current configuration generation.
// It advances whenever SetLimitAt or SetBurstAt is called. A Reservation
// captures the generation that was current when it was created; see
// Reservation.Generation.
func (lim *Limiter) Generation() uint64 {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.generation
}

// TokensAt returns the number of tokens available at time t.
func (lim *Limiter) TokensAt(t time.Time) float64 {
	lim.mu.Lock()
	tokens := lim.advance(t) // does not mutate lim
	lim.mu.Unlock()
	return tokens
}

// Tokens returns the number of tokens available now.
func (lim *Limiter) Tokens() float64 {
	return lim.TokensAt(time.Now())
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiter(r Limit, b int) *Limiter {
	return &Limiter{
		limit:      r,
		burst:      b,
		tokens:     float64(b),
		generation: 1,
	}
}

// Allow reports whether an event may happen now.
func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

// AllowN reports whether n events may happen at time t.
// Use this method if you intend to drop / skip events that exceed the rate limit.
// Otherwise use Reserve or Wait.
func (lim *Limiter) AllowN(t time.Time, n int) bool {
	return lim.reserveN(t, n, 0).ok
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
//
// A Reservation remembers the number of tokens it reserved and the limiter
// generation in effect when it was created. Revocation returns only the tokens
// that have neither matured nor been claimed by later reservations, and reports
// the outcome as a Revocation.
type Reservation struct {
	ok        bool
	lim       *Limiter
	tokens    int
	timeToAct time.Time
	// This is the Limit at reservation time, it can change later.
	limit Limit
	// generation is the Limiter generation at reservation time. Canceling a
	// reservation from an older generation must not modify the current bucket.
	generation uint64
	// revoked records that the reservation has reached a terminal state.
	// It is guarded by lim.mu.
	revoked bool
}

// OK returns whether the limiter can provide the requested number of tokens
// within the maximum wait time.  If OK is false, Delay returns InfDuration, and
// Cancel does nothing.
func (r *Reservation) OK() bool {
	return r.ok
}

// Generation returns the limiter generation that was current when the
// reservation was created.
func (r *Reservation) Generation() uint64 {
	return r.generation
}

// Tokens returns the number of tokens held by the reservation. It returns
// zero if the reservation was not granted.
func (r *Reservation) Tokens() int {
	return r.tokens
}

// Delay is shorthand for DelayFrom(time.Now()).
func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// InfDuration is the duration returned by Delay when a Reservation is not OK.
const InfDuration = time.Duration(math.MaxInt64)

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
// InfDuration means the limiter cannot grant the tokens requested in this
// Reservation within the maximum wait time.
func (r *Reservation) DelayFrom(t time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(t)
	if delay < 0 {
		return 0
	}
	return delay
}

// Cancel is shorthand for CancelAt(time.Now()).
func (r *Reservation) Cancel() {
	r.CancelAt(time.Now())
}

// CancelAt indicates that the reservation holder will not perform the reserved
// action and reverses the effects of this Reservation on the rate limit as much
// as possible, considering that other reservations may have already been made.
//
// CancelAt is shorthand for RevokeAt, discarding the result. It is idempotent
// and generation-aware: a reservation created before SetLimitAt or SetBurstAt
// no longer matches the limiter's generation, so canceling it leaves the
// current bucket untouched. Call RevokeAt directly when the caller needs to
// know why a cancellation returned no tokens.
func (r *Reservation) CancelAt(t time.Time) {
	r.RevokeAt(t)
}

// RevocationStatus describes the outcome of a call to Revoke or RevokeAt.
type RevocationStatus int

const (
	// RevocationInvalid means the reservation was never granted (OK reports
	// false), so there was nothing to revoke.
	RevocationInvalid RevocationStatus = iota
	// RevocationStale means the reservation belongs to an older limiter
	// generation. The current bucket is left untouched.
	RevocationStale
	// RevocationExpired means the reserved action time has already passed at
	// the revocation time. No tokens are returned.
	RevocationExpired
	// RevocationConsumed means the reserved tokens were already claimed by a
	// later reservation (or by the passage of time combined with later
	// reservations), so no more tokens are available to return.
	RevocationConsumed
	// RevocationPartial means some, but not all, of the reserved tokens were
	// returned to the bucket because later reservations had already claimed
	// the rest.
	RevocationPartial
	// RevocationFull means all reserved tokens were returned to the bucket.
	RevocationFull
	// RevocationRepeated means this reservation was already revoked. The
	// repeated call has no effect on the bucket.
	RevocationRepeated
)

// String returns a human-readable name for the status.
func (s RevocationStatus) String() string {
	switch s {
	case RevocationInvalid:
		return "invalid"
	case RevocationStale:
		return "stale generation"
	case RevocationExpired:
		return "expired"
	case RevocationConsumed:
		return "already consumed"
	case RevocationPartial:
		return "partially revoked"
	case RevocationFull:
		return "revoked"
	case RevocationRepeated:
		return "already revoked"
	default:
		return fmt.Sprintf("RevocationStatus(%d)", int(s))
	}
}

// A Revocation reports the result of revoking a Reservation.
type Revocation struct {
	// Status is the terminal state of the revocation attempt.
	Status RevocationStatus
	// Returned is the number of tokens the limiter actually gained. It can be
	// smaller than the number held by the reservation when later reservations
	// already claimed part of them, or when the bucket was already at its
	// burst limit. It is truncated when the exact refund is fractional.
	Returned int
}

// Revoked reports whether the reservation reached a terminal state that leaves
// it unusable: it was revoked, had expired, or its tokens were consumed.
func (v Revocation) Revoked() bool {
	switch v.Status {
	case RevocationPartial, RevocationFull, RevocationRepeated:
		return true
	case RevocationExpired, RevocationConsumed:
		return true
	default:
		return false
	}
}

// Revoke is shorthand for RevokeAt(time.Now()).
func (r *Reservation) Revoke() Revocation {
	return r.RevokeAt(time.Now())
}

// RevokeAt indicates that the reservation holder will not perform the reserved
// action and returns any tokens that are still unused to the limiter.
//
// Only tokens that have neither matured nor been claimed by later reservations
// are returned; a revocation itself never creates extra tokens, and the bucket
// is never credited beyond the reservation's original size. RevokeAt is
// idempotent: a second call on the same Reservation reports RevocationRepeated
// and does not change the bucket.
//
// A reservation created against an older limiter generation reports
// RevocationStale without modifying the current bucket. A reservation whose
// action time has passed reports RevocationExpired; one whose tokens have been
// claimed by later reservations reports RevocationConsumed. Once revoked, a
// reservation stays terminal even if RevokeAt is called again at an earlier
// time.
//
// RevokeAt is safe to call concurrently with other Limiter methods.
func (r *Reservation) RevokeAt(t time.Time) Revocation {
	if !r.ok || r.lim == nil {
		return Revocation{Status: RevocationInvalid}
	}

	lim := r.lim
	lim.mu.Lock()
	defer lim.mu.Unlock()

	if r.revoked {
		return Revocation{Status: RevocationRepeated}
	}
	if r.generation != lim.generation {
		return Revocation{Status: RevocationStale}
	}
	if lim.limit == Inf || r.tokens == 0 {
		// An infinite limiter has no bucket to refill. Treat a zero-token
		// reservation as a successful no-op revocation so repeated calls are
		// still reported as repeated.
		r.revoked = true
		return Revocation{Status: RevocationFull}
	}

	// Revoking after the action time cannot give anything back: the reserved
	// tokens were already available for use. Canceling exactly at the action
	// time is still honored, matching the historical CancelAt behavior.
	if r.timeToAct.Before(t) {
		r.revoked = true
		return Revocation{Status: RevocationExpired}
	}

	// The duration between lim.lastEvent and r.timeToAct tells us how many
	// tokens were reserved after r was obtained. These tokens should not be
	// restored.
	restoreTokens := float64(r.tokens) - r.limit.tokensFromDuration(lim.lastEvent.Sub(r.timeToAct))
	if restoreTokens <= 0 {
		r.revoked = true
		return Revocation{Status: RevocationConsumed}
	}

	// Advance time to the revocation instant, then return the still-unused
	// tokens. Only the refund is bounded; the bucket itself may legitimately
	// carry a negative balance from other outstanding reservations. Record the
	// actual change so Returned reflects what the bucket really received.
	tokens := lim.advance(t)
	before := tokens
	tokens += restoreTokens
	if burst := float64(lim.burst); tokens > burst {
		tokens = burst
	}

	// Update state.
	lim.last = t
	lim.tokens = tokens
	if r.timeToAct.Equal(lim.lastEvent) {
		prevEvent := r.timeToAct.Add(r.limit.durationFromTokens(float64(-r.tokens)))
		if !prevEvent.Before(t) {
			lim.lastEvent = prevEvent
		}
	}
	r.revoked = true

	if restoreTokens > float64(r.tokens) {
		restoreTokens = float64(r.tokens)
	}
	status := RevocationPartial
	if restoreTokens >= float64(r.tokens) {
		status = RevocationFull
	}
	return Revocation{Status: status, Returned: int(tokens - before)}
}

// Reserve is shorthand for ReserveN(time.Now(), 1).
func (lim *Limiter) Reserve() *Reservation {
	return lim.ReserveN(time.Now(), 1)
}

// ReserveN returns a Reservation that indicates how long the caller must wait before n events happen.
// The Limiter takes this Reservation into account when allowing future events.
// The returned Reservation’s OK() method returns false if n exceeds the Limiter's burst size.
// Usage example:
//
//	r := lim.ReserveN(time.Now(), 1)
//	if !r.OK() {
//	  // Not allowed to act! Did you remember to set lim.burst to be > 0 ?
//	  return
//	}
//	time.Sleep(r.Delay())
//	Act()
//
// Use this method if you wish to wait and slow down in accordance with the rate limit without dropping events.
// If you need to respect a deadline or cancel the delay, use Wait instead.
// To drop or skip events exceeding rate limit, use Allow instead.
func (lim *Limiter) ReserveN(t time.Time, n int) *Reservation {
	r := lim.reserveN(t, n, InfDuration)
	return &r
}

// Wait is shorthand for WaitN(ctx, 1).
func (lim *Limiter) Wait(ctx context.Context) (err error) {
	return lim.WaitN(ctx, 1)
}

// WaitN blocks until lim permits n events to happen.
// It returns an error if n exceeds the Limiter's burst size, the Context is
// canceled, or the expected wait time exceeds the Context's Deadline.
// The burst limit is ignored if the rate limit is Inf.
func (lim *Limiter) WaitN(ctx context.Context, n int) (err error) {
	// The test code calls lim.wait with a fake timer generator.
	// This is the real timer generator.
	newTimer := func(d time.Duration) (<-chan time.Time, func() bool, func()) {
		timer := time.NewTimer(d)
		return timer.C, timer.Stop, func() {}
	}

	return lim.wait(ctx, n, time.Now(), newTimer)
}

// wait is the internal implementation of WaitN.
func (lim *Limiter) wait(ctx context.Context, n int, t time.Time, newTimer func(d time.Duration) (<-chan time.Time, func() bool, func())) error {
	lim.mu.Lock()
	burst := lim.burst
	limit := lim.limit
	lim.mu.Unlock()

	if n > burst && limit != Inf {
		return fmt.Errorf("rate: Wait(n=%d) exceeds limiter's burst %d", n, burst)
	}
	// Check if ctx is already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// Determine wait limit
	waitLimit := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(t)
	}
	// Reserve
	r := lim.reserveN(t, n, waitLimit)
	if !r.ok {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", n)
	}
	// Wait if necessary
	delay := r.DelayFrom(t)
	if delay == 0 {
		return nil
	}
	ch, stop, advance := newTimer(delay)
	defer stop()
	advance() // only has an effect when testing
	select {
	case <-ch:
		// We can proceed.
		return nil
	case <-ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservation, which may permit other events to proceed sooner.
		r.Cancel()
		return ctx.Err()
	}
}

// SetLimit is shorthand for SetLimitAt(time.Now(), newLimit).
func (lim *Limiter) SetLimit(newLimit Limit) {
	lim.SetLimitAt(time.Now(), newLimit)
}

// SetLimitAt sets a new Limit for the limiter. The new Limit, and Burst, may be violated
// or underutilized by those which reserved (using Reserve or Wait) but did not yet act
// before SetLimitAt was called.
func (lim *Limiter) SetLimitAt(t time.Time, newLimit Limit) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	tokens := lim.advance(t)

	lim.last = t
	lim.tokens = tokens
	lim.limit = newLimit
	lim.generation++
}

// SetBurst is shorthand for SetBurstAt(time.Now(), newBurst).
func (lim *Limiter) SetBurst(newBurst int) {
	lim.SetBurstAt(time.Now(), newBurst)
}

// SetBurstAt sets a new burst size for the limiter.
func (lim *Limiter) SetBurstAt(t time.Time, newBurst int) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	tokens := lim.advance(t)

	lim.last = t
	lim.tokens = tokens
	lim.burst = newBurst
	lim.generation++
}

// reserveN is a helper method for AllowN, ReserveN, and WaitN.
// maxFutureReserve specifies the maximum reservation wait duration allowed.
// reserveN returns Reservation, not *Reservation, to avoid allocation in AllowN and WaitN.
func (lim *Limiter) reserveN(t time.Time, n int, maxFutureReserve time.Duration) Reservation {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	if lim.limit == Inf {
		return Reservation{
			ok:         true,
			lim:        lim,
			tokens:     n,
			timeToAct:  t,
			generation: lim.generation,
		}
	}

	tokens := lim.advance(t)

	// Calculate the remaining number of tokens resulting from the request.
	tokens -= float64(n)

	// Calculate the wait duration
	var waitDuration time.Duration
	if tokens < 0 {
		waitDuration = lim.limit.durationFromTokens(-tokens)
	}

	// Decide result
	ok := n <= lim.burst && waitDuration <= maxFutureReserve

	// Prepare reservation
	r := Reservation{
		ok:         ok,
		lim:        lim,
		limit:      lim.limit,
		generation: lim.generation,
	}
	if ok {
		r.tokens = n
		r.timeToAct = t.Add(waitDuration)

		// Update state
		lim.last = t
		lim.tokens = tokens
		lim.lastEvent = r.timeToAct
	}

	return r
}

// advance calculates and returns an updated number of tokens for lim
// resulting from the passage of time.
// lim is not changed.
// advance requires that lim.mu is held.
func (lim *Limiter) advance(t time.Time) (newTokens float64) {
	last := lim.last
	if t.Before(last) {
		last = t
	}

	// Calculate the new number of tokens, due to time that passed.
	elapsed := t.Sub(last)
	delta := lim.limit.tokensFromDuration(elapsed)
	tokens := lim.tokens + delta
	if burst := float64(lim.burst); tokens > burst {
		tokens = burst
	}
	return tokens
}

// durationFromTokens is a unit conversion function from the number of tokens to the duration
// of time it takes to accumulate them at a rate of limit tokens per second.
func (limit Limit) durationFromTokens(tokens float64) time.Duration {
	if limit <= 0 {
		return InfDuration
	}

	duration := (tokens / float64(limit)) * float64(time.Second)

	// Cap the duration to the maximum representable int64 value, to avoid overflow.
	if duration > float64(math.MaxInt64) {
		return InfDuration
	}

	return time.Duration(duration)
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of limit tokens per second.
func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	if limit <= 0 {
		return 0
	}
	return d.Seconds() * float64(limit)
}
