// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mterrors

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// customError is a test helper that implements both error and slog.LogValuer.
// Used to test wrapping of non-mterrors errors that have LogValuer implementations.
type customError struct {
	message string
	attrs   []slog.Attr
}

func (e *customError) Error() string {
	return e.message
}

func (e *customError) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, len(e.attrs)+1)
	attrs = append(attrs, slog.String("message", e.message))
	attrs = append(attrs, e.attrs...)
	return slog.GroupValue(attrs...)
}

func TestWrapNil(t *testing.T) {
	got := Wrap(nil, "no error")
	if got != nil {
		t.Errorf("Wrap(nil, \"no error\"): got %#v, expected nil", got)
	}
}

func TestWrap(t *testing.T) {
	tests := []struct {
		err         error
		message     string
		wantMessage string
		wantCode    mtrpcpb.Code
	}{
		{io.EOF, "read error", "read error: EOF", mtrpcpb.Code_UNKNOWN},
		{New(mtrpcpb.Code_ALREADY_EXISTS, "oops"), "client error", "client error: oops", mtrpcpb.Code_ALREADY_EXISTS},
	}

	for _, tt := range tests {
		got := Wrap(tt.err, tt.message)
		if got.Error() != tt.wantMessage {
			t.Errorf("Wrap(%v, %q): got: [%v], want [%v]", tt.err, tt.message, got, tt.wantMessage)
		}
		if Code(got) != tt.wantCode {
			t.Errorf("Wrap(%v, %v): got: [%v], want [%v]", tt.err, tt, Code(got), tt.wantCode)
		}
	}
}

func TestUnwrap(t *testing.T) {
	tests := []struct {
		err       error
		isWrapped bool
	}{
		{fmt.Errorf("some error: %d", 17), false},
		{errors.New("some new error"), false},
		{Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "some msg %d", 19), false},
		{Wrapf(errors.New("some wrapped error"), "some msg"), true},
		{nil, false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.err), func(t *testing.T) {
			{
				wasWrapped, unwrapped := Unwrap(tt.err)
				assert.Equal(t, tt.isWrapped, wasWrapped)
				if !wasWrapped {
					assert.Equal(t, tt.err, unwrapped)
				}
			}
			{
				wrapped := Wrap(tt.err, "some message")
				wasWrapped, unwrapped := Unwrap(wrapped)
				assert.Equal(t, wasWrapped, (tt.err != nil))
				assert.Equal(t, tt.err, unwrapped)
			}
		})
	}
}

func TestUnwrapAll(t *testing.T) {
	tests := []struct {
		err error
	}{
		{fmt.Errorf("some error: %d", 17)},
		{errors.New("some new error")},
		{Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "some msg %d", 19)},
		{nil},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.err), func(t *testing.T) {
			{
				// see that unwrapping a non-wrapped error just returns the same error
				unwrapped := UnwrapAll(tt.err)
				assert.Equal(t, tt.err, unwrapped)
			}
			{
				// see that unwrapping a 5-times wrapped error returns the original error
				wrapped := tt.err
				for range rand.Perm(5) {
					wrapped = Wrap(wrapped, "some message")
				}
				unwrapped := UnwrapAll(wrapped)
				assert.Equal(t, tt.err, unwrapped)
			}
		})
	}
}

type nilError struct{}

func (nilError) Error() string { return "nil error" }

func TestRootCause(t *testing.T) {
	x := New(mtrpcpb.Code_FAILED_PRECONDITION, "error")
	tests := []struct {
		err  error
		want error
	}{{
		// nil error is nil
		err:  nil,
		want: nil,
	}, {
		// explicit nil error is nil
		err:  (error)(nil),
		want: nil,
	}, {
		// typed nil is nil
		err:  (*nilError)(nil),
		want: (*nilError)(nil),
	}, {
		// uncaused error is unaffected
		err:  io.EOF,
		want: io.EOF,
	}, {
		// caused error returns cause
		err:  Wrap(io.EOF, "ignored"),
		want: io.EOF,
	}, {
		err:  x, // return from errors.New
		want: x,
	}}

	for i, tt := range tests {
		got := RootCause(tt.err)
		if got != tt.want {
			t.Errorf("test %d: got %#v, want %#v", i+1, got, tt.want)
		}
	}
}

func TestCause(t *testing.T) {
	x := New(mtrpcpb.Code_FAILED_PRECONDITION, "error")
	tests := []struct {
		err  error
		want error
	}{{
		// nil error is nil
		err:  nil,
		want: nil,
	}, {
		// uncaused error is nil
		err:  io.EOF,
		want: nil,
	}, {
		// caused error returns cause
		err:  Wrap(io.EOF, "ignored"),
		want: io.EOF,
	}, {
		err:  x, // return from errors.New
		want: nil,
	}}

	for i, tt := range tests {
		got := Cause(tt.err)
		if got != tt.want {
			t.Errorf("test %d: got %#v, want %#v", i+1, got, tt.want)
		}
	}
}

func TestWrapfNil(t *testing.T) {
	got := Wrapf(nil, "no error")
	if got != nil {
		t.Errorf("Wrapf(nil, \"no error\"): got %#v, expected nil", got)
	}
}

func TestWrapf(t *testing.T) {
	tests := []struct {
		err     error
		message string
		want    string
	}{
		{io.EOF, "read error", "read error: EOF"},
		{Wrapf(io.EOF, "read error without format specifiers"), "client error", "client error: read error without format specifiers: EOF"},
		{Wrapf(io.EOF, "read error with %d format specifier", 1), "client error", "client error: read error with 1 format specifier: EOF"},
	}

	for _, tt := range tests {
		got := Wrap(tt.err, tt.message).Error()
		if got != tt.want {
			t.Errorf("Wrapf(%v, %q): got: %v, want %v", tt.err, tt.message, got, tt.want)
		}
	}
}

func TestErrorf(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{Errorf(mtrpcpb.Code_DATA_LOSS, "read error without format specifiers"), "read error without format specifiers"},
		{Errorf(mtrpcpb.Code_DATA_LOSS, "read error with %d format specifier", 1), "read error with 1 format specifier"},
	}

	for _, tt := range tests {
		got := tt.err.Error()
		if got != tt.want {
			t.Errorf("Errorf(%v): got: %q, want %q", tt.err, got, tt.want)
		}
	}
}

func innerMost() error {
	return Wrap(io.ErrNoProgress, "oh noes")
}

func middle() error {
	return innerMost()
}

func outer() error {
	return middle()
}

func TestStackFormat(t *testing.T) {
	err := outer()
	got := fmt.Sprintf("%v", err)

	assertContains(t, got, "innerMost", false)
	assertContains(t, got, "middle", false)
	assertContains(t, got, "outer", false)

	setLogErrStacks(true)
	defer func() { setLogErrStacks(false) }()
	got = fmt.Sprintf("%v", err)
	assertContains(t, got, "innerMost", true)
	assertContains(t, got, "middle", true)
	assertContains(t, got, "outer", true)
}

// errors.New, etc values are not expected to be compared by value
// but the change in errors#27 made them incomparable. Assert that
// various kinds of errors have a functional equality operator, even
// if the result of that equality is always false.
func TestErrorEquality(t *testing.T) {
	vals := []error{
		nil,
		io.EOF,
		errors.New("EOF"),
		New(mtrpcpb.Code_ALREADY_EXISTS, "EOF"),
		Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "EOF"),
		Wrap(io.EOF, "EOF"),
		Wrapf(io.EOF, "EOF%d", 2),
	}

	for i := range vals {
		for j := range vals {
			_ = vals[i] == vals[j] // mustn't panic
		}
	}
}

func TestCreation(t *testing.T) {
	testcases := []struct {
		in, want mtrpcpb.Code
	}{{
		in:   mtrpcpb.Code_CANCELED,
		want: mtrpcpb.Code_CANCELED,
	}, {
		in:   mtrpcpb.Code_UNKNOWN,
		want: mtrpcpb.Code_UNKNOWN,
	}}
	for _, tcase := range testcases {
		if got := Code(New(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(New(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
		if got := Code(Errorf(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(Errorf(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestCode(t *testing.T) {
	testcases := []struct {
		in   error
		want mtrpcpb.Code
	}{{
		in:   nil,
		want: mtrpcpb.Code_OK,
	}, {
		in:   errors.New("generic"),
		want: mtrpcpb.Code_UNKNOWN,
	}, {
		in:   New(mtrpcpb.Code_CANCELED, "generic"),
		want: mtrpcpb.Code_CANCELED,
	}, {
		in:   context.Canceled,
		want: mtrpcpb.Code_CANCELED,
	}, {
		in:   context.DeadlineExceeded,
		want: mtrpcpb.Code_DEADLINE_EXCEEDED,
	}}
	for _, tcase := range testcases {
		if got := Code(tcase.in); got != tcase.want {
			t.Errorf("Code(%v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestWrapping(t *testing.T) {
	err1 := Errorf(mtrpcpb.Code_UNAVAILABLE, "foo")
	err2 := Wrapf(err1, "bar")
	err3 := Wrapf(err2, "baz")
	errorWithoutStack := fmt.Sprintf("%v", err3)

	setLogErrStacks(true)
	errorWithStack := fmt.Sprintf("%v", err3)
	setLogErrStacks(false)

	assertEquals(t, err3.Error(), "baz: bar: foo")
	assertContains(t, errorWithoutStack, "foo", true)
	assertContains(t, errorWithoutStack, "bar", true)
	assertContains(t, errorWithoutStack, "baz", true)
	assertContains(t, errorWithoutStack, "TestWrapping", false)

	assertContains(t, errorWithStack, "foo", true)
	assertContains(t, errorWithStack, "bar", true)
	assertContains(t, errorWithStack, "baz", true)
	assertContains(t, errorWithStack, "TestWrapping", true)
}

func assertContains(t *testing.T, s, substring string, contains bool) {
	t.Helper()
	if doesContain := strings.Contains(s, substring); doesContain != contains {
		t.Errorf("string `%v` contains `%v`: %v, want %v", s, substring, doesContain, contains)
	}
}

func assertEquals(t *testing.T, a, b any) {
	if a != b {
		t.Fatalf("expected [%s] to be equal to [%s]", a, b)
	}
}

func TestWrapWithAttrs(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		message     string
		attrs       []slog.Attr
		wantMessage string
		wantAttrs   []slog.Attr
	}{
		{
			name:        "nil error returns nil",
			err:         nil,
			message:     "test",
			attrs:       []slog.Attr{slog.String("key", "value")},
			wantMessage: "",
			wantAttrs:   nil,
		},
		{
			name:        "simple key-value pair",
			err:         io.EOF,
			message:     "read error",
			attrs:       []slog.Attr{slog.String("query", "SELECT *")},
			wantMessage: "read error: EOF",
			wantAttrs:   []slog.Attr{slog.String("query", "SELECT *")},
		},
		{
			name:    "multiple attributes",
			err:     errors.New("base error"),
			message: "operation failed",
			attrs: []slog.Attr{
				slog.String("query", "SELECT *"),
				slog.Int("attempt", 3),
				slog.Bool("timeout", true),
			},
			wantMessage: "operation failed: base error",
			wantAttrs: []slog.Attr{
				slog.String("query", "SELECT *"),
				slog.Int("attempt", 3),
				slog.Bool("timeout", true),
			},
		},
		{
			name:        "empty attrs",
			err:         io.EOF,
			message:     "test",
			attrs:       []slog.Attr{},
			wantMessage: "test: EOF",
			wantAttrs:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := WrapWithAttrs(tt.err, tt.message, tt.attrs...)

			if tt.err == nil {
				assert.Nil(t, got)
				return
			}

			assert.Equal(t, tt.wantMessage, got.Error())

			// Check attributes
			attrs := Attrs(got)
			assert.Equal(t, tt.wantAttrs, attrs)
		})
	}
}

func TestWrapWithKVs(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		message     string
		kvs         []any
		wantMessage string
		wantAttrs   []slog.Attr
		wantPanic   bool
	}{
		{
			name:        "nil error returns nil",
			err:         nil,
			message:     "test",
			kvs:         []any{"key", "value"},
			wantMessage: "",
			wantAttrs:   nil,
			wantPanic:   false,
		},
		{
			name:        "simple key-value pair",
			err:         io.EOF,
			message:     "read error",
			kvs:         []any{"query", "SELECT *"},
			wantMessage: "read error: EOF",
			wantAttrs:   []slog.Attr{slog.Any("query", "SELECT *")},
			wantPanic:   false,
		},
		{
			name:        "multiple key-value pairs",
			err:         errors.New("base error"),
			message:     "operation failed",
			kvs:         []any{"query", "SELECT *", "attempt", 3, "timeout", true},
			wantMessage: "operation failed: base error",
			wantAttrs: []slog.Attr{
				slog.Any("query", "SELECT *"),
				slog.Any("attempt", 3),
				slog.Any("timeout", true),
			},
			wantPanic: false,
		},
		{
			name:      "odd number of args panics",
			err:       io.EOF,
			message:   "test",
			kvs:       []any{"key"},
			wantPanic: true,
		},
		{
			name:      "non-string key panics",
			err:       io.EOF,
			message:   "test",
			kvs:       []any{123, "value"},
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				assert.Panics(t, func() {
					//nolint:errcheck // testing panic, not return value
					WrapWithKVs(tt.err, tt.message, tt.kvs...)
				})
				return
			}

			got := WrapWithKVs(tt.err, tt.message, tt.kvs...)

			if tt.err == nil {
				assert.Nil(t, got)
				return
			}

			assert.Equal(t, tt.wantMessage, got.Error())

			// Check attributes
			attrs := Attrs(got)
			assert.Equal(t, tt.wantAttrs, attrs)
		})
	}
}

func TestAttrs(t *testing.T) {
	tests := []struct {
		name      string
		buildErr  func() error
		wantAttrs []slog.Attr
	}{
		{
			name:      "nil error returns nil",
			buildErr:  func() error { return nil },
			wantAttrs: nil,
		},
		{
			name:      "regular error without attrs",
			buildErr:  func() error { return errors.New("test") },
			wantAttrs: nil,
		},
		{
			name: "error with attrs",
			buildErr: func() error {
				return WrapWithAttrs(io.EOF, "test", slog.String("key", "value"))
			},
			wantAttrs: []slog.Attr{slog.String("key", "value")},
		},
		{
			name: "nested errors collect all attrs",
			buildErr: func() error {
				err := io.EOF
				err = WrapWithAttrs(err, "inner", slog.Int("attempt", 1))
				err = WrapWithAttrs(err, "outer", slog.String("query", "SELECT *"))
				return err
			},
			wantAttrs: []slog.Attr{
				slog.String("query", "SELECT *"),
				slog.Int("attempt", 1),
			},
		},
		{
			name: "multiple wraps with same keys (outer first)",
			buildErr: func() error {
				err := io.EOF
				err = WrapWithAttrs(err, "inner", slog.String("status", "old"))
				err = WrapWithAttrs(err, "outer", slog.String("status", "new"))
				return err
			},
			wantAttrs: []slog.Attr{
				slog.String("status", "new"),
				slog.String("status", "old"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.buildErr()
			attrs := Attrs(err)
			assert.Equal(t, tt.wantAttrs, attrs)
		})
	}
}

func TestWrapWithAttrsChaining(t *testing.T) {
	baseErr := errors.New("database connection failed")
	wrappedOnce := WrapWithAttrs(baseErr, "query failed",
		slog.String("query", "SELECT * FROM users"))
	wrappedTwice := WrapWithAttrs(wrappedOnce, "request failed",
		slog.String("endpoint", "/api/users"),
		slog.String("method", "GET"))

	// Check error message
	expected := "request failed: query failed: database connection failed"
	assert.Equal(t, expected, wrappedTwice.Error())

	// Check all attributes are collected
	attrs := Attrs(wrappedTwice)
	expectedAttrs := []slog.Attr{
		slog.String("endpoint", "/api/users"),
		slog.String("method", "GET"),
		slog.String("query", "SELECT * FROM users"),
	}
	assert.Equal(t, expectedAttrs, attrs)

	// Check that RootCause still works
	assert.Equal(t, baseErr, RootCause(wrappedTwice))
}

func TestLogValueIntegration(t *testing.T) {
	tests := []struct {
		name     string
		buildErr func() error
		validate func(t *testing.T, value slog.Value)
	}{
		{
			name: "fundamental error with attrs",
			buildErr: func() error {
				return NewWithAttrs(mtrpcpb.Code_INVALID_ARGUMENT, "invalid query",
					slog.String("query", "SELECT *"),
					slog.Int("line", 42))
			},
			validate: func(t *testing.T, value slog.Value) {
				// Should be a group with message and attributes
				assert.Equal(t, slog.KindGroup, value.Kind())
				attrs := value.Group()
				assert.Len(t, attrs, 3) // message + 2 attrs
				assert.Equal(t, "message", attrs[0].Key)
				assert.Equal(t, "invalid query", attrs[0].Value.String())
				assert.Equal(t, "query", attrs[1].Key)
				assert.Equal(t, "SELECT *", attrs[1].Value.String())
				assert.Equal(t, "line", attrs[2].Key)
				assert.Equal(t, int64(42), attrs[2].Value.Int64())
			},
		},
		{
			name: "wrapped error with attrs",
			buildErr: func() error {
				base := errors.New("connection failed")
				return WrapWithAttrs(base, "query execution failed",
					slog.String("query", "SELECT *"),
					slog.String("database", "postgres"))
			},
			validate: func(t *testing.T, value slog.Value) {
				// Should be a group with message, attributes, and cause
				assert.Equal(t, slog.KindGroup, value.Kind())
				attrs := value.Group()
				assert.Len(t, attrs, 4) // message + 2 attrs + cause
				assert.Equal(t, "message", attrs[0].Key)
				assert.Equal(t, "query execution failed", attrs[0].Value.String())
				assert.Equal(t, "query", attrs[1].Key)
				assert.Equal(t, "SELECT *", attrs[1].Value.String())
				assert.Equal(t, "database", attrs[2].Key)
				assert.Equal(t, "postgres", attrs[2].Value.String())
				assert.Equal(t, "cause", attrs[3].Key)
				assert.Equal(t, "connection failed", attrs[3].Value.String())
			},
		},
		{
			name: "nested wrapped errors with attrs",
			buildErr: func() error {
				base := NewWithAttrs(mtrpcpb.Code_UNAVAILABLE, "connection failed",
					slog.String("host", "localhost"))
				wrapped := WrapWithAttrs(base, "query failed",
					slog.String("query", "SELECT *"))
				return wrapped
			},
			validate: func(t *testing.T, value slog.Value) {
				// Outer error should be a group
				assert.Equal(t, slog.KindGroup, value.Kind())
				attrs := value.Group()
				assert.Len(t, attrs, 3) // message + query + cause
				assert.Equal(t, "message", attrs[0].Key)
				assert.Equal(t, "query failed", attrs[0].Value.String())
				assert.Equal(t, "query", attrs[1].Key)
				assert.Equal(t, "SELECT *", attrs[1].Value.String())
				assert.Equal(t, "cause", attrs[2].Key)

				// Cause should also be a group with its own attributes
				cause := attrs[2].Value
				assert.Equal(t, slog.KindGroup, cause.Kind())
				causeAttrs := cause.Group()
				assert.Len(t, causeAttrs, 2) // message + host
				assert.Equal(t, "message", causeAttrs[0].Key)
				assert.Equal(t, "connection failed", causeAttrs[0].Value.String())
				assert.Equal(t, "host", causeAttrs[1].Key)
				assert.Equal(t, "localhost", causeAttrs[1].Value.String())
			},
		},
		{
			name: "error without attrs returns simple string",
			buildErr: func() error {
				return New(mtrpcpb.Code_INTERNAL, "simple error")
			},
			validate: func(t *testing.T, value slog.Value) {
				assert.Equal(t, slog.KindString, value.Kind())
				assert.Equal(t, "simple error", value.String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.buildErr()

			// Verify the error implements slog.LogValuer
			lv, ok := err.(slog.LogValuer)
			assert.True(t, ok, "error should implement slog.LogValuer")

			// Get the log value
			value := lv.LogValue()

			// Validate the structure
			tt.validate(t, value)
		})
	}
}

func TestLogValueWithCustomErrorWrapping(t *testing.T) {
	// Create a custom error with attributes
	customErr := &customError{
		message: "connection failed",
		attrs: []slog.Attr{
			slog.String("host", "db1.example.com"),
			slog.Int("port", 5432),
		},
	}

	// Wrap it with mterrors adding different attributes
	wrapped := WrapWithAttrs(customErr, "query failed",
		slog.String("operation", "connect"),
		slog.String("timeout", "5s"))

	// Verify the error implements slog.LogValuer
	lv, ok := wrapped.(slog.LogValuer)
	assert.True(t, ok, "wrapped error should implement slog.LogValuer")

	// Get the log value
	value := lv.LogValue()

	// Should be a group with message, attributes, and cause
	assert.Equal(t, slog.KindGroup, value.Kind())
	attrs := value.Group()
	assert.Len(t, attrs, 4) // message + 2 attrs (operation, timeout) + cause

	// Check wrapper's message
	assert.Equal(t, "message", attrs[0].Key)
	assert.Equal(t, "query failed", attrs[0].Value.String())

	// Check wrapper's attributes
	assert.Equal(t, "operation", attrs[1].Key)
	assert.Equal(t, "connect", attrs[1].Value.String())
	assert.Equal(t, "timeout", attrs[2].Key)
	assert.Equal(t, "5s", attrs[2].Value.String())

	// Check cause field
	assert.Equal(t, "cause", attrs[3].Key)

	// Cause should be a group (from customError's LogValue)
	cause := attrs[3].Value
	assert.Equal(t, slog.KindGroup, cause.Kind())
	causeAttrs := cause.Group()
	assert.Len(t, causeAttrs, 3) // message + host + port

	// Check custom error's message
	assert.Equal(t, "message", causeAttrs[0].Key)
	assert.Equal(t, "connection failed", causeAttrs[0].Value.String())

	// Check custom error's attributes
	assert.Equal(t, "host", causeAttrs[1].Key)
	assert.Equal(t, "db1.example.com", causeAttrs[1].Value.String())
	assert.Equal(t, "port", causeAttrs[2].Key)
	assert.Equal(t, int64(5432), causeAttrs[2].Value.Int64())
}

func TestLogValueDeepNesting(t *testing.T) {
	// Build a 4-level error chain
	// Level 1: Custom error (base)
	base := &customError{
		message: "network error",
		attrs: []slog.Attr{
			slog.String("errno", "ECONNREFUSED"),
		},
	}

	// Level 2: First mterrors wrap
	level2 := WrapWithAttrs(base, "connection failed",
		slog.String("host", "db1.example.com"),
		slog.Int("port", 5432))

	// Level 3: Second mterrors wrap
	level3 := WrapWithAttrs(level2, "query execution failed",
		slog.String("query", "SELECT * FROM users"),
		slog.Duration("elapsed", 500))

	// Level 4: Third mterrors wrap (outermost)
	level4 := WrapWithAttrs(level3, "request processing failed",
		slog.String("request_id", "req-123"),
		slog.Int("attempt", 3))

	// Verify the error implements slog.LogValuer
	lv, ok := level4.(slog.LogValuer)
	assert.True(t, ok, "error should implement slog.LogValuer")

	// Get the log value
	value := lv.LogValue()

	// Level 4 (outermost): Should be a group with message, attributes, and cause
	assert.Equal(t, slog.KindGroup, value.Kind())
	attrs := value.Group()
	assert.Len(t, attrs, 4) // message + 2 attrs + cause
	assert.Equal(t, "message", attrs[0].Key)
	assert.Equal(t, "request processing failed", attrs[0].Value.String())
	assert.Equal(t, "request_id", attrs[1].Key)
	assert.Equal(t, "req-123", attrs[1].Value.String())
	assert.Equal(t, "attempt", attrs[2].Key)
	assert.Equal(t, int64(3), attrs[2].Value.Int64())
	assert.Equal(t, "cause", attrs[3].Key)

	// Level 3: Navigate to cause
	level3Value := attrs[3].Value
	assert.Equal(t, slog.KindGroup, level3Value.Kind())
	level3Attrs := level3Value.Group()
	assert.Len(t, level3Attrs, 4) // message + 2 attrs + cause
	assert.Equal(t, "message", level3Attrs[0].Key)
	assert.Equal(t, "query execution failed", level3Attrs[0].Value.String())
	assert.Equal(t, "query", level3Attrs[1].Key)
	assert.Equal(t, "SELECT * FROM users", level3Attrs[1].Value.String())
	assert.Equal(t, "elapsed", level3Attrs[2].Key)
	assert.Equal(t, "cause", level3Attrs[3].Key)

	// Level 2: Navigate to cause
	level2Value := level3Attrs[3].Value
	assert.Equal(t, slog.KindGroup, level2Value.Kind())
	level2Attrs := level2Value.Group()
	assert.Len(t, level2Attrs, 4) // message + 2 attrs + cause
	assert.Equal(t, "message", level2Attrs[0].Key)
	assert.Equal(t, "connection failed", level2Attrs[0].Value.String())
	assert.Equal(t, "host", level2Attrs[1].Key)
	assert.Equal(t, "db1.example.com", level2Attrs[1].Value.String())
	assert.Equal(t, "port", level2Attrs[2].Key)
	assert.Equal(t, int64(5432), level2Attrs[2].Value.Int64())
	assert.Equal(t, "cause", level2Attrs[3].Key)

	// Level 1 (base): Navigate to cause
	level1Value := level2Attrs[3].Value
	assert.Equal(t, slog.KindGroup, level1Value.Kind())
	level1Attrs := level1Value.Group()
	assert.Len(t, level1Attrs, 2) // message + 1 attr
	assert.Equal(t, "message", level1Attrs[0].Key)
	assert.Equal(t, "network error", level1Attrs[0].Value.String())
	assert.Equal(t, "errno", level1Attrs[1].Key)
	assert.Equal(t, "ECONNREFUSED", level1Attrs[1].Value.String())
}

func TestLogValueDuplicateAttributeNames(t *testing.T) {
	// Create a custom error with a "database" attribute
	customErr := &customError{
		message: "connection timeout",
		attrs: []slog.Attr{
			slog.String("database", "inner-db"),
			slog.String("host", "db1.example.com"),
		},
	}

	// Wrap with mterrors also adding a "database" attribute
	wrapped := WrapWithAttrs(customErr, "query failed",
		slog.String("database", "outer-db"),
		slog.String("query", "SELECT 1"))

	// Verify the error implements slog.LogValuer
	lv, ok := wrapped.(slog.LogValuer)
	assert.True(t, ok, "error should implement slog.LogValuer")

	// Get the log value
	value := lv.LogValue()

	// Outer group should have the outer "database" value
	assert.Equal(t, slog.KindGroup, value.Kind())
	attrs := value.Group()
	assert.Len(t, attrs, 4) // message + 2 attrs + cause
	assert.Equal(t, "message", attrs[0].Key)
	assert.Equal(t, "query failed", attrs[0].Value.String())

	// Outer "database" attribute
	assert.Equal(t, "database", attrs[1].Key)
	assert.Equal(t, "outer-db", attrs[1].Value.String())

	assert.Equal(t, "query", attrs[2].Key)
	assert.Equal(t, "SELECT 1", attrs[2].Value.String())
	assert.Equal(t, "cause", attrs[3].Key)

	// Inner group (cause) should have the inner "database" value
	cause := attrs[3].Value
	assert.Equal(t, slog.KindGroup, cause.Kind())
	causeAttrs := cause.Group()
	assert.Len(t, causeAttrs, 3) // message + 2 attrs
	assert.Equal(t, "message", causeAttrs[0].Key)
	assert.Equal(t, "connection timeout", causeAttrs[0].Value.String())

	// Inner "database" attribute (different value)
	assert.Equal(t, "database", causeAttrs[1].Key)
	assert.Equal(t, "inner-db", causeAttrs[1].Value.String())

	assert.Equal(t, "host", causeAttrs[2].Key)
	assert.Equal(t, "db1.example.com", causeAttrs[2].Value.String())

	// Both "database" values exist but at different nesting levels
	// This demonstrates that nesting prevents attribute name collisions
}

func TestLogValueDuplicateAttributesMultipleLevels(t *testing.T) {
	// Build a 3-level chain where each level has an "attempt" attribute with different values
	// Level 1: Base error
	base := &customError{
		message: "network error",
		attrs: []slog.Attr{
			slog.Int("attempt", 1),
			slog.String("detail", "base-detail"),
		},
	}

	// Level 2: First wrap
	level2 := WrapWithAttrs(base, "connection failed",
		slog.Int("attempt", 2),
		slog.String("detail", "level2-detail"))

	// Level 3: Second wrap (outermost)
	level3 := WrapWithAttrs(level2, "query failed",
		slog.Int("attempt", 3),
		slog.String("detail", "level3-detail"))

	// Verify the error implements slog.LogValuer
	lv, ok := level3.(slog.LogValuer)
	assert.True(t, ok, "error should implement slog.LogValuer")

	// Get the log value
	value := lv.LogValue()

	// Level 3 (outermost): Should have attempt=3
	assert.Equal(t, slog.KindGroup, value.Kind())
	level3Attrs := value.Group()
	assert.Len(t, level3Attrs, 4) // message + 2 attrs + cause
	assert.Equal(t, "message", level3Attrs[0].Key)
	assert.Equal(t, "query failed", level3Attrs[0].Value.String())
	assert.Equal(t, "attempt", level3Attrs[1].Key)
	assert.Equal(t, int64(3), level3Attrs[1].Value.Int64())
	assert.Equal(t, "detail", level3Attrs[2].Key)
	assert.Equal(t, "level3-detail", level3Attrs[2].Value.String())
	assert.Equal(t, "cause", level3Attrs[3].Key)

	// Level 2: Should have attempt=2
	level2Value := level3Attrs[3].Value
	assert.Equal(t, slog.KindGroup, level2Value.Kind())
	level2Attrs := level2Value.Group()
	assert.Len(t, level2Attrs, 4) // message + 2 attrs + cause
	assert.Equal(t, "message", level2Attrs[0].Key)
	assert.Equal(t, "connection failed", level2Attrs[0].Value.String())
	assert.Equal(t, "attempt", level2Attrs[1].Key)
	assert.Equal(t, int64(2), level2Attrs[1].Value.Int64())
	assert.Equal(t, "detail", level2Attrs[2].Key)
	assert.Equal(t, "level2-detail", level2Attrs[2].Value.String())
	assert.Equal(t, "cause", level2Attrs[3].Key)

	// Level 1 (base): Should have attempt=1
	level1Value := level2Attrs[3].Value
	assert.Equal(t, slog.KindGroup, level1Value.Kind())
	level1Attrs := level1Value.Group()
	assert.Len(t, level1Attrs, 3) // message + 2 attrs
	assert.Equal(t, "message", level1Attrs[0].Key)
	assert.Equal(t, "network error", level1Attrs[0].Value.String())
	assert.Equal(t, "attempt", level1Attrs[1].Key)
	assert.Equal(t, int64(1), level1Attrs[1].Value.Int64())
	assert.Equal(t, "detail", level1Attrs[2].Key)
	assert.Equal(t, "base-detail", level1Attrs[2].Value.String())

	// Each level maintains its own "attempt" value
	// This demonstrates that duplicate keys across levels don't collide
}

// testHandler is a slog.Handler implementation that captures logged records for testing.
type testHandler struct {
	records []testRecord
}

type testRecord struct {
	level slog.Level
	msg   string
	attrs []slog.Attr
}

func (h *testHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *testHandler) Handle(_ context.Context, r slog.Record) error {
	record := testRecord{
		level: r.Level,
		msg:   r.Message,
		attrs: make([]slog.Attr, 0, r.NumAttrs()),
	}
	r.Attrs(func(a slog.Attr) bool {
		// Resolve LogValuer values
		a.Value = a.Value.Resolve()
		record.attrs = append(record.attrs, a)
		return true
	})
	h.records = append(h.records, record)
	return nil
}

func (h *testHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// For this test, we don't need to implement WithAttrs
	return h
}

func (h *testHandler) WithGroup(name string) slog.Handler {
	// For this test, we don't need to implement WithGroup
	return h
}

func TestSlogIntegrationWithCustomErrors(t *testing.T) {
	tests := []struct {
		name     string
		buildErr func() error
		validate func(t *testing.T, attrs []slog.Attr)
	}{
		{
			name: "custom error wrapped by mterrors",
			buildErr: func() error {
				customErr := &customError{
					message: "connection timeout",
					attrs: []slog.Attr{
						slog.String("host", "db1.example.com"),
						slog.Int("port", 5432),
					},
				}
				return WrapWithAttrs(customErr, "query failed",
					slog.String("query", "SELECT * FROM users"),
					slog.Duration("timeout", 5000))
			},
			validate: func(t *testing.T, attrs []slog.Attr) {
				// Should have an "error" attribute
				require.Len(t, attrs, 1)
				assert.Equal(t, "error", attrs[0].Key)

				// The error attribute should be a group (from LogValue)
				errValue := attrs[0].Value
				assert.Equal(t, slog.KindGroup, errValue.Kind())

				// Check the structure
				errAttrs := errValue.Group()
				require.Len(t, errAttrs, 4) // message + 2 attrs + cause

				// Wrapper level
				assert.Equal(t, "message", errAttrs[0].Key)
				assert.Equal(t, "query failed", errAttrs[0].Value.String())
				assert.Equal(t, "query", errAttrs[1].Key)
				assert.Equal(t, "timeout", errAttrs[2].Key)
				assert.Equal(t, "cause", errAttrs[3].Key)

				// Cause level (customError)
				cause := errAttrs[3].Value
				assert.Equal(t, slog.KindGroup, cause.Kind())
				causeAttrs := cause.Group()
				assert.Len(t, causeAttrs, 3) // message + host + port
				assert.Equal(t, "message", causeAttrs[0].Key)
				assert.Equal(t, "connection timeout", causeAttrs[0].Value.String())
				assert.Equal(t, "host", causeAttrs[1].Key)
				assert.Equal(t, "port", causeAttrs[2].Key)
			},
		},
		{
			name: "deep nesting (3 levels)",
			buildErr: func() error {
				base := &customError{
					message: "network error",
					attrs: []slog.Attr{
						slog.String("errno", "ECONNREFUSED"),
					},
				}
				level2 := WrapWithAttrs(base, "connection failed",
					slog.String("host", "db1.example.com"))
				return WrapWithAttrs(level2, "query failed",
					slog.String("query", "SELECT 1"))
			},
			validate: func(t *testing.T, attrs []slog.Attr) {
				// Navigate through the nested structure
				require.Len(t, attrs, 1)
				assert.Equal(t, "error", attrs[0].Key)

				// Level 3 (outermost)
				level3 := attrs[0].Value
				assert.Equal(t, slog.KindGroup, level3.Kind())
				level3Attrs := level3.Group()
				assert.Len(t, level3Attrs, 3) // message + query + cause
				assert.Equal(t, "query failed", level3Attrs[0].Value.String())
				assert.Equal(t, "query", level3Attrs[1].Key)

				// Level 2
				level2 := level3Attrs[2].Value
				assert.Equal(t, slog.KindGroup, level2.Kind())
				level2Attrs := level2.Group()
				assert.Len(t, level2Attrs, 3) // message + host + cause
				assert.Equal(t, "connection failed", level2Attrs[0].Value.String())
				assert.Equal(t, "host", level2Attrs[1].Key)

				// Level 1 (base)
				level1 := level2Attrs[2].Value
				assert.Equal(t, slog.KindGroup, level1.Kind())
				level1Attrs := level1.Group()
				assert.Len(t, level1Attrs, 2) // message + errno
				assert.Equal(t, "network error", level1Attrs[0].Value.String())
				assert.Equal(t, "errno", level1Attrs[1].Key)
			},
		},
		{
			name: "duplicate attribute names at different levels",
			buildErr: func() error {
				customErr := &customError{
					message: "base error",
					attrs: []slog.Attr{
						slog.String("database", "inner-db"),
					},
				}
				return WrapWithAttrs(customErr, "wrapped error",
					slog.String("database", "outer-db"))
			},
			validate: func(t *testing.T, attrs []slog.Attr) {
				require.Len(t, attrs, 1)
				assert.Equal(t, "error", attrs[0].Key)

				// Outer level has "outer-db"
				outer := attrs[0].Value
				assert.Equal(t, slog.KindGroup, outer.Kind())
				outerAttrs := outer.Group()
				assert.Len(t, outerAttrs, 3) // message + database + cause
				assert.Equal(t, "database", outerAttrs[1].Key)
				assert.Equal(t, "outer-db", outerAttrs[1].Value.String())

				// Inner level has "inner-db"
				inner := outerAttrs[2].Value
				assert.Equal(t, slog.KindGroup, inner.Kind())
				innerAttrs := inner.Group()
				assert.Len(t, innerAttrs, 2) // message + database
				assert.Equal(t, "database", innerAttrs[1].Key)
				assert.Equal(t, "inner-db", innerAttrs[1].Value.String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test handler
			handler := &testHandler{}
			logger := slog.New(handler)

			// Build error
			err := tt.buildErr()

			// Log the error
			logger.Error("operation failed", "error", err)

			// Verify we captured one record
			require.Len(t, handler.records, 1)
			record := handler.records[0]

			// Verify log level and message
			assert.Equal(t, slog.LevelError, record.level)
			assert.Equal(t, "operation failed", record.msg)

			// Validate attributes
			tt.validate(t, record.attrs)
		})
	}
}
