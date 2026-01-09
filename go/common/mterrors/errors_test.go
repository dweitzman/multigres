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

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

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
