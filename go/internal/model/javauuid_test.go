package model

import "testing"

// Expected values were measured against the original Java service image (OpenJDK 1.8.0_292, Spring,
// Jackson): accepted -> canonical id from the 404 message / Location URL, rejected -> 400.
// Source: migration/integration-reports/2026-11-02-run2 (uuidprobe).

func TestSpringUUIDMatchesJava8(t *testing.T) {
	for _, c := range []struct {
		in, want string
		ok       bool
	}{
		{"00000000-0000-4000-8000-00000000abcd", "00000000-0000-4000-8000-00000000abcd", true},
		{"00000000-0000-4000-8000-00000000ABCD", "00000000-0000-4000-8000-00000000abcd", true},
		{"1-1-1-1-1", "00000001-0001-0001-0001-000000000001", true},
		{"0-0-0-0-0", "00000000-0000-0000-0000-000000000000", true}, // 2nd run: 401 (existing secret)
		{"ffffffff-ffff-ffff-ffff-ffffffffffff", "ffffffff-ffff-ffff-ffff-ffffffffffff", true},
		{"00000001-0001-0001-0001-0000000000001", "00000001-0001-0001-0001-000000000001", true},
		{"1-1-1-1-123456789012345", "00000001-0001-0001-0123-456789012345", true},
		{"1-1-1-1-ffffffffffffffff", "", false},
		{"1-1-1-1-7fffffffffffffff", "00000001-0001-0001-7fff-ffffffffffff", true},
		{"1-10000-1-1-1", "00000001-0000-0001-0001-000000000001", true},
		{"123456789-1-1-1-1", "23456789-0001-0001-0001-000000000001", true},
		{"1-1-1-1-1-", "00000001-0001-0001-0001-000000000001", true},
		{"1-1-1-1-1--", "00000001-0001-0001-0001-000000000001", true},
		{"-1-1-1-1-1", "", false},
		{"+1-1-1-1-1", "", false},
		{"1-+1-1-1-1", "", false},
		{"0x1-1-1-1-1", "", false},
		{" 1-1-1-1-1", "00000001-0001-0001-0001-000000000001", true},
		{"1-1-1-1-1 ", "00000001-0001-0001-0001-000000000001", true},
		{"\t1-1-1-1-1", "00000001-0001-0001-0001-000000000001", true},
		{"1-1-1-1", "", false},
		{"1-1-1-1-1-1", "", false},
		{"1--1-1-1", "", false},
		{"g-1-1-1-1", "", false},
		{"00000000000000000000000000000000", "", false},
		{"{00000000-0000-4000-8000-00000000abcd}", "", false},
		{"1-1-1-1-1\n", "00000001-0001-0001-0001-000000000001", true},
		{"\uff11-1-1-1-1", "00000001-0001-0001-0001-000000000001", true},
		{"\uff21-1-1-1-1", "0000000a-0001-0001-0001-000000000001", true},
		{"\u0661-1-1-1-1", "00000001-0001-0001-0001-000000000001", true},
		{"\U0001d7cf-1-1-1-1", "", false},
		{"-----", "", false},
	} {
		got, ok := SpringUUID(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("SpringUUID(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestJacksonUUIDMatchesJava(t *testing.T) {
	for _, c := range []struct {
		in, want string
		ok       bool
	}{
		{"00000000-0000-0000-0000-c0ffee0007d0", "00000000-0000-0000-0000-c0ffee0007d0", true},
		{"00000000-0000-0000-0000-C0FFEE0007D1", "00000000-0000-0000-0000-c0ffee0007d1", true},
		{"1-1-1-1-1", "", false},
		{" 00000000-0000-0000-0000-c0ffee0007d3", "00000000-0000-0000-0000-c0ffee0007d3", true},
		{"00000000-0000-0000-0000-c0ffee0007d4 ", "00000000-0000-0000-0000-c0ffee0007d4", true},
		{"AAAAAAAAAAAAAAAAAAAAAA==", "00000000-0000-0000-0000-000000000000", true}, // 2nd run: 409 (exists)
		{"QUJDREVGR0hJSktMTU5PUA==", "41424344-4546-4748-494a-4b4c4d4e4f50", true}, // 2nd run: 409 (exists)
		{"00000001-0001-0001-0001-0000000000001", "", false},
		{"{00000000-0000-0000-0000-c0ffee0007d8}", "", false},
		{"00000000-0000-0000-0000-c0ffee0007d9-", "", false},
		{"00000000000000000000000000000000", "", false},
		{"", "", false},
		{"AAAAAAAAAAA AAAAAAAAAA==", "", false},
		{"AAAAAAAAAAAAAAAAAAAAAA", "", false},
		{"AAAAAAAAAAAAAAAAAAAAAAA=", "", false},
	} {
		got, ok := JacksonUUID(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("JacksonUUID(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}
