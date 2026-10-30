package stomp

import (
	"bytes"
	"testing"
)

// FuzzParseFrames: the frame parser never panics, never claims more than it was given and is
// insensitive to where the transport splits the byte stream.
func FuzzParseFrames(f *testing.F) {
	f.Add([]byte("CONNECT\naccept-version:1.2\nheart-beat:10000,0\n\n\x00"), 5)
	f.Add([]byte("\n\r\nSEND\ndestination:/app/x\ncontent-length:3\n\na\x00b\x00SUBSCRIBE\nid:s\\c1\ndestination:/queue/response\n\n\x00"), 17)
	f.Add([]byte("SEND\r\ndestination:/x\r\n\r\nbody\x00"), 9)
	f.Add([]byte("SEND\ncontent-length:-1\n\n\x00"), 3)
	f.Add([]byte("SEND\ncontent-length:99999999999999999999\n\n"), 3)
	f.Add([]byte("SEND\nnocolon\n\n\x00"), 1)
	f.Add([]byte("\x00\x00\x00"), 1)
	f.Fuzz(func(t *testing.T, in []byte, split int) {
		const max = 1 << 20
		frames, rest, err := parseFrames(in, max)
		if err != nil {
			if frames != nil || rest != nil {
				t.Fatal("results returned with an error")
			}
			return
		}
		if len(rest) > len(in) {
			t.Fatal("rest longer than input")
		}
		for _, fr := range frames {
			if fr.Command == "" {
				t.Fatal("empty command")
			}
			_ = encode(fr.Command, fr.Headers, fr.Body)
		}
		// feeding the stream in two pieces yields the same frames
		if split < 0 {
			split = -split
		}
		k := 0
		if len(in) > 0 {
			k = split % (len(in) + 1)
		}
		f1, r1, err := parseFrames(in[:k], max)
		if err != nil {
			t.Fatalf("prefix of a valid stream rejected: %v", err)
		}
		f2, r2, err := parseFrames(append(append([]byte(nil), r1...), in[k:]...), max)
		if err != nil {
			t.Fatalf("second half rejected: %v", err)
		}
		all := append(f1, f2...)
		if len(all) != len(frames) || !bytes.Equal(r2, rest) {
			t.Fatalf("split at %d: %d+%d frames vs %d", k, len(f1), len(f2), len(frames))
		}
		for i := range all {
			if all[i].Command != frames[i].Command || !bytes.Equal(all[i].Body, frames[i].Body) || len(all[i].Headers) != len(frames[i].Headers) {
				t.Fatalf("frame %d differs after split", i)
			}
		}
	})
}
