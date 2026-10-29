// Package stomp is a minimal STOMP 1.2 server over WebSocket, sufficient for Castor's upload protocol.
package stomp

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
)

// Frame is a parsed STOMP frame.
type Frame struct {
	Command string
	Headers [][2]string // ordered; first occurrence wins on lookup (STOMP 1.2)
	Body    []byte
}

// Header returns the first value for name.
func (f *Frame) Header(name string) (string, bool) {
	for _, h := range f.Headers {
		if h[0] == name {
			return h[1], true
		}
	}
	return "", false
}

var errMalformed = errors.New("stomp: malformed frame")

func unescape(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 'c':
				b.WriteByte(':')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteByte('\\')
				b.WriteByte(s[i])
			}
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func escape(s string) string {
	r := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\r", "\\r", ":", "\\c")
	return r.Replace(s)
}

// parseFrames extracts as many complete frames as buf holds and returns the unconsumed rest.
// Heart-beat EOLs between frames are skipped. maxBody limits a single frame's total size.
func parseFrames(buf []byte, maxSize int) (frames []*Frame, rest []byte, err error) {
	for {
		// skip heart-beats
		for len(buf) > 0 && (buf[0] == '\n' || (buf[0] == '\r' && len(buf) > 1 && buf[1] == '\n')) {
			if buf[0] == '\r' {
				buf = buf[2:]
			} else {
				buf = buf[1:]
			}
		}
		if len(buf) == 0 {
			return frames, nil, nil
		}
		hdrEnd := bytes.Index(buf, []byte("\n\n"))
		crlf := false
		if i := bytes.Index(buf, []byte("\r\n\r\n")); i >= 0 && (hdrEnd < 0 || i < hdrEnd) {
			hdrEnd, crlf = i, true
		}
		if hdrEnd < 0 {
			if len(buf) > maxSize {
				return nil, nil, errMalformed
			}
			return frames, buf, nil // need more data
		}
		sep := 2
		if crlf {
			sep = 4
		}
		head := string(buf[:hdrEnd])
		head = strings.ReplaceAll(head, "\r\n", "\n")
		lines := strings.Split(head, "\n")
		f := &Frame{Command: strings.TrimSpace(lines[0])}
		if f.Command == "" {
			return nil, nil, errMalformed
		}
		contentLen := -1
		for _, l := range lines[1:] {
			i := strings.IndexByte(l, ':')
			if i < 0 {
				return nil, nil, errMalformed
			}
			k, v := l[:i], l[i+1:]
			if f.Command != "CONNECT" && f.Command != "CONNECTED" {
				k, v = unescape(k), unescape(v)
			}
			f.Headers = append(f.Headers, [2]string{k, v})
			if k == "content-length" && contentLen < 0 {
				if n, e := strconv.Atoi(v); e == nil && n >= 0 {
					contentLen = n
				}
			}
		}
		bodyStart := hdrEnd + sep
		var bodyEnd int
		if contentLen >= 0 {
			if contentLen > maxSize {
				return nil, nil, errMalformed
			}
			bodyEnd = bodyStart + contentLen
			if len(buf) < bodyEnd+1 {
				return frames, buf, nil
			}
			if buf[bodyEnd] != 0 {
				return nil, nil, errMalformed
			}
		} else {
			i := bytes.IndexByte(buf[bodyStart:], 0)
			if i < 0 {
				if len(buf) > maxSize {
					return nil, nil, errMalformed
				}
				return frames, buf, nil
			}
			bodyEnd = bodyStart + i
		}
		f.Body = append([]byte(nil), buf[bodyStart:bodyEnd]...)
		frames = append(frames, f)
		buf = buf[bodyEnd+1:]
	}
}

// encode serialises a frame; content-length is added when body is non-empty.
func encode(cmd string, headers [][2]string, body []byte) []byte {
	var b bytes.Buffer
	b.WriteString(cmd)
	b.WriteByte('\n')
	for _, h := range headers {
		if cmd == "CONNECTED" {
			b.WriteString(h[0] + ":" + h[1] + "\n")
		} else {
			b.WriteString(escape(h[0]) + ":" + escape(h[1]) + "\n")
		}
	}
	if len(body) > 0 {
		b.WriteString("content-length:" + strconv.Itoa(len(body)) + "\n")
	}
	b.WriteByte('\n')
	b.Write(body)
	b.WriteByte(0)
	return b.Bytes()
}
