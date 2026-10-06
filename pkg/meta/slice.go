/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import "github.com/juicedata/juicefs/pkg/utils"

type slice struct {
	id    uint64
	size  uint32
	off   uint32
	len   uint32
	pos   uint32
	left  *slice
	right *slice
}

func newSlice(pos uint32, id uint64, cleng, off, len uint32) *slice {
	if len == 0 {
		return nil
	}
	s := &slice{}
	s.pos = pos
	s.id = id
	s.size = cleng
	s.off = off
	s.len = len
	s.left = nil
	s.right = nil
	return s
}

func (s *slice) read(buf []byte) {
	rb := utils.ReadBuffer(buf)
	s.pos = rb.Get32()
	s.id = rb.Get64()
	s.size = rb.Get32()
	s.off = rb.Get32()
	s.len = rb.Get32()
}

func (s *slice) cut(pos uint32) (left, right *slice) {
	if s == nil {
		return nil, nil
	}
	if pos <= s.pos {
		if s.left == nil {
			s.left = newSlice(pos, 0, 0, 0, s.pos-pos)
		}
		left, s.left = s.left.cut(pos)
		return left, s
	} else if pos < s.pos+s.len {
		l := pos - s.pos
		right = newSlice(pos, s.id, s.size, s.off+l, s.len-l)
		right.right = s.right
		s.len = l
		s.right = nil
		return s, right
	} else {
		if s.right == nil {
			s.right = newSlice(s.pos+s.len, 0, 0, 0, pos-s.pos-s.len)
		}
		s.right, right = s.right.cut(pos)
		return s, right
	}
}

func (s *slice) visit(f func(*slice)) {
	if s == nil {
		return
	}
	s.left.visit(f)
	right := s.right
	f(s) // s could be freed
	right.visit(f)
}

const sliceBytes = 24

func marshalSlice(pos uint32, id uint64, size, off, len uint32) []byte {
	w := utils.NewBuffer(sliceBytes)
	w.Put32(pos)
	w.Put64(id)
	w.Put32(size)
	w.Put32(off)
	w.Put32(len)
	return w.Bytes()
}

func readSlices(vals []string) []*slice {
	slices := make([]slice, len(vals))
	ss := make([]*slice, len(vals))
	for i, val := range vals {
		if len(val) != sliceBytes {
			logger.Errorf("corrupt slice: len=%d, val=%v", len(val), []byte(val))
			return nil
		}
		s := &slices[i]
		s.read([]byte(val))
		ss[i] = s
	}
	return ss
}

func readSliceBuf(buf []byte) []*slice {
	if len(buf)%sliceBytes != 0 {
		logger.Errorf("corrupt slices: len=%d", len(buf))
		return nil
	}
	nSlices := len(buf) / sliceBytes
	slices := make([]slice, nSlices)
	ss := make([]*slice, nSlices)
	for i := 0; i < len(buf); i += sliceBytes {
		s := &slices[i/sliceBytes]
		s.read(buf[i:])
		ss[i/sliceBytes] = s
	}
	return ss
}

func buildSlice(ss []*slice) []Slice {
	var root *slice
	for i := range ss {
		s := new(slice)
		*s = *ss[i]
		var right *slice
		s.left, right = root.cut(s.pos)
		_, s.right = right.cut(s.pos + s.len)
		root = s
	}
	var pos uint32
	var chunk []Slice
	root.visit(func(s *slice) {
		if s.pos > pos {
			chunk = append(chunk, Slice{Size: s.pos - pos, Len: s.pos - pos})
			pos = s.pos
		}
		chunk = append(chunk, Slice{Id: s.id, Size: s.size, Off: s.off, Len: s.len})
		pos += s.len
	})
	return chunk
}

func compactChunk(ss []*slice) (uint32, uint32, []Slice) {
	var chunk = buildSlice(ss)
	var pos uint32
	n := len(chunk)
	for n > 1 {
		if chunk[0].Id == 0 {
			pos += chunk[0].Len
			chunk = chunk[1:]
			n--
		} else if chunk[n-1].Id == 0 {
			chunk = chunk[:n-1]
			n--
		} else {
			break
		}
	}
	if n == 1 && chunk[0].Id == 0 {
		chunk[0].Len = 1
	}
	var size uint32
	for _, c := range chunk {
		size += c.Len
	}
	return pos, size, chunk
}

func skipSome(chunk []*slice) int {
	var skipped int
	var total = len(chunk)
OUT:
	for skipped < total {
		ss := chunk[skipped:]
		pos, size, c := compactChunk(ss)
		first := ss[0]
		if first.len < (1<<20) || first.len*5 < size || size == 0 {
			// it's too small
			break
		}
		isFirst := func(pos uint32, s Slice) bool {
			return pos == first.pos && s.Id == first.id && s.Off == first.off && s.Len == first.len
		}
		if !isFirst(pos, c[0]) {
			// it's not the first slice, compact it
			break
		}
		for _, s := range ss[1:] {
			if *s == *first {
				break OUT
			}
		}
		skipped++
	}
	return skipped
}

// isHole reports whether [off, off+l) of a chunk already reads as zeros: it is
// covered only by holes (slices with id 0) or lies past the chunk's last
// slice. Punching such a range again changes nothing but adds a slice record,
// and an fstrim of a block image re-punches every free range on every mount
// (ext4 forgets what it trimmed when it unmounts), so without this check
// each mount grew every chunk by a record until reads triggered compaction,
// which rewrites the holes as zero-filled objects.
//
// It runs inside the punch's transaction, on chunks that may hold thousands
// of records, so it walks them newest first and stops as soon as the answer
// is known (usually at the previous trim's record for the same range),
// instead of building the chunk's whole slice tree.
func isHole(ss []*slice, off, l uint32) bool {
	if ss == nil { // corrupt: punch as before
		return false
	}
	// The parts of the range no newer record has covered yet.
	open := [][2]uint32{{off, off + l}}
	for i := len(ss) - 1; i >= 0 && len(open) > 0; i-- {
		s := ss[i]
		lo, hi := s.pos, s.pos+s.len
		next := open[:0:0]
		for _, r := range open {
			if hi <= r[0] || lo >= r[1] {
				next = append(next, r)
				continue
			}
			if s.id != 0 {
				return false // the newest record over these bytes is data
			}
			if r[0] < lo {
				next = append(next, [2]uint32{r[0], lo})
			}
			if hi < r[1] {
				next = append(next, [2]uint32{hi, r[1]})
			}
		}
		open = next
	}
	return true // what is left was never written: it reads as zeros
}
