/*
 * Copyright (C) 2010 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tape

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/blang/vfs"
)

const (
	initialLength int64 = 4096
	// headerLength consists of 32 bytes, beware the difference that in the
	// original tape implementation, the header is 16 bytes in length.
	// The modification reflects the int64 type we used in this implementation.
	//   Header:
	//     File Length            (8 bytes)
	//     Element Count          (8 bytes)
	//     First Element Position (8 bytes, =0 if null)
	//     Last Element Position  (8 bytes, =0 if null)
	headerLength int64 = 32
	// elementHeaderLength consists of 8 bytes instead of 4 bytes in the
	// original tape implementation.
	elementHeaderLength int64 = 8
)

var (
	// ErrNullData indicates the error on data is nil
	ErrNullData = errors.New("data == null")
	// ErrIndexOutOfBounds indicates the error when given index is out of boundary of array
	ErrIndexOutOfBounds = errors.New("Index out of bounds")
	// ErrNoSuchElement indicates the error when no such element is found in collection of element
	ErrNoSuchElement = errors.New("No such element")

	nullElement                = element{}
	zeroes                     = make([]byte, initialLength)
	fs          vfs.Filesystem = vfs.OS()
)

type element struct {
	position int64
	length   int64
}

// brokenFile is a wrapped file that decorates the WriteAt function with a controllable
// rejectCommit field to control if the WriteAt function succeeds or not
type brokenFile struct {
	vfs.File
	rejectCommit bool
}

func (b *brokenFile) ReadAt(data []byte, off int64) (int, error) {
	if _, err := b.File.Seek(off, 0); err != nil {
		return 0, err
	}
	return b.File.Read(data)
}

func (b *brokenFile) WriteAt(data []byte, off int64) (int, error) {
	if b.rejectCommit {
		return 0, errors.New("commit failed")
	}
	if _, err := b.File.Seek(off, 0); err != nil {
		return 0, err
	}
	return b.File.Write(data)
}

type queueFileImpl struct {
	buffer       []byte
	file         *brokenFile
	fileLength   int64
	elementCount int64
	first        *element
	last         *element
	sync.RWMutex
}

func createQueueFileImpl(dir, name string) (*queueFileImpl, error) {
	if name == "" {
		return nil, errors.New("empty queue file name")
	} else if err := ensureBase(dir); err != nil {
		return nil, fmt.Errorf("ensure queue file dir path: %s, err: %v", dir, err)
	}
	fullpath := filepath.Join(dir, name)
	if _, err := fs.Stat(fullpath); os.IsNotExist(err) {
		if err := initialize(fullpath); err != nil {
			return nil, err
		}
	}
	file, err := fs.OpenFile(fullpath, os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}
	qfile := &queueFileImpl{
		buffer: make([]byte, headerLength),
		file:   &brokenFile{File: file},
	}
	if err := qfile.readHeader(); err != nil {
		return nil, err
	}
	return qfile, nil
}

func ensureBase(basePath string) error {
	if basePath == "" {
		return nil
	}
	return os.MkdirAll(basePath, 0755)
}

func initialize(fullpath string) error {
	f, err := fs.OpenFile(fullpath+".tmp", os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	fname := f.Name()
	if err := f.Truncate(initialLength); err != nil {
		return err
	}
	headerBuffer := make([]byte, headerLength)
	writeInt64(headerBuffer, 0, initialLength)
	f.Seek(0, 0)
	f.Write(headerBuffer)
	f.Sync()
	f.Close()
	return fs.Rename(fname, fullpath)
}

// readHeader reads the header to initialize the queue file. This function is
// called only once at createQueueFileImpl()
func (q *queueFileImpl) readHeader() error {
	q.file.Seek(0, 0)
	q.file.Read(q.buffer)
	q.fileLength = readInt64(q.buffer, 0)
	stat, err := fs.Stat(q.file.Name())
	if err != nil {
		return err
	}
	if q.fileLength > stat.Size() {
		return fmt.Errorf("File is truuncated. Expected length: %d, Actual length: %d",
			q.fileLength, stat.Size())
	} else if q.fileLength <= 0 {
		return fmt.Errorf("File is corrupt; length stored in header (%d) is invalid", q.fileLength)
	}
	q.elementCount = readInt64(q.buffer, 8)
	firstOffset := readInt64(q.buffer, 16)
	lastOffset := readInt64(q.buffer, 24)
	if q.first, err = q.readElement(firstOffset); err != nil {
		return err
	}
	if q.last, err = q.readElement(lastOffset); err != nil {
		return err
	}
	return nil
}

func (q *queueFileImpl) writeHeader(fileLength, elementCount, firstPosition, lastPosition int64) error {
	writeInt64(q.buffer, 0, fileLength)
	writeInt64(q.buffer, 8, elementCount)
	writeInt64(q.buffer, 16, firstPosition)
	writeInt64(q.buffer, 24, lastPosition)
	_, err := q.file.WriteAt(q.buffer, 0)
	return err
}

func (q *queueFileImpl) wrapPosition(position int64) int64 {
	if position < q.fileLength {
		return position
	}
	return headerLength + position - q.fileLength
}

func (q *queueFileImpl) ringRead(position int64, buffer []byte, offset, count int64) error {
	position = q.wrapPosition(position)
	if position+count <= q.fileLength {
		if _, err := q.file.ReadAt(buffer[:offset+count], position); err != nil {
			return err
		}
	} else {
		beforeEOF := q.fileLength - position
		if _, err := q.file.ReadAt(buffer[offset:offset+beforeEOF], position); err != nil {
			return err
		}
		if _, err := q.file.ReadAt(buffer[offset+beforeEOF:offset+count], headerLength); err != nil {
			return err
		}
	}
	return nil
}

func (q *queueFileImpl) ringWrite(position int64, buffer []byte, offset, count int64) error {
	position = q.wrapPosition(position)
	if position+count <= q.fileLength {
		if _, err := q.file.WriteAt(buffer[offset:offset+count], position); err != nil {
			return err
		}
	} else {
		beforeEOF := q.fileLength - position
		if _, err := q.file.WriteAt(buffer[offset:offset+beforeEOF], position); err != nil {
			return err
		}
		if _, err := q.file.WriteAt(buffer[offset+beforeEOF:offset+count], headerLength); err != nil {
			return err
		}
	}
	return nil
}

func (q *queueFileImpl) ringErase(position, length int64) error {
	for length > 0 {
		chunk := minInt64(length, int64(len(zeroes)))
		if err := q.ringWrite(position, zeroes, 0, chunk); err != nil {
			return err
		}
		length -= chunk
		position += chunk
	}
	return nil
}

func (q *queueFileImpl) readElement(position int64) (*element, error) {
	if position == 0 {
		return &nullElement, nil
	}
	if err := q.ringRead(position, q.buffer, 0, elementHeaderLength); err != nil {
		return nil, err
	}
	length := readInt64(q.buffer, 0)
	return &element{
		position: position,
		length:   length,
	}, nil
}

func (q *queueFileImpl) usedBytes() int64 {
	if q.elementCount == 0 {
		return headerLength
	}
	if q.last.position >= q.first.position {
		// Contiguous queue.
		return (q.last.position - q.first.position) + // all but last entry
			elementHeaderLength + q.last.length + // last entry
			headerLength
	}
	// tail < head. The queue wraps.
	return q.last.position + // buffer front + header
		elementHeaderLength + q.last.length + // last entry
		q.fileLength - q.first.position // buffer end
}

func (q *queueFileImpl) remainingBytes() int64 {
	return q.fileLength - q.usedBytes()
}

func (q *queueFileImpl) setLength(newLength int64) error {
	return q.file.Truncate(newLength)
}

func (q *queueFileImpl) expandIfNecessary(dataLength int64) error {
	elementLength := elementHeaderLength + dataLength
	remainingBytes := q.remainingBytes()
	if remainingBytes >= elementLength {
		return nil
	}

	previousLength := q.fileLength
	var newLength int64
	// Double the length until we can fit the new data.
	for {
		remainingBytes += previousLength
		newLength = previousLength << 1
		previousLength = newLength
		if remainingBytes >= elementLength {
			break
		}
	}

	if err := q.setLength(newLength); err != nil {
		return err
	}

	// Calculate the position of the tail end of the data in the ring buffer
	endOfLastElement := q.wrapPosition(q.last.position + elementHeaderLength + q.last.length)

	if endOfLastElement <= q.first.position {
		target := io.WriteSeeker(q.file)
		var src io.ReadSeeker
		src, err := fs.OpenFile(q.file.Name(), os.O_RDONLY, 0440)
		if err != nil {
			return err
		}
		target.Seek(q.fileLength, 0)
		src.Seek(headerLength, 0)
		count := endOfLastElement - headerLength

		if _, err := io.CopyN(target, src, count); err != nil {
			return err
		}
		err = q.ringErase(headerLength, count)
		if err != nil {
			return err
		}
	}

	if q.last.position < q.first.position {
		newLastPosition := q.fileLength + q.last.position - headerLength
		if err := q.writeHeader(newLength, q.elementCount, q.first.position, newLastPosition); err != nil {
			return err
		}
		q.last = &element{
			position: newLastPosition,
			length:   q.last.length,
		}
	} else {
		if err := q.writeHeader(newLength, q.elementCount, q.first.position, q.last.position); err != nil {
			return err
		}
	}
	q.fileLength = newLength
	return nil
}

func (q *queueFileImpl) Empty() bool {
	q.RLock()
	defer q.RUnlock()
	return q.elementCount == 0
}

func (q *queueFileImpl) Write(data []byte) (int, error) {
	q.Lock()
	defer q.Unlock()
	return q.enqueueLocked(data, 0)
}

func (q *queueFileImpl) WriteAt(data []byte, offset int64) (int, error) {
	q.Lock()
	defer q.Unlock()
	return q.enqueueLocked(data, offset)
}

func (q *queueFileImpl) enqueueLocked(data []byte, offset int64) (int, error) {
	if data == nil {
		return 0, ErrNullData
	}
	if offset < 0 {
		return 0, ErrIndexOutOfBounds
	}
	count := int64(len(data[offset:]))
	if err := q.expandIfNecessary(int64(count)); err != nil {
		return 0, err
	}

	wasEmpty := (q.elementCount == 0)
	var position int64
	if wasEmpty {
		position = headerLength
	} else {
		position = q.wrapPosition(q.last.position + elementHeaderLength + q.last.length)
	}
	newLast := &element{
		position: position,
		length:   int64(count),
	}

	writeInt64(q.buffer, 0, int64(count))
	if err := q.ringWrite(newLast.position, q.buffer, 0, elementHeaderLength); err != nil {
		return 0, err
	}
	if err := q.ringWrite(newLast.position+elementHeaderLength, data, int64(offset), int64(count)); err != nil {
		return 0, err
	}

	var firstPosition int64
	if wasEmpty {
		firstPosition = newLast.position
	} else {
		firstPosition = q.first.position
	}
	if err := q.writeHeader(q.fileLength, q.elementCount+1, firstPosition, newLast.position); err != nil {
		return 0, err
	}
	q.last = newLast
	q.elementCount++
	if wasEmpty {
		q.first = q.last
	}
	return int(count), nil
}

func (q *queueFileImpl) PeekN(n int64) ([][]byte, error) {
	q.RLock()
	defer q.RUnlock()
	if q.elementCount == 0 {
		return nil, nil
	}

	readElementCount := minInt64(n, q.elementCount)
	data := make([][]byte, readElementCount)
	newFirstPosition := q.first.position
	newFirstLength := q.first.length
	for i := int64(0); i < readElementCount; i++ {
		data[i] = make([]byte, newFirstLength)
		if err := q.ringRead(newFirstPosition+elementHeaderLength, data[i], 0, newFirstLength); err != nil {
			return nil, err
		}
		newFirstPosition = q.wrapPosition(newFirstPosition + elementHeaderLength + newFirstLength)
		if err := q.ringRead(newFirstPosition, q.buffer, 0, elementHeaderLength); err != nil {
			return nil, err
		}
		newFirstLength = readInt64(q.buffer, 0)
	}

	return data, nil
}

func (q *queueFileImpl) Size() int64 {
	q.RLock()
	defer q.RUnlock()
	return q.elementCount
}

func (q *queueFileImpl) RemoveN(n int64) error {
	q.Lock()
	defer q.Unlock()
	return q.removeLocked(n)
}

func (q *queueFileImpl) removeLocked(n int64) error {
	if n < 0 {
		return fmt.Errorf("Cannot remove negative (%d) elements", n)
	}
	if n == 0 {
		return nil
	}
	if n == q.elementCount {
		return q.clearLocked()
	}
	if q.elementCount == 0 {
		return ErrNoSuchElement
	}
	if n > q.elementCount {
		return fmt.Errorf("Cannot remove more elements (%d) than present in queue (%d).", n, q.elementCount)
	}

	eraseStartPosition := q.first.position
	eraseTotalLength := int64(0)

	// Read the position and length of the new first element.
	newFirstPosition := q.first.position
	newFirstLength := q.first.length
	for i := int64(0); i < n; i++ {
		eraseTotalLength += elementHeaderLength + newFirstLength
		newFirstPosition = q.wrapPosition(newFirstPosition + elementHeaderLength + newFirstLength)
		if err := q.ringRead(newFirstPosition, q.buffer, 0, elementHeaderLength); err != nil {
			return err
		}
		newFirstLength = readInt64(q.buffer, 0)
	}

	// Commit the header.
	if err := q.writeHeader(q.fileLength, q.elementCount-n, newFirstPosition, q.last.position); err != nil {
		return err
	}
	q.elementCount -= n
	q.first = &element{
		position: newFirstPosition,
		length:   newFirstLength,
	}

	// Commit the erase.
	if err := q.ringErase(eraseStartPosition, eraseTotalLength); err != nil {
		return err
	}
	return nil
}

func (q *queueFileImpl) Clear() error {
	q.Lock()
	defer q.Unlock()
	return q.clearLocked()
}

func (q *queueFileImpl) clearLocked() error {
	// Commit the header.
	q.writeHeader(initialLength, 0, 0, 0)

	// Zero out data.
	if _, err := q.file.WriteAt(zeroes[:initialLength-headerLength], headerLength); err != nil {
		return err
	}

	q.elementCount = 0
	q.first = &nullElement
	q.last = &nullElement
	if q.fileLength > initialLength {
		q.setLength(initialLength)
	}
	q.fileLength = initialLength
	return nil
}

func (q *queueFileImpl) Close() error {
	q.Lock()
	defer q.Unlock()
	return q.file.Close()
}

func writeInt64(buffer []byte, offset, value int64) {
	buffer[offset] = byte(value >> 56)
	buffer[offset+1] = byte(value >> 48)
	buffer[offset+2] = byte(value >> 40)
	buffer[offset+3] = byte(value >> 32)
	buffer[offset+4] = byte(value >> 24)
	buffer[offset+5] = byte(value >> 16)
	buffer[offset+6] = byte(value >> 8)
	buffer[offset+7] = byte(value)
}

func readInt64(buf []byte, offset int64) int64 {
	return int64(buf[offset]&0xFF)<<56 +
		int64(buf[offset+1]&0xFF)<<48 +
		int64(buf[offset+2]&0xFF)<<40 +
		int64(buf[offset+3]&0xFF)<<32 +
		int64(buf[offset+4]&0xFF)<<24 +
		int64(buf[offset+5]&0xFF)<<16 +
		int64(buf[offset+6]&0xFF)<<8 +
		int64(buf[offset+7]&0xFF)
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
