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
	"container/list"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blang/vfs"
	"github.com/blang/vfs/memfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	/**
	 * Takes up 33401 bytes in the queue (N*(N+1)/2+4*N). Picked 254 instead of 255 so that the number
	 * of bytes isn't a multiple of 4.
	 */
	n = 254
)

type QueueFileSuite struct {
	suite.Suite
	fpath  string
	q      *queueFileImpl
	buf    []byte
	values [n][]byte
}

func TestQueueFileSuite(t *testing.T) {
	suite.Run(t, new(QueueFileSuite))
}

func (s *QueueFileSuite) SetupSuite() {
	for i := 0; i < n; i++ {
		value := make([]byte, i)
		for ii := 0; ii < i; ii++ {
			value[ii] = byte(i - ii)
		}
		s.values[i] = value
	}
}

func (s *QueueFileSuite) SetupTest() {
	fs = memfs.Create()
	vfs.MkdirAll(fs, os.TempDir(), 0777)
	s.buf = make([]byte, headerLength)
	s.fpath = filepath.Join(os.TempDir(), "fq.data")
	fs.Remove(s.fpath)
	var err error
	s.q, err = createQueueFileImpl(filepath.Split(s.fpath))
	s.NoError(err)
}

func (s *QueueFileSuite) TearDownTest() {
	fs.Remove(s.fpath)
	fs = vfs.OS()
}

func (s *QueueFileSuite) TestEmptyFileName() {
	_, err := createQueueFileImpl("dir", "")
	s.EqualError(err, "empty queue file name")
	_, err = createQueueFileImpl("", "name")
	s.NoError(err)
}

func (s *QueueFileSuite) TestAddOneElement() {
	expected := s.values[253]
	_, err := s.q.Write(expected)
	s.NoError(err)
	peek, err := s.q.PeekN(1)
	s.NoError(err)
	s.Equal(expected, peek[0])
	s.q.Close()
	file, _ := createQueueFileImpl(filepath.Split(s.fpath))
	peek, _ = file.PeekN(1)
	s.NoError(err)
	s.Equal(expected, peek[0])
}

func (s *QueueFileSuite) TestClearErases() {
	expected := s.values[253]
	_, err := s.q.Write(expected)
	s.NoError(err)

	// Confirm that the data was in the file before we cleared.
	peek := make([]byte, len(expected))
	s.q.file.ReadAt(peek, int64(headerLength+elementHeaderLength))
	s.Equal(expected, peek)
	s.q.Clear()

	// Should have been erased.
	s.q.file.ReadAt(peek, int64(headerLength+elementHeaderLength))
	s.Equal(make([]byte, len(expected)), peek)
}

func (s *QueueFileSuite) TestClearDoesNotCorrupt() {
	stuff := s.values[253]
	_, err := s.q.Write(stuff)
	s.NoError(err)
	s.NoError(s.q.Clear())
	file, _ := createQueueFileImpl(filepath.Split(s.fpath))
	s.True(file.Empty())
	peek, err := file.PeekN(1)
	s.NoError(err)
	s.Nil(peek)
	file.Write(s.values[25])
	peek, err = file.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[25], peek[0])
}

func (s *QueueFileSuite) TestRemoveErasesEagerly() {
	firstStuff := s.values[127]
	_, err := s.q.Write(firstStuff)
	s.NoError(err)
	secondStuff := s.values[253]
	_, err = s.q.Write(secondStuff)
	s.NoError(err)

	// Confirm that first stuff was in the file before we remove.
	peek := make([]byte, len(firstStuff))
	s.q.file.ReadAt(peek, int64(headerLength+elementHeaderLength))
	s.Equal(firstStuff, peek)
	s.q.RemoveN(1)

	// Next record is intact
	peeks, err := s.q.PeekN(1)
	s.NoError(err)
	s.Equal(secondStuff, peeks[0])

	// First should have been erased.
	peek = make([]byte, len(firstStuff))
	s.q.file.ReadAt(peek, int64(headerLength+elementHeaderLength))
	s.Equal(make([]byte, len(firstStuff)), peek)
}

func (s *QueueFileSuite) TestRemoveMultipleDosNotCorrupt() {
	for i := 0; i < 10; i++ {
		_, err := s.q.Write(s.values[i])
		s.NoError(err)
	}
	s.q.RemoveN(1)
	s.Equal(int64(9), s.q.Size())
	peek, err := s.q.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[1], peek[0])

	s.q.RemoveN(3)
	s.Equal(int64(6), s.q.Size())
	peek, err = s.q.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[4], peek[0])

	s.q.RemoveN(6)
	s.True(s.q.Empty())
	peek, err = s.q.PeekN(1)
	s.NoError(err)
	s.Nil(peek)
}

func (s *QueueFileSuite) TestRemoveDoesNotCorrupt() {
	s.q.Write(s.values[127])
	secondStuff := s.values[253]
	s.q.Write(secondStuff)
	s.q.RemoveN(1)
	file, _ := createQueueFileImpl(filepath.Split(s.fpath))
	peek, err := file.PeekN(1)
	s.NoError(err)
	s.Equal(secondStuff, peek[0])
}

func (s *QueueFileSuite) TestRemoveFromEmptyFileThrows() {
	s.Equal(ErrNoSuchElement, s.q.RemoveN(1), "Should have thrown about removing from empty file.")
}

func (s *QueueFileSuite) TestRemoveZeroFromEmptyFileDoesNothing() {
	s.q.RemoveN(0)
	s.True(s.q.Empty())
}

func (s *QueueFileSuite) TestRemoveNegativeNumberOfElementsThrows() {
	_, err := s.q.Write(s.values[127])
	s.NoError(err)
	s.Equal(s.q.RemoveN(-1), errors.New("Cannot remove negative (-1) elements"))
}

func (s *QueueFileSuite) TestRemoveMoreElementsThanPresentInQueueThrows() {
	_, err := s.q.Write(s.values[127])
	s.NoError(err)
	s.Equal(s.q.RemoveN(2), errors.New("Cannot remove more elements (2) than present in queue (1)."))
}

func (s *QueueFileSuite) TestRemoveZeroElementsDoesNothing() {
	s.q.Write(s.values[127])
	s.q.RemoveN(0)
	s.Equal(int64(1), s.q.Size())
}

func (s *QueueFileSuite) TestRemovingBigDamnBlocksErasesEffectively() {
	bigBoy := make([]byte, 7000)
	for i := 0; i < 7000; i += 100 {
		copy(bigBoy[i:], s.values[100])
	}

	s.q.Write(bigBoy)
	s.q.Write(s.values[123])

	// Confirm that bigBoy was in the file before we remove.
	data := make([]byte, len(bigBoy))
	s.q.file.ReadAt(data, int64(headerLength+elementHeaderLength))
	s.Equal(bigBoy, data)
	s.NoError(s.q.RemoveN(1))

	// Next record is intact
	peek, err := s.q.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[123], peek[0])

	// First should have been erased.
	data = make([]byte, len(bigBoy))
	s.q.file.ReadAt(data, int64(headerLength+elementHeaderLength))
	s.Equal(make([]byte, len(bigBoy)), data)
}

func (s *QueueFileSuite) TestAddAndRemoveElements() {
	expected := list.New()
	for round := 0; round < 5; round++ {
		file, _ := createQueueFileImpl(filepath.Split(s.fpath))
		for i := 0; i < n; i++ {
			file.Write(s.values[i])
			expected.PushBack(s.values[i])
		}

		// Leave N elements in round N, 15 total for 5 rounds. Removing all the
		// elements would be like starting with an empty queue.
		for i := 0; i < n-round-1; i++ {
			peek, err := file.PeekN(1)
			s.NoError(err)
			s.Equal(expected.Remove(expected.Front()), peek[0])
			file.RemoveN(1)
		}
		file.Close()
	}

	// Remove and validate remaining 15 elements.
	file, _ := createQueueFileImpl(filepath.Split(s.fpath))
	s.Equal(int64(15), file.Size())
	s.Equal(int64(expected.Len()), file.Size())

	for expected.Len() != 0 {
		peek, err := file.PeekN(1)
		s.NoError(err)
		s.Equal(expected.Remove(expected.Front()), peek[0])
		file.RemoveN(1)
	}
	file.Close()
}

func (s *QueueFileSuite) TestSplitExpansion() {
	max := 80
	expected := list.New()
	for i := 0; i < max; i++ {
		s.q.Write(s.values[i])
		expected.PushBack(s.values[i])
	}

	// Remove all but 1.
	for i := 1; i < max; i++ {
		peek, err := s.q.PeekN(1)
		s.NoError(err)
		s.Equal(expected.Remove(expected.Front()), peek[0])
		s.q.RemoveN(1)
	}

	// This should wrap around before expanding.
	for i := 0; i < n; i++ {
		s.q.Write(s.values[i])
		expected.PushBack(s.values[i])
	}

	for expected.Len() != 0 {
		peek, err := s.q.PeekN(1)
		s.NoError(err)
		s.Equal(expected.Remove(expected.Front()), peek[0])
		s.q.RemoveN(1)
	}
	s.NoError(s.q.Close())
}

func (s *QueueFileSuite) TestFailedAdd() {
	_, err := s.q.Write(s.values[253])
	s.NoError(err)
	s.q.file.rejectCommit = true
	_, err = s.q.Write(s.values[252])
	s.Error(err)

	s.q.file.rejectCommit = false

	// Allow a subsequent add to succeed.
	s.q.Write(s.values[251])
	s.q.Close()

	file, _ := createQueueFileImpl(filepath.Split(s.fpath))
	s.Equal(int64(2), file.Size())
	peek, err := file.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[253], peek[0])
	file.RemoveN(1)
	peek, err = file.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[251], peek[0])
}

func (s *QueueFileSuite) TestFailedRemove() {
	_, err := s.q.Write(s.values[253])
	s.NoError(err)
	s.q.Close()

	file, _ := createQueueFileImpl(filepath.Split(s.fpath))
	file.file.rejectCommit = true
	s.Error(file.RemoveN(1))
	file.Close()

	s.q.file.rejectCommit = false
	file, _ = createQueueFileImpl(filepath.Split(s.fpath))
	s.Equal(int64(1), file.Size())
	peek, err := file.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[253], peek[0])
	file.Write(s.values[99])
	file.RemoveN(1)
	peek, err = file.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[99], peek[0])
}

func (s *QueueFileSuite) TestFailedExpansion() {
	s.q.Write(s.values[253])
	s.q.Close()

	file, _ := createQueueFileImpl(filepath.Split(s.fpath))
	file.file.rejectCommit = true
	_, err := file.Write(make([]byte, 8000))
	s.Error(err)
	file.Close()

	file.file.rejectCommit = false
	file, _ = createQueueFileImpl(filepath.Split(s.fpath))
	s.Equal(int64(1), file.Size())
	peek, err := file.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[253], peek[0])
	s.Equal(initialLength, file.fileLength)

	_, err = file.Write(s.values[99])
	s.NoError(file.RemoveN(1))
	peek, err = file.PeekN(1)
	s.NoError(err)
	s.Equal(s.values[99], peek[0])
}

func (s *QueueFileSuite) TestFileExpansionDoesNotCorruptWrappedElements() {
	// Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
	values := make([][]byte, 5)
	for blockNum := 0; blockNum < len(values); blockNum++ {
		values[blockNum] = make([]byte, 1024)
		for i := 0; i < len(values[blockNum]); i++ {
			values[blockNum][i] = byte(blockNum + 1)
		}
	}

	// First, add the first two blocks to the queue, remove one leaving a
	// 1K space at the beginning of the buffer.
	s.q.Write(values[0])
	s.q.Write(values[1])
	s.q.RemoveN(1)

	// The trailing end of block "4" will be wrapped to the start of the buffer.
	s.q.Write(values[2])
	s.q.Write(values[3])

	// Cause buffer to expand as there isn't space between the end of block "4"
	// and the start of block "2".  Internally the queue should cause block "4"
	// to be contiguous, but there was a bug where that wasn't happening.
	s.q.Write(values[4])

	// Make sure values are not corrupted, specifically block "4" that wasn't
	// being made contiguous in the version with the bug.
	for blockNum := 1; blockNum < len(values); blockNum++ {
		value, err := s.q.PeekN(1)
		s.NoError(err)
		s.q.RemoveN(1)

		for i := 0; i < len(values[0]); i++ {
			s.Equal(byte(blockNum+1), value[0][i])
		}
	}
	s.NoError(s.q.Close())
}

func (s *QueueFileSuite) TestFileExpansionCorrectlyMovesElements() {

	// Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
	values := make([][]byte, 5)
	for blockNum := 0; blockNum < len(values); blockNum++ {
		values[blockNum] = make([]byte, 1024)
		for i := 0; i < len(values[blockNum]); i++ {
			values[blockNum][i] = byte(blockNum + 1)
		}
	}

	// smaller data elements
	smaller := make([][]byte, 3)
	for blockNum := 0; blockNum < len(smaller); blockNum++ {
		smaller[blockNum] = make([]byte, 256)
		for i := 0; i < len(smaller[blockNum]); i++ {
			smaller[blockNum][i] = byte(blockNum + 6)
		}
	}

	// First, add the first two blocks to the queue, remove one leaving a
	// 1K space at the beginning of the buffer.
	s.q.Write(values[0])
	s.q.Write(values[1])
	s.q.RemoveN(1)

	// The trailing end of block "4" will be wrapped to the start of the buffer.
	s.q.Write(values[2])
	s.q.Write(values[3])

	// Now fill in some space with smaller blocks, none of which will cause
	// an expansion.
	s.q.Write(smaller[0])
	s.q.Write(smaller[1])
	s.q.Write(smaller[2])

	// Cause buffer to expand as there isn't space between the end of the
	// smaller block "8" and the start of block "2".  Internally the queue
	// should cause all of tbe smaller blocks, and the trailing end of
	// block "5" to be moved to the end of the file.
	s.q.Write(values[4])

	expectedBlockNumbers := []byte{2, 3, 4, 6, 7, 8, 5}

	// Make sure values are not corrupted, specifically block "4" that wasn't
	// being made contiguous in the version with the bug.
	for _, expectedBlockNumber := range expectedBlockNumbers {
		peek, err := s.q.PeekN(1)
		s.NoError(err)
		s.q.RemoveN(1)
		for _, peekValue := range peek[0] {
			s.Equal(expectedBlockNumber, peekValue)
		}
	}
	s.q.Close()
}

func (s *QueueFileSuite) TestFileExpansionCorrectlyZeroesData() {
	// Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
	values := make([][]byte, 5)
	for blockNum := 0; blockNum < len(values); blockNum++ {
		values[blockNum] = make([]byte, 1024)
		for i := 0; i < len(values[blockNum]); i++ {
			values[blockNum][i] = byte(blockNum + 1)
		}
	}

	// First, add the first two blocks to the queue, remove one leaving a
	// 1K space at the beginning of the buffer.
	s.q.Write(values[0])
	s.q.Write(values[1])
	s.q.RemoveN(1)

	// The trailing end of block "4" will be wrapped to the start of the buffer.
	s.q.Write(values[2])
	s.q.Write(values[3])

	// Cause buffer to expand as there isn't space between the end of block "4"
	// and the start of block "2". Internally the queue will cause block "4"
	// to be contiguous. There was a bug where the start of the buffer still
	// contained the tail end of block "4", and garbage was copied after the tail
	// end of the last element.
	s.q.Write(values[4])

	// Read from header to first element and make sure it's zeroed.
	firstElementPadding := int64(1024 + elementHeaderLength)
	data := make([]byte, firstElementPadding)
	s.q.file.ReadAt(data, headerLength)
	s.Equal(make([]byte, firstElementPadding), data)

	// Read from the last element to the end and make sure it's zeroed.
	endOfLastElement := headerLength + firstElementPadding + 4*(elementHeaderLength+1024)
	readLength := s.q.fileLength - endOfLastElement
	data = make([]byte, readLength)
	s.q.file.ReadAt(data, endOfLastElement)
	s.Equal(make([]byte, readLength), data)
}

func (s *QueueFileSuite) TestSaturatedFileExpansionMovesElements() {

	// Create test data - 1016-byte blocks marked consecutively 1, 2, 3, 4, 5 and 6,
	// four of which perfectly fill the queue file, taking into account the file header
	// and the item headers.
	// Each item is of length
	// (QueueFile.INITIAL_LENGTH - QueueFile.HEADER_LENGTH) / 4 - element_header_length
	// = 1008 bytes
	values := make([][]byte, 6)
	for blockNum := 0; blockNum < len(values); blockNum++ {
		values[blockNum] = make([]byte, 1008)
		for i := 0; i < len(values[blockNum]); i++ {
			values[blockNum][i] = byte(blockNum + 1)
		}
	}

	// Saturate the queue file
	s.q.Write(values[0])
	s.q.Write(values[1])
	s.q.Write(values[2])
	s.q.Write(values[3])

	// Remove an element and add a new one so that the position of the start and
	// end of the queue are equal
	s.q.RemoveN(1)
	s.q.Write(values[4])

	// Cause the queue file to expand
	s.q.Write(values[5])

	// Make sure values are not corrupted
	for i := 1; i < 6; i++ {
		peek, err := s.q.PeekN(1)
		s.NoError(err)
		s.Equal(values[i], peek[0])
		s.q.RemoveN(1)
	}
	s.q.Close()
}

func (s *QueueFileSuite) TestReadHeadersFromNonContiguousQueueWorks() {
	// Fill the queue up to `length - 2` (i.e. remainingBytes() == 2).
	for i := 0; i < 15; i++ {
		s.q.Write(s.values[n-1])
	}
	s.q.Write(s.values[219])

	// Remove first item so we have room to add another one without growing the file.
	s.q.RemoveN(1)

	// Add any element element and close the queue.
	s.q.Write(s.values[6])
	queueSize := s.q.Size()
	s.q.Close()

	// File should not be corrupted.
	file, err := createQueueFileImpl(filepath.Split(s.fpath))
	s.NoError(err)
	s.Equal(queueSize, file.Size())
}

func (s *QueueFileSuite) TestInitialize() {
	fs.Remove(s.fpath)
	err := initialize(s.fpath)
	s.NoError(err)
	f, err := fs.OpenFile(s.fpath, os.O_RDWR, 0660)
	s.NoError(err)
	s.Equal(s.fpath, f.Name())
	filestat, err := fs.Stat(f.Name())
	s.NoError(err)
	s.Equal(int64(initialLength), filestat.Size())
	f.Read(s.buf)
	s.Equal(initialLength, readInt64(s.buf, 0))
}

func (s *QueueFileSuite) TestCreateQueueFile() {
	s.Equal(headerLength, int64(len(s.q.buffer)))
	s.Equal(initialLength, s.q.fileLength)
	s.Equal(int64(0), s.q.elementCount)
	s.Equal(int64(0), s.q.first.position)
	s.Equal(int64(0), s.q.first.length)
	s.Equal(int64(0), s.q.last.position)
	s.Equal(int64(0), s.q.last.length)
}

func (s *QueueFileSuite) TestWriteHeader() {
	err := s.q.writeHeader(5, 5, 6, 6)
	s.NoError(err)
	s.q.file.Seek(0, 0)
	s.q.file.Read(s.buf)
	s.Equal(int64(5), readInt64(s.buf, 0))
	s.Equal(int64(5), readInt64(s.buf, 8))
	s.Equal(int64(6), readInt64(s.buf, 16))
	s.Equal(int64(6), readInt64(s.buf, 24))
}

func (s *QueueFileSuite) TestWrapPosition() {
	s.Equal(int64(0), s.q.wrapPosition(0))
	s.Equal(int64(1), s.q.wrapPosition(1))
	s.Equal(headerLength, s.q.wrapPosition(s.q.fileLength))
	s.Equal(headerLength+1, s.q.wrapPosition(s.q.fileLength+1))
}

func (s *QueueFileSuite) TestAddWithError() {
	_, err := s.q.WriteAt(nil, 0)
	s.Equal(ErrNullData, err)
	_, err = s.q.WriteAt(s.buf, -1)
	s.Equal(ErrIndexOutOfBounds, err)
}

func (s *QueueFileSuite) TestPeekWithEmptyQueue() {
	peek, err := s.q.PeekN(1)
	s.NoError(err)
	s.Nil(peek)
}

func (s *QueueFileSuite) TestPeekWithInvalidNumber() {
	peek, err := s.q.PeekN(0)
	s.NoError(err)
	s.Nil(peek)

	peek, err = s.q.PeekN(-1)
	s.NoError(err)
	s.Nil(peek)
}

func (s QueueFileSuite) TestMultiplePeekAndRemove() {
	for i := 0; i < 10; i++ {
		_, err := s.q.Write(s.values[i])
		s.NoError(err)
	}
	s.Equal(int64(10), s.q.Size())
	peek, err := s.q.PeekN(100)
	s.NoError(err)
	s.Equal(10, len(peek))
	for i := 0; i < 10; i++ {
		s.Equal(s.values[0], peek[0])
	}

	peek, err = s.q.PeekN(5)
	s.NoError(err)
	s.Equal(5, len(peek))
	for i := 0; i < 5; i++ {
		s.Equal(s.values[0], peek[0])
	}
	s.q.RemoveN(1)
	s.Equal(int64(9), s.q.Size())

	peek, err = s.q.PeekN(10)
	s.NoError(err)
	s.Equal(9, len(peek))
	for i := 0; i < 9; i++ {
		s.Equal(s.values[i+1], peek[i])
	}

	s.q.RemoveN(9)
	s.Equal(int64(0), s.q.Size())

	peek, err = s.q.PeekN(10)
	s.NoError(err)
	s.Nil(peek)
}

func (s *QueueFileSuite) TestPeekOnWrappedElement() {

	// Create test data - 1k blocks marked consecutively 1, 2, 3, 4 and 5.
	values := make([][]byte, 5)
	for blockNum := 0; blockNum < len(values); blockNum++ {
		values[blockNum] = make([]byte, 1024)
		for i := 0; i < len(values[blockNum]); i++ {
			values[blockNum][i] = byte(blockNum + 1)
		}
	}

	// smaller data elements
	smaller := make([][]byte, 3)
	for blockNum := 0; blockNum < len(smaller); blockNum++ {
		smaller[blockNum] = make([]byte, 256)
		for i := 0; i < len(smaller[blockNum]); i++ {
			smaller[blockNum][i] = byte(blockNum + 6)
		}
	}

	// First, add the first two blocks to the queue, remove one leaving a
	// 1K space at the beginning of the buffer.
	s.q.Write(values[0])
	s.q.Write(values[1])
	s.q.RemoveN(1)

	// The trailing end of block "4" will be wrapped to the start of the buffer.
	s.q.Write(values[2])
	s.q.Write(values[3])

	// Now fill in some space with smaller blocks, none of which will cause
	// an expansion.
	s.q.Write(smaller[0])
	s.q.Write(smaller[1])
	s.q.Write(smaller[2])

	expectedBlockNumbers := []byte{2, 3, 4, 6, 7, 8}

	peek, err := s.q.PeekN(10)
	s.NoError(err)
	s.Equal(len(peek), len(expectedBlockNumbers))
	for i := 0; i < len(peek); i++ {
		for _, peekValue := range peek[i] {
			s.Equal(expectedBlockNumbers[i], peekValue)
		}
	}
	s.q.Close()
}

func TestReadInt64(t *testing.T) {
	var value int64 = (1 << 56) + (1 << 48) + (1 << 40) + (1 << 32) + (1 << 24) + (1 << 16) + (1 << 8) + 1
	buffer := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	readValue := readInt64(buffer, 0)
	assert.Equal(t, readValue, int64(value))
}

func TestWriteInt64(t *testing.T) {
	var buffer []byte
	buffer = make([]byte, 8, 8)
	var value int64 = (1 << 56) + (1 << 48) + (1 << 40) + (1 << 32) + (1 << 24) + (1 << 16) + (1 << 8) + 1
	writeInt64(buffer, 0, int64(value))
	assert.Equal(t, byte(1), buffer[0])
	assert.Equal(t, byte(1), buffer[1])
	assert.Equal(t, byte(1), buffer[2])
	assert.Equal(t, byte(1), buffer[3])
	assert.Equal(t, byte(1), buffer[4])
	assert.Equal(t, byte(1), buffer[5])
	assert.Equal(t, byte(1), buffer[6])
	assert.Equal(t, byte(1), buffer[7])
}

func (s *QueueFileSuite) TestFileLengthConsistentWithHeader() {
	assertHeader := func() {
		v := readInt64(s.q.buffer, 0)
		s.EqualValues(v, s.q.fileLength)
	}
	// initial size
	s.EqualValues(initialLength, s.q.fileLength)
	// write
	s.q.Write([]byte("浩廷針男人"))
	s.EqualValues(initialLength, s.q.fileLength)
	assertHeader()
	// expanded
	s.q.expandIfNecessary(initialLength + 10)
	expanded := initialLength << 1
	s.EqualValues(expanded, s.q.fileLength)
	assertHeader()
	// clear
	s.q.Clear()
	s.EqualValues(initialLength, s.q.fileLength)
	assertHeader()
	// re-write
	s.q.Write([]byte("浩廷針男人"))
	s.EqualValues(initialLength, s.q.fileLength)
	assertHeader()
	// ensure no error when re-create from file
	another, err := createQueueFileImpl(filepath.Split(s.fpath))
	if s.NoError(err) {
		s.EqualValues(initialLength, another.fileLength)
	}
}
