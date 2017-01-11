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
	"io"
)

// QueueFile is a file-based FIFO queue based on the implementation of Tape.
type QueueFile interface {
	// PeekN reads the eldest N element, nil if the queue is empty
	PeekN(n int64) ([][]byte, error)
	// RemoveN removes the eldest N elements
	RemoveN(n int64) error
	// Empty returns true if the queue contains no entries
	Empty() bool
	// Size returns the number of elements in the queue
	Size() int64
	// Clear clears this queue. Truncate the file to the initial size
	Clear() error
	io.Writer
	io.WriterAt
	io.Closer
}

// CreateQueueFile creates a QueueFile based on the given directory and file name.
func CreateQueueFile(dir, name string) (QueueFile, error) {
	return createQueueFileImpl(dir, name)
}
