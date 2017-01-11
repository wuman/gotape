gotape
======

Gotape is a golang port of the atomic `QueueFile` that is part of the [Tape][1]
implementation by [Square][2].  It is a file-based FIFO queue with O(1) atomic
operations.  Writes to the queue file are synchronous and the integrity of the
file will survive process/system crashes.


Why in Golang?
--------------

The original Tape implementation is written separately in Java and C to support
Android and iOS, respectively.  We want the same `QueueFile` backing our
persistent task queue to work for both Android and iOS, but with shared code
base.  Since we are already using [gomobile binding][3], it is natural to port
to golang.


License
-------

    Copyright 2017 David Wu
    Copyright 2012 Square, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.



[1]: https://github.com/square/tape
[2]: https://squareup.com
[3]: https://godoc.org/golang.org/x/mobile/cmd/gobind
