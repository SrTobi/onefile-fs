# OneFileFS
A logical file system, that stores its content in a single file.

Multiple input and output streams to different logical files can be opened.
A single stream is not thread safe, but multiple streams can safely be used in different threads.

## Physical file layout

All logical files of the filesystem are backed by physical memory in form of a single file.
The content of logical files are stored in so called _blocks_.
All blocks have the same size and each block has a fixed location in the physical file.
Blocks are organized in _chunks_. Each _chunk_ starts with a header which describes the following blocks.

With that the format of the physical file looks as follows:

```
[Header] [Chunk 1 Header][Chunk 1 Data] [Chunk 2 Header][Chunk 2 Data] ...
```

### Header

The initial header contains basic information about the physical file. Especially the size of blocks and the number of blocks are flexible.

```
Header: [8 byte: magic number]
        [4 byte: size of blocks in byte]
        [4 byte: blocks per chunk]
        [4 byte: number of chunks]
        [8 byte: directory structure size]
```

### Chunk Header

A _chunk header_ holds a single integer for every block in its chunk.
These are used to track which block follows which block if a logical file is split over multiple blocks.
This is necessary because consecutive blocks might not be stored consecutively in the physical file.

## Directory structure

The directory structure contains directories and files.
It is organized in a tree.
The application will load and hold the directory structure in memory as long as the filesystem is open.

Directories can hold subdirectories and files.
File entries hold the file's size and the index of the first physical block that stores the file's data.
Consecutive blocks with data can be found through the chunk header.

When the filesystem is closed the directory structure itself is serialized into blocks, just like a normal logical file.
This "directory structure file" always begins in the block with the index 0 and its size is stored in the main header.


## Writing and reading data

Multiple threads can read and write data.
A single file can be read by multiple InputStreams but can only be written by one single OutputStream at a time.

Reading can be done in parallel, as the physical file is not altered.
Because the api prevents the user from writing and reading the same logical file at the same time,
blocks that belong to files that are currently not being written can therefor safely be read.

For writing blocks, a single writer thread is created. This thread consumes a queue with write tasks.
If an OutputStream wants to write to a logical file, it creates a write task and hands it to the writer thread,
which in turn will write the data to the physical file.

If no existing chunk has free blocks left, a new chunk is added at the end of the file.

## A few notes

There are three major drawbacks with the implementation:

1. The filesystem has to be held in memory all the time.
2. The physical file can easily become corrupted if the filesystem is not closed properly.
3. The single writer thread is a bottleneck for the write performance.

Memory mapped files could have improved the performance, but they have some weird semantics for growing files in Java/Scala.
