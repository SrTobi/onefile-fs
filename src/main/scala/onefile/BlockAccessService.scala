package onefile

import java.io.{IOException, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingDeque

import scala.collection.mutable


private class BlockAccessService private(underlyingFilepath: String, _openExisting: Boolean, _blockSize: Int, _blocksPerChunk: Int) {
    def this(underlyingFilepath: String) = this(underlyingFilepath, true, -1, -1)
    def this(underlyingFilepath: String, blockSize: Int, blocksPerChunk: Int) = this(underlyingFilepath, false, blockSize, blocksPerChunk)

    private class BWFilePointer(var firstBlock: Option[Int], var originalFileSize: Long, var curBlockIdx: Option[Int], var blockOffset: Int = 0, var globalFileOffset: Long) extends FilePointer {
        assert(originalFileSize >= 0)
        assert(globalFileOffset >= 0 && globalFileOffset <= originalFileSize)
        assert(blockOffset <= blockSize)

        private var closed = false
        private val waitMonitor = new Object
        private var scheduledTasks: Long = 0
        private var finishedTasks: Long = 0

        def remainingTasks: Long = scheduledTasks - finishedTasks

        override def write(data: Array[Byte]): Unit = {
            if (data.isEmpty) {
                return
            }

            monitor.synchronized {
                waitMonitor.synchronized {
                    if (!running) {
                        throw new IOException("Service not running")
                    }

                    if (closed) {
                        throw new IOException("File already closed")
                    }

                    scheduledTasks += 1
                }
            }
            writeTasks.put(WriteTask(this, data))
        }

        def read(byteBuffer: ByteBuffer): Int = {
            val fileSize = originalFileSize

            waitMonitor.synchronized {
                if (!running) {
                    throw new IOException("Service not running")
                }

                if (closed) {
                    throw new IOException("File already closed")
                }
            }

            if (curBlockIdx.isEmpty || globalFileOffset == fileSize) {
                return -1
            }

            assert(0 <= blockOffset && blockOffset < blockSize)
            var block = curBlockIdx.get

            val dataSize = Math.min(Math.min(byteBuffer.remaining(), blockSize - blockOffset), fileSize - globalFileOffset).toInt
            val blockPosition = calculateBlockPosition(block)
            byteBuffer.limit(dataSize)

            while (true) {
                channel.read(byteBuffer, blockPosition + blockOffset) match {
                    case -1 => throw new IOException("Corrupt data")
                    case 0 => // try again
                    case n if n > 0 =>
                        blockOffset += n
                        globalFileOffset += n

                        assert(0 <= blockOffset && blockOffset <= blockSize)
                        if (blockOffset == blockSize) {
                            curBlockIdx = getNextBlock(block)
                            blockOffset = 0
                        }
                        return n
                }
            }
            throw new AssertionError("not reachable")
        }

        override def flush(): Unit = {
            waitMonitor.synchronized {
                val targetFinishedTasks = scheduledTasks
                while (finishedTasks < targetFinishedTasks) {
                    waitMonitor.wait()
                }
            }
        }

        override def close(): FileContent = {
            waitMonitor.synchronized {
                closed = true
                flush()
                assert(scheduledTasks == finishedTasks)
                FileContent(firstBlock, Math.max(originalFileSize, globalFileOffset))
            }
        }

        def notifyTaskFinish(): Unit = {
            waitMonitor.synchronized {
                finishedTasks += 1
                waitMonitor.notifyAll()
            }
        }
    }

    private sealed abstract class ServiceTask
    private object EndTask extends ServiceTask
    private case class WriteTask(filePointer: BWFilePointer, data: Array[Byte]) extends ServiceTask
    private class Block(val idx: Int, var free: Boolean = true, var next: Option[Int] = None)


    private val underlyingFile = new RandomAccessFile(underlyingFilepath, "rw")
    private val channel = underlyingFile.getChannel

    private val monitor = new Object
    private val blocks = mutable.ArrayBuffer.empty[Block]
    private val taskThread = new Thread(() => processBlockTasks())
    private val writeTasks = new LinkedBlockingDeque[ServiceTask](100)
    private var _running = false

    def running: Boolean = _running

    // HEADER: 8 byte magic number, 4 byte blockSize, 4 byte blocksPerChunk, 4 byte chunkCount, 8 byte fs structure size
    val magicNumber: Long = 3829840452293812348L
    val headerSize: Int = 8 + 4 + 4 + 4 + 8

    var chunkCount = 0
    var fsSize: Option[Long] = None

    val (blockSize, blocksPerChunk): (Int, Int) = if (_openExisting) {
        loadHeader()
    } else {
        (_blockSize, _blocksPerChunk)
    }

    // the chunk header is a long list of block entries
    // a block entry is an integer
    // -2 means the block is free
    // -1 means the block is not free but has no next block
    // n >= 0 means that block n is the next block
    val bytesPerBlockEntry: Int = Integer.BYTES
    val chunkHeaderSize: Int = bytesPerBlockEntry * blocksPerChunk
    val chunkSize: Int = chunkHeaderSize + blocksPerChunk * blockSize

    if (_openExisting) {
        loadChunkHeaders()
    } else {
        addEmptyChunk()
    }

    startService()

    private def startService(): Unit = {
        if (running) {
            throw new IllegalStateException("Service already running")
        }

        _running = true
        taskThread.start()
    }

    def stopService(): Unit = {
        if (!running) {
            throw new IllegalStateException("Service was not running")
        }

        monitor.synchronized {
            _running = false
            writeTasks.push(EndTask)
        }

        taskThread.join()
        saveChunkHeaders()
        saveHeader()
        channel.close()
        underlyingFile.close()
    }

    def createFilePointer(firstBlockIdx: Option[Int], fileSize: Long, append: Boolean): FilePointer = {
        (append, firstBlockIdx) match {
            case (true, Some(firstBlock)) =>
                def findLastBlock(blockIdx: Int, blockCount: Int): (Int, Int) = {
                    val block = blocks(blockIdx)
                    block.next match {
                        case Some(next) =>
                            findLastBlock(next, blockCount + 1)
                        case None =>
                            val blockOffset = fileSize - blockCount * blockSize
                            if (blockOffset < 0 || blockSize < blockOffset) {
                                throw new IOException("Corrupt file system")
                            }
                            (blockIdx, blockOffset.toInt)
                    }
                }

                val (lastBlock, blockOffset) = monitor.synchronized {
                    findLastBlock(firstBlock, 0)
                }

                new BWFilePointer(Some(firstBlock), fileSize, Some(lastBlock), blockOffset, fileSize)

            case _ =>
                new BWFilePointer(firstBlockIdx, fileSize, firstBlockIdx, 0, 0)
        }
    }

    def deleteBlockChain(initialBlockIdx: Int): Unit = monitor.synchronized {
        var blockIdx = initialBlockIdx

        // iterate through the blocks and set them free
        while (true) {
            val block = blocks(blockIdx)
            val next = block.next

            block.free = true
            block.next = None

            blockIdx = next.getOrElse {
                return
            }
        }
    }


    private def getNextBlock(curBlockIdx: Int): Option[Int] = {
        monitor.synchronized {
            blocks(curBlockIdx).next
        }
    }

    private def ensureNextBlock(blockIdx: Int): Int = monitor.synchronized {
        val block = blocks(blockIdx)
        block.next match {
            case Some(next) =>
                next
            case None =>
                val nextIdx = reserveNextFreeBlock()
                block.next = Some(nextIdx)
                nextIdx
        }
    }

    private def reserveNextFreeBlock(): Int = monitor.synchronized {
        val freeBlock = blocks.tail.find(_.free).getOrElse {
            val blockCount = blocks.size
            addEmptyChunk()
            blocks(blockCount)
        }
        freeBlock.free = false
        freeBlock.idx
    }

    private def addEmptyChunk(): Unit = monitor.synchronized {
        val blockCount = blocks.size
        for (i <- blockCount until (blockCount + blocksPerChunk)) {
            blocks.append(new Block(i))
        }

        chunkCount += 1
    }

    private def readAt(filePosition: Long, buffer: ByteBuffer): ByteBuffer = {
        buffer.mark()
        val initialPosition = buffer.position()
        do {
            if (channel.read(buffer, filePosition + buffer.position() - initialPosition) == -1) {
                throw new IOException("Failed to read data")
            }
        } while (buffer.hasRemaining)
        buffer.reset()
        buffer
    }

    private def writeAt(filePosition: Long, buffer: ByteBuffer): Unit = {
        val initialPosition = buffer.position()
        do {
            channel.write(buffer, filePosition + buffer.position() - initialPosition)
        } while (buffer.hasRemaining)
    }

    private def loadHeader(): (Int, Int) = {
        val buffer = readAt(0, ByteBuffer.allocate(headerSize))

        val extractedMagicNumber = buffer.getLong
        if (extractedMagicNumber != magicNumber) {
            throw new IOException("File has wrong format!")
        }

        val blockSize = buffer.getInt
        val blocksPerChunk = buffer.getInt
        chunkCount = buffer.getInt
        fsSize = Some(buffer.getLong).filter(_ >= 0)

        (blockSize, blocksPerChunk)
    }

    private def saveHeader(): Unit = {
        val buffer = ByteBuffer.allocate(headerSize)
        buffer.mark()

        buffer.putLong(magicNumber)
        buffer.putInt(blockSize)
        buffer.putInt(blocksPerChunk)
        buffer.putInt(chunkCount)
        buffer.putLong(fsSize.getOrElse(-1))

        buffer.reset()
        writeAt(0, buffer)
    }

    private def loadChunkHeaders(): Unit = {
        for (chunkIdx <- 0 until chunkCount) {
            val buffer = readAt(headerSize + chunkIdx * chunkSize, ByteBuffer.allocate(chunkHeaderSize))

            for (i <- 0 until blocksPerChunk) {
                val (isFree, next) = buffer.getInt match {
                    case -2 => (true, None)
                    case -1 => (false, None)
                    case next if next >= 0 => (false, Some(next))
                    case _ => throw new IOException("Corrupt chunk header")
                }
                blocks += new Block(chunkIdx * blocksPerChunk + i, isFree, next)
            }
        }
    }

    private def saveChunkHeaders(): Unit = {
        for ((chunkBlocks, chunkIdx) <- blocks.iterator.grouped(blocksPerChunk).zipWithIndex) {
            val buffer = ByteBuffer.allocate(chunkHeaderSize)
            buffer.mark()

            for (block <- chunkBlocks) {
                buffer.putInt(if (block.free) -2 else block.next.getOrElse(-1))
            }

            buffer.reset()
            writeAt(headerSize + chunkSize * chunkIdx, buffer)
        }
    }

    // this is the writer thread
    private def processBlockTasks(): Unit = {
        while (true) {
            writeTasks.take() match {
                case EndTask =>
                    assert(!running)
                    return

                case WriteTask(filePointer, data) =>
                    var blockIdx = filePointer.curBlockIdx.getOrElse{
                        val firstBlock = reserveNextFreeBlock()
                        filePointer.firstBlock = Some(firstBlock)
                        firstBlock
                    }
                    var globalFileOffset = filePointer.globalFileOffset
                    var blockOffset = filePointer.blockOffset
                    var dataOffset = 0

                    monitor.synchronized {
                        // ensure that the current block is in use (important for the first block, which contains the fs)
                        blocks(blockIdx).free = false
                    }

                    while (dataOffset < data.length) {
                        assert(0 <= blockOffset && blockOffset <= blockSize)
                        if (blockOffset >= blockSize) {
                            blockIdx = ensureNextBlock(blockIdx)
                            blockOffset = 0
                        }

                        var writeSize = Math.min(blockSize - blockOffset, data.length - dataOffset)
                        val writeBuffer = ByteBuffer.wrap(data, dataOffset, writeSize)

                        val blockPosition = calculateBlockPosition(blockIdx)
                        while (writeBuffer.hasRemaining) {
                            channel.write(writeBuffer, blockPosition + blockOffset + writeBuffer.position() - dataOffset)
                        }

                        blockOffset += writeSize
                        dataOffset += writeSize
                        globalFileOffset += writeSize
                    }

                    filePointer.curBlockIdx = Some(blockIdx)
                    filePointer.blockOffset = blockOffset
                    filePointer.globalFileOffset = globalFileOffset
                    filePointer.notifyTaskFinish()
            }
        }
    }

    private def calculateBlockPosition(block: Int) : Long = {
        val chunkIdx: Long = block / blocksPerChunk
        headerSize + (chunkIdx + 1) * chunkHeaderSize + block.toLong * blockSize.toLong
    }
}
