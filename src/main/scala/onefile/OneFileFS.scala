package onefile

import java.io._
import java.nio.ByteBuffer

import scala.util.Try
import scala.util.control.NonFatal


/**
  * A logical file system, that stores its content in a single file.
  *
  * All paths have to be absolute and of the form `/<dir 1>/<dir 2> .../filename`.
  *
  * Multiple input and output streams to different logical files can be opened.
  * A single stream is not thread safe, but multiple streams can safely be used in different threads.
  *
  * @note The underlying file is only valid if the corresponding OneFileFS object was properly closed with `OneFileFS.close()`.
  */
class OneFileFS private[OneFileFS](private val service: BlockAccessService, _openExisting: Boolean) extends Closeable {
    private sealed abstract class Node(var name: String, var parent: Directory) {
        def path: String = if (this == root) "" else s"${parent.path}/$name"

        def dir: Directory
        def file: File
    }

    private class File(_name: String, _parent: Directory, var content: FileContent = FileContent(None)) extends Node(_name, _parent) {
        override def dir: Directory = throw new FileNotFoundException(s"$path is not a directory")
        override def file: File = this

        private[this] val accessMonitor = new Object
        private[this] var writing: Boolean = false
        private[this] var reading: Int = 0

        def startReading(): Unit = accessMonitor.synchronized {
            if (writing) {
                throw new FileNotFoundException(s"$path is currently being written")
            }
            reading += 1
        }

        def endReading(): Unit = accessMonitor.synchronized {
            assert(reading > 0)
            reading -= 1
        }

        def startWriting(): Unit = accessMonitor.synchronized {
            if (reading > 0) {
                throw new FileNotFoundException(s"$path is currently being read")
            }
            writing = true
        }

        def endWriting(): Unit = accessMonitor.synchronized {
            assert(writing)
            writing = false
        }

        def currentlyAccessed: Boolean = writing || reading > 0
    }

    private class Directory(_name: String, _parent: Directory) extends Node(_name, _parent) {
        var children: Map[String, Node] = Map.empty

        def getChild(name: String): Node = children.getOrElse(name, throw new FileNotFoundException(s"$name is not an element in $path"))
        def getOrCreateChild(name: String, _newNode: => Node): Node = children.getOrElse(name, {
            val newNode = _newNode
            children += name -> newNode
            newNode
        })

        override def dir: Directory = this
        override def file: File = throw new FileNotFoundException(s"$path is not a file")
    }

    private class BlockWriteStream(filePointer: FilePointer, file: File) extends OutputStream {
        override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
            filePointer.write(bytes.slice(off, off + len))
        }

        override def write(i: Int): Unit = {
            filePointer.write(Array((i & 0xFF).toByte))
        }

        override def flush(): Unit = {
            super.flush()
            filePointer.flush()
        }

        override def close(): Unit = {
            super.close()
            file.content = filePointer.close()
            file.endWriting()
        }
    }

    private class BlockReadStream(filePointer: FilePointer, file: File) extends InputStream {
        private val oneByteArray: Array[Byte] = Array(1)

        override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
            filePointer.read(ByteBuffer.wrap(bytes, off, len))
        }

        override def read(): Int = {
            if (read(oneByteArray) == -1) {
                -1
            } else {
                oneByteArray.head & 0xFF
            }
        }

        override def close(): Unit = {
            super.close()
            filePointer.close()
            file.endReading()
        }
    }

    // lock for the filesystem
    private val lock = new ReadWriteLock
    private val fsFile: File = new File("__fs", null)
    private val root: Directory = if (_openExisting) loadFileSystem() else new Directory("", null)
    private var closed = false

    private def splitPath(path: String): Seq[String] = {
        if (!path.startsWith("/") || path.tail.endsWith("/")) {
            throw new IllegalArgumentException("Expected absolute path")
        }

        path.split("/")
            .filter(_.nonEmpty)
    }

    private def getNode(path: String): Node = {
        getNode(splitPath(path))
    }

    private def getNode(pathParts: Seq[String]): Node = {
        pathParts.foldLeft[Node](root) {
            case (cur, part) =>
                cur.dir.getChild(part)
        }
    }
    private def getOrCreateDir(pathParts: Seq[String]): Directory = {
        val node = pathParts.foldLeft[Node](root) {
            case (cur, part) =>
            val curDir = cur.dir
            curDir.getOrCreateChild(part, new Directory(part, curDir))
        }
        node.dir
    }

    private def getOrCreateFile(pathParts: Seq[String]): File = {

        val fileName = pathParts.last
        val dir = getOrCreateDir(pathParts.init)
        dir.getOrCreateChild(fileName, new File(fileName, dir)).file
    }

    /**
      * Creates an OutputStream which writes into the logical file referenced by `path`.
      *
      * If `path` does not exist, it is created. Otherwise the OutputStream will override the present content if append is not true.
      *
      * @param path the file that should be written to
      * @param append if true the OutputStream will append data to the file, otherwise it will overwrite the present content
      * @return an OutputStream that write to the logical file
      */
    def createOutputStream(path: String, append: Boolean = true): OutputStream = lock.write {
        val pathParts = splitPath(path)

        if (pathParts.isEmpty) {
            throw new IllegalArgumentException("Can not write to root")
        }

        val file = getOrCreateFile(pathParts)

        file.startWriting()
        val content = file.content

        val filePointer = service.createFilePointer(content.firstBlockIdx, content.size, append)
        new BlockWriteStream(filePointer, file)
    }

    /**
      * Creates an InputStream which reads from the logical file referenced by `path`
      *
      * @param path the file that should be read
      * @return an InputStream that reads the logical file
      */
    def createInputStream(path: String): InputStream = lock.read {
        val file = getNode(path).file
        file.startReading()

        val content = file.content

        val filePointer = service.createFilePointer(content.firstBlockIdx, content.size, append = false)
        new BlockReadStream(filePointer, file)
    }

    /**
      * Checks whether a given path is either a directory or a file
      *
      * @param path the path that should be checked
      * @return true if the path references a directory or a file, false otherwise
      */
    def exists(path: String): Boolean = lock.read {
        Try {
            getNode(path)
        }.isSuccess
    }

    /**
      * Moves a path into another directory
      *
      * @param targetPath the path that should be moved
      * @param destinationPath the directory where the path should be moved
      */
    def move(targetPath: String, destinationPath: String): Unit = lock.write {
        val target = getNode(targetPath)
        if (target == root) {
            throw new IllegalArgumentException("Cannot move root directory")
        }

        val destination = getOrCreateDir(splitPath(destinationPath))

        val name = target.name

        if (destination.children contains name) {
            throw new IllegalArgumentException(s"$name already exists in ${destination.path}")
        }

        target.parent.children -= name
        target.parent = destination
        destination.children += (name -> target)
    }

    /**
      * Renames a file or a directory
      *
      * @param targetPath the file or directory that should be renamed
      * @param newName the new name
      */
    def rename(targetPath: String, newName: String): Unit = lock.write {
        val target = getNode(targetPath)
        if (target == root) {
            throw new IllegalArgumentException("Cannot move root directory")
        }

        val oldName = target.name
        target.parent.children -= oldName
        target.name = newName
        target.parent.children += (newName -> target)
    }

    /**
      * Deletes a file or a directory
      *
      * @param targetPath the path that should be deleted
      */
    def delete(targetPath: String): Unit = lock.write {
        val target = getNode(targetPath)
        if (target == root) {
            throw new IllegalArgumentException("Cannot delete root directory")
        }

        def deleteNode(node: Node): Unit = {
            node match {
                case file: File =>
                    if (file.currentlyAccessed) {
                        throw new IllegalArgumentException("File is currently in use!")
                    }
                    file.content.firstBlockIdx.foreach(service.deleteBlockChain)
                case dir: Directory =>
                    dir.children.values.foreach(deleteNode)
            }
            node.parent.children -= node.name
        }

        deleteNode(target)
    }

    /**
      * Returns all direct subdirectories or files of a given directory.
      *
      * @param path The path of the directory that should be listed
      * @return an Iterable with all direct subdirectories and files
      */
    def list(path: String): Iterable[String] = lock.read {
        getNode(path).dir.children.keys
    }

    /**
      * Closes the file system
      */
    override def close(): Unit = {
        saveFileSystem()
        service.fsSize = Some(fsFile.content.size)
        service.stopService()
    }

    private def createFileSystemFilePointer(size: Long): FilePointer = {
        service.createFilePointer(Some(0), size, append = false)
    }

    private def saveFileSystem(): Unit = lock.read {
        fsFile.startWriting()
        val outputStream = new DataOutputStream(new BufferedOutputStream(new BlockWriteStream(createFileSystemFilePointer(0), fsFile), service.blockSize))
        def writeNode(node: Node): Unit = node match {
            case file: File =>
                // is file?, name, block, size
                outputStream.writeBoolean(true /* is file*/)
                outputStream.writeUTF(file.name)
                outputStream.writeInt(file.content.firstBlockIdx.getOrElse(-1))
                outputStream.writeLong(file.content.size)
            case dir: Directory =>
                outputStream.writeBoolean(false)
                outputStream.writeUTF(dir.name)
                outputStream.writeInt(dir.children.size)
                dir.children.values.foreach(writeNode)
        }

        try {
            writeNode(root)
        } finally {
            outputStream.close()
        }
    }

    private def loadFileSystem(): Directory = lock.write {
        fsFile.startReading()
        val inputStream = new DataInputStream(new BufferedInputStream(new BlockReadStream(createFileSystemFilePointer(service.fsSize.get), fsFile), service.blockSize))

        def readNode(parent: Directory): Node = {
            val isFile = inputStream.readBoolean()
            if (isFile) {
                readFile(parent)
            } else {
                readDirectory(parent)
            }
        }

        def readFile(parent: Directory): File = {
            val name = inputStream.readUTF()
            val firstBlock = Some(inputStream.readInt()).filter(_ >= 0)
            val size = inputStream.readLong()

            val content = FileContent(firstBlock, size)
            new File(name, parent, content)
        }

        def readDirectory(parent: Directory): Directory = {
            val name = inputStream.readUTF()
            val numChildren = inputStream.readInt()

            val dir = new Directory(name, parent)
            for (i <- 0 until numChildren) {
                val child = readNode(dir)
                dir.children += child.name -> child
            }
            dir
        }

        try {
            val isFile = inputStream.readBoolean()
            if (isFile) {
                throw new IOException("Filesystem corrupt. Expected directory as first fs entry")
            }
            readDirectory(null)
        } finally {
            inputStream.close()
        }
    }
}

object OneFileFS {
    private def create(service: BlockAccessService, openExisting: Boolean): OneFileFS = {
        try {
            new OneFileFS(service, openExisting)
        } catch {
            case NonFatal(e) =>
                service.stopService()
                throw e
        }
    }

    /**
      * Opens an already existing fileystem
      *
      * @param underlyingFilepath the path to the filesystem
      * @return the filesystem
      */
    def apply(underlyingFilepath: String): OneFileFS = create(new BlockAccessService(underlyingFilepath), openExisting = true)

    /**
      * Creates a new fileystem
      *
      * @param underlyingFilepath the path where the logical filesystem should be saved physically
      * @param blockSize the amount of bytes in a physical block
      * @param blocksPerChunk the amount of blocks in a chunk
      * @return the newly created, empty filesystem
      */
    def apply(underlyingFilepath: String, blockSize: Int, blocksPerChunk: Int = 512): OneFileFS = create(new BlockAccessService(underlyingFilepath, blockSize, blocksPerChunk), openExisting = false)
}
