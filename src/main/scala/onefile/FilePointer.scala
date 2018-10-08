package onefile

import java.nio.ByteBuffer

private abstract class FilePointer {
    def write(data: Array[Byte]): Unit
    def read(buffer: ByteBuffer): Int
    def flush(): Unit
    def close(): FileContent
}
