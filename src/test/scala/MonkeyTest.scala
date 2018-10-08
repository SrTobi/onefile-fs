import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit}

import onefile.OneFileFS
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

// a test that executes random operations on the filesystem
// a simple in-memory reference filesystem is used to check the validity
class MonkeyTest extends FunSuite {

    class File {
        var readers = 0
        def reading: Boolean = readers > 0
        var writing = false
        var data = Array.empty[Byte]
        val monitor = new Object
    }
    val random = new Random(100)
    val randNames: Array[String] = Array.fill(8) { random.alphanumeric.take(4).mkString }

    def randomName(): String = randNames(random.nextInt(randNames.length))

    def makeRandomDirectory(): String = {
        "/" + ((1 to random.nextInt(4)) map { _ => "d" + randomName() } mkString "/")
    }

    val dirnames: Array[String] = Array.fill(15) { makeRandomDirectory() }

    def randomDirectory(): String = {
        dirnames(random.nextInt(dirnames.length))
    }

    val monitor = new Object
    val referenceFs = mutable.Map.empty[String, File]
    val fsFile = "test.txt"
    var fs = OneFileFS(fsFile, 2048, 128)

    def randomFilename(): String = randomDirectory() + "/f" + randomName()

    def doWrite(): Unit = {
        val filename = randomFilename()
        val append = random.nextBoolean() && random.nextBoolean()
        val data = new Array[Byte](random.nextInt(1000))
        random.nextBytes(data)

        val (reffile, stream) = monitor.synchronized {
            val reffile = referenceFs.getOrElseUpdate(filename, new File)
            reffile.monitor.synchronized {
                if (reffile.writing || reffile.reading) {
                    return
                }
                reffile.writing = true
                reffile -> fs.createOutputStream(filename, append)
            }
        }
        println("action " + (if(append) "append " else "write  ") + filename + " " + Thread.currentThread().getId)

        try {
            if (data.length == 0) {
                stream.write(Array.empty[Byte])
            } else {
                data.grouped(1 + random.nextInt(data.length)).foreach(stream.write)
            }
        } finally {
            stream.close()
        }

        reffile.monitor.synchronized {
            if (append)
                reffile.data ++= data
            else if (reffile.data.length <= data.length)
                reffile.data = data
            else {
                reffile.data = reffile.data.clone()
                Array.copy(data, 0, reffile.data, 0, data.length)
            }

            reffile.writing = false
        }
    }

    def doRead(): Unit = {
        val filename = randomFilename()

        val (reffile, stream) = monitor.synchronized {
            val reffile = referenceFs.getOrElse(filename, {
                assert(!fs.exists(filename))
                return
            })
            reffile.monitor.synchronized {
                if (reffile.writing) {
                    return
                }
                reffile.readers += 1
            }
            reffile -> fs.createInputStream(filename)
        }
        println("action read   " + filename + " " + Thread.currentThread().getId)

        val data = try {
            Stream.continually(stream.read).takeWhile(_ != -1).map(_.toByte).toArray
        } finally {
            stream.close()
        }

        reffile.monitor.synchronized {
            assert(data sameElements reffile.data)
            reffile.readers -= 1
        }
    }

    def doMove(): Unit = {
        val file = "/f" + randomName()
        val fromDir = randomDirectory()
        val fromFile = fromDir + file
        val toDir = randomDirectory()
        val toFile = toDir + file

        monitor.synchronized {
            if (referenceFs.contains(fromFile) && !referenceFs.contains(toFile)) {
                println(s"action move   $fromFile -> $toFile")
                fs.move(fromFile, toDir)
                if (toFile != fromFile) {
                    referenceFs += toFile -> referenceFs(fromFile)
                    referenceFs -= fromFile
                }
                assert(fs.exists(toFile))
            }
        }
    }

    def doRename(): Unit = {
        val dir = randomDirectory()
        val oldName = "f" + randomName()
        val newName = "f" + randomName()

        val oldPath = dir + "/" + oldName
        val newPath = dir + "/" + newName

        monitor.synchronized {
            if (referenceFs.contains(oldPath) && !referenceFs.contains(newPath)) {
                println(s"action rename $oldPath -> $newName")
                fs.rename(oldPath, newName)
                if (oldPath != newPath) {
                    referenceFs += newPath -> referenceFs(oldPath)
                    referenceFs -= oldPath
                }
                assert(fs.exists(newPath))
            }
        }
    }

    def doList(): Unit = {
        val dir = randomDirectory()

        monitor.synchronized {
            val children = referenceFs.keys
                .filter(_.startsWith(dir))
                .map(_.substring(dir.length).dropWhile(_ == '/').split("/").head)
                .toSet
            if (children.nonEmpty) {
                println(s"action list   $dir -> [${children.mkString(", ")}]")
                val list = fs.list(dir).toSet
                assert(list == children)
            }
        }
    }

    def doDelete(): Unit = {
        val filename = randomFilename()

        monitor.synchronized {
            if (referenceFs.get(filename).exists(file => !file.reading && !file.writing)) {
                println(s"action delete $filename")
                fs.delete(filename)
                referenceFs -= filename

                assert(!fs.exists(filename))
            }
        }
    }

    val actions = Array(doWrite _, doRead _, doMove _, doRename _/*, doList _*/, doDelete _)

    def doAction(): Unit = {
        val action = actions(random.nextInt(actions.length))
        action()
    }



    test("monkey test") {

        var failed: AtomicReference[Option[Throwable]] = new AtomicReference(None)
        for (_ <- 1 to 50) {
            val pool = Executors.newFixedThreadPool(16)
            for (_ <- 1 to 10000) {
                pool.submit(new Runnable {
                    override def run(): Unit = try {
                        doAction()
                    } catch {
                        case NonFatal(e) =>
                            failed.compareAndSet(None, Some(e))
                    }
                })
            }

            pool.shutdown()
            pool.awaitTermination(1000, TimeUnit.SECONDS)

            failed.get().foreach(e => fail(e))

            println("reload")
            fs.close()
            fs = OneFileFS(fsFile)
        }
    }
}
