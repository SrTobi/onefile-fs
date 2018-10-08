import java.io.PrintStream

import onefile.OneFileFS

object Example {

    private def write(): Unit = {
        val fs = OneFileFS("example.fs", 128)

        try {

            def logStuff(id: Int): Unit = {
                val stream = new PrintStream(fs.createOutputStream("/log " + id + ".log"))
                try {
                    for (i <- 1 to 1000) {
                        stream.println(s"Thread $id says $i")
                    }
                } finally {
                    stream.close()
                }
            }

            val threads =
                for (id <- 1 to 10) yield {
                    val thread = new Thread(() => logStuff(id))
                    thread.start()
                    thread
                }

            threads.foreach(_.join())

        } finally {
            fs.close()
        }
    }

    private def read(): Unit = {
        val fs = OneFileFS("example.fs")

        try {
            val source = io.Source.fromInputStream(fs.createInputStream("/log 3.log"))

            try {
                source.getLines().foreach(println)

            } finally {
                source.close()
            }

        } finally {
            fs.close()
        }
    }


    def main(args: Array[String]): Unit = {
        write()
        read()
    }
}
