package onefile

private case class FileContent(var firstBlockIdx: Option[Int], size: Long = 0) {
    assert(firstBlockIdx.isDefined || size == 0)
}
