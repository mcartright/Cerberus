package cerberus.io

object Util {
  import java.io._
  def deleteFile(path: String) {
    if(exists(path)) {
      try {
        (new File(path)).delete()
      } catch {
        case _: SecurityException => {
          println("Permission error on delete")
        }
      }
    }
  }

  def abspath(path: String): String = (new File(path)).getCanonicalPath
  def isDir(path: String): Boolean = {
    val fp = new File(path)
    fp.exists() && fp.isDirectory() && fp.canRead() && fp.canWrite()
  }
  def isFile(path: String): Boolean = {
    val fp = new File(path)
    fp.exists() && fp.isFile() && fp.canRead() && fp.canWrite()
  }
  def exists(path: String): Boolean = (new File(path)).exists()

  def freeSpace(path: String) = (new File(path)).getUsableSpace

  def listDir(path: String): Set[String] = {
    val dir = new File(path)
    if(dir.isDirectory()) {
      dir.listFiles().map(_.getCanonicalPath).toSet
    } else {
      Set(dir.getCanonicalPath)
    }
  }

  def delete(path: String) {
    val dir = new File(path)
    if(dir.isDirectory()) {
      // recurse if this is a directory
      dir.listFiles().foreach(fd => {
        if(fd.isDirectory()) {
          delete(fd.getCanonicalPath)
        } else {
          fd.delete()
        }
      })
    }
    dir.delete()
  }

  def makeParentDirs(f: File) {
    val parent = f.getParentFile()
    if(parent != null) {
      parent.mkdirs()
    }
  }

  def makeParentDirs(path: String) {
    makeParentDirs(new File(path))
  }

  def mkdir(path: String): String = {
    (new File(path)).mkdirs()
    path
  }

  def generatePath(path: String): String = {
    makeParentDirs(path)
    path
  }

  def fileLines(path: String): Iterator[String] = scala.io.Source.fromFile(path).getLines()
}
