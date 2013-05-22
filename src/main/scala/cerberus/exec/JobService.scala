package cerberus.exec

trait JobService {
  val JavaBinary = sys.env.getOrElse("JAVA_HOME", "/usr")+"/bin/java"
  val ClassPath = sys.props.getOrElse("java.class.path","")
  val WorkingDir = sys.props.getOrElse("user.dir","/")

  // disparity between heap size and java process size
  //val DefaultMem = MemoryLimit("2G", "1700m")

  def spawnJob(className: String, args: Array[String]): String
  def shutdown(): Unit

  def snooze(ms: Int=1000) {
    try {
      Thread.sleep(ms)
    } catch {
      case _: java.lang.InterruptedException => { }
    }
  }
}

class LocalJobService extends JobService {
  def spawnJob(className: String, args: Array[String]): String = {
    java.lang.Runtime.getRuntime.exec(Array(JavaBinary, "-cp", ClassPath, className) ++ args)

    "locally" //nonsense id
  }
  def shutdown() { }
}


