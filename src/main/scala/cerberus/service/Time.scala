package cerberus.service

object Time {
  def snooze(ms: Int=1000) {
    try {
      Thread.sleep(ms)
    } catch {
      case _: java.lang.InterruptedException => { }
    }
  }
}

