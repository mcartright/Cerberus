package cerberus.exec

import cerberus.io._

class SharedConfig extends Encodable {
  val tmpDirectories = Set[String]("/tmp")
}

class RuntimeConfig(
  val jobUniq: String,
  val shared: SharedConfig,
  val nodeId: Int=0,
  val numNodes:Int=1
) extends Encodable {
  // for helping make files 
  var uid = 0

  val myTempFolder = Util.mkdir("/tmp/"+jobUniq)
  
  def nextScratchName() = {
    uid += 1
    Util.generatePath(myTempFolder+"/scratch"+uid)
  }

  def deleteAllTemporaries() {
    Util.delete(myTempFolder)
  }
}

