package cerberus.exec

import cerberus.io._

class SharedConfig extends Encodable {
  val tmpDirectories = Set[String]("/tmp")
  val jobDirectory = "."
  val distrib = 10
}

class RuntimeConfig(
  val jobUniq: String,
  val shared: SharedConfig,
  val nodeId: Int=0,
  val split: Boolean = false
) extends Encodable {
  val myTempFolder = Util.mkdir("/tmp/"+jobUniq)

  def isSplitJob = split
  def numNodes = shared.distrib
  def nodeIds = (0 until numNodes)
  
  // for helping make files 
  var uid = 0
  def nextScratchName() = {
    uid += 1
    Util.generatePath(myTempFolder+"/scratch"+uid)
  }

  def deleteAllTemporaries() {
    Util.delete(myTempFolder)
  }
}

