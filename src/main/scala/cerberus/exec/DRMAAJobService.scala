package cerberus.exec

import org.ggf.drmaa.{DrmaaException, JobTemplate, Session, SessionFactory}

class DRMAAJobService extends JobService {

  // initialze the DRMAA session
  var session = SessionFactory.getFactory.getSession()
  session.init("")

  def spawnJob(className: String, args: Array[String]): String = {
    val jt = session.createJobTemplate()
    jt.setJobName(className)
    jt.setRemoteCommand(JavaBinary)
    jt.setWorkingDirectory(WorkingDir)
    jt.setArgs(Array(
      "-cp", ClassPath, className
      ) ++ args)

    val id = session.runJob(jt)

    Time.snooze()

    if(session.getJobProgramStatus(id) == Session.FAILED) {
      Console.err.println("ERROR: Job Failed! "+className+" "+args.mkString(" ")+id)
    }

    // why isn't this deleted for you? ugh.
    session.deleteJobTemplate(jt)

    id
  }

  def shutdown() {
    session.exit()
  }
}

