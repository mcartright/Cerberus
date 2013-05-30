package cerberus.exec

import cerberus.io._

/**
 * Custom subclass of Functions that allows for extra init() and close() methods
 */
abstract class FlowFunction[A,B] extends Function1[A,B] with Encodable {
  def init() { }
  def close() { }
  def apply(x: A): B
}


/**
 * utility methods that duck types using reflection;
 *
 * calling init and close if they're available on function objects
 */
object TryInitAndClose {
  def init(duck: Any) {
    val methods = duck.getClass.getMethods.filter { m => 
      m.getName.contains("init") &&
      m.getParameterTypes.size == 0 &&
      m.getTypeParameters.size == 0
    }

    if(methods.nonEmpty) methods.head.invoke(duck)
  }
  def close(duck: Any) {
    val methods = duck.getClass.getMethods.filter { m => 
      m.getName == "close" &&
      m.getParameterTypes.size == 0 &&
      m.getTypeParameters.size == 0
    }

    if(methods.nonEmpty) methods.head.invoke(duck)
  }
}


