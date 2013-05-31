package cerberus

import cerberus.exec._
import cerberus.io._
import scala.reflect.ClassTag
import collection.{GenTraversableOnce, Seq, SeqProxy}
import math.Ordering

import scala.reflect.runtime.universe._


class FlowGraph {
  var nodes: Map[Int,FlowNode]
  var edges: Set[Edge]
  
  /**
   * Anything that can be computed is a FlowNode
   */
  trait FlowNode {
    def available: Boolean
    def done: Boolean
  }
  class FlowSource() extends FlowNode {
    def available = true
    def done = true
  }
  class FlowMapping() extends FlowNode { 
    var storedPath: Option[String] = None
    var computed = false
    
    def available = storedPath.isSome
    def done = computed
  }
  class FlowSink() extends FlowNode {
    var sunk = false
    def available = true
    def done = sunk
  }

  /**
   * Any connection is an edge
   */
  case class Edge(val in: Int, val out: Int)
  // Building the graph:

  def addSource(): Int = {
    val id = nodes.size
    nodes += (id, new Source)
    id
  }
  def addMapping(src: Int): Int = {
    val id = nodes.size
    nodes += (id, new Mapping)

  }

  // accessing the graph
  def apply(id: Int): FlowNode = nodes(id)
  def children(id: Int) = edges.filter(_.in == id).map(_.out)
  def parent(id: Int) = edges.filter(_.out == id).map(_.in)

  def srcs: Set[FlowNode] = nodes.filter { 
    case _: Source => true
    case _ => false
  }
  def steps: Set[FlowNode] = nodes.filter {
    case _: Mapping => true
    case _ => false
  }
  def sinks: Set[FlowNode] = nodes.filter {
    case _: Sink => true
    case _ => false
  }
}

object Flow {
  def main(args: Array[String]) {
    
  }
}

object FlowBuilder {

}

abstract class FlowBuilder[T](implicit graph: FlowGraph) = {
  def map[U] = 
}

