import org.specs2.Specification

class MergerTest extends Specification {
  def is = s2"""

    Specification of Merger

    Merger should
      merge empty graph                          $e1
      merge one-vertex and duplicate edges graph $e2
      merge bigger graph                         $e3
  """

  val g1 = Graph("g1", Set.empty[Vertex], Set.empty[Edge])
  val g2 = Graph("g2", Set(Vertex(0, "v0")), Set(Edge("e0", 0, 0), Edge("e1", 0, 0)))
  val g3 = {
    val vertices = Set(
      Vertex(1, "A"),
      Vertex(2, "B", Map("version" -> "1.1")),
      Vertex(3, "B", Map("name" -> "ubuntu")),
      Vertex(4, "C")
    )
    val edges = Set(Edge("e1", 1, 2), Edge("e2", 1, 3), Edge("e3", 2, 4), Edge("e4", 3, 4))
    Graph("g3", vertices, edges)
  }

  def e1 = Merger.merge(g1) must_=== g1

  def e2 = Merger.merge(g2) must_=== g2.copy(edges = Set(Edge("0-0", 0, 0)))

  def e3 = Merger.merge(g3) must_=== g3.copy(
    vertices = Set(
      Vertex(1, "A"),
      Vertex(2, "B", Map("version" -> "1.1", "name" -> "ubuntu")),
      Vertex(4, "C")
    ),
    edges = Set(Edge("1-2", 1, 2), Edge("2-4", 2, 4)))

  case class Graph(name: String, vertices: Set[Vertex], edges: Set[Edge])

  case class Edge(id: String, tail: Long, head: Long)

  case class Vertex(id: Long, label: String, properties: Map[String, String] = Map())

  object Merger {
    def merge(graph: Graph): Graph = {
      def reconcile(vertices: Set[Vertex], edges: Set[Edge]): (Set[Vertex], Set[Edge]) = {
        val groupedV = vertices.groupBy(_.label)
        val mapperV = groupedV.map(_._2).flatMap(l => l.map(g => (g.id, l.head))).toMap
        val recE = edges.map(e => Edge(mapperV(e.tail).id + "-" + mapperV(e.head).id, mapperV(e.tail).id, mapperV(e.head).id))
        val recV = groupedV.map(p => Vertex(p._2.head.id, p._2.head.label, p._2.flatMap(v => v.properties).toMap)).toSet
        (recV, recE)
      }
      val (mergedVertices, mergedEdges) = reconcile(graph.vertices, graph.edges)

      graph.copy(
        vertices = mergedVertices,
        edges = mergedEdges
      )
    }
  }

}