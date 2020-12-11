import org.graphframes.GraphFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


import org.apache.spark.{SparkConf, SparkContext}
object WikiGraph {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Insufficient number of args")
    }
    val conf = new SparkConf().setAppName("Graphx")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder.
      master("local")
      .appName("TweetProcessing")
      .getOrCreate()
    val input_file = spark.read.option("header", "true").option("delimiter", "\t").csv(args(0)).toDF

    val from_vertices = input_file.select("FromNodeId").toDF("node_id").distinct()
    val to_vertices = input_file.select("ToNodeId").toDF("node_id").distinct()
    val vertex_set = from_vertices.union(to_vertices).toDF("id").distinct()

    val edge_set = input_file.select("FromNodeId", "ToNodeId").toDF("src","dst")

    val wiki_graph = GraphFrame(vertex_set, edge_set)

    val out_degree = wiki_graph.outDegrees

    var out_res="Out Degree\n"
    val out_degreee = out_degree.orderBy(desc("outDegree")).take(5)
    out_degreee.foreach { row =>
      row.toSeq.foreach{col => out_res=out_res.concat(col.toString)
      }
      out_res= out_res.concat("\n\n")
    }
    out_res=out_res.concat("\nIn Degree\n")

    val in_degree = wiki_graph.inDegrees

    val ir=in_degree.orderBy(desc("inDegree")).take(5)
    ir.foreach { row =>
      row.toSeq.foreach{col => out_res=out_res.concat(col.toString)
      }
      out_res= out_res.concat("\n\n")
    }

    val page_rank = wiki_graph.pageRank.resetProbability(0.15).maxIter(10).run()

    val pg=page_rank.vertices.orderBy(desc("pagerank")).select("id", "pagerank").take(10)
    out_res=out_res.concat("\nPageRank\n")
    pg.foreach { row =>
      row.toSeq.foreach{col => out_res=out_res.concat(col.toString)
      }
      out_res= out_res.concat("\n\n")
    }

    sc.setCheckpointDir("/checkpoints")
    val min_graph = GraphFrame(vertex_set, edge_set.sample(false, 0.1))
    val connected_components = min_graph.connectedComponents.run()

    val cd= connected_components.where("component != 0").orderBy(desc("component")).take(5)
    out_res= out_res.concat("\n\nconnected components\n")
    cd.foreach { row =>
      row.toSeq.foreach{col => out_res=out_res.concat(col.toString)
      }
      out_res= out_res.concat("\n\n")
    }

    val triangle_count = wiki_graph.triangleCount.run()
    val triangle_count_result = triangle_count.select("id", "count").orderBy(desc("count"))

    var results = triangle_count_result.take(5)
    out_res= out_res.concat("\n\nTriangle Count\n")
    results.foreach { row =>
      row.toSeq.foreach{col => out_res=out_res.concat(col.toString)
      }
      out_res= out_res.concat("\n\n")
    }
    val rdd = sc.parallelize(out_res.split("\n"))

    rdd.saveAsTextFile(args(1))
    sc.stop()
  }
}


