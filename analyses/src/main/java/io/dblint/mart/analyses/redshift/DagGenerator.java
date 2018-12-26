package io.dblint.mart.analyses.redshift;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DagGenerator {
  private static Logger logger = LoggerFactory.getLogger(DagGenerator.class);

  /**
   * Generate a di-graph with tables as nodes and insert data movement as dependency.
   * @param infos Query information POJO
   * @return A Guava immutable graph of inserts
   */
  public static ImmutableGraph<String> buildGraph(List<QueryInfo> infos) {
    MutableGraph<String> dag = GraphBuilder.directed().allowsSelfLoops(true).build();
    infos.forEach(info -> info.sources.forEach(src -> dag.putEdge(src, info.targetTable)));
    logger.info(dag.toString());
    return ImmutableGraph.copyOf(dag);
  }
}
