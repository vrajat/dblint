package io.dblint.mart.analyses.redshift;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;

import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Dag {
  private static Logger logger = LoggerFactory.getLogger(Dag.class);

  static class Graph {
    public final ImmutableGraph<String> dag;

    public Graph(ImmutableGraph<String> dag) {
      this.dag = dag;
    }
  }

  /**
   * Generate a di-graph with tables as nodes and insert data movement as dependency.
   * @param infos Query information POJO
   * @return A Guava immutable graph of inserts
   */
  public static Graph buildGraph(List<QueryInfo> infos) {
    MutableGraph<String> dag = GraphBuilder.directed().allowsSelfLoops(true).build();
    infos.forEach((info) -> {
          InsertVisitor visitor = info.classes.insertContext;
          visitor.getSources().forEach(src -> dag.putEdge(src, visitor.getTargetTable()));
        }
    );
    return new Graph(ImmutableGraph.copyOf(dag));
  }
}
