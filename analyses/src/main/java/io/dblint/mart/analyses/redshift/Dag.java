package io.dblint.mart.analyses.redshift;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;

import io.dblint.mart.sqlplanner.visitors.CopyVisitor;
import io.dblint.mart.sqlplanner.visitors.CtasVisitor;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import io.dblint.mart.sqlplanner.visitors.UnloadVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Dag {
  private static Logger logger = LoggerFactory.getLogger(Dag.class);

  static class Graph {
    public final ImmutableGraph<String> dag;
    public Graph(ImmutableGraph<String> dag) {
      this.dag = dag;
    }
  }

  static class Phase {
    public final Set<String> tables;

    public Phase(Set<String> tables) {
      this.tables = tables;
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
      if (info.classes.insertContext.isPassed()) {
        InsertVisitor visitor = info.classes.insertContext;
        visitor.getSources().forEach(src -> dag.putEdge(src, visitor.getTargetTable()));
      } else if (info.classes.ctasContext.isPassed()) {
        CtasVisitor visitor = info.classes.ctasContext;
        visitor.getSources().forEach(src -> dag.putEdge(src, visitor.getTargetTable()));
      } else if (info.classes.unloadContext.isPassed()) {
        UnloadVisitor visitor = info.classes.unloadContext;
        visitor.getSources().forEach(src -> dag.putEdge(src, "S3 Sink"));
      } else if (info.classes.copyContext.isPassed()) {
        CopyVisitor visitor = info.classes.copyContext;
        dag.putEdge("S3 Source", visitor.getTargetTable());
      }
    });
    return new Graph(ImmutableGraph.copyOf(dag));
  }

  static List<Phase> topologicalSort(Graph graph) {
    List<Phase> phases = new ArrayList<>();
    Map<Integer, Set<String>> numEdgesMap = new HashMap<>();

    graph.dag.nodes().forEach((node) -> {
      Integer numEdges = graph.dag.inDegree(node);
      if (!numEdgesMap.containsKey(numEdges)) {
        numEdgesMap.put(numEdges, new HashSet<>());
      }
      numEdgesMap.get(numEdges).add(node);
    });

    numEdgesMap.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> phases.add(new Phase(entry.getValue())));

    return phases;
  }
}
