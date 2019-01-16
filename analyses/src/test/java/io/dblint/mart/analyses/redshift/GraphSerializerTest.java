package io.dblint.mart.analyses.redshift;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphSerializerTest {
  @Test
  void simpleGraph() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    SimpleModule module = new SimpleModule();
    module.addSerializer(Dag.Graph.class, new GraphSerializer());
    mapper.registerModule(module);

    MutableGraph<Dag.Node> graph = GraphBuilder.directed().allowsSelfLoops(true).build();
    Dag.Node aNode = new Dag.Node("a");
    aNode.addStartTime(LocalDateTime.of(2019, 1, 15, 6, 30));
    aNode.updateExecutionTimes(10);

    Dag.Node bNode = new Dag.Node("b");
    bNode.addStartTime(LocalDateTime.of(2019, 1, 15, 7, 30));
    bNode.updateExecutionTimes(10);

    Dag.Node cNode = new Dag.Node("c");
    cNode.addStartTime(LocalDateTime.of(2019, 1, 15, 7, 30));
    cNode.updateExecutionTimes(10);

    graph.putEdge(aNode, bNode);
    graph.putEdge(bNode, cNode);
    String serialized = mapper.writeValueAsString(new Dag.Graph(ImmutableGraph.copyOf(graph)));
    assertEquals("{\"nodes\":[{\"table\":\"a\",\"startTimes\":[\"2019-01-15 06:30:00\"],"
        + "\"mean\":10.0},{\"table\":\"b\","
        + "\"startTimes\":[\"2019-01-15 07:30:00\"],\"mean\":10.0}"
        + ",{\"table\":\"c\",\"startTimes\":[\"2019-01-15 07:30:00\"],"
        +    "\"mean\":10.0}]"
        +    ",\"edges\":[{\"source\":\"a\",\"target\":\"b\"},{\"source\":\"b\",\"target\":\"c\"}]}"
        , serialized);
  }
}
