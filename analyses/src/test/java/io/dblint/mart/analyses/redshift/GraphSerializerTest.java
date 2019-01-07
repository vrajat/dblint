package io.dblint.mart.analyses.redshift;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphSerializerTest {
  @Test
  void simpleGraph() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    SimpleModule module = new SimpleModule();
    module.addSerializer(Dag.Graph.class, new GraphSerializer());
    mapper.registerModule(module);

    MutableGraph<String> graph = GraphBuilder.directed().allowsSelfLoops(true).build();
    graph.putEdge("a", "b");
    graph.putEdge("b", "c");
    String serialized = mapper.writeValueAsString(new Dag.Graph(ImmutableGraph.copyOf(graph)));
    assertEquals("{\"nodes\":[\"a\",\"b\",\"c\"],"
        + "\"edges\":[{\"source\":\"a\",\"target\":\"b\"},{\"source\":\"b\",\"target\":\"c\"}]}"
        , serialized);
  }
}
