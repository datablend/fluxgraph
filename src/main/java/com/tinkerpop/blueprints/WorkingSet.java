package com.tinkerpop.blueprints;

import java.util.HashSet;
import java.util.Set;

/**
 * Container used to defined the working set of vertices and edges
 * @author Davy Suvee (http://datablend.be)
 */
public class WorkingSet {

    public Set<Object> vertexIdList = new HashSet<Object>();
    public Set<Object> edgeIdList = new HashSet<Object>();

    public WorkingSet() {
    }

    public void addVertex(Vertex vertex) {
        vertexIdList.add(vertex.getId());
    }

    public void addEdge(Edge edge) {
        edgeIdList.add(edge.getId());
    }

    public Iterable<Object> getVertices() {
        return vertexIdList;
    }

    public Iterable<Object> getEdges() {
        return edgeIdList;
    }

}
