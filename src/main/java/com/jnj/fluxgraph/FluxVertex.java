package com.jnj.fluxgraph;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultQuery;
import com.tinkerpop.blueprints.util.MultiIterable;
import com.tinkerpop.blueprints.util.StringFactory;
import datomic.*;

import java.util.*;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class FluxVertex extends FluxElement implements TimeAwareVertex {

    protected FluxVertex(final FluxGraph fluxGraph, final Database database) {
        super(fluxGraph, database);
        fluxGraph.addToTransaction(Util.map(":db/id", id,
                                              ":graph.element/type", ":graph.element.type/vertex",
                                              ":db/ident", uuid));
    }

    public FluxVertex(final FluxGraph fluxGraph, final Database database, final Object id) {
        super(fluxGraph, database);
        this.id = id;
    }

    @Override
    public TimeAwareVertex getPreviousVersion() {
        // Retrieve the previous version time id
        Object previousTimeId = FluxUtil.getPreviousTransaction(fluxGraph, this);
        if (previousTimeId != null) {
            // Create a new version of the vertex timescoped to the previous time id
            return new FluxVertex(fluxGraph, fluxGraph.getRawGraph(previousTimeId), id);
        }
        return null;
    }

    @Override
    public TimeAwareVertex getNextVersion() {
        // Retrieve the next version time id
        Object nextTimeId = FluxUtil.getNextTransactionId(fluxGraph, this);
        if (nextTimeId != null) {
            FluxVertex nextVertexVersion = new FluxVertex(fluxGraph, fluxGraph.getRawGraph(nextTimeId), id);
            // If no next version exists, the version of the edge is the current version (timescope with a null database)
            if (FluxUtil.getNextTransactionId(fluxGraph, nextVertexVersion) == null) {
                return new FluxVertex(fluxGraph, null, id);
            }
            else {
                return nextVertexVersion;
            }
        }
        return null;
    }

    @Override
    public Iterable<TimeAwareVertex> getNextVersions() {
        return new FluxTimeIterable(this, true);
    }

    @Override
    public Iterable<TimeAwareVertex> getPreviousVersions() {
        return new FluxTimeIterable(this, false);
    }

    @Override
    public Iterable<TimeAwareVertex> getPreviousVersions(TimeAwareFilter timeAwareFilter) {
        return new FluxTimeIterable(this, false, timeAwareFilter);
    }

    @Override
    public Iterable<TimeAwareVertex> getNextVersions(TimeAwareFilter timeAwareFilter) {
        return new FluxTimeIterable(this, true, timeAwareFilter);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        if (direction.equals(Direction.OUT)) {
            return this.getOutEdges(labels);
        } else if (direction.equals(Direction.IN))
            return this.getInEdges(labels);
        else {
            return new MultiIterable<Edge>(Arrays.asList(this.getInEdges(labels), this.getOutEdges(labels)));
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        if (direction.equals(Direction.OUT)) {
            Iterator<Edge> edgesit = this.getOutEdges(labels).iterator();
            List<Object> vertices = new ArrayList<Object>();
            while (edgesit.hasNext()) {
                vertices.add(edgesit.next().getVertex(Direction.IN).getId());
            }
            return new FluxIterable(vertices, fluxGraph, database, Vertex.class);
        } else if (direction.equals(Direction.IN)) {
            Iterator<Edge> edgesit = this.getInEdges(labels).iterator();
            List<Object> vertices = new ArrayList<Object>();
            while (edgesit.hasNext()) {
                vertices.add(edgesit.next().getVertex(Direction.OUT).getId());
            }
            return new FluxIterable(vertices, fluxGraph, database, Vertex.class);
        }
        else {
            Iterator<Edge> outEdgesIt = this.getOutEdges(labels).iterator();
            List<Object> outvertices = new ArrayList<Object>();
            while (outEdgesIt.hasNext()) {
                outvertices.add(outEdgesIt.next().getVertex(Direction.IN).getId());
            }
            Iterator<Edge> inEdgesIt = this.getInEdges(labels).iterator();
            List<Object> invertices = new ArrayList<Object>();
            while (inEdgesIt.hasNext()) {
                invertices.add(inEdgesIt.next().getVertex(Direction.OUT).getId());
            }
            return new MultiIterable<Vertex>(Arrays.<Iterable<Vertex>>asList(new FluxIterable(outvertices, fluxGraph, database, Vertex.class), new FluxIterable(invertices, fluxGraph, database, Vertex.class)));
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Query query() {
        return new DefaultQuery(this);
    }

    @Override
    public Set<Object> getFacts() {
        // Retrieve the facts of the vertex itself
        Set<Object> theFacts = super.getFacts();
        // Get the facts associated with the edges of this vertex
        Iterator<Edge> edgesIt = getEdges(Direction.BOTH).iterator();
        while (edgesIt.hasNext()) {
            Edge edge = edgesIt.next();
            // Add the fact that the edge entity is an edge
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.element/type", ":graph.element.type/edge"));
            // Add the out and in vertex
            theFacts.add(FluxUtil.map(":db/id", edge.getVertex(Direction.IN).getId(), ":graph.element/type", ":graph.element.type/vertex"));
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.edge/inVertex", edge.getVertex(Direction.IN).getId()));
            theFacts.add(FluxUtil.map(":db/id", edge.getVertex(Direction.OUT).getId(), ":graph.element/type", ":graph.element.type/vertex"));
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.edge/outVertex", edge.getVertex(Direction.OUT).getId()));
            // Add the label
            theFacts.add(FluxUtil.map(":db/id", edge.getId(), ":graph.edge/label", edge.getLabel()));
        }
        return theFacts;
    }

    private Iterable<Edge> getInEdges(final String... labels) {
        if (labels.length == 0) {
            return getInEdges();
        }
        Collection<List<Object>> inEdges = Peer.q("[:find ?edge " +
                                                   ":in $ ?vertex [?label ...] " +
                                                   ":where [?edge :graph.edge/inVertex ?vertex] " +
                                                          "[?edge :graph.edge/label ?label ] ]", getDatabase(), id, labels);
        return new FluxIterable(inEdges, fluxGraph, database, Edge.class);
    }

    private Iterable<Edge> getInEdges() {
        Iterable<Datom> inEdges = getDatabase().datoms(Database.AVET, fluxGraph.GRAPH_EDGE_IN_VERTEX, getId());
        return new FluxIterable(inEdges, fluxGraph, database, Edge.class);
    }

    private Iterable<Edge> getOutEdges(final String... labels) {
        if (labels.length == 0) {
            return getOutEdges();
        }
        Collection<List<Object>> outEdges = Peer.q("[:find ?edge " +
                                                    ":in $ ?vertex [?label ...] " +
                                                    ":where [?edge :graph.edge/outVertex ?vertex] " +
                                                           "[?edge :graph.edge/label ?label ] ]", getDatabase(), id, labels);
        return new FluxIterable(outEdges, fluxGraph, database, Edge.class);
    }

    private Iterable<Edge> getOutEdges() {
        Iterable<Datom> outEdges = getDatabase().datoms(Database.AVET, fluxGraph.GRAPH_EDGE_OUT_VERTEX, getId());
        return new FluxIterable(outEdges, fluxGraph, database, Edge.class);
    }

}
