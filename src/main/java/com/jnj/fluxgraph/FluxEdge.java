package com.jnj.fluxgraph;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import datomic.Database;
import datomic.Util;

import java.util.Set;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class FluxEdge extends FluxElement implements TimeAwareEdge {

    public FluxEdge(final FluxGraph fluxGraph, final Database database) {
        super(fluxGraph, database);
        fluxGraph.addToTransaction(Util.map(":db/id", id,
                                               ":graph.element/type", ":graph.element.type/edge",
                                               ":db/ident", uuid));
    }

    public FluxEdge(final FluxGraph fluxGraph, final Database database, final Object id) {
        super(fluxGraph, database);
        this.id = id;
    }

    @Override
    public TimeAwareEdge getPreviousVersion() {
        // Retrieve the previous version time id
        Object previousTimeId = FluxUtil.getPreviousTransaction(fluxGraph, this);
        if (previousTimeId != null) {
            // Create a new version of the edge timescoped to the previous time id
            return new FluxEdge(fluxGraph, fluxGraph.getRawGraph(previousTimeId), id);
        }
        return null;
    }

    @Override
    public TimeAwareEdge getNextVersion() {
        // Retrieve the next version time id
        Object nextTimeId = FluxUtil.getNextTransactionId(fluxGraph, this);
        if (nextTimeId != null) {
            // Create a new version of the edge timescoped to the next time id
            FluxEdge nextVertexVersion = new FluxEdge(fluxGraph, fluxGraph.getRawGraph(nextTimeId), id);
            // If no next version exists, the version of the edge is the current version (timescope with a null database)
            if (FluxUtil.getNextTransactionId(fluxGraph, nextVertexVersion) == null) {
                return new FluxEdge(fluxGraph, null, id);
            }
            else {
                return nextVertexVersion;
            }
        }
        return null;
    }

    @Override
    public Iterable<TimeAwareEdge> getNextVersions() {
        return new FluxTimeIterable(this, true);
    }

    @Override
    public Iterable<TimeAwareEdge> getPreviousVersions() {
        return new FluxTimeIterable(this, false);
    }

    @Override
    public Iterable<TimeAwareEdge> getPreviousVersions(TimeAwareFilter timeAwareFilter) {
        return new FluxTimeIterable(this, false, timeAwareFilter);
    }

    @Override
    public Iterable<TimeAwareEdge> getNextVersions(TimeAwareFilter timeAwareFilter) {
        return new FluxTimeIterable(this, true, timeAwareFilter);
    }

    @Override
    public TimeAwareVertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.OUT))
            return new FluxVertex(fluxGraph, database, getDatabase().datoms(Database.EAVT, getId(), fluxGraph.GRAPH_EDGE_OUT_VERTEX).iterator().next().v());
        else if (direction.equals(Direction.IN))
            return new FluxVertex(fluxGraph, database, getDatabase().datoms(Database.EAVT, getId(), fluxGraph.GRAPH_EDGE_IN_VERTEX).iterator().next().v());
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    @Override
    public String getLabel() {
        return (String)getDatabase().datoms(Database.EAVT, getId(), fluxGraph.GRAPH_EDGE_LABEL).iterator().next().v();
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Set<Object> getFacts() {
        // Retrieve the facts of the edge itself
        Set<Object> theFacts = super.getFacts();
        // Add the out and in vertex itself
        theFacts.add(FluxUtil.map(":db/id", getVertex(Direction.IN).getId(), ":graph.element/type", ":graph.element.type/vertex"));
        theFacts.add(FluxUtil.map(":db/id", getVertex(Direction.OUT).getId(), ":graph.element/type", ":graph.element.type/vertex"));
        return theFacts;
    }

    @Override
    public void remove() {
        fluxGraph.removeEdge(this);
    }
}
