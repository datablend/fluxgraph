package com.jnj.fluxgraph;

import com.tinkerpop.blueprints.*;
import datomic.*;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created with IntelliJ IDEA.
 * User: dsuvee
 * Date: 09/09/12
 * Time: 16:42
 * To change this template use File | Settings | File Templates.
 */
public class ImmutableFluxGraph extends FluxGraph {

    private FluxGraph originGraph;

    public ImmutableFluxGraph(final String graphURI, FluxGraph originGraph, Set<Object> differenceFacts) {
        super(graphURI);
        this.originGraph = originGraph;
        // Add the additional meta model
        try {
            setupAdditionalMetaModel();
            // Add the various fact that together define the difference graph
            for (Object differenceFact : differenceFacts) {
                addToTransaction(differenceFact);
            }
            transact();
        } catch (ExecutionException e) {
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE);
        } catch (InterruptedException e) {
            throw new RuntimeException(FluxGraph.DATOMIC_ERROR_EXCEPTION_MESSAGE);
        }
    }

    public ImmutableFluxGraph(final String graphURI, final Date date) {
        super(graphURI);
        this.checkpointTime.set(date.getTime());
    }

    @Override
    public TimeAwareEdge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        throw new IllegalArgumentException("FluxGraph instance is immutable");
    }

    @Override
    public void removeEdge(Edge edge) {
        throw new IllegalArgumentException("FluxGraph instance is immutable");
    }

    @Override
    public TimeAwareVertex addVertex(Object id) {
        throw new IllegalArgumentException("FluxGraph instance is immutable");
    }

    @Override
    public void removeVertex(Vertex vertex) {
        throw new IllegalArgumentException("FluxGraph instance is immutable");
    }

    @Override
    public void setTransactionTime(Date transactionTime) {
        throw new IllegalArgumentException("FluxGraph instance is immutable");
    }

    @Override
    public void clear() {
        throw new IllegalArgumentException("FluxGraph instance is immutable");
    }

    protected void setupAdditionalMetaModel() throws ExecutionException, InterruptedException {
        // Add the attribute types contained in the origin graph
        // Retrieve the attributes
        Iterator<List<Object>> schemaIds = Peer.q("[:find ?id " +
                                                   ":where [?id :db/valueType _] " +
                                                          "[?id ?attribute ?value] ]", originGraph.getRawGraph().since(new Date(1))).iterator();

        // Add the various custom attributes
        while (schemaIds.hasNext())  {
            Entity t = originGraph.getRawGraph().entity(schemaIds.next().get(0));
            addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                                      ":db/ident", t.get(":db/ident"),
                                      ":db/valueType", t.get(":db/valueType"),
                                      ":db/cardinality", t.get(":db/cardinality"),
                                      ":db.install/_attribute", ":db.part/db"));
        }

        // Add the original id attribute types for both vertex and edge
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                ":db/ident", ":original$id.long.vertex",
                ":db/valueType", ":db.type/long",
                ":db/cardinality", ":db/cardinality",
                ":db.install/_attribute", ":db.part/db"));
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                ":db/ident", ":original$id.long.edge",
                ":db/valueType", ":db.type/long",
                ":db/cardinality", ":db/cardinality",
                ":db.install/_attribute", ":db.part/db"));

        // Transact it
        transact();
    }

}
