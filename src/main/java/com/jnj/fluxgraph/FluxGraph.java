package com.jnj.fluxgraph;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import datomic.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * A Blueprints implementation of a graph on top of Datomic
 *
 * @author Davy Suvee (http://datablend.be)
 */
public class FluxGraph implements MetaGraph<Database>, KeyIndexableGraph, TimeAwareGraph {

    private final String graphURI;
    private final Connection connection;

    public final Object GRAPH_ELEMENT_TYPE;
    public final Object GRAPH_ELEMENT_TYPE_VERTEX;
    public final Object GRAPH_ELEMENT_TYPE_EDGE;
    public final Object GRAPH_EDGE_IN_VERTEX;
    public final Object GRAPH_EDGE_OUT_VERTEX;
    public final Object GRAPH_EDGE_LABEL;

    private final FluxIndex vertexIndex;
    private final FluxIndex edgeIndex;

    protected final ThreadLocal<List> tx = new ThreadLocal<List>() {
        protected List initialValue() {
            return new ArrayList();
        }
    };
    protected final ThreadLocal<Long> checkpointTime = new ThreadLocal<Long>() {
        protected Long initialValue() {
            return null;
        }
    };
    protected final ThreadLocal<Date> transactionTime = new ThreadLocal<Date>() {
        protected Date initialValue() {
            return null;
        }
    };

    private static final Features FEATURES = new Features();

    static {
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = false;
        FEATURES.isRDFModel = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsIndices = false;

        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.isWrapper = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsThreadedTransactions = false;
    }

    public FluxGraph(final String graphURI) {
        this.graphURI = graphURI;
        Peer.createDatabase(graphURI);
        // Retrieve the connection
        this.connection = Peer.connect(graphURI);

        try {
            // Setup the meta model for the graph
            if (requiresMetaModel()) {
                setupMetaModel();
            }
            // Retrieve the relevant ids for the properties (for raw index access later on)
            GRAPH_ELEMENT_TYPE = FluxUtil.getIdForAttribute(this, "graph.element/type");
            GRAPH_ELEMENT_TYPE_VERTEX = FluxUtil.getIdForAttribute(this, "graph.element.type/vertex");
            GRAPH_ELEMENT_TYPE_EDGE = FluxUtil.getIdForAttribute(this, "graph.element.type/edge");
            GRAPH_EDGE_IN_VERTEX = FluxUtil.getIdForAttribute(this, "graph.edge/inVertex");
            GRAPH_EDGE_OUT_VERTEX = FluxUtil.getIdForAttribute(this, "graph.edge/outVertex");
            GRAPH_EDGE_LABEL = FluxUtil.getIdForAttribute(this, "graph.edge/label");
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        // Create the required indexes
        this.vertexIndex = new FluxIndex("vertexIndex", this, null, Vertex.class);
        this.edgeIndex = new FluxIndex("edgeIndex", this, null, Edge.class);
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public void shutdown() {
        // No actions required
    }

    @Override
    public TimeAwareEdge getEdge(final Object id) {
        if (null == id)
            throw ExceptionFactory.edgeIdCanNotBeNull();
        try {
            return new FluxEdge(this, this.getRawGraph(), Long.valueOf(id.toString()).longValue());
        } catch (NumberFormatException e) {
            return null;
        } catch (RuntimeException re) {
            return null;
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
        Iterable<Datom> edges = this.getRawGraph().datoms(Database.AVET, GRAPH_ELEMENT_TYPE, GRAPH_ELEMENT_TYPE_EDGE);
        return new FluxIterable<Edge>(edges, this, this.getRawGraph(), Edge.class);
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return edgeIndex.get(key, value);
    }

    @Override
    public TimeAwareEdge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        // Create the new edge
        final FluxEdge edge = new FluxEdge(this, null);
        addToTransaction(Util.map(":db/id", edge.getId(),
                              ":graph.edge/label", label,
                              ":graph.edge/inVertex", inVertex.getId(),
                              ":graph.edge/outVertex", outVertex.getId()));

        // Update the transaction info of both vertices (moving up their current transaction)
        addTransactionInfo((TimeAwareVertex)inVertex, (TimeAwareVertex)outVertex);

        return edge;
    }

    @Override
    public TimeAwareVertex addVertex(final Object id) {
        return new FluxVertex(this, null);
    }

    @Override
    public TimeAwareVertex getVertex(final Object id) {
        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();
        try {
            final Long longId = Long.valueOf(id.toString()).longValue();
            return new FluxVertex(this, this.getRawGraph(), longId);
        } catch (NumberFormatException e) {
            return null;
        } catch (RuntimeException re) {
            return null;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        Iterable<Datom> vertices = this.getRawGraph().datoms(Database.AVET, this.GRAPH_ELEMENT_TYPE, this.GRAPH_ELEMENT_TYPE_VERTEX);
        return new FluxIterable<Vertex>(vertices, this, this.getRawGraph(), Vertex.class);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return vertexIndex.get(key, value);
    }

    @Override
    public Database getRawGraph() {
        if (checkpointTime.get() != null) {
            return getRawGraph(checkpointTime.get());
        }
        return connection.db();
    }

    @Override
    public void setCheckpointTime(Date date) {
        Long transaction = null;
        // Retrieve the transactions
        Iterator<List<Object>> tx = (Peer.q("[:find ?tx ?when " +
                                           ":where [?tx :db/txInstant ?when]]", connection.db().asOf(date))).iterator();
        while (tx.hasNext()) {
            List<Object> txobject = tx.next();
            Long transactionid = (Long)txobject.get(0);
            if (transaction == null) {
                transaction = transactionid;
            }
            else {
                if (transactionid > transaction) {
                    transaction = transactionid;
                }
            }
        }
        this.checkpointTime.set(transaction);
    }

    @Override
    public void setTransactionTime(Date transactionTime) {
        this.transactionTime.set(transactionTime);
    }

    @Override
    public Graph difference(WorkingSet workingSet, Date date1, Date date2) {
        Set<Object> factsAtDate1 = new HashSet<Object>();
        Set<Object> factsAtDate2 = new HashSet<Object>();
        // Set graph at checkpoint date1
        setCheckpointTime(date1);
        for (Object vertex : workingSet.getVertices()) {
            factsAtDate1.addAll(((FluxVertex)getVertex(vertex)).getFacts());
        }
        for (Object edge : workingSet.getEdges()) {
            factsAtDate1.addAll(((FluxEdge)getEdge(edge)).getFacts());
        }
        // Set graph at checkpoint date2
        setCheckpointTime(date2);
        for (Object vertex : workingSet.getVertices()) {
            factsAtDate2.addAll(((FluxVertex)getVertex(vertex)).getFacts());
        }
        for (Object edge : workingSet.getEdges()) {
            factsAtDate2.addAll(((FluxEdge)getEdge(edge)).getFacts());
        }
        // Calculate the difference between the facts of both time aware elements
        Set<Object> difference = FluxUtil.difference(factsAtDate1, factsAtDate2);
        return new ImmutableFluxGraph("datomic:mem://temp" + UUID.randomUUID(), this, difference);
    }

    @Override
    public Graph difference(TimeAwareElement element1, TimeAwareElement element2) {
        // Calculate the difference between the facts of both time aware elements
        Set<Object> difference = FluxUtil.difference(((FluxElement) element1).getFacts(), ((FluxElement) element2).getFacts());
        return new ImmutableFluxGraph("datomic:mem://temp" + UUID.randomUUID(), this, difference);
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, graphURI);
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        FluxUtil.removeAttributeIndex(key, elementClass, this);
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass) {
        FluxUtil.createAttributeIndex(key, elementClass, this);
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        return FluxUtil.getIndexedAttributes(elementClass, this);
    }

    public Date getTransactionTime() {
        return transactionTime.get();
    }

    public void clear() {
        Iterator<Vertex> verticesit = getVertices().iterator();
        while (verticesit.hasNext()) {
            removeVertex(verticesit.next(), false);
        }
        transact();
    }

    public Database getRawGraph(Object transaction) {
        if (transaction == null) {
            return connection.db();
        }
        return connection.db().asOf(transaction);
    }

    public void addToTransaction(Object o) {
        tx.get().add(o);
    }

    public void transact() {
        try {
            // We are adding a fact which dates back to the past. Add the required meta data on the transaction
            if (transactionTime.get() != null) {
                addToTransaction(datomic.Util.map(":db/id", datomic.Peer.tempid(":db.part/tx"), ":db/txInstant", transactionTime.get()));
            }
            connection.transact(tx.get()).get();
            tx.get().clear();
        } catch (InterruptedException e) {
            tx.get().clear();
            throw new RuntimeException(e.getMessage(), e);
        } catch (ExecutionException e) {
            tx.get().clear();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    // Ensures that add-transaction-info database function is called during the
    // transaction execution. This will setup the linked list of transactions
    public void addTransactionInfo(TimeAwareElement... elements) {
        for (TimeAwareElement element : elements) {
            addToTransaction(Util.list(":add-transaction-info", element.getId(), element.getTimeId()));
        }
    }

    @Override
    private void removeEdge(final Edge edge) {
        // Retract the edge element in its totality
        FluxEdge theEdge =  (FluxEdge)edge;
        addToTransaction(Util.list(":db.fn/retractEntity", theEdge.getId()));

        // Get the in and out vertex (as their version also needs to be updated)
        FluxVertex inVertex = (FluxVertex)theEdge.getVertex(Direction.IN);
        FluxVertex outVertex = (FluxVertex)theEdge.getVertex(Direction.OUT);

        // Update the transaction info of the edge and both vertices (moving up their current transaction)
        addTransactionInfo(theEdge, inVertex, outVertex);
    }

    @Override
    private void removeVertex(Vertex vertex) {
        // Retrieve all edges associated with this vertex and remove them one bye one
        Iterator<Edge> edgesIt = vertex.getEdges(Direction.BOTH).iterator();
        while (edgesIt.hasNext()) {
            removeEdge(edgesIt.next());
        }
        // Retract the vertex element in its totality
        addToTransaction(Util.list(":db.fn/retractEntity", vertex.getId()));

        // Update the transaction info of the vertex
        addTransactionInfo((FluxVertex)vertex);
    }

    // Helper method to check whether the meta model of the graph still needs to be setup
    protected boolean requiresMetaModel() {
        return !Peer.q("[:find ?entity " +
                       ":in $ " +
                       ":where [?entity :db/ident :graph.element/type] ] ", getRawGraph()).iterator().hasNext();
    }

    // Setup of the various attribute types required for FluxGraph
    protected void setupMetaModel() throws ExecutionException, InterruptedException {

        // The graph element type attribute
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph.element/type",
                              ":db/valueType", ":db.type/ref",
                              ":db/cardinality", ":db.cardinality/one",
                              ":db/doc", "A graph element type",
                              ":db/index", true,
                              ":db.install/_attribute", ":db.part/db"));

        // The graph vertex element type
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/user"),
                              ":db/ident", ":graph.element.type/vertex"));

        // The graph edge element type
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/user"),
                              ":db/ident", ":graph.element.type/edge"));

        // The incoming vertex of an edge attribute
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph.edge/inVertex",
                              ":db/valueType", ":db.type/ref",
                              ":db/cardinality", ":db.cardinality/one",
                              ":db/doc", "The incoming vertex of an edge",
                              ":db/index", true,
                              ":db.install/_attribute", ":db.part/db"));

        // The outgoing vertex of an edge attribute
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph.edge/outVertex",
                              ":db/valueType", ":db.type/ref",
                              ":db/cardinality", ":db.cardinality/one",
                              ":db/doc", "The outgoing vertex of an edge",
                              ":db/index", true,
                              ":db.install/_attribute", ":db.part/db"));

        // The outgoing vertex of an edge attribute
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph.edge/label",
                              ":db/valueType", ":db.type/string",
                              ":db/cardinality", ":db.cardinality/one",
                              ":db/doc", "The label of a vertex",
                              ":db/index", true,
                              ":db.install/_attribute", ":db.part/db"));

        // The previous transaction through which the entity (vertex or edge) was changed
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph.element/previousTransaction",
                              ":db/valueType", ":db.type/ref",
                              ":db/cardinality", ":db.cardinality/many",
                              ":db/doc", "The previous transactions of the elements that wer changed",
                              ":db/index", true,
                              ":db.install/_attribute", ":db.part/db"));

        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph.element/previousTransaction/elementId",
                              ":db/valueType", ":db.type/ref",
                              ":db/cardinality", ":db.cardinality/one",
                              ":db/doc", "The element id of the entity that was part of the previous transaction",
                              ":db/index", true,
                              ":db.install/_attribute", ":db.part/db"));

        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph.element/previousTransaction/transactionId",
                              ":db/valueType", ":db.type/ref",
                              ":db/cardinality", ":db.cardinality/one",
                              ":db/doc", "The transaction id for the entity that was part of the previous transaction",
                              ":db/index", true,
                              ":db.install/_attribute", ":db.part/db"));

        String addTransactionInfoCode =
          "Object transactInfoId = tempid(\":db.part/user\");\n" +
          "return list(list(\":db/add\", transactInfoId, \":graph.element/previousTransaction/transactionId\", lastTransaction), " +
                      "list(\":db/add\", transactInfoId, \":graph.element/previousTransaction/elementId\", id), " +
                      "list(\":db/add\", tempid(\":db.part/tx\"), \":graph.element/previousTransaction\", transactInfoId));\n";


        // Database function that retrieves the previous transaction and sets the new one
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/user"),
                              ":db/ident", ":add-transaction-info",
                              ":db/fn", Peer.function(Util.map("lang", "java",
                                                      "params", Util.list("db", "id", "lastTransaction"),
                                                      "code", addTransactionInfoCode))));

        // Add new graph partition
        addToTransaction(Util.map(":db/id", Peer.tempid(":db.part/db"),
                              ":db/ident", ":graph",
                              ":db.install/_partition", ":db.part/db"));

        addToTransaction(datomic.Util.map(":db/id", datomic.Peer.tempid(":db.part/tx"), ":db/txInstant", new Date(0)));
        connection.transact(tx.get()).get();
        tx.get().clear();
    }

}
