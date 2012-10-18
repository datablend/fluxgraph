package com.jnj.fluxgraph;

import clojure.lang.Keyword;
import com.tinkerpop.blueprints.TimeAwareElement;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import datomic.Database;
import datomic.Entity;
import datomic.Peer;
import datomic.Util;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.*;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public abstract class FluxElement implements TimeAwareElement {

    protected final Database database;
    protected final FluxGraph fluxGraph;
    protected Object uuid;
    protected Object id;

    protected FluxElement(final FluxGraph fluxGraph, final Database database) {
        this.database = database;
        this.fluxGraph = fluxGraph;
        // UUID used to retrieve the actual datomic id later on
        uuid = Keyword.intern(UUID.randomUUID().toString());
        id = Peer.tempid(":graph");
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public Object getTimeId() {
        return FluxUtil.getActualTimeId(getDatabase(), this);
    }

    @Override
    public boolean isCurrentVersion() {
        return database == null;
    }

    @Override
    public boolean isDeleted() {
        // An element is deleted if we can no longer find any reference to it in the current version of the graph
        Collection<List<Object>> found = (Peer.q("[:find ?id " +
                                                  ":in $ ?id " +
                                                  ":where [?id _ _ ] ]", getDatabase(), id));
        return found.isEmpty();
    }

    @Override
    public Set<String> getPropertyKeys() {
        if (isDeleted()) {
            throw new IllegalArgumentException("It is not possible to get properties on a deleted element");
        }
        Set<String> finalproperties = new HashSet<String>();
        Set properties = getDatabase().entity(id).keySet();
        Iterator<Keyword> propertiesit = properties.iterator();
        while (propertiesit.hasNext()) {
            Keyword property = propertiesit.next();
            if (!FluxUtil.isReservedKey(property.toString())) {
                finalproperties.add(FluxUtil.getPropertyName(property));
            }
        }
        return finalproperties;
    }

    @Override
    public Object getProperty(final String key) {
        if (isDeleted()) {
            throw new IllegalArgumentException("It is not possible to get properties on a deleted element");
        }
        if (!FluxUtil.isReservedKey(key)) {
            Set properties = getDatabase().entity(id).keySet();
            Iterator<Keyword> propertiesit = properties.iterator();
            // We need to iterate, as we don't know the exact type (although we ensured that only one attribute will have that name)
            while (propertiesit.hasNext()) {
                Keyword property = propertiesit.next();
                String propertyname = FluxUtil.getPropertyName(property);
                if (key.equals(propertyname)) {
                    return getDatabase().entity(id).get(property);
                }
            }
            // We didn't find the value
            return null;
        }
        else {
            return getDatabase().entity(id).get(key);
        }
    }

    @Override
    public void setProperty(final String key, final Object value) {
        validate();
        if (key.equals(StringFactory.ID))
            throw ExceptionFactory.propertyKeyIdIsReserved();
        if (key.equals(StringFactory.LABEL))
            throw new IllegalArgumentException("Property key is reserved for all nodes and edges: " + StringFactory.LABEL);
        if (key.equals(StringFactory.EMPTY_STRING))
            throw ExceptionFactory.elementKeyCanNotBeEmpty();
        // A user-defined property
        if (!FluxUtil.isReservedKey(key)) {
            // If the property does not exist yet, create the attribute if required and create the appropriate transaction
            if (getProperty(key) == null) {
                // We first need to create the new attribute on the fly
                FluxUtil.createAttributeDefinition(key, value.getClass(), this.getClass(), fluxGraph);
                fluxGraph.addToTransaction(Util.map(":db/id", id,
                        FluxUtil.createKey(key, value.getClass(), this.getClass()), value));
            }
            else {
                // Value types match, just perform an update
                if (getProperty(key).getClass().equals(value.getClass())) {
                    fluxGraph.addToTransaction(Util.map(":db/id", id,
                            FluxUtil.createKey(key, value.getClass(), this.getClass()), value));
                }
                // Value types do not match. Retract original fact and add new one
                else {
                    FluxUtil.createAttributeDefinition(key, value.getClass(), this.getClass(), fluxGraph);
                    fluxGraph.addToTransaction(Util.list(":db/retract", id,
                            FluxUtil.createKey(key, value.getClass(), this.getClass()), getProperty(key)));
                    fluxGraph.addToTransaction(Util.map(":db/id", id,
                            FluxUtil.createKey(key, value.getClass(), this.getClass()), value));
                }
            }
        }
        // A datomic graph specific property
        else {
            fluxGraph.addToTransaction(Util.map(":db/id", id,
                    key, value));
        }
        fluxGraph.addTransactionInfo(this);
        fluxGraph.transact();
    }

    public Interval getTimeInterval() {
        DateTime startTime = new DateTime(FluxUtil.getTransactionDate(fluxGraph, getTimeId()));
        TimeAwareElement nextElement = this.getNextVersion();
        if (nextElement == null) {
            return new Interval(startTime, new DateTime(Long.MAX_VALUE));
        }
        else {
            DateTime stopTime = new DateTime(FluxUtil.getTransactionDate(fluxGraph, nextElement.getTimeId()));
            return new Interval(startTime, stopTime);
        }
    }

    @Override
    public Object removeProperty(final String key) {
        validate();
        Object oldvalue = getProperty(key);
        if (oldvalue != null) {
            if (!FluxUtil.isReservedKey(key)) {
                fluxGraph.addToTransaction(Util.list(":db/retract", id,
                                       FluxUtil.createKey(key, oldvalue.getClass(), this.getClass()), oldvalue));
            }
        }
        fluxGraph.addTransactionInfo(this);
        fluxGraph.transact();
        return oldvalue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FluxElement that = (FluxElement) o;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    protected Database getDatabase() {
        if (database == null) {
            return fluxGraph.getRawGraph();
        }
        return database;
    }

    private void validate() {
        if (!isCurrentVersion()) {
            throw new IllegalArgumentException("It is not possible to set a property on a non-current version of the element");
        }
        if (isDeleted()) {
            throw new IllegalArgumentException("It is not possible to set a property on a deleted element");
        }
    }

    // Creates a collection containing the set of datomic facts describing this entity
    protected Set<Object> getFacts() {
        // Create the set of facts
        Set<Object> theFacts = new HashSet<Object>();
        // Get the entity
        Entity entity = getDatabase().entity(id);
        // Add the base facts associated with this edge
        Set properties = entity.keySet();
        Iterator<Keyword> propertiesIt = properties.iterator();
        while (propertiesIt.hasNext()) {
            Keyword property = propertiesIt.next();
            // Add all properties (except the ident property (is only originally used for retrieving the id of the created elements)
            if (!property.toString().equals(":db/ident")) {
                theFacts.add(FluxUtil.map(":db/id", id, property.toString(), entity.get(property).toString()));
            }
        }
        return theFacts;
    }

}