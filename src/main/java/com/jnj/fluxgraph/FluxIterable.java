package com.jnj.fluxgraph;

import com.tinkerpop.blueprints.*;
import datomic.Database;
import datomic.Datom;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class FluxIterable<T extends Element> implements CloseableIterable<T> {

    private Iterable<Datom> datoms;
    private Collection<List<Object>> objects;
    private List<Object> ids;
    private final FluxGraph graph;
    private final Database database;
    private Class<T> clazz;

    private FluxIterable(final FluxGraph graph, final Database database, final Class<T> clazz) {
        this.graph = graph;
        this.clazz = clazz;
        this.database = database;
    }

    public FluxIterable(final Iterable<Datom> datoms, final FluxGraph graph, final Database database, final Class<T> clazz) {
        this(graph, database, clazz);
        this.datoms = datoms;
    }

    public FluxIterable(final Collection<List<Object>> objects, final FluxGraph graph, final Database database, final Class<T> clazz) {
        this(graph, database, clazz);
        this.objects = objects;
    }

    public FluxIterable(final List<Object> ids, final FluxGraph graph, final Database database, final Class<T> clazz) {
        this(graph, database, clazz);
        this.ids = ids;
    }

    public Iterator<T> iterator() {
        if (datoms != null) {
            return new DatomicDatomIterator();
        }
        else {
            if (objects != null) {
                return new DatomicQueryIterator();
            }
            else {
                return new DatomicIdIterator();
            }
        }
    }

    public void close() {
    }

    private abstract class DatomicIterator implements Iterator<T> {

        protected abstract Object getNext();

        public T next() {
            Object object = getNext();
            T ret = null;
            if (clazz == Vertex.class) {
                ret = (T) new FluxVertex(graph, database, object);
            } else if (clazz == Edge.class) {
                ret = (T) new FluxEdge(graph, database, object);
            } else {
                throw new IllegalStateException();
            }
            return ret;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    // Iterator for datomic datoms
    private class DatomicDatomIterator extends DatomicIterator {
        private Iterator<Datom> iterator = datoms.iterator();

        public boolean hasNext() {
            return iterator.hasNext();
        }

        protected Object getNext() {
            return iterator.next().e();
        }

    }

    // Iterator for datomic query results
    private class DatomicQueryIterator extends DatomicIterator {
        private Iterator<List<Object>> iterator = objects.iterator();

        public boolean hasNext() {
            return iterator.hasNext();
        }

        protected Object getNext() {
            return iterator.next().get(0);
        }

    }

    // Iterator for datomic ids
    private class DatomicIdIterator extends DatomicIterator {
        private Iterator<Object> iterator = ids.iterator();

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        protected Object getNext() {
            return iterator.next();
        }

    }

}