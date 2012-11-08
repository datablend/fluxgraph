package com.tinkerpop.rexster.config;

import com.jnj.fluxgraph.FluxGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.rexster.Tokens;
import org.apache.commons.configuration.Configuration;

/**
 * Rexster configuration for FluxGraph.  Accepts configuration in rexster.xml as follows:
 *
 * <code>
 *  <graph>
 *    <graph-name>fluxgraphexample</graph-name>
 *    <graph-type>com.tinkerpop.rexster.config.FluxGraphConfiguration</graph-type>
 *    <graph-location>datomic:free://localhost:4334/flux</graph-location>
 *  </graph>
 * </code>
 *
 * To deploy copy the FluxGraph jar (with dependencies) to the Rexster ext directory.   Ensure that the FluxGraph
 * is running.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FluxGraphConfiguration implements GraphConfiguration {

    @Override
    public Graph configureGraphInstance(Configuration properties) throws GraphConfigurationException {
        final String graphFile = properties.getString(Tokens.REXSTER_GRAPH_LOCATION);

        if (graphFile == null || graphFile.length() == 0) {
            throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_LOCATION);
        }

        try {

            return new FluxGraph(graphFile);

        } catch (Exception ex) {
            throw new GraphConfigurationException(ex);
        }
    }
}
