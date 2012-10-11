package com.tinkerpop.blueprints;

import java.util.Date;

/**
 * A time aware graph supports an explicit notion of time for all graph elements (vertices and edges). Each element has a specific scope in time and allows for time-scoped iteration
 * where a user can retrieve previous or next versions of each element
 *
 * @author Davy Suvee (http://datablend.be)
 */
public interface TimeAwareGraph extends Graph {

    /**
     * Sets the time scope of the graph on a specific date
     * @param time the time at which the scope should be placed
     */
    public void setCheckpointTime(Date time);

    /**
     * Sets the time scope at which new elements (vertices and edges) need to be added
     * @param time the time at which the transaction scope should be placed
     */
    public void setTransactionTime(Date time);

    /**
     * Calculates the difference for a particular working set of vertices and edges at two different points in time
     * @param workingSet the working set of vertices and edges to calculate the difference for
     * @param date1 the first point in time
     * @param date2 the second point in time
     * @return an (immutable) graph representing the difference in working set
     */
    public Graph difference(WorkingSet workingSet, Date date1, Date date2);

    /**
     * Calculates the difference for a particular graph element (vertex or edge)
     * Difference is only calculated for the elements with the same id. In case the their id's differs, an exception should be thrown
     * @param element1 the first element
     * @param element2 the second element
     * @return an (immutable) graph representing the difference between the two graph elements
     */
    public Graph difference(TimeAwareElement element1, TimeAwareElement element2);

}
