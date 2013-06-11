package simpledb;

import java.util.HashMap;
import java.util.Vector;

import simpledb.CombinationEnumerator.Enumeration;

/**
 * A PlanCache is a helper class that can be used to store the best way to order a given set of joins
 */
public class EfficientPlanCache {
    HashMap<Enumeration, Vector<LogicalJoinNode>> bestOrders = new HashMap<Enumeration, Vector<LogicalJoinNode>>();
    HashMap<Enumeration, Double> bestCosts = new HashMap<Enumeration, Double>();
    HashMap<Enumeration, Integer> bestCardinalities = new HashMap<Enumeration, Integer>();

    /**
     * Add a new cost, cardinality and ordering for a particular join set. Does not verify that the new cost is less
     * than any previously added cost -- simply adds or replaces an existing plan for the specified join set
     * 
     * @param comb
     *            the set of joins for which a new ordering (plan) is being added
     * @param cost
     *            the estimated cost of the specified plan
     * @param card
     *            the estimatied cardinality of the specified plan
     * @param order
     *            the ordering of the joins in the plan
     */
    void addPlan(Enumeration comb, double cost, int card, Vector<LogicalJoinNode> order) {
        bestOrders.put(comb, order);
        bestCosts.put(comb, cost);
        bestCardinalities.put(comb, card);
    }

    /**
     * Find the best join order in the cache for the specified plan
     * 
     * @param comb
     *            the set of joins to look up the best order for
     * @return the best order for s in the cache
     */
    Vector<LogicalJoinNode> getOrder(Enumeration comb) {
        return bestOrders.get(comb);
    }

    /**
     * Find the cost of the best join order in the cache for the specified plan
     * 
     * @param comb
     *            the set of joins to look up the best cost for
     * @return the cost of the best order for s in the cache
     */
    double getCost(Enumeration comb) {
        return bestCosts.get(comb);
    }

    /**
     * Find the cardinality of the best join order in the cache for the specified plan
     * 
     * @param comb
     *            the set of joins to look up the best cardinality for
     * @return the cardinality of the best order for s in the cache
     */
    int getCard(Enumeration comb) {
        return bestCardinalities.get(comb);
    }
}
