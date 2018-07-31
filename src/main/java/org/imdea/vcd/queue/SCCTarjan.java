package org.imdea.vcd.queue;

import static java.lang.Math.min;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 *
 * Based on
 * https://github.com/williamfiset/Algorithms/blob/master/com/williamfiset/algorithms/graphtheory/TarjanSccSolverAdjacencyList.java
 *
 * @author Vitor Enes
 */
public class SCCTarjan {

    private int n;
    private List<List<Integer>> graph;

    private boolean solved;
    private int sccCount, id;
    private boolean[] onStack;
    private int[] ids, low;
    private Deque<Integer> stack;

    private static final int UNVISITED = -1;

    public SCCTarjan(List<List<Integer>> graph) {
        if (graph == null) {
            throw new IllegalArgumentException("Graph cannot be null.");
        }
        n = graph.size();
        this.graph = graph;
    }

    // Returns the number of strongly connected components in the graph.
    public int sccCount() {
        if (!solved) {
            solve();
        }
        return sccCount;
    }

    // Get the connected components of this graph. If two indexes
    // have the same value then they're in the same SCC.
    public int[] getSccs() {
        if (!solved) {
            solve();
        }
        return low;
    }

    public void solve() {
        if (solved) {
            return;
        }

        ids = new int[n];
        low = new int[n];
        onStack = new boolean[n];
        stack = new ArrayDeque<>();
        Arrays.fill(ids, UNVISITED);

        for (int i = 0; i < n; i++) {
            if (ids[i] == UNVISITED) {
                dfs(i);
            }
        }

        solved = true;
    }

    private void dfs(int at) {

        stack.push(at);
        onStack[at] = true;
        ids[at] = low[at] = id++;

        for (int to : graph.get(at)) {
            if (ids[to] == UNVISITED) {
                dfs(to);
            }
            if (onStack[to]) {
                low[at] = min(low[at], low[to]);
            }
        }

        // On recursive callback, if we're at the root node (start of SCC) 
        // empty the seen stack until back to root.
        if (ids[at] == low[at]) {
            for (int node = stack.pop();; node = stack.pop()) {
                onStack[node] = false;
                low[node] = ids[at];
                if (node == at) {
                    break;
                }
            }
            sccCount++;
        }

    }

    // Initializes adjacency list with n nodes. 
    public static List<List<Integer>> createGraph(int n) {
        List<List<Integer>> graph = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            graph.add(new ArrayList<>());
        }
        return graph;
    }

    // Adds a directed edge from node 'from' to node 'to'
    public static void addEdge(List<List<Integer>> graph, int from, int to) {
        graph.get(from).add(to);
    }
}
