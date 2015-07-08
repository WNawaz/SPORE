/*
 Based on the following paper
 Intra GraphTraversal Clustering using Collaborative Similarity Measure (DASFAA-2012)
 */
package GraphClus;

import RGraphAPI.GraphTraversal;
import java.util.List;

/**
 *
 * @author Waqas Nawaz (wicky786@[khu.ac.kr, gmail.com, oslab.khu.ac.kr]), Phd Sage, UCLab, Computer Engineering Depratement,Kyung Hee University Korea
 */
public class GCMemory {

//    /* Breif Description about data members
//     * 
//     */
//    private GraphTraversal graph;
//    private float alpha;
//    private int numClus;
//    private final float INF = 999999999.0f;
//
//    /*
//     * Zero-Argument Constructor
//     */
//    public GCMemory(GraphTraversal g) {
//        graph = g;
//        alpha = 1.0f;
//    }
//    //***************************//
//    /*
//     * Member functions
//     */
//
//    public void CalcSim() {
//        //calculate the similarity from one vertex to every other vertex
//        String node;
//        graph.ResetReadCounter();
//        while ((node = graph.ReadNextNode()) != null) {
//            Node2AllSim(node); // Dijkstra-like implementation 
//        }
//    }//end of function
//    //***************************//
//
//    public void FindClusters(int K, int iter) {
//        numClus = K;
//        if (numClus >= graph.CountNodes()) {
//            System.err.println("No. of clusters exceed the no. of objects");
//            System.exit(1);
//        }
//
//        //K-Medoid Clustering Strategy is utilized
//
//        //Choose the cluster centroids (Random K nodes)
//        List<String> centroids = graph.RandomKNodes(String.valueOf(numClus));
//
//        do {
//            //Cluster Nodes (modify the nodes cluster membership)
//            String node;
//            int c = 0;
//            graph.ResetReadCounter();
//            while ((node = graph.ReadNextNode()) != null) {
//                int clusID = 0;
//                float simVal = -1.0f;
//                for (c = 0; c < centroids.size(); c++) {
//                    if (!centroids.contains(node)) {
//                        float tempSim = graph.PairwiseSim(node, centroids.get(c));
//                        if (tempSim > simVal) {
//                            simVal = tempSim;
//                            clusID = c;
//                        }
//                    } else {
//                        clusID = centroids.indexOf(node);
//                    }
//                }
//                //update the cluster lable for this node
//                graph.UpdateClusterID(node, clusID + 1);
//            }
//
//            //evaluate the clustering solution
//            Density();
//
//            //update the centroids
//            for (c = 0; c < numClus; c++) {
//                List<String> clusNodes = graph.ReadClusterNodes(c + 1);
//                float value = -1.0f;
//                String newCenter = "";
//                for (int n = 0; n < clusNodes.size(); n++) {
//                    float simSum = 0.0f;
//                    for (int m = 0; m < clusNodes.size(); m++) {
//                        if (n != m) {
//                            simSum += graph.PairwiseSim(clusNodes.get(n), clusNodes.get(m));
//                        }
//                    }
//                    if (simSum > value) {
//                        value = simSum;
//                        newCenter = clusNodes.get(n);
//                    }
//                }
//                centroids.set(c, newCenter);
//            }
//
//            iter--;
//        } while (iter > 0);
//
//    }
//
//    private void Density() {
//
//        //density estimation
//        float density = 0.0f;
//        int c;
//        String node;
//        int clusECount[] = new int[numClus];
//        int totalEdges = graph.CountEdges();
//        for (c = 0; c < numClus; c++) {
//            clusECount[c] = 0;
//        }
//        graph.ResetReadCounter();
//        while ((node = graph.ReadNextNode()) != null) {
//            int clusID = graph.ReadClusterID(node);
//            List<String> edges = graph.ReadSeqEdges(node);
//            for (int e = 0; e < edges.size(); e++) {
//                if (clusID == graph.ReadClusterID(edges.get(e))) {
//                    clusECount[clusID - 1] += 1;
//                }
//            }
//        }
//        for (c = 0; c < numClus; c++) {
//            density += (float) clusECount[c] / totalEdges;
//        }
//        System.out.println("\nDensity = " + density);
//
//    }
//
//    private float DirectStrctSim(String node1, String node2) {
//        float sim;
//        float weight = graph.ReadEdgeWeight(node1, node2);
//        int common = graph.CommonNeighbors(node1, node2);
//        int neighbors1 = graph.ReadAdjNodes(node1).size();
//        int neighbors2 = graph.ReadAdjNodes(node2).size();
//        sim = ((float) common / (neighbors1 + neighbors2 - common)) * weight;
//        return sim;
//    }//end of function
//    //***************************//
//
//    private void Node2AllSim(String node) {
//        //This uses a fast 1 to ALL SP algorithm based in a Dijkstra-like implementation
//        //using a window-queue with minimum variable size (studied and developed in 2010 by E.Tiakas)
//        //for an un-directed graph
//        int nodeCount = graph.CountNodes();
//        List<String> adjNodes;
//        float dist[] = new float[nodeCount];
//        float simList[] = new float[nodeCount];
//        int inque[] = new int[nodeCount];
//        int q[] = new int[nodeCount + 1];
//        float qw[] = new float[nodeCount + 1];
//        int i, v, u;
//        int qe, qs, qv, temp;
//        float p, mind, weight, tempw;
//
//        for (i = 0; i < nodeCount; i++) {
//            dist[i] = INF;
//            inque[i] = 0;
//            simList[i] = 0.0f;
//        }
//        for (i = 0; i < nodeCount + 1; i++) {
//            q[i] = -1;
//            qw[i] = INF;
//        }
//        qs = 0;
//        qe = 0;
//        v = Integer.parseInt(node) - 1;
//        dist[v] = 0;
//        simList[v] = 1;
//        p = 1.0f;
//
//        while (qe >= qs) {
//            //Traverse the adjacent neighbors
//            adjNodes = graph.ReadAdjNodes(String.valueOf(v + 1));
//            if (adjNodes != null) {
//                for (i = 0; i < adjNodes.size(); i++) {
//                    u = Integer.parseInt(adjNodes.get(i)) - 1;
//                    //read the edge weight from DB
//                    if (v < u) {
//                        weight = graph.ReadEdgeWeight(String.valueOf(v + 1), String.valueOf(u + 1));
//                    } else {
//                        weight = graph.ReadEdgeWeight(String.valueOf(u + 1), String.valueOf(v + 1));
//                    }
//                    //check the dist and compute the similarity
//                    if (dist[u] > dist[v] + weight) {
//                        dist[u] = dist[v] + weight;
//                        simList[u] = simList[v] * ((alpha * DirectStrctSim(String.valueOf(v + 1), String.valueOf(u + 1)))); // + ((1 - alpha) * DirectContextSim(v, u)));
//                    }
//                    //check wether it is already in queue or not, and update queue
//                    if (inque[u] == 0) {
//                        q[qe] = u;
//                        qw[qe] = dist[u];
//                        qe = qe + 1;
//                        inque[u] = 1;
//                    }
//                }
//            }
//            //Select the min-dist neighbor
//            mind = INF;
//            qv = qs;
//            for (i = qs; i < qe; i++) {
//                if (dist[q[i]] < mind) {
//                    mind = dist[q[i]];
//                    v = q[i];
//                    qv = i;
//                }
//            }
//            //swapping the values to process the min-dist neighbor first
//            temp = q[qv];
//            q[qv] = q[qs];
//            q[qs] = temp;
//
//            tempw = qw[qv];
//            qw[qv] = qw[qs];
//            qw[qs] = tempw;
//
//            qs = qs + 1;
//        }
//        //updating the similarity values
//        for (int j = 0; j < nodeCount; j++) {
//            if ((Integer.parseInt(node) - 1) != j) {
//                if ((Integer.parseInt(node) - 1) < j) {
//                    boolean status = graph.UpdateSimValue(node, String.valueOf(j + 1), simList[j]);
//                } else {
//                    boolean status = graph.UpdateSimValue(String.valueOf(j + 1), node, simList[j]);
//                }
//            }
//        }
//    }//end of function
//    //***************************//
}// GCMemory class

