/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package RGraphAPI;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Waqas Nawaz (wicky786@[khu.ac.kr, gmail.com, oslab.khu.ac.kr]), Phd
 * Sage, DKE Lab, Computer Engineering Depratement,Kyung Hee University Korea
 */
public class GraphTraversal {
    //data members

    private Connection conn;
    private Statement statement;
    private GraphDB gDBObj;
    private boolean index;

    private char indexChar = '_';
    private static String order = "desc";
    private float ALPHA, BETA; // alpha(SegTable), beta(SPORE)
    private static double inf = Double.POSITIVE_INFINITY;

    private int topDegN;
    private float density;
    private int expansions, SporeCount;
    private float preDensity, diffThd;
    private double SporeTime;
    //for logging
    private boolean doLog = true;
    private Writer log;
    //constructors

    public GraphTraversal(boolean logging, Writer w, boolean indexing, float alpha, float beta, String ordr) {
        // one arguemnt constructor
        density = 0.0f;
        SporeTime = 0.0d;
        expansions = 0;
        SporeCount = 0;
        preDensity = 0.0f;
        diffThd = 0.1f;
        ALPHA = alpha;
        BETA = beta;
        topDegN = 10;
        order = ordr;

        doLog = logging;
        index = indexing;
        if (index) {
            indexChar = 'I';
        }
        if (doLog) {
            log = new BufferedWriter(w);
        }
        gDBObj = new GraphDB(doLog, log, index);
        conn = gDBObj.getDBConnection();
        statement = gDBObj.getStatement();
        gDBObj.SetupGraphDB();

        /*
         * Augment the edges with similarity value calculated for each directly
         * connected pair of vertices
         */
        DirectSim();
    }

    public void Close() {
        gDBObj.Close();
    }

    //***** member functions ****//
    private void DirectSim() {
        try {
            if (doLog) {
                log.write("\nAugmentEdges...dataset:" + GraphDB.datasetName);
                log.flush();
            }
            double time = System.currentTimeMillis();

            //empty AEDGES table
            String query = "truncate table AEDGES";
            statement.executeUpdate(query);

            //Calculate similarity value for directly connected vertices (Transition Probability => TP(v1, vn) = 1/deg(v1))
            query = "INSERT INTO AEDGES (SELECT t2.fid,t2.tid, t2.fid, log ( 10, 1+ 1/(t2.elabel*t1.sim)) FROM (SELECT fid, 1/COUNT(*) AS sim FROM TEDGES GROUP BY (fid))t1, TEDGES t2 WHERE t1.fid=t2.fid)";
            statement.executeUpdate(query);
            gDBObj.DropTable("TEMP");
            query = "CREATE TABLE TEMP as select fid, tid, pid, 1.0*(val-minVal)/valRange as val from ( select fid, tid, pid, val, min(val) over() as minVal, max(val) over () - min(val) over () as valRange from AEDGES)";
            statement.executeUpdate(query);
            gDBObj.DropTable("AEDGES");
            query = "CREATE TABLE AEDGES as SELECT * from TEMP";
            statement.executeUpdate(query);
            gDBObj.DropTable("TEMP");
            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write(", Time:" + result);
                log.flush();
            }

            /**
             * Add edges from original graph to TOUTSEGS table only once
             */
            //DDL operation: empty the TOUTSETS table
            query = "truncate table TOUTSEGS";
            statement.executeUpdate(query);
            query = "insert into TOUTSEGS (fid, tid, pid, cost) SELECT ae.fid, ae.tid, ae.pid, ae.val from AEDGES ae";
            statement.executeUpdate(query);

        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void DensityCheck() {
        double time = System.currentTimeMillis();
        try {
            ResultSet rs = null;
            //Compute the density value from NCLUS and TEDGES tables 
            String query = "SELECT SUM(COUNT(*)/(2* "
                    + "  (SELECT COUNT(*)/2 FROM TEDGES "
                    + "  ))) AS density "
                    + "FROM "
                    + "  (SELECT fid, "
                    + "    tid, "
                    + "    clus_id "
                    + "  FROM TEDGES, "
                    + "    NCLUS "
                    + "  WHERE fid   = nid "
                    + "  AND clus_id = "
                    + "    (SELECT clus_id FROM NCLUS WHERE nid=tid "
                    + "    ) "
                    + "  ) "
                    + "GROUP BY clus_id";
            rs = statement.executeQuery(query);
            while (rs.next()) {
                density = rs.getFloat("density");
            }
            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write("\n\tDensity: " + density + " Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    public void RPAMElement_Serial(int K) {
        try {
            if (doLog) {
                log.write("\nRPAMElement(" + K + ")..." + "Alpha(" + ALPHA + ")..." + "Beta(" + BETA + ")...Order(" + order + ")");
                log.flush();
            }
            String query = "";
            int eStepExp = 0, mStepExp = 0, sporeExp = 0;
            double time = System.currentTimeMillis();
            double eStepTime = 0, mStepTime = 0, AvgDiff = 0.0d, APAvgDistClus = 0.0d, APAvgDistFull = 0.0d;
            ResultSet rs = null;
            Statement st = null;
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            //Compute the Stats
            query = "select min(val) as min, max(val) as max, avg(val) as avg, stddev(val) as std, median(val) as med, variance(val) as var from AEDGES";
            rs = st.executeQuery(query);
            if (rs.first()) {
                if (doLog) {
                    log.write("\n Graph-EdgeWeights>> min:" + rs.getDouble("min") + ", max:" + rs.getDouble("max") + ", avg:" + rs.getDouble("avg") + ", std:" + rs.getDouble("std") + ", var:" + rs.getDouble("var") + ", med:" + rs.getDouble("med"));
                    log.flush();
                    //BETA = (float) rs.getDouble("avg");
                    //ALPHA = BETA;
                }
            }
            /*
             * Construct Shortcuts using SegTable Approach
             */
            ConstructSegTable_Serial(ALPHA);
            /*
             * Initialize k centroids
             */
            //DDL operation: Empty Centroids table
            query = "truncate table CENTROIDS";
            statement.executeUpdate(query);
            if (order.equals("asc") || order.equals("desc")) {
                //Select centroids from TNODES table top degree nodes
                query = "insert into CENTROIDS select rownum, fid, 1  from (select fid, count(*) as degree from TEDGES group by fid order by degree " + order + ") where degree>=" + topDegN + " and rownum<=" + K;
            } else {
                //Select centroids from TNODES table randomly
                //query = "insert into CENTROIDS select rownum, nid, 1 from (select nid from TNODES order by dbms_random.value) where rownum <= " + K;
                query = "insert into CENTROIDS select rownum, fid, 1  from (select fid, count(*) as degree from TEDGES group by fid order by dbms_random.value) where degree>=" + topDegN + " and rownum<=" + K;
            }
            statement.executeUpdate(query);

            //DDL operation: Empty NCLUS table
            query = "truncate table NCLUS";
            statement.executeUpdate(query);
            //Intialize the NCLUS table
            query = "insert into NCLUS select nid, 0 from TNODES";
            statement.executeUpdate(query);

            int iter = 1;
            while (true) {
                if (doLog) {
                    log.write("\n Iteration:" + iter);
                    log.flush();
                }
                //Update the centroid's cluster membership in NCLUS
                query = "merge into NCLUS target using CENTROIDS src on (src.nid=target.nid) when matched then update set target.clus_id=src.cid";
                statement.executeUpdate(query);

                /*
                 * Compute similarity from K centroids to all the other vertices
                 */
                //DDL operation: Empty NSIM table
                query = "truncate table NSIM";
                statement.executeUpdate(query);

                //choose each centroid one by one for similarity computation
                query = "select nid from CENTROIDS";
                rs = st.executeQuery(query);
                String node_id = "";
                while (rs.next()) {
                    node_id = rs.getString("nid");
                    //Compute single source shortest path using Dijkstra's Algo
                    if (ALPHA >= BETA) {
                        Dijkstra(node_id, ALPHA, false);
                    } else {
                        Dijkstra(node_id, BETA, false);
                    }
                    eStepExp += expansions;
                    //Extract the SPOREs from SP-Tree rooted at current node
                    ExtractSPORES_Serial(BETA);
                    sporeExp += expansions;
                    //Insert values for similarity between centroids and nodes into NSIM from TVisited
                    query = "insert into NSIM select fid,nid,d2s from TVISITED where fid!=nid and nid not in (select nid from CENTROIDS)";
                    statement.executeUpdate(query);
                }

                /*
                 * Vertex association to nearest centroid
                 */
                //DDL operation:EMPTY NSIMMIN table
                query = "truncate table NSIMMIN";
                statement.executeUpdate(query);
                //NSIMNEW: Selective NSIM rows with minimum cost using CENTROIDS table
                query = "insert into NSIMMIN select src, dest, val from (select ROW_NUMBER() over (partition by dest order by val asc) as rid, src, dest, val from NSIM) where rid=1";
                statement.executeUpdate(query);
                //Determine the association of nodes with centroids using NSIM and store into NCLUS
                //kind of correlated update required here
                query = "merge into NCLUS target using (select nsm.dest,c.cid from NSIMMIN nsm, CENTROIDS c where nsm.src=c.nid) src on (target.nid = src.dest) when matched then update set target.clus_id = src.cid";
                statement.executeUpdate(query);

                eStepTime += System.currentTimeMillis() - time;
                //*************************************************************//
                //Estimate the quality of clusters
                DensityCheck();
                if (density - preDensity < 0) {
                    break;
                } else {
                    preDensity = density;
                }
//                if (Math.abs(density - preDensity) < diffThd) {
//                    break;
//                } else {
//                    preDensity = density;
//                }
                //dump the tables into files
                String prefix = indexChar + "E_Thd_" + ALPHA + "_" + BETA + "_" + order + "_", suffix = "k" + K + "iter" + iter, meta = "Density: " + density + "SPORE Construction Time: " + SporeTime;
                gDBObj.DumpStaticTables(prefix);
                gDBObj.DumpDynamicTables(prefix, suffix, meta);
                //*************************************************************//
                time = System.currentTimeMillis();

                /*
                 * Update the centroids
                 */
                boolean noChange = true;
                for (int clus_id = 1; clus_id <= K; clus_id++) {
                    if (doLog) {
                        log.write("\n\t\t(Cluster:" + clus_id + ")");
                        log.flush();
                    }
                    String best_centroid_id = "";
                    Double best_centroid_val = inf;
                    boolean isNewCentroid = false;
                    /*
                     * find the similarity among all pairs in each cluster, i.e.
                     * clus_id
                     */

                    //Set the avg similarity of current centroid as threshod for others
                    query = "select nid from CENTROIDS where cid=" + clus_id;
                    //st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    rs = st.executeQuery(query);
                    if (rs.first()) {
                        best_centroid_id = rs.getString("nid");

                        query = "select avg(val) as avgVal from NSIM where src=" + best_centroid_id + " and dest in (select nid from NCLUS where clus_id=" + clus_id + ")";
                        rs = st.executeQuery(query);
                        if (rs.first()) {
                            best_centroid_val = rs.getDouble("avgVal");
                        }
                    }

                    //DDL operation: empty the CEDGES table
                    query = "truncate table CEDGES";
                    statement.executeUpdate(query);
                    //Insert into CEDGES table (To store EDGES with in a cluster)
                    query = "insert into CEDGES select tos.fid, tos.tid, tos.pid, tos.cost from (select fid, tid, pid, min(cost) as cost from TOUTSEGS group by (fid, tid, pid)) tos where tos.fid in (select nid from NCLUS where clus_id=" + clus_id + ") and tos.tid in (select nid from NCLUS where clus_id=" + clus_id + ")";
                    statement.executeUpdate(query);

                    //Compute similarity among all pair of vertices in cluster clus_id using Dijkstra Algorithm
                    ResultSet rs2 = null;
                    Statement st2 = null;
                    st2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

                    query = "select nid from NCLUS where clus_id=" + clus_id + " and nid != " + best_centroid_id;
                    rs2 = st2.executeQuery(query);
                    node_id = "";

                    boolean isSpore = true;
                    while (rs2.next()) {
                        node_id = rs2.getString("nid");
//                        //Compute SSSP over entire graph
//                        if (ALPHA >= BETA) {
//                            Dijkstra(node_id, ALPHA, false);
//                        } else {
//                            Dijkstra(node_id, BETA, false);
//                        }
//                        query = "truncate table TVisited2";
//                        statement.executeUpdate(query);
//                        query = "insert into TVisited2 select * from TVisited";
//                        statement.executeUpdate(query);

                        // Compute SSSP using dijkstra algorithm
                        if (ALPHA >= BETA) {
                            Dijkstra(node_id, ALPHA, true);
                        } else {
                            Dijkstra(node_id, BETA, true);
                        }
                        mStepExp += expansions;

//                        //Compute the differenece in Similarity Values
//                        query = "select min(d2s) as min, max(d2s) as max, avg(d2s) as avg, stddev(d2s) as std, median(d2s) as med, variance(d2s) as var from TVisited";
//                        rs = st.executeQuery(query);
//                        if (rs.first()) {
//                            if (doLog) {
//                                log.write("\n\t\t\t SubGraph>> min:" + rs.getDouble("min") + ", max:" + rs.getDouble("max") + ", avg:" + rs.getDouble("avg") + ", std:" + rs.getDouble("std") + ", var:" + rs.getDouble("var") + ", med:" + rs.getDouble("med"));
//                                log.flush();
//                            }
//                        }
//                        query = "select min(d2s) as min, max(d2s) as max, avg(d2s) as avg, stddev(d2s) as std, median(d2s) as med, variance(d2s) as var from TVisited2";
//                        rs = st.executeQuery(query);
//                        if (rs.first()) {
//                            if (doLog) {
//                                log.write("\n\t\t\t FullGraph>> min:" + rs.getDouble("min") + ", max:" + rs.getDouble("max") + ", avg:" + rs.getDouble("avg") + ", std:" + rs.getDouble("std") + ", var:" + rs.getDouble("var") + ", med:" + rs.getDouble("med"));
//                                log.flush();
//                            }
//                        }
                        //Extract the SPOREs from SP-Tree rooted at current node
                        if (!isSpore) {
                            // once for each cluster
                            ExtractSPORES_Serial(BETA);
                            sporeExp += expansions;
                            isSpore = true;
                        }
                        //Insert values for similarity between nodes into NSIM from TVisited
                        query = "select avg(d2s) as avgVal from TVISITED where fid!=nid";
                        //query = "select avg(d2s) as avgVal from TVISITED where fid!=nid and nid in (select nid from NCLUS where clus_id=" + clus_id + ")";
                        rs = st.executeQuery(query);
                        if (rs.first()) {
                            Double temp = rs.getDouble("avgVal");
                            if (temp < best_centroid_val) {
                                best_centroid_val = temp;
                                best_centroid_id = node_id;
                                isNewCentroid = true;
                                noChange = false;
                            }
                        }
                    }

                    /*
                     * update the centroid (if different)
                     */
                    if (isNewCentroid) {
                        query = "update CENTROIDS set nid=" + best_centroid_id + ",flag=1 where cid=" + clus_id;
                        statement.executeUpdate(query);
                    }
                } //Update the centroids 
                mStepTime += System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                if (noChange) {
                    break;
                }
                //go for next iteration
                iter++;
            }//repeat RPAM until convergence
            //Count actual number of Segments (TOUTSEGS-AEDGES)
            Double edgeCount = 0.0d, shortcuts = 0.0d;
            query = "select count(*) as ECount from AEDGES";
            rs = st.executeQuery(query);
            if (rs.first()) {
                edgeCount = rs.getDouble("ECount");
            }
            query = "select count(*) as SEGS from TOUTSEGS";
            rs = st.executeQuery(query);
            if (rs.first()) {
                shortcuts = rs.getDouble("SEGS");
            }
            shortcuts = shortcuts - edgeCount;
            if (doLog) {
                log.write("\nRPAMElement Done. E-Step Expansions:" + eStepExp + ",M-Step Expansions:" + mStepExp + ",SPORE-Step Expansions:" + sporeExp + ", Time(E-step,M-step): (" + eStepTime + " , " + mStepTime + "), SPORE(time,UpdateCount,SPORES): (" + SporeTime + " , " + SporeCount + " , " + shortcuts + ")");
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void Dijkstra(String src_id, float thd, boolean isSubGraph) {
        try {
            if (doLog) {
                log.write("\n\tSSSP from " + src_id + ">>>...");
                log.flush();
            }
            double time = System.currentTimeMillis();
            int iteration = 0;
            //Statement statement=null;
            //DDL operation: clear the TVisited Table
            String query = "truncate table TVISITED";
            statement.executeUpdate(query);
            //(1) Initialize using source node

            query = "insert into TVISITED (nid, d2s, p2s, f, fid) values(" + src_id + ",0," + src_id + ",0," + src_id + ")";
            statement.executeUpdate(query);

            // (2) Searching shortest path (repeat expand,merge,update,drop)
            //double itertime = System.currentTimeMillis();
            int fwd = 1;
            while (true) {

                //F-Operator: selecting frontier node
                query = "update TVISITED set f=2 where nid in (select nid from TVISITED where f=0 and ( d2s<=" + (fwd * thd) + " or d2s=(select min(d2s) from TVISITED where f=0)) )";
                int row_count = statement.executeUpdate(query);
                if (row_count == 0) {
                    //if there is no frontier node then stop
                    break;
                }
                // select nid from TVISITED where f=0 and d2s=(select min(d2s) from TVISITED where f=0)
                //E-Operator: expand the frontier node
                if (isSubGraph) {
                    query = "CREATE VIEW EK(nid, p2s, dist, rnum) AS SELECT * FROM (SELECT ce.tid, ce.pid, ce.val+q.d2s, ROW_NUMBER() over (partition by ce.tid order by ce.val+q.d2s asc) rnum FROM TVISITED q, CEDGES ce WHERE q.nid=ce.fid and q.nid in (select nid from TVISITED where f=2) ) WHERE rnum=1";
                } else {
                    query = "CREATE VIEW EK(nid, p2s, dist, rnum) AS SELECT * FROM (SELECT tos.tid, tos.pid, tos.cost+q.d2s, ROW_NUMBER() over (partition by tos.tid order by tos.cost+q.d2s asc) rnum FROM TVISITED q, TOUTSEGS tos WHERE q.nid=tos.fid and q.nid in (select nid from TVISITED where f=2) ) WHERE rnum=1";
                }
                statement.executeUpdate(query);

                //M-Operator: merge the expanded result with the TVisited table
                query = "merge into TVISITED target using (select nid, p2s, dist from EK) src on (src.nid=target.nid) when matched then update set target.d2s=src.dist, target.p2s=src.p2s, f=0 where target.d2s>src.dist when not matched then insert (target.nid, target.d2s, target.p2s, target.f, target.fid) VALUES (src.nid, src.dist, src.p2s, 0, " + src_id + ")";
                statement.executeUpdate(query);

                //Finalize the frontier node
                query = "update TVISITED set f=1 where f=2";
                //System.out.println("updated nid:" + fr.tid);
                statement.executeUpdate(query);

                //Drop the view EK
                query = "drop view EK";
                statement.executeUpdate(query);

                fwd++;
                //System.out.println(System.currentTimeMillis() - itertime);
                iteration++;
            }
            expansions = iteration;

            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write("Expansions: " + iteration + " Time: " + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    public void RPAMSet_Serial(int K) {
        try {
            if (doLog) {
                log.write("\nRPAMSet(" + K + ")..." + "Alpha(" + ALPHA + ")..." + "Beta(" + BETA + ")...Order(" + order + "):FullGraph");
                log.flush();
            }
            String query = "";
            int eStepExp = 0, mStepExp = 0;
            double time = System.currentTimeMillis();
            double eStepTime = 0, mStepTime = 0;
            Double edgeCount = 0.0d, shortcuts = 0.0d, AvgDiff = 0.0d, APAvgDistClus = 0.0d, APAvgDistFull = 0.0d;
            ResultSet rs = null;
            Statement st = null;
            st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            //Compute the Stats
            query = "select min(val) as min, max(val) as max, avg(val) as avg, stddev(val) as std, median(val) as med, variance(val) as var from AEDGES";
            rs = st.executeQuery(query);
            if (rs.first()) {
                if (doLog) {
                    log.write("\n Graph-EdgeWeights>> min:" + rs.getDouble("min") + ", max:" + rs.getDouble("max") + ", avg:" + rs.getDouble("avg") + ", std:" + rs.getDouble("std") + ", var:" + rs.getDouble("var") + ", med:" + rs.getDouble("med"));
                    log.flush();
                    //BETA = (float) rs.getDouble("avg");
                    //ALPHA = BETA;
                }
            }
            /*
             * Construct Shortcuts using SegTable Approach
             */
            ConstructSegTable_Serial(ALPHA);
            /*
             * Initialize k centroids
             */
            //DDL operation: Empty Centroids table
            query = "truncate table CENTROIDS";
            statement.executeUpdate(query);
            if (order.equals("asc") || order.equals("desc")) {
                //Select centroids from TNODES table top degree nodes
                query = "insert into CENTROIDS select rownum, fid, 1  from (select fid, count(*) as degree from TEDGES group by fid order by degree " + order + ") where degree>=" + topDegN + " and rownum<=" + K;
            } else {
                //Select centroids from TNODES table randomly
                //query = "insert into CENTROIDS select rownum, nid, 1 from (select nid from TNODES order by dbms_random.value) where rownum <= " + K;
                query = "insert into CENTROIDS select rownum, fid, 1  from (select fid, count(*) as degree from TEDGES group by fid order by dbms_random.value) where degree>=" + topDegN + " and rownum<=" + K;
            }
            statement.executeUpdate(query);

            //DDL operation: Empty NCLUS table
            query = "truncate table NCLUS";
            statement.executeUpdate(query);
            //Intialize the NCLUS table
            query = "insert into NCLUS select nid, 0 from TNODES";
            statement.executeUpdate(query);

            int iter = 1;
            while (true) {
                if (doLog) {
                    log.write("\n Iteration:" + iter);
                    log.flush();
                }
                //Update the centroid's cluster membership in NCLUS
                query = "merge into NCLUS target using CENTROIDS src on (src.nid=target.nid) when matched then update set target.clus_id=src.cid";
                statement.executeUpdate(query);

                /*
                 * Compute similarity from K centroids to all the other vertices
                 */
                //DDL operation: Empty NSIM table
                query = "truncate table NSIM";
                statement.executeUpdate(query);
                //compute the similarity from each centroid to all the vertices
                if (ALPHA >= BETA) {
                    CentroidNSim(ALPHA);
                } else {
                    CentroidNSim(BETA);
                }
                eStepExp += expansions;
                expansions = 0;
                //Constructs SPOREs from SP-Tree 
                ExtractSPORES_Serial(BETA);
                //update the NSIM table with TVISITED table entries
                query = "insert into NSIM select fid,nid,d2s from TVISITED where fid!=nid and nid not in (select nid from CENTROIDS)";
                statement.executeUpdate(query);
                /*
                 * Vertex association to nearest centroid
                 */
                //DDL operation:EMPTY NSIMMIN table
                query = "truncate table NSIMMIN";
                statement.executeUpdate(query);
                //NSIMNEW: Selective NSIM rows with minimum cost using CENTROIDS table
                query = "insert into NSIMMIN select src, dest, val from (select ROW_NUMBER() over (partition by dest order by val asc) as rid, src, dest, val from NSIM) where rid=1";
                statement.executeUpdate(query);
                //Determine the association of nodes with centroids using NSIM and store into NCLUS
                //kind of correlated update required here
                query = "merge into NCLUS target using (select nsm.dest,c.cid from NSIMMIN nsm, CENTROIDS c where nsm.src=c.nid) src on (target.nid = src.dest) when matched then update set target.clus_id = src.cid";
                statement.executeUpdate(query);

                eStepTime += System.currentTimeMillis() - time;
                //*************************************************************//
                //Estimate the quality of clusters
                DensityCheck();
                if (density - preDensity < 0) {
                    break;
                } else {
                    preDensity = density;
                }
//                if (Math.abs(density - preDensity) < diffThd) {
//                    break;
//                } else {
//                    preDensity = density;
//                }
                //dump the tables into files
                String prefix = indexChar + "S_Thd_" + ALPHA + "_" + BETA + "_" + order + "_", suffix = "k" + K + "iter" + iter, meta = "Density: " + density + "SPORE Construction Time: " + SporeTime;
                gDBObj.DumpStaticTables(prefix);
                gDBObj.DumpDynamicTables(prefix, suffix, meta);
                //*************************************************************//
                time = System.currentTimeMillis();

                /*
                 * Update the centroids
                 */
                boolean noChange = true;
                for (int clus_id = 1; clus_id <= K; clus_id++) {
                    if (doLog) {
                        log.write("\n\t\t(Cluster:" + clus_id + ")");
                        log.flush();
                    }
                    String best_centroid_id = "";
                    Double best_centroid_val = inf;
                    boolean isNewCentroid = false;
                    /*
                     * find the similarity among all pairs in each cluster, i.e.
                     * clus_id
                     */

                    //Set the avg similarity of current centroid as threshod for others
                    query = "select nid from CENTROIDS where cid=" + clus_id;
                    //st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    rs = st.executeQuery(query);
                    if (rs.first()) {
                        best_centroid_id = rs.getString("nid");

                        query = "select avg(val) as avgVal from NSIM where src=" + best_centroid_id + " and dest in (select nid from NCLUS where clus_id=" + clus_id + ")";
                        rs = st.executeQuery(query);
                        if (rs.first()) {
                            best_centroid_val = rs.getDouble("avgVal");
                        }
                    }
//                    //Compute similarity among all pair of vertices in cluster clus_id using Dijkstra Algorithm
//                    if (ALPHA >= BETA) {
//                        ClustPWSim(clus_id, best_centroid_id, ALPHA, false);
//                    } else {
//                        ClustPWSim(clus_id, best_centroid_id, BETA, false);
//                    }
//                    query = "truncate table TVisited2";
//                    statement.executeUpdate(query);
//                    query = "insert into TVisited2 select * from TVisited";
//                    statement.executeUpdate(query);

                    //Compute similarity among all pair of vertices in cluster clus_id using Dijkstra Algorithm
                    if (ALPHA >= BETA) {
                        ClustPWSim(clus_id, best_centroid_id, ALPHA, true);
                    } else {
                        ClustPWSim(clus_id, best_centroid_id, BETA, true);
                    }
                    mStepExp += expansions;
                    expansions = 0;

//                    //Compute the differenece in Similarity Values
//                    query = "select min(d2s) as min, max(d2s) as max, avg(d2s) as avg, stddev(d2s) as std, median(d2s) as med, variance(d2s) as var from TVisited";
//                    rs = st.executeQuery(query);
//                    if (rs.first()) {
//                        if (doLog) {
//                            log.write("\n\t\t\t SubGraph>> min:" + rs.getDouble("min") + ", max:" + rs.getDouble("max") + ", avg:" + rs.getDouble("avg") + ", std:" + rs.getDouble("std") + ", var:" + rs.getDouble("var") + ", med:" + rs.getDouble("med"));
//                            log.flush();
//                        }
//                    }
//                    query = "select min(d2s) as min, max(d2s) as max, avg(d2s) as avg, stddev(d2s) as std, median(d2s) as med, variance(d2s) as var from TVisited2";
//                    rs = st.executeQuery(query);
//                    if (rs.first()) {
//                        if (doLog) {
//                            log.write("\n\t\t\t FullGraph>> min:" + rs.getDouble("min") + ", max:" + rs.getDouble("max") + ", avg:" + rs.getDouble("avg") + ", std:" + rs.getDouble("std") + ", var:" + rs.getDouble("var") + ", med:" + rs.getDouble("med"));
//                            log.flush();
//                        }
//                    }
                    //check for potential new centroid if exists
                    query = "select fid, avgVal from (select fid, avg(d2s) as avgVal from TVISITED group by fid order by avgVal asc) where rownum=1";
//                    query = "select fid, avgVal from (select fid, avg(d2s) as avgVal from TVISITED where fid in (select nid from NCLUS where clus_id=" + clus_id + ") group by fid order by avgVal asc) where rownum=1";
                    rs = st.executeQuery(query);
                    if (rs.first()) {
                        String newCentroidID = rs.getString("fid");
                        Double newVal = rs.getDouble("avgVal");
                        if (best_centroid_val > newVal) {
                            noChange = false;
                            isNewCentroid = true;
                            best_centroid_id = newCentroidID;
                            best_centroid_val = newVal;
                        }

                    }

                    /*
                     * update the centroid (if different)
                     */
                    if (isNewCentroid) {
                        query = "update CENTROIDS set nid = " + best_centroid_id + ",flag=1 where cid = " + clus_id;
                        statement.executeUpdate(query);
                    }
                } //Update the centroids 
                mStepTime += System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                if (noChange) {
                    break;
                }
                //go for next iteration
                iter++;
            }//repeat RPAM until convergence
            //Count actual number of Segments (TOUTSEGS-AEDGES)
            query = "select count(*) as ECount from AEDGES";
            rs = st.executeQuery(query);
            if (rs.first()) {
                edgeCount = rs.getDouble("ECount");
            }
            query = "select count(*) as SEGS from TOUTSEGS";
            rs = st.executeQuery(query);
            if (rs.first()) {
                shortcuts = rs.getDouble("SEGS");
            }
            shortcuts = shortcuts - edgeCount;
            if (doLog) {
                log.write("\nRPAMSet Done. E-Step Expansions:" + eStepExp + ",M-Step Expansions:" + mStepExp + ", Time(E-step,M-step): (" + eStepTime + " , " + mStepTime + "), SPORE(time,UpdateCount,SPORES): (" + SporeTime + " , " + SporeCount + " , " + shortcuts + ")");
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void CentroidNSim(float thd) {
        try {
            if (doLog) {
                log.write("\n\tCentroids Nodes Similarity...");
                log.flush();
            }
            double time = System.currentTimeMillis();
            int iterations = 0;
            String query = "";
            //(1) Initializing Tvisited table with source nodes
            //DDL operation: clear the TVisited Table
            query = "truncate table TVISITED";
            statement.executeUpdate(query);
            query = "insert into TVISITED (nid, d2s, p2s, f, fid) select c.nid as nid, '0' as d2s, c.nid as p2s, '0' as f, c.nid as fid from CENTROIDS c";
            statement.executeUpdate(query);

            String e_query = "CREATE table ek(nid, p2s, dist, rnum, flag, srcid) AS SELECT nid, p2s, d2s, '1', f, fid from TVisited";
            statement.executeUpdate(e_query);
            if (index) {
                String index_query_nid = "CREATE INDEX ek_nid ON ek (nid)";
                String index_query_srcid = "CREATE INDEX ek_srcid ON ek (srcid)";
                statement.executeUpdate(index_query_nid);
                statement.executeUpdate(index_query_srcid);
            }
            //boolean continue_loof=true;
            int fwd = 1;
            while (true) {
                //F-Operator: selecting frontier node
                query = "update EK set flag=2 where flag=0 and (nid, srcid) in (select t.nid, t.srcid from (select ROW_NUMBER() over (partition by srcid order by dist asc) as rid, srcid, nid, dist, flag from EK where flag=0) t where (t.rid =1 or t.dist<=" + (fwd * thd) + ") )";
                int row_count = statement.executeUpdate(query);
                if (row_count == 0) {
                    //if there is no frontier node then stop
                    break;
                }

                // create temporary view temp_ek
                /*
                 * Expand-Operator
                 */
                query = "CREATE table temp_ek(nid, p2s, dist, rnum, flag, srcid) AS SELECT * FROM (SELECT tos.tid, tos.pid, tos.cost+q.dist dst, ROW_NUMBER() over (partition by q.srcid, tos.tid order by tos.cost+q.dist asc) rnum, '0', q.srcid as src FROM ek q, TOUTSEGS tos WHERE q.nid=tos.fid and q.flag=2) temp WHERE temp.rnum=1";
                statement.executeUpdate(query);
                if (index) {
                    String temp_ek_index = "CREATE INDEX temp_ek_dist ON temp_ek (dist)";
                    statement.executeUpdate(temp_ek_index);
                }
                /*
                 * Update-Flag
                 */
                String update_ek = "update ek set flag=1 where flag=2";
                statement.executeUpdate(update_ek);

                /*
                 * Merge-Operator
                 */
                String merge_ek = "merge into ek target using (select nid, p2s, dist, rnum, flag, srcid from temp_ek) src on (src.nid=target.nid and src.srcid=target.srcid) when matched then update set target.dist=src.dist, flag=0, target.p2s=src.p2s where target.dist>src.dist when not matched then insert(target.nid, target.dist, target.p2s, target.flag, target.rnum, target.srcid) values(src.nid, src.dist, src.p2s, '0', src.rnum, src.srcid)";
                row_count = statement.executeUpdate(merge_ek);

                /*
                 * Drop temporary table temp_ek
                 */
                String drop_temp_ek = "drop table temp_ek";
                statement.executeUpdate(drop_temp_ek);

                //increment the variable fwd by 1 to indicate the next fwd expansion
                fwd++;
                iterations++;
            }
            expansions = iterations;
            //DDL operation: clear the TVisited Table
            query = "truncate table TVISITED";
            statement.executeUpdate(query);
            query = "insert into TVISITED (nid, d2s, p2s, f, fid) select nid,dist,p2s,flag,srcid from EK";
            statement.executeUpdate(query);
            //Drop the EK table
            String d_query = "drop table EK";
            statement.executeUpdate(d_query);

            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write("...Expansions: " + iterations + " Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void ClustPWSim(int cluster_id, String curr_centroid, float thd, boolean isSubGraph) {
        try {
            if (doLog) {
                log.write("\n\t\tCluster Pair-wise Similarity...isSubgraph:" + isSubGraph);
                log.flush();
            }
            double time = System.currentTimeMillis();
            int iterations = 0;
            String query = "";
            if (isSubGraph) {
                //DDL operation: empty the CEDGES table
                query = "truncate table CEDGES";
                statement.executeUpdate(query);
                //Insert into CEDGES table (To store EDGES with in a cluster)
                query = "insert into CEDGES select tos.fid, tos.tid, tos.pid, tos.cost from (select fid, tid, pid, min(cost) as cost from TOUTSEGS group by (fid, tid, pid)) tos where tos.fid in (select nid from NCLUS where clus_id=" + cluster_id + ") and tos.tid in (select nid from NCLUS where clus_id=" + cluster_id + ")";
                statement.executeUpdate(query);
            }
            //(1) Initializing Tvisited table with source nodes
            //DDL operation: clear the TVisited Table
            query = "truncate table TVISITED";
            statement.executeUpdate(query);
            query = "insert into TVISITED (nid, d2s, p2s, f, fid) select nc.nid as nid, '0' as d2s, nc.nid as p2s, '0' as f, nc.nid as fid from NCLUS nc where nc.clus_id=" + cluster_id + " and nc.nid!=" + curr_centroid;
            statement.executeUpdate(query);

            String e_query = "CREATE table ek(nid, p2s, dist, rnum, flag, srcid) AS SELECT nid, p2s, d2s, '1', f, fid from TVisited";
            statement.executeUpdate(e_query);
            if (index) {
                String index_query_nid = "CREATE INDEX ek_nid ON ek (nid)";
                String index_query_srcid = "CREATE INDEX ek_srcid ON ek (srcid)";
                statement.executeUpdate(index_query_nid);
                statement.executeUpdate(index_query_srcid);
            }
            //boolean continue_loof=true;
            int fwd = 1;
            while (true) {
                //F-Operator: selecting frontier node
                query = "update EK set flag=2 where flag=0 and (nid, srcid) in (select t.nid, t.srcid from (select ROW_NUMBER() over (partition by srcid order by dist asc) as rid, srcid, nid, dist, flag from EK where flag=0) t where (t.rid =1 or t.dist<=" + (fwd * thd) + ") )";
                int row_count = statement.executeUpdate(query);
                if (row_count == 0) {
                    //if there is no frontier node then stop
                    break;
                }
                // create temporary view temp_ek
                /*
                 * Expand-Operator
                 */
                if (isSubGraph) {
                    query = "CREATE table temp_ek(nid, p2s, dist, rnum, flag, srcid) AS SELECT * FROM (SELECT ce.tid, ce.pid, ce.val+q.dist dst, ROW_NUMBER() over (partition by q.srcid, ce.tid order by ce.val+q.dist asc) rnum, '0', q.srcid as src FROM ek q, CEDGES ce WHERE q.nid=ce.fid and q.flag=2) temp WHERE temp.rnum=1";
                } else {
                    query = "CREATE table temp_ek(nid, p2s, dist, rnum, flag, srcid) AS SELECT * FROM (SELECT tos.tid, tos.pid, tos.cost+q.dist dst, ROW_NUMBER() over (partition by q.srcid, tos.tid order by tos.cost+q.dist asc) rnum, '0', q.srcid as src FROM ek q, TOUTSEGS tos WHERE q.nid=tos.fid and q.flag=2) temp WHERE temp.rnum=1";
                }
                statement.executeUpdate(query);
                if (index) {
                    String temp_ek_index = "CREATE INDEX temp_ek_dist ON temp_ek (dist)";
                    statement.executeUpdate(temp_ek_index);
                }
                /*
                 * Update-Flag
                 */
                String update_ek = "update ek set flag=1 where flag=2";
                statement.executeUpdate(update_ek);

                /*
                 * Merge-Operator
                 */
                String merge_ek = "merge into ek target using (select nid, p2s, dist, rnum, flag, srcid from temp_ek) src on (src.nid=target.nid and src.srcid=target.srcid) when matched then update set target.dist=src.dist, flag=0, target.p2s=src.p2s where target.dist>src.dist when not matched then insert(target.nid, target.dist, target.p2s, target.flag, target.rnum, target.srcid) values(src.nid, src.dist, src.p2s, '0', src.rnum, src.srcid)";
                statement.executeUpdate(merge_ek);

                /*
                 * Drop temporary table temp_ek
                 */
                String drop_temp_ek = "drop table temp_ek";
                statement.executeUpdate(drop_temp_ek);

                //increment the variable fwd by 1 to indicate the next fwd expansion
                fwd++;
                iterations++;
            }
            expansions = iterations;
            //DDL operation: clear the TVisited Table
            query = "truncate table TVISITED";
            statement.executeUpdate(query);
            //String initial_query = "insert into TVISITED (nid, d2s, p2s, f, fid) values(" + src_id + ",0," + src_id + ",0," + src_id + ")";
            query = "insert into TVISITED (nid, d2s, p2s, f, fid) (select nid,dist,p2s,flag,srcid from EK where srcid!=nid and nid in (select nid from NCLUS where clus_id=" + cluster_id + "))";
            statement.executeUpdate(query);
            //Drop the EK table
            String d_query = "drop table EK";
            statement.executeUpdate(d_query);

            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write("...Expansions: " + iterations + " Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void ExtractSPORES_Serial(float beta) {
        /**
         * Extracts the SPOREs from SP tree to speedup the SP process. Traverse
         * the SP tree to construct the shortcuts
         */
        try {
            if (beta == 0f) {
                return;
            }
            if (doLog) {
                log.write("\n\t\tSPOREs Extraction...Threshold: " + BETA);
                log.flush();
            }
            double time = System.currentTimeMillis();
            int iterations = 0;
            String query = "";
            /**
             * Extract shortcuts from Root to all child nodes by following
             * SP-tree paths.
             */
            query = "merge into TOUTSEGS target using (SELECT tv.fid as fid, tv.nid as tid, tv.p2s as pid, tv.d2s as cost from TVISITED tv where tv.p2s!=tv.fid and tv.d2s<= " + beta + ") src on ( target.fid=src.fid and target.tid=src.tid) when matched then update set target.cost=src.cost, target.pid=src.pid where target.cost > src.cost when not matched then insert (target.fid, target.tid,target.pid,target.cost)  values(src.fid, src.tid, src.pid, src.cost)";
            SporeCount += statement.executeUpdate(query);

            /**
             * Extract shortcuts from Non-Root to all child nodes by following
             * SP-tree paths.
             */
            //Create EK table using TVisited Table
            query = "CREATE table ek(nid, p2s, dist, rnum,flag, srcid, rid) AS SELECT tv.nid, tv.p2s, ae.val, cast(1 as number), cast(2 as number), tv.p2s, tv.fid from TVISITED tv, AEDGES ae where tv.p2s=ae.fid and tv.nid=ae.tid and tv.p2s!=tv.fid and ae.val<=" + beta;
            statement.executeUpdate(query);
            if (index) {
                String index_query_nid = "CREATE INDEX ek_nid ON ek (nid)";
                String index_query_srcid = "CREATE INDEX ek_srcid ON ek (srcid)";
                statement.executeUpdate(index_query_nid);
                statement.executeUpdate(index_query_srcid);
            }
            //boolean continue_loof=true;
            while (true) {
                /**
                 * Expand.
                 */
                // create temporary view temp_ek (SELF JOIN OPERATION)
                if (iterations == 0) {
                    query = "CREATE table temp_ek(nid, p2s, dist, rnum, flag, srcid,rid) AS SELECT * FROM (SELECT ae.nid, ae.p2s, cast(ae.dist+q.dist as float) dst, cast(ROW_NUMBER() over (partition by q.rid, ae.nid order by ae.dist+q.dist asc) as number) rnum,cast(0 as number), q.p2s as src,q.rid FROM ek q, ek ae WHERE q.nid=ae.p2s and q.rid=ae.rid and ae.flag=2) temp WHERE temp.rnum=1";
                } else {
                    query = "CREATE table temp_ek(nid, p2s, dist, rnum, flag, srcid,rid) AS SELECT * FROM (SELECT ae.nid, ae.p2s, cast(ae.dist+q.dist as float) dst, cast(ROW_NUMBER() over (partition by q.rid, ae.nid order by ae.dist+q.dist asc) as number) rnum,cast(0 as number), q.srcid as src,q.rid FROM ek q, ek ae WHERE q.nid=ae.p2s and q.rid=ae.rid and q.flag=0 and ae.flag=2) temp WHERE temp.rnum=1";
                }
                statement.executeUpdate(query);
                if (index) {
                    //query = "CREATE INDEX temp_ek_dist ON temp_ek (dist)";
                    //statement.executeUpdate(query);
                    query = "CREATE INDEX temp_ek_srcid ON temp_ek (srcid)";
                    statement.executeUpdate(query);
                    query = "CREATE INDEX temp_ek_nid ON temp_ek (nid)";
                    statement.executeUpdate(query);
                }

                /**
                 * Update-Flag.
                 */
                query = "update ek set flag=1 where flag=0";
                statement.executeUpdate(query);

                /**
                 * Merge.
                 */
                query = "insert into ek (nid,p2s,dist,rnum,flag,srcid,rid) select nid, p2s, dist, rnum, flag, srcid,rid from temp_ek where dist<=" + beta + " and srcid!=nid";
                int row_count = statement.executeUpdate(query);
                /**
                 * Drop temporary table temp_ek.
                 */
                String drop_temp_ek = "drop table temp_ek";
                statement.executeUpdate(drop_temp_ek);

                iterations++;
                //termination check
                if (row_count == 0) {
                    break;
                }
            }
            expansions = iterations;
            /*
             * Merge shortcuts from non-root nodes into TOUTSEGS table.
             */
            query = "merge into TOUTSEGS target using (select * from (SELECT srcid as fid, nid as tid, p2s as pid, dist as cost,ROW_NUMBER() over (partition by srcid,nid order by dist asc) as rnum from EK where flag=1) where rnum=1) src on ( target.fid=src.fid and target.tid=src.tid ) when matched then update set target.pid=src.pid, target.cost=src.cost where target.cost>src.cost when not matched then insert (target.fid, target.tid,target.pid,target.cost)  values(src.fid, src.tid, src.pid, src.cost)";
            SporeCount += statement.executeUpdate(query);
            //Drop the EK table
            String d_query = "drop table EK";
            statement.execute(d_query);
            double result = System.currentTimeMillis() - time;
            SporeTime += result;
            if (doLog) {
                log.write("...SPORE Update Count: " + SporeCount + ", Expansions: " + iterations + ", Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            //System.out.println(e.getMessage());
            e.printStackTrace(System.out);
            System.exit(1);
        }

    }

    private void ConstructSegTable_Serial(float alpha) {
        /**
         * To construct the segmentation table to speedup the process of SP
         *
         */
        try {
            if (alpha == 0f) {
                return;
            }

            if (doLog) {
                log.write("\nSegTable Construct...Threshold: " + alpha);
                log.flush();
            }
            double time = System.currentTimeMillis();
            int iterations = 0, segCount = 0;
            /*
             * Frontier-Operator
             */
            //Initialize TVisited Table
            //DDL operation: clear the TVisited Table
            String query = "truncate table TVISITED";
            statement.executeUpdate(query);

            //use this query to include direct edges from original graph
            query = "insert into TVISITED (nid, d2s, p2s, f, fid) SELECT ae.tid as nid, ae.val as d2s, ae.fid as p2s, '0' as f, ae.fid as fid from AEDGES ae where ae.val <=" + alpha;
            statement.executeUpdate(query);

            String e_query = "CREATE table ek(nid, p2s, dist, rnum, flag, srcid) AS SELECT nid, p2s, d2s, cast(1 as number), cast(f as number), fid from TVisited";
            statement.executeUpdate(e_query);
            if (index) {
                String index_query_nid = "CREATE INDEX ek_nid ON ek (nid)";
                String index_query_srcid = "CREATE INDEX ek_srcid ON ek (srcid)";
                statement.executeUpdate(index_query_nid);
                statement.executeUpdate(index_query_srcid);
            }
            //boolean continue_loof=true;
            while (true) {
                /*
                 * Expand-Operator
                 */
                // create temporary view temp_ek
                query = "CREATE table temp_ek(nid, p2s, dist, rnum, flag, srcid) AS SELECT * FROM (SELECT ae.tid, ae.fid, cast(ae.val+q.dist as float) dst, cast(ROW_NUMBER() over (partition by q.srcid, ae.tid order by ae.val+q.dist asc) as number) rnum, cast(0 as number), q.srcid as src FROM ek q, AEDGES ae WHERE q.nid=ae.fid and q.flag=0) temp WHERE temp.rnum=1";
                statement.executeUpdate(query);
                if (index) {
                    query = "CREATE INDEX temp_ek_srcid ON temp_ek (srcid)";
                    statement.executeUpdate(query);
                    query = "CREATE INDEX temp_ek_nid ON temp_ek (nid)";
                    statement.executeUpdate(query);
                }

                /*
                 * Update-Flag
                 */
                String update_ek = "update ek set flag=1 where flag=0";
                statement.executeUpdate(update_ek);

                /*
                 * Merge-Operator
                 */
                String merge_ek = "merge into ek target using (select nid, p2s, dist, rnum, flag, srcid from temp_ek where dist<=" + alpha + " and srcid!=nid) src on ( target.nid=src.nid and target.srcid=src.srcid ) when matched then update set target.dist=src.dist, target.flag=0, target.p2s=src.p2s where target.dist > src.dist when not matched then insert (target.nid, target.p2s,target.dist,target.rnum, target.flag,  target.srcid)  values(src.nid, src.p2s, src.dist, src.rnum,'0',  src.srcid)";
                int row_count = statement.executeUpdate(merge_ek);
                /*
                 * Drop temporary table temp_ek
                 */
                String drop_temp_ek = "drop table temp_ek";
                statement.executeUpdate(drop_temp_ek);

                iterations++;
                //termination check
                if (row_count == 0) {
                    break;
                }
            }
            /*
             * Merge New Segments into TOUTSEGS table.
             */
            query = "merge into TOUTSEGS target using (SELECT srcid as fid, nid as tid, p2s as pid, dist as cost from EK) src on ( target.fid=src.fid and target.tid=src.tid) when matched then update set target.pid=src.pid, target.cost=src.cost where target.cost>src.cost when not matched then insert (target.fid, target.tid,target.pid,target.cost)  values(src.fid, src.tid, src.pid, src.cost)";
            segCount += statement.executeUpdate(query);
            //Drop the EK table
            String d_query = "drop table EK";
            statement.executeUpdate(d_query);
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write("...SegCount: " + segCount + ", Expansions: " + iterations + ", Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            //System.out.println(e.getMessage());
            e.printStackTrace(System.out);
            System.exit(1);
        }

    }

    //***************************//
    /*
     * Track the path from destination towards the source id For example,
     * TrackingPath(src, dest); where src and dest are source and destination
     * vertices respectively
     */
    private void TrackingPath(int src_id) {
        Statement statmnt = null;
        ResultSet rs = null, rs2 = null;
        String query = "select nid from TNODES";
        try {
            if (doLog) {
                log.write("\n\nTracking Path...\n");
                log.flush();
            }
            statmnt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            rs = statmnt.executeQuery(query);
            int dest_id = 0;
            while (rs.next()) {
                dest_id = rs.getInt("nid");

                // constraint to neglect repeated paths
                // e.g. p1 = 1->2->3 and p2 = 3->2->1
                if (dest_id < src_id) {
                    continue;
                }
                //output the destination vertex first to this path
                if (doLog && dest_id != src_id) {
                    log.write("\n" + dest_id + "->");
                    log.flush();
                }
                //traverse all the connected sequence of edges in backward direction
                while (dest_id != src_id) {
                    query = "select p2s from TVISITED where nid=" + dest_id;
                    //System.out.println(query);
                    rs2 = statement.executeQuery(query);
                    while (rs2.next()) {
                        String path = rs2.getString("p2s");
                        dest_id = Integer.parseInt(path);
                        if (dest_id == src_id) {
                            break;
                        }
                    }
                    if (doLog) {
                        log.write(dest_id + "->");
                        log.flush();
                    }
                }
            }
            //close statements to release the resources explicitly
            rs.close();
            if (rs2 != null) {
                rs2.close();
            }
            statmnt.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);

        }
    }//end of function
}
