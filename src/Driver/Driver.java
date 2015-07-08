/*
 * Driver Class -- Entry point
 */
package Driver;

import RGraphAPI.GraphTraversal;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 *
 * @author Waqas Nawaz (wicky786@[khu.ac.kr, gmail.com, oslab.khu.ac.kr]), Phd
 * Sage, DKE Lab, Computer Engineering Department,Kyung-Hee University Korea
 */
public class Driver {

    /**
     * @param args the command line arguments
     */
    private static Writer log;
    private static boolean logging = true;
    private static boolean indexing = false;
    private static int K = 10;

    public static void main(String[] args) {
        try {
            initLog();
            GraphTraversal gt = null;
            double time = 0, result = 0;
            float alpha = 0.0f, beta = 0.0f;

            //**********************************************************************
            //SegTable Approach
            //**********************************************************************
//            alpha = 0.1f;
//            beta = 0.0f;
//            log.write("\n........................................\nStarted...");
//            System.out.print("Started...");
//            log.flush();
//            time = System.currentTimeMillis();
//            while (alpha < 0.9f) {
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta);
//                gt.RPAMSet_Serial(K);
//                alpha += 0.2f;
//            }
//            gt.Close();//closes all the DB connections along with droping the tables
//            result = System.currentTimeMillis() - time;
//            System.out.print("\n...Ended");
//            log.write("\n...Ended, Time: " + result);
//            log.flush();
            //**********************************************************************
            //SPORE Approach
            //**********************************************************************
            alpha = 0.0f;
            beta = 0.0f;
            log.write("\n........................................\nStarted...");
            System.out.print("Started...");
            log.flush();
            time = System.currentTimeMillis();
            while (beta < 0.9f) {
                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"asc");
                gt.RPAMElement_Serial(K);
                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"asc");
                gt.RPAMSet_Serial(K);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"asc");
//                gt.RPAMSet_Serial(15);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"asc");
//                gt.RPAMSet_Serial(20);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"desc");
//                gt.RPAMSet_Serial(10);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"desc");
//                gt.RPAMSet_Serial(15);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"desc");
//                gt.RPAMSet_Serial(20);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"rand");
//                gt.RPAMSet_Serial(10);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"rand");
//                gt.RPAMSet_Serial(15);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta,"rand");
//                gt.RPAMSet_Serial(20);
//                beta += 0.2f;
                break;
            }
            gt.Close();//closes all the DB connections along with droping the tables
            result = System.currentTimeMillis() - time;
            System.out.print("\n...Ended");
            log.write("\n...Ended, Time: " + result);
            log.flush();
            //**********************************************************************
            //Hybrid (SegTable and SPORE)
            //**********************************************************************
//            alpha = 0.05f;
//            beta = 0.05f;
//            log.write("\n........................................\nStarted...");
//            System.out.print("Started...");
//            log.flush();
//            time = System.currentTimeMillis();
//            while (beta < 0.9f) {
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta);
//                gt.RPAMElement_Serial(K);
//                gt = new GraphTraversal(logging, log, indexing, alpha, beta);
//                gt.RPAMSet_Serial(K);
//                beta += 0.05f;
//                alpha += 0.05f;
//            }
//            gt.Close();//closes all the DB connections along with droping the tables
//            result = System.currentTimeMillis() - time;
//            System.out.print("\n...Ended");
//            log.write("\n...Ended, Time: " + result);
//            log.flush();
            //**********************************************************************

            log.close();
        } catch (IOException ex) {
            ex.printStackTrace(System.out);
            System.exit(1);
        }

    }

    private static void initLog() {
        try {
            File statText = new File(System.getProperty("user.dir") + "\\dataset\\Log.txt");
            FileOutputStream is = new FileOutputStream(statText, true);
            OutputStreamWriter osw = new OutputStreamWriter(is);
            log = new BufferedWriter(osw);
        } catch (SecurityException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }
}
