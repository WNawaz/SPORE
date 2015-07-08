/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package RGraphAPI;

/**
 *
 * @author Waqas Nawaz (wicky786@[khu.ac.kr, gmail.com, oslab.khu.ac.kr]), Phd
 * Sage, DKE Lab, Computer Engineering Depratement,Kyung Hee University Korea
 */
public class Frontier {

    String fid;
    String tid;
    String p2s;

    public Frontier() {
        fid = "";
        tid = "";
        p2s = "";
    }

    public Frontier(String f, String t, String p) {
        fid = f;
        tid = t;
        p2s = p;
    }

    public void setFid(String f) {
        fid = f;
    }

    public void setTid(String t) {
        tid = t;
    }

    public void setP2s(String p) {
        p2s = p;
    }

    public void print() {
        System.out.println("tid:" + tid + ", fid:" + fid + ", p2s:" + p2s);
    }
}
