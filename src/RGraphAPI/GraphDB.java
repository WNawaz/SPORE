/*
 * To create the graph database with set of required tables.
 */
package RGraphAPI;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Waqas Nawaz (wicky786@[khu.ac.kr, gmail.com, oslab.khu.ac.kr]), Phd
 * Sage, DKE Lab, Computer Engineering Depratement,Kyung Hee University Korea
 */
public class GraphDB {
    //data members

    //common
    private static final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String DB_USER = "SYSTEM", DB_USER2 = "SYS AS SYSDBA";
//    public static String datasetName = "synth", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "pblog", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "facebook", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = " ", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "collaboration", isDirected="true" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = " ", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
    public static String datasetName = "p2pnet", isDirected="true" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "roadnet", isDirected="true" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "sfwdg", isDirected="true" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "sfwug", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "arxivhepth", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "email", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "dblp", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";
//    public static String datasetName = "coauthor", isDirected="false" ,outFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName + "\\results", inFilePath = System.getProperty("user.dir") + "\\dataset\\" + datasetName, delimeter = "\t", nodesFileName = "edges.txt", edgesFileName = "edges.txt";

    //for pc
    private static final String DB_PASSWORD = "1234", DB_PSSWORD2 = "1234";
    private static final String DB_CONNECTION = "jdbc:oracle:thin:@127.0.0.1:1521:xe";
    private static final String datafileAddr = "C:\\ORACLEXE\\APP\\ORACLE\\ORADATA\\XE\\SYSTEM.DBF"; /*to check the addr --> select * from dba_data_files;*/
    private static final String tempdatafileAddr = "C:\\ORACLEXE\\APP\\ORACLE\\ORADATA\\XE\\TEMP.DBF"; /*to check the addr --> select * from dba_data_files;*/

    //for server
//    private static final String DB_PASSWORD = "Dkelab2950", DB_PSSWORD2 = "Dkelab2950";
//    private static final String DB_CONNECTION = "jdbc:oracle:thin:@127.0.0.1:1521:dkelab";
//    private static final String datafileAddr = "D:\\APP\\DKELAB22\\ORADATA\\DKE\\SYSTEM01.DBF";
//    private static final String tempdatafileAddr = "D:\\APP\\DKELAB22\\ORADATA\\DKE\\TEMP01.DBF";

    private Statement statement = null;
    private Connection conn = null;
    private boolean index = true;

    //for logging
    private boolean doLog = true;
    private Writer log;
    //constructors

    public GraphDB(boolean logging, Writer w, boolean indexing) {
        doLog = logging;
        index = indexing;
        if (doLog) {
            log = new BufferedWriter(w);
        }
        conn = InitDBConnection();
    }

    //member functions
    //***************************//
    //getters
    public Connection getDBConnection() {
        return conn;
    }

    public Statement getStatement() {
        return statement;
    }

    public void Close() {
        try {
            //DropTables();
            if (conn != null) {
                conn.close();
            }
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }
    // DB connection

    private Connection InitDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
        try {
            //here :xe is the existing database along with the connection string, 
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            statement = dbConnection.createStatement();
            if (doLog) {
                log.write("...connected...");
                log.flush();
            }
            return dbConnection;
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
        return dbConnection;
    }
    /*
     * Setting up the DB for Graph Traversal Operations
     */
    //***************************//

    public void SetupGraphDB() {
        double time = System.currentTimeMillis();
        ExtendGraphDBSize();
        DropGraphDB();
        CreateGraphDB();
        //estimate the time in milliseconds 
        double result = System.currentTimeMillis() - time;
        if (doLog) {
            try {
                log.write("Time:" + result);
                log.flush();
            } catch (IOException ex) {
                ex.printStackTrace(System.out);
                System.exit(1);
            }
        }

        //InitGraphDB();
        ReadFiles2Tables(Boolean.parseBoolean(isDirected));
    }

    /**
     * Reading from text files to tables. Before using UTL_FILE package you need
     * to grant privilage as follows connect system/1234 as sysdba; grant
     * execute on utl_file to system; grant read, write on directory DATASET to
     * system; // directory should already exist or created useful link
     * http://mikesmithers.wordpress.com/2010/10/02/utl_file-in-plsql-io-io-its-off-to-work-we-go/
     */
    private void LoadGraph() {
        try {
            if (doLog) {
                log.write("\nReading data from files into tables...");
                log.flush();
            }
            String query = "", dirName = "DATASET";
            double time = System.currentTimeMillis();

            //Create or update Directory in DBMS
            query = "CREATE OR REPLACE DIRECTORY " + dirName + " AS '" + inFilePath + "'";
            statement.execute(query);

            //Grand necessary permission to system user using sysdba user account 
            Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER2, DB_PSSWORD2);
            Statement stment = conn.createStatement();
            query = "grant execute on oracle_loader to " + DB_USER;
            stment.execute(query);
            query = "grant read, write on directory " + dirName + " to " + DB_USER;
            stment.execute(query);
            stment.close();
            conn.close();

            //*****************************************************************
            //Reading Nodes from file into database table i.e. TNODES
            //*****************************************************************
            //Drop Table TNodes
            DropTable("TNODES");
            DropTable("TNODES_EXT");
            //Create Table TNodes
            query = "create table TNODES ( NID number, LABEL varchar2(20), constraint pk_TNODES primary key(NID), constraint uni_TNODES UNIQUE (LABEL) )";
            statement.executeUpdate(query);
            //Reading Nodes Information using ORACLE_LOADER package
            query = "create table tnodes_ext ( nid number, label varchar2(20)) organization external ( type oracle_loader default directory " + dirName + " access parameters ( "
                    + "records delimited by newline fields terminated by '" + delimeter + "' missing field values are null ( nid, label) ) "
                    + "location ('" + nodesFileName + "') ) parallel reject limit unlimited";
            query = "create table tedges_ext ( fid number, tid number, elabel float) organization external ( type oracle_loader default directory " + dirName + " access parameters ( "
                    + "records delimited by newline fields terminated by '" + delimeter + "' missing field values are null ( fid, tid, elabel) ) "
                    + "location ('" + edgesFileName + "') ) parallel reject limit unlimited";
            //statement.executeUpdate(query);
            //Enable parallel for loading (good if lots of data to load)
            query = "alter session enable parallel dml";
            statement.executeUpdate(query);
            //Load the data in TNODES table from external table
            query = "insert into TNODES (nid, label) select unique nid, label from TNODES_EXT";
            //statement.executeUpdate(query);
            //*****************************************************************
            //Reading Edges from file into database table i.e. TEDGES
            //*****************************************************************            
            //Drop Table TEDGES
            DropTable("TEDGES");
            DropTable("TEDGES_EXT");
            //Create Table TEDGES
            query = "create table TEDGES (FID number, TID number, ELABEL float, constraint pk_TEDGES primary key(FID,TID) )";
            statement.execute(query);
            if (index) {
                query = "create index TE_FID on TEDGES (FID)";
                statement.execute(query);
                query = "create index TE_TID on TEDGES (TID)";
                statement.execute(query);
            }

            //Reading Edges Information using ORACLE_LOADER package
            query = "create table tedges_ext ( fid number, tid number, elabel float) organization external ( type oracle_loader default directory " + dirName + " access parameters ( "
                    + "records delimited by newline fields terminated by '" + delimeter + "' missing field values are null ( fid, tid, elabel) ) "
                    + "location ('" + edgesFileName + "') ) parallel reject limit unlimited";
            statement.executeUpdate(query);
            //Enable parallel for loading (good if lots of data to load)
            query = "alter session enable parallel dml";
            statement.executeUpdate(query);
            //Load the data in TNODES table from external table
            query = "insert into TEDGES (fid, tid, elabel) select * from TEDGES_EXT";
            statement.executeUpdate(query);
            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write(" Done...Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void ReadFiles2Tables(boolean isDirected) {
        try {
            if (doLog) {
                log.write("\nReading data from files into tables...");
                log.flush();
            }
            String query = "", dirName = "DATASET";
            double time = System.currentTimeMillis();

            //Create or update Directory in DBMS
            query = "CREATE OR REPLACE DIRECTORY " + dirName + " AS '" + inFilePath + "'";
            statement.executeUpdate(query);

            //Grand necessary permission to system user using sysdba user account 
            Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER2, DB_PSSWORD2);
            Statement stment = conn.createStatement();
            query = "grant execute on utl_file to " + DB_USER;
            stment.executeUpdate(query);
            query = "grant read, write on directory " + dirName + " to " + DB_USER;
            stment.executeUpdate(query);
            stment.close();
            conn.close();

            //*****************************************************************
            //Reading Nodes from file into database table i.e. TNODES
            //*****************************************************************
            //Drop Table TNodes
            DropTable("TNODES");
            //Create Table TNodes
            query = "create table TNODES ( NID number, LABEL varchar2(20), constraint pk_TNODES primary key(NID), constraint uni_TNODES UNIQUE (LABEL) )";
            statement.executeUpdate(query);
            //Reading Nodes file using UTL_FILE package
            query = "DECLARE "
                    + "p_file UTL_FILE.file_type; "
                    + "fid TNODES.LABEL%TYPE; "
                    + "tid TNODES.LABEL%TYPE; "
                    + "v_newLine VARCHAR2(100); "
                    + "v_firstDelimiter Number; "
                    + "v_secondDelimiter Number; "
                    + "v_counter Number := 1; "
                    + "BEGIN "
                    + " p_file:= utl_file.fopen('" + dirName + "','" + nodesFileName + "','R'); "
                    + " LOOP "
                    + "  BEGIN "
                    + "  utl_file.get_line(p_file,v_newLine); "
                    + "  IF v_newLine IS NULL THEN "
                    + "   EXIT; "
                    + "  END IF; "
                    + "  v_firstDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 1); "
                    + "  v_secondDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 2); "
                    + "  fid := SUBSTR(v_NewLine, 1, v_firstDelimiter - 1); "
                    + "  IF v_secondDelimiter > 0 THEN "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1, (v_secondDelimiter)-(v_firstDelimiter+1) ); "
                    + "  ELSE "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1 ); "
                    + "  END IF; "
                    + "  insert into TNODES (NID, LABEL) VALUES (v_counter, fid); "
                    + "  v_counter := v_counter +1;"
                    + "  EXCEPTION "
                    + "   WHEN DUP_VAL_ON_INDEX THEN"
                    + "   CONTINUE;"
                    + "   WHEN NO_DATA_FOUND THEN"
                    + "   EXIT;"
                    + "  END; "
                    + " END LOOP; "
                    + "utl_file.fclose_all(); "
                    //+ "  IF " + isDirected + " THEN "
                    //+ "   RETURN; "
                    //+ "  END IF; "
                    + " p_file:= utl_file.fopen('" + dirName + "','" + nodesFileName + "','R'); "
                    + " LOOP "
                    + "  BEGIN "
                    + "  utl_file.get_line(p_file,v_newLine); "
                    + "  IF v_newLine IS NULL THEN "
                    + "   EXIT; "
                    + "  END IF; "
                    + "  v_firstDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 1); "
                    + "  v_secondDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 2); "
                    + "  fid := SUBSTR(v_NewLine, 1, v_firstDelimiter - 1); "
                    + "  IF v_secondDelimiter > 0 THEN "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1, (v_secondDelimiter)-(v_firstDelimiter+1) ); "
                    + "  ELSE "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1 ); "
                    + "  END IF; "
                    + "  insert into TNODES (NID, LABEL) VALUES (v_counter, tid); "
                    + "  v_counter := v_counter +1;"
                    + "  EXCEPTION "
                    + "   WHEN DUP_VAL_ON_INDEX THEN"
                    + "   CONTINUE;"
                    + "   WHEN NO_DATA_FOUND THEN"
                    + "   EXIT;"
                    + "  END; "
                    + " END LOOP; "
                    + "utl_file.fclose_all(); "
                    + "END;";
            statement.executeUpdate(query);

            //*****************************************************************
            //Reading Edges from file into database table i.e. TEDGES
            //*****************************************************************            
            //Drop Table TEDGES
            DropTable("TEDGES");
            //Create Table TEDGES
            query = "create table TEDGES (FID number, TID number, ELABEL float, constraint pk_TEDGES primary key(FID,TID) )";
            statement.executeUpdate(query);
            if (index) {
                query = "create index TE_FID on TEDGES (FID)";
                statement.executeUpdate(query);
                query = "create index TE_TID on TEDGES (TID)";
                statement.executeUpdate(query);
            }

            //Reading Edges file using UTL_FILE package
            query = "DECLARE "
                    + "p_file UTL_FILE.file_type; "
                    + "fid TNODES.LABEL%TYPE; "
                    + "tid TNODES.LABEL%TYPE; "
                    + "eLabel TEDGES.ELABEL%TYPE; "
                    + "v_newLine VARCHAR2(100); "
                    + "v_firstDelimiter Number; "
                    + "v_secondDelimiter Number; "
                    + "v_counter Number := 1; "
                    + "BEGIN "
                    + "p_file:= utl_file.fopen('" + dirName + "','" + edgesFileName + "','R'); "
                    + "LOOP "
                    + "  BEGIN "
                    + "  utl_file.get_line(p_file,v_newLine); "
                    + "  IF v_newLine IS NULL THEN "
                    + "   EXIT; "
                    + "  END IF; "
                    + "  v_firstDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 1); "
                    + "  v_secondDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 2); "
                    + "  fid := SUBSTR(v_NewLine, 1, v_firstDelimiter - 1); "
                    + "  IF v_secondDelimiter > 0 THEN "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1, (v_secondDelimiter)-(v_firstDelimiter+1) ); "
                    + "   eLabel := to_number(SUBSTR(v_NewLine, v_secondDelimiter + 1)); "
                    + "  ELSE "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1 ); "
                    + "   eLabel := to_number(1); "
                    + "  END IF; "
                    + "  insert into TEDGES (FID,TID,ELABEL) values( (select nid from TNODES where label=fid), (select nid from TNODES where label=tid), eLabel); "
                    + "  EXCEPTION "
                    + "   WHEN DUP_VAL_ON_INDEX THEN"
                    + "   CONTINUE;"
                    + "   WHEN NO_DATA_FOUND THEN"
                    + "   EXIT;"
                    + "  END; "
                    + "END LOOP; "
                    + "utl_file.fclose_all(); "
                    + "  IF " + isDirected + " THEN "
                    + "   RETURN; "
                    + "  END IF; "
                    + "p_file:= utl_file.fopen('" + dirName + "','" + edgesFileName + "','R'); "
                    + "LOOP "
                    + "  BEGIN "
                    + "  utl_file.get_line(p_file,v_newLine); "
                    + "  IF v_newLine IS NULL THEN "
                    + "   EXIT; "
                    + "  END IF; "
                    + "  v_firstDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 1); "
                    + "  v_secondDelimiter := INSTR(v_newLine, '" + delimeter + "', 1, 2); "
                    + "  fid := SUBSTR(v_NewLine, 1, v_firstDelimiter - 1); "
                    + "  IF v_secondDelimiter > 0 THEN "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1, (v_secondDelimiter)-(v_firstDelimiter+1) ); "
                    + "   eLabel := to_number(SUBSTR(v_NewLine, v_secondDelimiter + 1)); "
                    + "  ELSE "
                    + "   tid := SUBSTR(v_NewLine, v_firstDelimiter+1 ); "
                    + "   eLabel := to_number(1); "
                    + "  END IF; "
                    + "  insert into TEDGES (FID,TID,ELABEL) values( (select nid from TNODES where label=tid), (select nid from TNODES where label=fid), eLabel); "
                    + "  EXCEPTION "
                    + "   WHEN DUP_VAL_ON_INDEX THEN"
                    + "   CONTINUE;"
                    + "   WHEN NO_DATA_FOUND THEN"
                    + "   EXIT;"
                    + "  END; "
                    + "END LOOP; "
                    + "utl_file.fclose_all(); "
                    + "END;";
            statement.executeUpdate(query);
            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write(" Done...Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    /**
     * Writing results from table to text files. Before using UTL_FILE package
     * you need to grant privilage as follows connect system/1234 as sysdba;
     * grant execute on utl_file to system; grant read, write on directory
     * RESULTS to system; // directory should already exist or created useful
     * link
     * http://mikesmithers.wordpress.com/2010/10/02/utl_file-in-plsql-io-io-its-off-to-work-we-go/
     */
    public void DumpStaticTables(String fileNamePrefix) {
        try {
            if (doLog) {
                log.write("\n\tWriting static tables into files...");
                log.flush();
            }
            String fileName = "", query = "", dirName = "RESULTS";
            //create directory if not exists
            new File(outFilePath).mkdirs();
            double time = System.currentTimeMillis();

            //Create or update Directory in DBMS
            query = "CREATE OR REPLACE DIRECTORY " + dirName + " AS '" + outFilePath + "'";
            statement.execute(query);

            //Grand necessary permission to system user using sysdba user account 
            Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER2, DB_PSSWORD2);
            Statement stment = conn.createStatement();
            query = "grant execute on utl_file to " + DB_USER;
            stment.execute(query);
            query = "grant read, write on directory " + dirName + " to " + DB_USER;
            stment.execute(query);
            stment.close();
            conn.close();

            //Writing TNODES table
            fileName = datasetName + "_" + fileNamePrefix + "_nodes.txt";
            query = "DECLARE "
                    + "p_file UTL_FILE.file_type; "
                    + "l_table TNODES%ROWTYPE; "
                    + "l_delimited VARCHAR2(1) := '" + delimeter + "'; "
                    + "BEGIN "
                    + "p_file:= utl_file.fopen('" + dirName + "','" + fileName + "','W'); "
                    + "utl_file.put_line(p_file,'NodeID'||l_delimited||'Label'); "
                    + "FOR l_table IN "
                    + "(SELECT * FROM TNODES)"
                    + "LOOP "
                    + "utl_file.put_line(p_file,l_table.nid||l_delimited||l_table.label); "
                    + "END LOOP; "
                    + "utl_file.fclose_all(); "
                    + "END;";
            statement.execute(query);
            //Writing AEDGES table
            fileName = datasetName + "_" + fileNamePrefix + "_edges.txt";
            query = "DECLARE "
                    + "p_file UTL_FILE.file_type; "
                    + "l_table AEDGES%ROWTYPE; "
                    + "l_delimited VARCHAR2(1) := '" + delimeter + "'; "
                    + "BEGIN "
                    + "p_file:= utl_file.fopen('" + dirName + "','" + fileName + "','W'); "
                    + "utl_file.put_line(p_file,'FromNodeID'||l_delimited||'ToNodeID'||l_delimited||'Value'); "
                    + "FOR l_table IN "
                    + "(SELECT fid, tid, val FROM AEDGES)"
                    + "LOOP "
                    + "utl_file.put_line(p_file,l_table.fid||l_delimited||l_table.tid||l_delimited||l_table.val); "
                    + "END LOOP; "
                    + "utl_file.fclose_all(); "
                    + "END;";
            statement.execute(query);
            //Writing TOUTSEGS table
            fileName = datasetName + "_" + fileNamePrefix + "_segments.txt";
            query = "DECLARE "
                    + "p_file UTL_FILE.file_type; "
                    + "l_table TOUTSEGS%ROWTYPE; "
                    + "l_delimited VARCHAR2(1) := '" + delimeter + "'; "
                    + "BEGIN "
                    + "p_file:= utl_file.fopen('" + dirName + "','" + fileName + "','W'); "
                    + "utl_file.put_line(p_file,'FromNodeID'||l_delimited||'ToNodeID'||l_delimited||'Cost'); "
                    + "FOR l_table IN "
                    + "(SELECT fid, tid, cost FROM TOUTSEGS)"
                    + "LOOP "
                    + "utl_file.put_line(p_file,l_table.fid||l_delimited||l_table.tid||l_delimited||l_table.cost); "
                    + "END LOOP; "
                    + "utl_file.fclose_all(); "
                    + "END;";
            statement.execute(query);
            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write(" Done...Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    public void DumpDynamicTables(String fileNamePrefix, String fileNameSuffix, String metaData) {
        try {
            if (doLog) {
                log.write("\n\tWriting dynamic tables into files...");
                log.flush();
            }
            String fileName = "", query = "", dirName = "RESULTS";
            //create directory if not exists
            new File(outFilePath).mkdirs();
            double time = System.currentTimeMillis();

            //Create or update Directory in DBMS
            query = "CREATE OR REPLACE DIRECTORY " + dirName + " AS '" + outFilePath + "'";
            statement.execute(query);

            //Grand necessary permission to system user using sysdba user account 
            Connection conn = DriverManager.getConnection(DB_CONNECTION, DB_USER2, DB_PSSWORD2);
            Statement stment = conn.createStatement();
            query = "grant execute on utl_file to " + DB_USER;
            stment.execute(query);
            query = "grant read, write on directory " + dirName + " to " + DB_USER;
            stment.execute(query);
            stment.close();
            conn.close();

            //Writing NCLUS table
            fileName = datasetName + "_" + fileNamePrefix + "_clusters_" + fileNameSuffix + ".txt";
            query = "DECLARE "
                    + "p_file UTL_FILE.file_type; "
                    + "l_table NCLUS%ROWTYPE; "
                    + "l_delimited VARCHAR2(1) := '" + delimeter + "'; "
                    + "BEGIN "
                    + "p_file:= utl_file.fopen('" + dirName + "','" + fileName + "','W'); "
                    + "utl_file.put_line(p_file,'" + metaData + "'); "
                    + "utl_file.put_line(p_file,'NodeID'||l_delimited||'ClusterID'); "
                    + "FOR l_table IN "
                    + "(SELECT nid, clus_id FROM NCLUS)"
                    + "LOOP "
                    + "utl_file.put_line(p_file,l_table.nid||l_delimited||l_table.clus_id); "
                    + "END LOOP; "
                    + "utl_file.fclose_all(); "
                    + "END;";
            statement.execute(query);
            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write(" Done...Time:" + result);
                log.flush();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void ExtendGraphDBSize() {
        try {
            if (doLog) {
                log.write("Config...");
                log.flush();
            }

            String query;

            //Set the initial size for the Database Datafile
//            query = "ALTER DATABASE DATAFILE '" + datafileAddr + "' RESIZE 3G";
//            statement.execute(query);
            //Set the maximum size for the Database Datafile
            query = "ALTER DATABASE DATAFILE '" + datafileAddr + "' AUTOEXTEND ON MAXSIZE Unlimited";
            statement.execute(query);
            //Set the initial size for the Temp Datafile
//            query = "ALTER DATABASE TEMPFILE '" + tempdatafileAddr + "' RESIZE 3G";
//            statement.execute(query);
            //Set the maximum size for the Temp Datafile
            query = "ALTER DATABASE TEMPFILE '" + tempdatafileAddr + "' AUTOEXTEND ON MAXSIZE Unlimited";
            statement.execute(query);

            // Enable Parallelism in CPU and IO
            //SQL> show parameter parallel // to see the status of following parameters, need to restart the databse
            //query = "alter system set parallel_automatic_tuning=TRUE scope=spfile";
            //query = "alter system set parallel_io_cap_enabled=TRUE scope=spfile";
            //query = "alter system set parallel_degree_policy='AUTO' scope=spfile";
        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }

    }

    private void DropGraphDB() {

        //Drop Tables if exist
        DropTable("CEDGES");
        DropTable("AEDGES");
        DropTable("TVISITED");
        DropTable("TVISITED2");
        DropTable("TOUTSEGS");
        DropTable("CENTROIDS");
        DropTable("EK");
        DropTable("TEMP_EK");
        DropTable("NCLUS");
        DropTable("NSIM");
        DropTable("NSIMMIN");

        //Drop Views if exist
        DropView("EK");

    }

    public void DropTable(String tableName) {
        try {
            if (doLog) {
                log.write("DT.");
                log.flush();
            }

            String query;
            //commit to avoid resource busy error
            //query = "commit";

            //Drop Table tableName if exist
            query = "DECLARE "
                    + "t_count INTEGER; "
                    + "v_sql VARCHAR2(1000) := 'drop table " + tableName + "'; "
                    + "BEGIN "
                    + "SELECT COUNT(*) " + "INTO t_count " + "FROM user_tables " + "WHERE table_name = UPPER('" + tableName + "'); "
                    + "IF t_count > 0 THEN "
                    + "EXECUTE IMMEDIATE v_sql; "
                    + "END IF; "
                    + "END; ";
            statement.executeUpdate(query);

        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    public void DropView(String viewName) {
        try {
            if (doLog) {
                log.write("DV.");
                log.flush();
            }

            String query;
            //commit to avoid resource busy error
            //query = "commit";

            //droping view viewName if exists
            query = "DECLARE "
                    + "t_count INTEGER; "
                    + "v_sql VARCHAR2(1000) := 'drop view " + viewName + "'; "
                    + "BEGIN "
                    + "SELECT COUNT(*) " + "INTO t_count " + "FROM user_objects " + "WHERE object_type = 'VIEW' and object_name = '" + viewName + "'; "
                    + "IF t_count > 0 THEN "
                    + "EXECUTE IMMEDIATE v_sql;"
                    + "END IF; "
                    + "END; ";
            statement.executeUpdate(query);

        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    public void DropSequence(String seqName) {
        try {
            if (doLog) {
                log.write("DSeq.");
                log.flush();
            }

            String query;
            //commit to avoid resource busy error
            //query = "commit";

            //Drop Sequence
            query = "DECLARE "
                    + "t_count INTEGER; "
                    + "v_sql VARCHAR2(1000) := 'drop sequence " + seqName + "'; "
                    + "BEGIN "
                    + "SELECT COUNT(*) " + "INTO t_count " + "FROM user_sequences " + "WHERE sequence_name = UPPER('" + seqName + "'); "
                    + "IF t_count > 0 THEN "
                    + "EXECUTE IMMEDIATE v_sql; "
                    + "END IF; "
                    + "END; ";
            statement.execute(query);

        } catch (SQLException | IOException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void CreateGraphDB() {
        try {
            if (doLog) {
                log.write("CDB.");
                log.flush();
            }
            String query;

            //Create Table AEDGES
            query = "create table AEDGES (FID number, TID number,PID number, VAL float, constraint pk_AEDGES primary key(FID,TID,PID) )";
            statement.execute(query);
            if (index) {
                query = "create index AE_FID on AEDGES (FID)";
                statement.execute(query);
                query = "create index AE_TID on AEDGES (TID)";
                statement.execute(query);
            }
            //Create Table CEDGES
            query = "create table CEDGES (FID number, TID number, PID number,VAL float, constraint pk_CEDGES primary key(FID,TID,PID) )";
            statement.execute(query);
            if (index) {
                query = "create index CE_FID on CEDGES (FID)";
                statement.execute(query);
                query = "create index CE_TID on CEDGES (TID)";
                statement.execute(query);
            }
            //Create Table TVisited
            query = "create table TVISITED ( NID number, D2S float, P2S number, F number, FID number, constraint pk_TVISITED primary key(NID,P2S,FID) )";
            statement.execute(query);
            if (index) {
                query = "create index TV_NID on TVISITED (NID)";
                statement.execute(query);
                //query = "create index TV_P2S on TVISITED (P2S)";
                //statement.execute(query);
                query = "create index TV_FID on TVISITED (FID)";
                statement.execute(query);
            }
            //Create Table TVisited2
            query = "create table TVISITED2 ( NID number, D2S float, P2S number, F number, FID number, constraint pk_TVISITED2 primary key(NID,P2S,FID) )";
            statement.execute(query);
            if (index) {
                query = "create index TV2_NID on TVISITED2 (NID)";
                statement.execute(query);
                //query = "create index TV2_P2S on TVISITED2 (P2S)";
                //statement.execute(query);
                query = "create index TV2_FID on TVISITED2 (FID)";
                statement.execute(query);
            }
            //Create Table TOutSegs
            query = "create table TOUTSEGS ( FID number, TID number, PID number, COST float, constraint pk_TOUTSEGS primary key(FID,TID,PID) )";
            statement.execute(query);
            if (index) {
                query = "create index TOS_FID on TOUTSEGS (FID)";
                statement.execute(query);
                query = "create index TOS_TID on TOUTSEGS (TID)";
                statement.execute(query);
                //query = "create index TOS_PID on TOUTSEGS (PID)";
                //statement.execute(query);
            }
            //Create centroids table
            query = "create table CENTROIDS ( cid number, nid number, flag number, constraint pk_CENTROIDS primary key(cid) )";
            statement.executeUpdate(query);
            if (index) {
                query = "create index C_NID on CENTROIDS (NID)";
                statement.execute(query);
            }
            //Create NCLUS table (To store NODES with assciated Centroids)
            query = "create table NCLUS ( nid number,clus_id number, constraint pk_NCLUS primary key(nid) )";
            statement.executeUpdate(query);
            if (index) {
                query = "create index NC_CLUS_ID on NCLUS (clus_id)";
                statement.execute(query);
            }
            //Create NSIM table (Nodes to Nodes (Pair-wise) Similairty Values )
            query = "create table NSIM ( src number, dest number, val float, constraint pk_NSIM primary key(src,dest) )";
            statement.executeUpdate(query);
            if (index) {
                query = "create index NS_SRC on NSIM (SRC)";
                statement.execute(query);
                query = "create index NS_DEST on NSIM (DEST)";
                statement.execute(query);
            }
            //Create NSIMMIN table (Nodes to Nodes (Pair-wise) Similairty Values )
            query = "create table NSIMMIN ( src number,dest number, val float, constraint pk_NSIMMIN primary key(src,dest) )";
            statement.executeUpdate(query);
            if (index) {
                query = "create index NSM_SRC on NSIMMIN (SRC)";
                statement.execute(query);
                query = "create index NSM_DEST on NSIMMIN (DEST)";
                statement.execute(query);
            }
        } catch (Exception e) {
            //System.err.println("Invalid configuration file (problem with file reading)");
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void FormatGraph() {
        try {
            if (doLog) {
                log.write("\nGraph Formatting...");
                log.flush();
            }
            /*
             * Read Graph nodes and edges information from file
             */
            double time = System.currentTimeMillis();
            String query = "";
            String strLine, tokens[];
            FileInputStream fis;
            FileOutputStream fos;
            DataInputStream in;
            DataOutputStream out;
            BufferedReader br;
            BufferedWriter bw;

            //*****************************************************************
            //Reading Nodes from file into database table i.e. TNODES
            //*****************************************************************
            fis = new FileInputStream(inFilePath + "\\" + edgesFileName);
            fos = new FileOutputStream(inFilePath + "\\" + nodesFileName, false);
            // Get the object of DataInputStream
            in = new DataInputStream(fis);
            br = new BufferedReader(new InputStreamReader(in));
            //Read File Line By Line
            //br.readLine(); //skip first line for titles
            int count = 1;
            while ((strLine = br.readLine()) != null) {
                //System.out.println(strLine);
                tokens = strLine.split(delimeter);
                //insert each node into DB
                query = "insert into TNODES (NID, LABEL) VALUES (" + count + "," + tokens[0] + ")";
                try {
                    statement.executeUpdate(query);
                    count++;
                } catch (SQLException ex) {
                    //ex.printStackTrace(System.out);
                    // do nothing bcos node is already there
                }
            }//end while
            br.close();
            fis.close();
            //****************************************************************//
            //Drop Table TEDGES
            query = "DECLARE "
                    + "t_count INTEGER; "
                    + "v_sql VARCHAR2(1000) := 'drop table TEDGES'; "
                    + "BEGIN "
                    + "SELECT COUNT(*) " + "INTO t_count " + "FROM user_tables " + "WHERE table_name = UPPER('TEDGES'); "
                    + "IF t_count > 0 THEN "
                    + "EXECUTE IMMEDIATE v_sql; "
                    + "END IF; "
                    + "END; ";
            statement.executeUpdate(query);
            //Create Table TEDGES
            query = "create table TEDGES (FID number, TID number, ELABEL float, constraint pk_TEDGES primary key(FID,TID) )";
            statement.execute(query);
            if (index) {
                query = "create index TE_FID on TEDGES (FID)";
                statement.execute(query);
                query = "create index TE_TID on TEDGES (TID)";
                statement.execute(query);
            }
            //*****************************************************************
            //Reading Edges from file into database table i.e. TEDGES
            //*****************************************************************
            fis = new FileInputStream(inFilePath + "\\" + edgesFileName);
            // Get the object of DataInputStream
            in = new DataInputStream(fis);
            br = new BufferedReader(new InputStreamReader(in));
            float value = 1.0f;
            //Read File Line By Line
            //br.readLine(); //skip first line for titles
            while ((strLine = br.readLine()) != null) {
                //System.out.println(strLine);
                tokens = strLine.split(delimeter);
                //insert each edge into DB
                if (Integer.parseInt(tokens[0]) != Integer.parseInt(tokens[1])) {
                    if (tokens.length >= 3) {
                        query = "insert into TEDGES (FID, TID, ELABEL) select t1.nid, t2.nid," + Float.parseFloat(tokens[2]) + " from (select nid from TNODES where label=" + tokens[0] + ") t1, (select nid from TNODES where label=" + tokens[1] + ") t2";
                    } else if (tokens.length == 2) {
                        query = "insert into TEDGES (FID, TID, ELABEL) select t1.nid, t2.nid," + value + " from (select nid from TNODES where label=" + tokens[0] + ") t1, (select nid from TNODES where label=" + tokens[1] + ") t2";
                    } else {
                        //skip when pair of vertices are not present
                    }
                } else {
                    //skip when source and destination nodes are identical
                }
                try {
                    statement.executeUpdate(query);
                } catch (SQLException ex) {
                    //ex.printStackTrace(System.out);
                    //System.exit(1);
                    // do nothing bcos edge is already there
                }
            }//end while
            br.close();
            fis.close();
            //*****************************************************************
            //estimate the time in milliseconds 
            double result = System.currentTimeMillis() - time;
            if (doLog) {
                log.write("Time:" + result);
                log.flush();
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private void InsertAttribs() {
        try {
            if (doLog) {
                log.write("W...");
                log.flush();
            }
            /*
             * Read GraphOper nodes attributes from file and write into DB
             */
            String query = "";
            FileInputStream fstream = new FileInputStream(inFilePath + "\\nodes.txt");
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine, tokens[], delimeter = "\t";
            //Read File Line By Line
            //br.readLine(); //skip first line for titles
            while ((strLine = br.readLine()) != null) {
                //System.out.println(strLine);
                tokens = strLine.split(delimeter);
                //insert each node into DB
                query = "INSERT INTO attributes (idNode, attrib1, attrib2) VALUES (" + tokens[0] + "," + tokens[1] + "," + tokens[2] + ");";
                try {
                    statement.execute(query);
                } catch (SQLException ex) {
                    //ex.printStackTrace(System.out);
                    // do nothing bcos node's attributes are already there
                }
            }//end while
            br.close();
            fstream.close();
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }
}//Class Ending
