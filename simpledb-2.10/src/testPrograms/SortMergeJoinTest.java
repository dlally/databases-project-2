package testPrograms;

import simpledb.remote.SimpleDriver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Random;

/**
 * Test program to evaluate the performance of the modified
 * sort-merge-join operator as part of Project 2 Task 4 and Task 5.
 *
 * This program creates two tables, populates the tables with randomized
 * data, and then queries the tables. These queries are structured so as
 * to cause the DBMS to utilized the sort-merge-join operator.
 *
 * The ExploitSortQueryPlanner must be enabled in the SimpleDB configuration
 * in order for this evaluation to work properly. This custom query planner
 * always uses the sort-merge-join operator that has been modified to make
 * smarter decisions based on table sorted status.
 *
 * This program is a modified version of the test file provided by the course
 * staff for usage in our testing process.
 *
 * Created by Nate Miller & Doug Lally on 5/4/2014.
 */
public class SortMergeJoinTest {

    // The number of random records to insert into the test tables.
    final static int maxSize = 5000;

    /**
     * The main test program method.
     * @param args any arguments passed into the method.
     */
    public static void main(String[] args) {

        // Initialize needed variables
        Connection conn = null;
        Random rand = null;
        Statement s = null;

        try {
            // Initialize a log file to write test evaluations
            File log = new File("sort-merge-join_test_log.log");
            BufferedWriter smgTestLog = new BufferedWriter(new FileWriter(log, true));


            // Connect to the simpledb database
            Driver driver = new SimpleDriver();
            conn = driver.connect("jdbc:simpledb://localhost", null);

            // Create statements and SQL strings for table creation
            Statement stmt1 = conn.createStatement();
            String tbl1 = "CREATE TABLE test1(a1 int, a2 int)";
            smgTestLog.write(tbl1);
            smgTestLog.newLine();
            Statement stmt2 = conn.createStatement();
            String tbl2 = "CREATE TABLE test2(a1 int, a2 int)";
            smgTestLog.write(tbl2);
            smgTestLog.newLine();

            // Create the two test tables
            stmt1.executeUpdate(tbl1);
            System.out.println("Table test1 created.");
            smgTestLog.write("Table test1 created.");
            smgTestLog.newLine();
            stmt2.executeUpdate(tbl2);
            System.out.println("Table test2 created.");
            smgTestLog.write("Table test2 created.");
            smgTestLog.newLine();

            // Populate the test tables
            for(int i=1;i<3;i++)
            {
                rand=new Random(1);// ensure every table gets the same data
                for(int j=0;j<maxSize;j++)
                {
                    stmt1.executeUpdate("insert into test"+i+" (a1,a2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
                }
            }

            /** Begin querying and evaluation */
            long startTime;
            long endTime;
            long elapsedTime;
            String queryTbl1 = "SELECT a1, a2 FROM test1, test2 WHERE a1 = a1"; // simple query to join the two test tables based on a predicate
            ResultSet res1;

            // Measure the time required to execute the query the first time
            smgTestLog.write("Executing Query 1: " + queryTbl1);
            smgTestLog.newLine();
            startTime = System.nanoTime();
            res1 = stmt1.executeQuery(queryTbl1);
            endTime = System.nanoTime();

            // Calculate the elapsed time and print the results to the console
            elapsedTime = endTime - startTime;
            String msg = "Query 1 Executed in " + elapsedTime + " nano seconds.\n";
            System.out.println(msg);
            smgTestLog.write(msg);
            smgTestLog.newLine();

            // Repeat process for the second query,
            // the second time should see decreased run time due to the operator modifications
            smgTestLog.write("Executing Query 2: " + queryTbl1);
            smgTestLog.newLine();
            startTime = System.nanoTime();
            res1 = stmt1.executeQuery(queryTbl1);
            endTime = System.nanoTime();

            elapsedTime = endTime - startTime;
            msg = "Query 2 Executed in " + elapsedTime + " nano seconds.\n";
            System.out.println(msg);
            smgTestLog.write(msg);
            smgTestLog.newLine();

            smgTestLog.close();
            conn.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally
        {
            try {
                if(conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
