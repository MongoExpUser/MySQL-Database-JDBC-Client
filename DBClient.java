/****************************************************************************************************************************************************
*                                                                                                                                                   *
* @License Starts                                                                                                                                   *
*                                                                                                                                                   *
* Copyright © 2015 - present. MongoExpUser.  All Rights Reserved.                                                                                   *
*                                                                                                                                                   *
* License: MIT - https://github.com/MongoExpUser/MySQL-Database-JDBC-Client/blob/main/LICENSE                                                       *
*                                                                                                                                                   *
* @License Ends                                                                                                                                     *
*****************************************************************************************************************************************************
*                                                                                                                                                   *
*  DBClient.java implements DBClient() class for: including:                                                                                        *
*                                                                                                                                                   *
*  1) Connecting/disconnecting to MySQL database with JDBC.                                                                                         *
*                                                                                                                                                   *
*  2) Running queries against databases on the MySQl Databases                                                                                      *
*                                                                                                                                                   *                                                                                                                                                                                                                    
*  3) Software Version:                                                                                                                             *
*     Java: Java-17-openjdk                                                                                                                         *
*     MySQL: 8.0.33                                                                                                                                 *
*     Connector/J (Driver): 8.0.33                                                                                                                  *
*                                                                                                                                                   *        
# ***************************************************************************************************************************************************/
                                                                                                                                                                                                                                                                                   


// util and maths imports
import java.util.List;
import java.util.Arrays;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collection;
import java.math.BigDecimal;
// db/sql imoports
import java.sql.Time;
import java.sql.Date;
import java.sql.Types;
import java.sql.Timestamp;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
// io imports
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;


public class DBClient
{
  
    public void DBClient()
    {
        
    }
    
    public String dbConnectionString(String user, String password, String endpoint, int port, String databaseName, String rdmsName, boolean sslOption)
    {
        String connectionString = null;
        
        if(sslOption == true)
        {
            //connectionString = "jdbc:" + rdmsName + "://" + endpoint + ":" + String.valueOf(port) + "/" + databaseName + "?" +  "useSSL=true" + "&user=" + user + "&password=" + password;
            connectionString =  String.format("jdbc:%s://%s:%s/%s?useSSL=true&user=%s&password=%s", rdmsName, endpoint, String.valueOf(port), databaseName, user, password);
        }
        else
        {
            //connectionString = "jdbc:" + rdmsName + "://" + endpoint + ":" + String.valueOf(port) + "/" + databaseName + "?" +  "useSSL=false" + "&user=" + user + "&password=" + password;
            connectionString =  String.format("jdbc:%s://%s:%s/%s?useSSL=false&user=%s&password=%s", rdmsName, endpoint, String.valueOf(port), databaseName, user, password);
        }
        
        return connectionString;
    }
  
    public Connection connectDB(String user, String password, String endpoint, int port, String databaseName, String rdmsName, boolean sslOption)
    {
        Connection connection = null;

        try
        {
            String connectionString = dbConnectionString(user, password, endpoint, port, databaseName, rdmsName, sslOption);
            connection = DriverManager.getConnection(connectionString);
    
            if(connection instanceof Connection)
            {
                System.out.println("Successfully connected to " + databaseName + " database ...");
            }
          
        }
        catch(Exception error)
        {
            error.printStackTrace();
            System.out.println("Error: Could not connect to database ...");
        }

        return connection;
    }
    
    public void executeQueries(Connection connection, String [] queryList)
    {
        try
        {
            
            if(connection instanceof Connection)
            {
                DBClient dbClient = new DBClient();
                Statement st = connection.createStatement();
                int queryListLength = queryList.length;
     
                for(int index = 0; index < queryListLength; index++)
                {
                    dbClient.separator();
                    
                    String query = queryList[index];
                    System.out.println(query);
                    dbClient.separator();
                    
                    ResultSet rs = st.executeQuery(query);
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colCount = rsmd.getColumnCount();
                
                    // fetch and print (show) query results
                    while(rs.next())
                    {
                        for(int count = 1; count <= colCount; count++)
                        {
                            String colName = rsmd.getColumnName(count);
                            String colValue = rs.getString(count);
                            System.out.print(count  + " - " + colName + " : " + colValue);  
                            System.out.println();
                        }
                        
                        dbClient.smallSeparator();
                    }
                    
                    dbClient.separator();
                    rs.close();
                }
                
                st.close();
                connection.close();
                System.out.println("Connection closed....");
            }
        }
        catch (SQLException error)
        {
            System.out.println(error.getMessage());
        }

    }
     
    public static void separator()
    {
      System.out.println(".....................................................................");
    }

    public static void smallSeparator()
    {
      System.out.println("........................");
    }

    public String readQuery(String filename)
    {
        String line = "";
        String query = "";

        try
        {
            FileReader fr = new FileReader(filename);
            BufferedReader br = new BufferedReader(fr);
            
            while( (line = br.readLine()) != null )
            {
                query = query + line;
            }

            br.close();
        }
        catch(IOException error)
        {
            error.printStackTrace();
            System.out.println();
            System.out.println("Error: Reading file error...");
            System.out.println();
        }
        
        return query;
    }

    public void testDB()
    {
        //instantiate class and define all input and arguement variables for db query
        DBClient dbClient = new DBClient();
        String user = "user"; 
        String password = "password";
        String endpoint = "endpoint"; 
        int port = 3306;
        String rdmsName = "mysql";
        String databaseName = "databaseName";
        String tableName = "innodb_index_stats";
        boolean sslOption = false;
        String querySource = "file";
        String queryOne;
        String queryTwo;
        String queryThree;

        try
        {
            // option 1: define query within source code
            if(querySource != "file")
            {
                queryOne = "SELECT * FROM " + tableName + ";";
                queryTwo = "SHOW DATABASES";
                queryThree = "SELECT user, host, plugin from mysql.user;";
                String [] queryListOptionOne = { queryOne, queryTwo, queryThree };
                separator();
                Connection conn;
                conn = dbClient.connectDB(user, password, endpoint, port, databaseName,  rdmsName, sslOption);
                dbClient.executeQueries(conn, queryListOptionOne);
            }

            // option 2: read query from file
            if(querySource == "file")
            {
                System.out.println();
                System.out.println("Reading and running queries from file ....");
                queryOne = dbClient.readQuery("queryOne.sql");
                queryTwo = dbClient.readQuery("queryTwo.sql");
                queryThree = dbClient.readQuery("queryThree.sql");
                String [] queryListOptionTwo = { queryOne, queryTwo, queryThree };
                separator();
                Connection conn;
                conn = dbClient.connectDB(user, password, endpoint, port, databaseName,  rdmsName, sslOption);
                dbClient.executeQueries(conn, queryListOptionTwo);
            }
            
        }
        catch(Exception error)
        {
            error.printStackTrace();
            System.out.println();
            System.out.println("Error: Could not query db...");
            System.out.println();
        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException
    {
        DBClient dbClient = new DBClient();
        dbClient.testDB();
        
    }
}
