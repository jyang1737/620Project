import java.sql.*;
import java.io.*;
import java.util.*;
import java.text.*;

/*
 * App.java
 * Connects to mysql server and creates bikesharing database
 * Inserts data from the csv files in src/main/java/data
 * By default, program expects root account for MySQL database 
 * to have password admin. Change as needed.
 */
public class App {

    public static void main(String[] args) {

        String jdbcUrl = "jdbc:mysql://localhost/?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
        String username = "root";
        String password = "admin";

        Connection con = null;
        Statement s = null;
        
        try { 
            Class.forName("com.mysql.cj.jdbc.Driver")  ;

            con = DriverManager.getConnection(jdbcUrl, username, password);
            con.setAutoCommit(false);

            /* 
             * Creation of database and tables
             * 
             */
            PreparedStatement statement = null;
            statement = con.prepareStatement("DROP DATABASE IF EXISTS bikesharing");
            statement.execute();
            
            statement = con.prepareStatement("CREATE DATABASE bikesharing");

            statement.execute();
            System.out.println("Database created successfully");

            statement = con.prepareStatement("USE bikesharing");
            statement.execute();
            System.out.println("Using bikesharing db");

            statement = con.prepareStatement(
                        "CREATE TABLE IF NOT EXISTS nyc( " +
                        "rideId INTEGER not NULL, " +
                        "startTime DATETIME, " +
                        "endTime DATETIME, " +
                        "startStation VARCHAR(1000), " +
                        "endStation VARCHAR(1000), " +
                        "subscriber BOOLEAN, " +
                        "PRIMARY KEY (rideId))");
            statement.execute();
            System.out.println("nyc table created");
            
            statement = con.prepareStatement(
                        "CREATE TABLE IF NOT EXISTS dc( " +
                        "rideId INTEGER not NULL, " +
                        "startTime DATETIME, " + 
                        "endTime DATETIME, " + 
                        "startStation VARCHAR(1000), " + 
                        "endStation VARCHAR(1000), " + 
                        "subscriber BOOLEAN, " +
                        "PRIMARY KEY (rideId))");
            statement.execute();
            System.out.println("dc table created");

            statement = con.prepareStatement(
                        "CREATE TABLE IF NOT EXISTS chicago( " +
                        "rideId INTEGER not NULL, " +
                        "startTime DATETIME, " + 
                        "endTime DATETIME, " + 
                        "startStation VARCHAR(1000), " + 
                        "endStation VARCHAR(1000), " + 
                        "subscriber BOOLEAN, " +
                        "PRIMARY KEY (rideId))");
            statement.execute();
            System.out.println("chicago table created");
            
            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS nycweather( " +
                "date DATE not NULL, " +
                "temperature INTEGER, " + 
                "precipitation FLOAT, " +
                "PRIMARY KEY (date))");
            statement.execute();
            System.out.println("nycweather table created");

            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS chicagoweather( " +
                "date DATE not NULL, " +
                "temperature INTEGER, " + 
                "precipitation FLOAT, " +
                "PRIMARY KEY (date))");
            statement.execute();
            System.out.println("chicagoweather table created");
            
            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS dcweather( " +
                "date DATE not NULL, " +
                "temperature INTEGER, " + 
                "precipitation FLOAT, " +
                "PRIMARY KEY (date))");
            statement.execute();
            System.out.println("dcweather table created");
            
            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS nyctrip( " + 
                "rideId INT not NULL, " + 
                "tripduration TIME, " + 
                "PRIMARY KEY (rideId))");
            statement.execute();
            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS dctrip( " + 
                "rideId INT not NULL, " + 
                "tripduration TIME, " + 
                "PRIMARY KEY (rideId))");
            statement.execute();
            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS chicagotrip( " + 
                "rideId INT not NULL, " + 
                "tripduration TIME, " + 
                "PRIMARY KEY (rideId))");
            statement.execute();
            System.out.println("trip duration table created");

            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS nycfreq( " +
                "stationName VARCHAR(1000) not NULL, " + 
                "frequency INT, " +
                "PRIMARY KEY (stationName))");
            statement.execute();
            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS dcfreq( " +
                "stationName VARCHAR(1000) not NULL, " + 
                "frequency INT, " +
                "PRIMARY KEY (stationName))");
            statement.execute();
            statement = con.prepareStatement(
                "CREATE TABLE IF NOT EXISTS chicagofreq( " +
                "stationName VARCHAR(1000) not NULL, " + 
                "frequency INT, " +
                "PRIMARY KEY (stationName))");
            statement.execute();
            System.out.println("station freq table created");


            /*
             * Inserting data into the tables
             */
            String csvFile = "src/main/java/data/nyc_sample.csv";
            BufferedReader csvReader = new BufferedReader(new FileReader(csvFile));
            csvReader.readLine();
            String row;
            int ctr = 0;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                Timestamp start = Timestamp.valueOf(data[1].replace("\"",""));
                Timestamp end = Timestamp.valueOf(data[2].replace("\"",""));
                statement = con.prepareStatement(
                            "INSERT INTO nyc (rideId, startTime, " +
                            "endTime, startStation, endStation, subscriber)" +
                            " VALUES (?,?,?,?,?,?)");
                statement.setInt(1, ctr);
                statement.setObject(2, start);
                statement.setObject(3, end);
                statement.setString(4, data[4].replace("\"",""));
                statement.setString(5, data[8].replace("\"",""));
                boolean sub = data[12].replace("\"","").equals("Subscriber");
                statement.setBoolean(6, sub);

                statement.executeUpdate();
                ctr++;
            }
            con.commit();
            
            csvFile = "src/main/java/data/chicago_sample.csv";
            csvReader = new BufferedReader(new FileReader(csvFile));
            csvReader.readLine();
            ctr = 0;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                Timestamp start = Timestamp.valueOf(data[2].replace("\"",""));
                Timestamp end = Timestamp.valueOf(data[3].replace("\"",""));
                statement = con.prepareStatement(
                            "INSERT INTO chicago (rideId, startTime, " +
                            "endTime, startStation, endStation, subscriber)" +
                            " VALUES (?,?,?,?,?,?)");
                statement.setInt(1, ctr);
                statement.setObject(2, start);
                statement.setObject(3, end);
                statement.setString(4, data[4].replace("\"",""));
                statement.setString(5, data[6].replace("\"",""));
                boolean sub = data[12].replace("\"","").equals("member");
                statement.setBoolean(6, sub);

                statement.executeUpdate();
                ctr++;
            }
            con.commit();

            csvFile = "src/main/java/data/dc_sample.csv";
            csvReader = new BufferedReader(new FileReader(csvFile));
            csvReader.readLine();
            ctr = 0;
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                Timestamp start = Timestamp.valueOf(data[1].replace("\"",""));
                Timestamp end = Timestamp.valueOf(data[2].replace("\"",""));
                statement = con.prepareStatement(
                            "INSERT INTO dc (rideId, startTime, " +
                            "endTime, startStation, endStation, subscriber)" +
                            " VALUES (?,?,?,?,?,?)");
                statement.setInt(1, ctr);
                statement.setObject(2, start);
                statement.setObject(3, end);
                statement.setString(4, data[4].replace("\"",""));
                statement.setString(5, data[6].replace("\"",""));
                boolean sub = data[8].replace("\"","").equals("Member");
                statement.setBoolean(6, sub);

                statement.executeUpdate();
                ctr++;
            }
            con.commit();

            csvFile = "src/main/java/data/w_weather.csv";
            csvReader = new BufferedReader(new FileReader(csvFile));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                java.sql.Date d = java.sql.Date.valueOf(data[3].replace("\"",""));
                statement = con.prepareStatement(
                            "INSERT INTO dcweather (date, temperature, precipitation)" +
                            " VALUES (?,?,?)");
                statement.setObject(1, d);
                statement.setInt(2, Integer.parseInt(data[6]));
                statement.setFloat(3, Float.parseFloat(data[4]));
                statement.executeUpdate();
            }
            con.commit();

            csvFile = "src/main/java/data/n_weather.csv";
            csvReader = new BufferedReader(new FileReader(csvFile));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                java.sql.Date d = java.sql.Date.valueOf(data[6].replace("\"",""));
                statement = con.prepareStatement(
                            "INSERT INTO nycweather (date, temperature, precipitation)" +
                            " VALUES (?,?,?)");
                statement.setObject(1, d);
                statement.setInt(2, Integer.parseInt(data[32]));
                statement.setFloat(3, Float.parseFloat(data[15]));
                statement.executeUpdate();
            }
            con.commit();

            csvFile = "src/main/java/data/c_weather.csv";
            csvReader = new BufferedReader(new FileReader(csvFile));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                java.sql.Date d = java.sql.Date.valueOf(data[3].replace("\"",""));
                statement = con.prepareStatement(
                            "INSERT INTO chicagoweather (date, temperature, precipitation)" +
                            " VALUES (?,?,?)");
                statement.setObject(1, d);
                statement.setInt(2, Integer.parseInt(data[9]));
                if (data[6].equals("")) {
                    statement.setFloat(3, 0f);
                } else {
                    statement.setFloat(3, Float.parseFloat(data[6]));
                }
                statement.executeUpdate();
            }
            con.commit();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
