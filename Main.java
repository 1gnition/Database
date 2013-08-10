/*
 * Author: Ori Popowski
 */

package assignment4;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {
  
  Connection conn = null;
  
  public void createConnection() {
    
    try {
      Class.forName("com.mysql.jdbc.Driver"); 
      this.conn = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/dbs131_user7","dbs131_user7", "dbsdbs"
          
      );
    } catch (SQLException | ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
  
  public void createSchema() {
    
    Statement stmt = null;
    
    try {
      stmt = conn.createStatement();
//      stmt.executeUpdate("DROP TABLE IF EXISTS marriage");  
//      stmt.executeUpdate("DROP TABLE IF EXISTS cars_owned");
//      stmt.executeUpdate("DROP TABLE IF EXISTS cars");
//      stmt.executeUpdate("DROP TABLE IF EXISTS persons");
//      
//      stmt.executeUpdate(fileToString("persons"));
//      stmt.executeUpdate(fileToString("marriage"));
//      stmt.executeUpdate(fileToString("cars"));
//      stmt.executeUpdate(fileToString("cars_owned"));
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  public void createConstraints() {
    
    String persons_insert =
      "CREATE TRIGGER persons_insert BEFORE INSERT ON persons" +
        " FOR EACH ROW" +
        " BEGIN" +
          /* Constraint #1 */
            " IF (NEW.educationnum < 0) OR (NEW.educationnum > 20) THEN" +
              " CALL `Attempt to insert education years outside 0-20 range.`;" + 
            " END IF;" +
        " END;";
    
    String persons_update = 
      "CREATE TRIGGER persons_update BEFORE UPDATE ON persons" +
      " FOR EACH ROW" +
      " BEGIN" +
        /* Constraint #1 */
        " IF (NEW.educationnum < 0) OR (NEW.educationnum > 20) THEN" +
          " CALL `Attempt to insert education years outside 0-20 range.`;" +
        " END IF;" +
        /* Constraint #2 */
        " IF (NEW.age < 12) AND ((SELECT relationship FROM marriage where id_person = NEW.id) = 'child') THEN" +
          " CALL `Attempt to insert a parent younger than 12 years`;" +
        " END IF;" +
        /* Constraint #3 */
        " IF EXISTS (select *" +
            " from marriage as m, persons as p" +
            " where m.id_person = NEW.id and" +
                " m.id_relative = p.id and" +
                " relationship = 'child' and" +
                " p.age > NEW.age) THEN" +
          " CALL `Attempt to insert a parent younger than child.`;" +
        " END IF;" +
      " END;";
    
    String marriage_insert = 
      "CREATE TRIGGER marriage_insert BEFORE INSERT ON marriage" +
      " FOR EACH ROW" +
      " BEGIN" +
        /* Constraint #2 */
        " IF (NEW.relationship = 'child') AND ((SELECT age FROM persons where id = NEW.id_person) < 12) THEN" +
          " CALL `Attempt to insert a parent younger than 12 years.`;" +
        " ELSE" +
          /* Constraint #3 */
          " BEGIN" +
            " DECLARE parent_age INTEGER(3);" +
            " DECLARE child_age INTEGER(3);" +
            " SET parent_age =" +
              " (SELECT age" +
               " FROM persons" +
               " WHERE id = NEW.id_person);" +
            " SET child_age ="  +
              " (SELECT age" +
               " FROM persons" +
               " WHERE id = NEW.id_relative);" +
            " IF (parent_age < child_age) THEN" +
              " CALL `Attempt to insert a parent younger than child.`;" +
            " END IF;" +
          " END;" +
        " END IF;" +
      " END;";
    
    String marriage_update = 
        "CREATE TRIGGER marriage_update BEFORE UPDATE ON marriage" +
        " FOR EACH ROW" +
        " BEGIN" +
          /* Constraint #2 */
          " IF (NEW.relationship = 'child') AND ((SELECT age FROM persons where id = NEW.id_person) < 12) THEN" +
            " CALL `Attempt to insert a parent younger than 12 years.`;" +
          " ELSE" +
            /* Constraint #3 */
            " BEGIN" +
              " DECLARE parent_age INTEGER(3);" +
              " DECLARE child_age INTEGER(3);" +
              " SET parent_age =" +
                " (SELECT age" +
                 " FROM persons" +
                 " WHERE id = NEW.id_person);" +
              " SET child_age ="  +
                " (SELECT age" +
                 " FROM persons" +
                 " WHERE id = NEW.id_relative);" +
              " IF (parent_age < child_age) THEN" +
                " CALL `Attempt to insert a parent younger than child.`;" +
              " END IF;" +
            " END;" +
          " END IF;" +
        " END;";
    
    String cars_insert = 
      "CREATE TRIGGER cars_insert BEFORE INSERT ON cars_owned" +
      " FOR EACH ROW" +
      " BEGIN" +
        /* Constraint #6 */
        " DECLARE x INTEGER;" +
        " SET x =" +
        " (SELECT count(*) " +
        " FROM cars_owned" + 
        " WHERE id_person = NEW.id_person);" +
          " IF (x > 2) THEN" +
          " call `Attempt to insert a person who owns more than three cars.`;" + 
          " END IF;" +
      " END;";
    
    Statement stmt = null;
    
    try {
      stmt = conn.createStatement();
      stmt.executeUpdate("DROP TRIGGER IF EXISTS persons_insert;");
      stmt.executeUpdate("DROP TRIGGER IF EXISTS persons_update;");
      stmt.executeUpdate("DROP TRIGGER IF EXISTS marriage_insert;");
      stmt.executeUpdate("DROP TRIGGER IF EXISTS marriage_update;");
      stmt.executeUpdate("DROP TRIGGER IF EXISTS cars_insert;");
      
      stmt.executeUpdate(persons_insert);
      stmt.executeUpdate(persons_update);
      stmt.executeUpdate(marriage_insert);
      stmt.executeUpdate(marriage_update);
      stmt.executeUpdate(cars_insert);
      
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  public void add(String file, String table) {  
    
    if (table.equals("persons"))
      insertPersons(file);
    else if (table.equals("marriage"))
      insertMarriage(file);
    else if (table.equals("cars"))
      insertCars(file);
    else if (table.equals("cars_owned"))
      insertCarsOwned(file);
    else
      System.err.println("Error.");
  }
  
  private void insertPersons(String file) {
    
    String sql ="INSERT INTO persons (" +
          "id, " +
          "age, " +
          "workclass, " +
          "education, " +
          "educationnum, " +
          "maritalstatus, " +
          "occupation, " +
          "race, " +
          "sex, " +
          "capitalgain, " +
          "capitalloss, " +
          "country" +
          ") " +
          "VALUES(?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ?);";
    
    BufferedReader CSVFile = null;
    PreparedStatement stmt = null;
    
    try {
      CSVFile = new BufferedReader(new FileReader(file));
      String line = CSVFile.readLine();

      //while (line != null) {
      for (int i = 0; i < 1000; ++i) {
        String[] data = line.replace(" ", "").split(",");
        stmt = conn.prepareStatement(sql);
        
        /* id */
        if (data[0].equals("?"))
          stmt.setNull(1, java.sql.Types.INTEGER);
        else
          stmt.setInt(1, Integer.parseInt(data[0]));
        
        /* age */
        if (data[1].equals("?"))
          stmt.setNull(2, java.sql.Types.INTEGER);
        else
          stmt.setInt(2, Integer.parseInt(data[1]));
        
        /* workclass */
        if (data[2].equals("?"))
          stmt.setNull(3, java.sql.Types.VARCHAR);
        else
          stmt.setString(3, data[2]);
        
        /* education */
        if (data[4].equals("?"))
          stmt.setNull(4, java.sql.Types.VARCHAR);
        else
          stmt.setString(4, data[4]);
        
        /* educationnum */
        if (data[5].equals("?"))
          stmt.setNull(5, java.sql.Types.INTEGER);
        else
          stmt.setInt(5, Integer.parseInt(data[5]));
        
        /* maritalstatus */
        if (data[6].equals("?"))
          stmt.setNull(6, java.sql.Types.VARCHAR);
        else
          stmt.setString(6, (data[6]));
        
        /* occupation */
        if (data[7].equals("?"))
          stmt.setNull(7, java.sql.Types.VARCHAR);
        else
          stmt.setString(7, data[7]);
        
        /* race */
        if (data[9].equals("?"))
          stmt.setNull(8, java.sql.Types.VARCHAR);
        else
          stmt.setString(8, (data[9]));
        
        /* sex */
        if (data[10].equals("?"))
          stmt.setNull(9, java.sql.Types.VARCHAR);
        else
          stmt.setString(9, data[10]);
        
        /* capitalgain */
        if (data[11].equals("?"))
          stmt.setNull(10, java.sql.Types.INTEGER);
        else
          stmt.setInt(10, Integer.parseInt(data[11]));
        
        /* capitalloss */
        if (data[12].equals("?"))
          stmt.setNull(11, java.sql.Types.INTEGER);
        else
          stmt.setInt(11, Integer.parseInt(data[12]));
        
        /* country */
        if (data[14].equals("?"))
          stmt.setNull(12, java.sql.Types.VARCHAR);
        else
          stmt.setString(12, data[14]);
        
        stmt.executeUpdate();
        line = CSVFile.readLine();
      }
    } catch (IOException | SQLException e) {
      e.printStackTrace();
    }
  }
  
  private void insertMarriage(String file) {
    
    BufferedReader CSVFile = null;
    PreparedStatement stmt = null;
    String sql ="INSERT INTO marriage VALUES (?, ?, ?)";
    
    try {
      CSVFile = new BufferedReader(new FileReader(file));
      String line = CSVFile.readLine();

      while (line != null) {
        
        String[] data = line.replace(" ", "").split(",");
        stmt = conn.prepareStatement(sql);
        
        /* id_person */
        if (data[0].equals("?"))
          stmt.setNull(1, java.sql.Types.INTEGER);
        else
          stmt.setInt(1, Integer.parseInt(data[0]));
        
        /* id_relative */
        if (data[1].equals("?"))
          stmt.setNull(2, java.sql.Types.INTEGER);
        else
          stmt.setInt(2, Integer.parseInt(data[1]));
        
        /* relationship */
        if (data[2].equals("?"))
          stmt.setNull(3, java.sql.Types.VARCHAR);
        else
          stmt.setString(3, data[2]);
        
        stmt.executeUpdate();
        line = CSVFile.readLine();
      }      
    } catch (IOException | SQLException e) {
      e.printStackTrace();
    }
  }
  
  private void insertCars(String file) {
    
    BufferedReader CSVFile = null;
    PreparedStatement stmt = null;
    String sql ="INSERT INTO cars VALUES (?, ?, ?, ?)";
    
    try {
      CSVFile = new BufferedReader(new FileReader(file));
      String line = CSVFile.readLine();

      while (line != null) {
        
        String[] data = line.replace(" ", "").split(",");
        stmt = conn.prepareStatement(sql);
        
        /* id */
        if (data[0].equals("?"))
          stmt.setNull(1, java.sql.Types.INTEGER);
        else
          stmt.setInt(1, Integer.parseInt(data[0]));
        
        /* manufacturer */
        if (data[1].equals("?"))
          stmt.setNull(2, java.sql.Types.VARCHAR);
        else
          stmt.setString(2, data[1]);
        
        /* model */
        if (data[2].equals("?"))
          stmt.setNull(3, java.sql.Types.VARCHAR);
        else
          stmt.setString(3, data[2]);
        
        /* prod_year */
        if (data[3].equals("?"))
          stmt.setNull(4, java.sql.Types.INTEGER);
        else
          stmt.setInt(4, Integer.parseInt(data[3]));
        
        stmt.executeUpdate();
        line = CSVFile.readLine();
      }
      CSVFile.close();      
    } catch (IOException | SQLException e) {
      e.printStackTrace();
    }
  }
  
  private void insertCarsOwned(String file) {
    
    BufferedReader CSVFile = null;
    PreparedStatement stmt = null;
    String sql ="INSERT INTO cars_owned VALUES (?, ?, ?, ?)";
    
    try {
      CSVFile = new BufferedReader(new FileReader(file));
      String line = CSVFile.readLine();

      while (line != null) {
        
        String[] data = line.replace(" ", "").split(",");
        stmt = conn.prepareStatement(sql);
        
        if (Integer.parseInt(data[0]) <= 1000) {
        
        /* id_person */
        if (data[0].equals("?"))
          stmt.setNull(1, java.sql.Types.INTEGER);
        else
          stmt.setInt(1, Integer.parseInt(data[0]));
        
        /* id_car */
        if (data[1].equals("?"))
          stmt.setNull(2, java.sql.Types.INTEGER);
        else
          stmt.setInt(2, Integer.parseInt(data[1]));
        
        /* color */
        if (data[2].equals("?"))
          stmt.setNull(3, java.sql.Types.VARCHAR);
        else
          stmt.setString(3, data[2]);
        
        /* purchase_date */
        if (data[3].equals("?"))
          stmt.setNull(4, java.sql.Types.VARCHAR);
        else
          stmt.setString(4, data[3]);
        
        stmt.executeUpdate();
        }
        line = CSVFile.readLine();
      }      
    } catch (IOException | SQLException e) {
      e.printStackTrace();
    }
  }
  
  public void printTable(String table) {
    
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + ';');
      ResultSetMetaData rsmd = rs.getMetaData();
      int cols = rsmd.getColumnCount();
      
      while (rs.next()) {
        for (int i = 1; i <= cols; i++) {
          if (i > 1)
            System.out.print(",  ");
          System.out.print(rs.getString(i));
        }
        System.out.println("");  
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  public void query(String query) {
    
    Statement stmt;
    
    try {
      stmt = conn.createStatement();
      stmt.executeQuery(query);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  
  private void descs(int id, String table) {
    
    String select = "SELECT id" +
            " FROM persons, marriage" +
            " WHERE id_relative = id AND" +
            " id_person = ? AND" +
            " relationship = 'child';";
    
    String insert = "INSERT INTO " + table + " VALUES(?);";
    
    PreparedStatement stmt = null;
    ResultSet rs = null;
    
    try {
      stmt = conn.prepareStatement(select);
      stmt.setInt(1, id);
      rs = stmt.executeQuery();
      while (rs.next()) {
        stmt = conn.prepareStatement(insert);
        stmt.setInt(1, rs.getInt(1));
        stmt.executeUpdate();
        descs(rs.getInt(1), table);
      }    
    } catch (SQLException e) {
      e.printStackTrace();
    }  
  }
  
  public void reportDescendants() {

    String select = "SELECT DISTINCT id_person" +
            " FROM marriage" +
            " WHERE relationship = 'child'";
    
    String create = "CREATE TABLE descendants (" +
            " id INTEGER(7));";
    
    String det = "CREATE TABLE details" +
           " SELECT persons.*" +
           " FROM persons, descendants" +
           " WHERE persons.id = descendants.id" +
           " ORDER BY age asc;";
    
    Statement stmt1 = null;
    Statement stmt2 = null;
    ResultSet rs = null;

    try {
      stmt1 = conn.createStatement();
      stmt2 = conn.createStatement();
      
      stmt2.executeUpdate("DROP TABLE IF EXISTS descendants;");
      stmt2.executeUpdate(create);
      
      rs = stmt1.executeQuery(select);
      
      while (rs.next()) {
        System.out.println("PERSON ID " + rs.getInt(1) + " DESCENDANTS:");
        System.out.println("--------------------------------");
        
        descs(rs.getInt(1), "descendants");
        
        stmt2.executeUpdate("DROP TABLE IF EXISTS details;");
        stmt2.executeUpdate(det);
        
        printTable("details");
        
        stmt2.executeUpdate("TRUNCATE TABLE descendants;");
        stmt2.executeUpdate("TRUNCATE TABLE details;");
        System.out.println("");
      }
      stmt1.executeUpdate("DROP TABLE descendants");
      stmt1.executeUpdate("DROP TABLE details");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
    
  public void reportCars() {

    String select = "SELECT DISTINCT id_person" +
            " FROM cars_owned;";
    
    String create = "CREATE TABLE pcars" +
            " SELECT cars.*" +
            " FROM cars_owned, cars" +
            " WHERE id_person = %d AND id_car = id" +
            " ORDER BY prod_year ASC;";
    
    Statement stmt1 = null;
    Statement stmt2 = null;
    ResultSet rs = null;

    try {
      stmt1 = conn.createStatement();
      stmt2 = conn.createStatement();
      
      rs = stmt1.executeQuery(select);
      
      while (rs.next()) {
        System.out.println("PERSON ID " + rs.getInt(1) + " CARS:");
        System.out.println("--------------------------------");
        
        stmt2.executeUpdate("DROP TABLE IF EXISTS pcars;");
        String fs = String.format(create, rs.getInt(1));

        stmt2.executeUpdate(fs);
        
        printTable("pcars");
        
        System.out.println("");
      }
      stmt2.executeUpdate("DROP TABLE IF EXISTS pcars;");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  public void doQueries(int n) {
    
    Statement stmt = null;
    
    String create_children = 
      "CREATE TABLE children" +
      "(SELECT *" +
      " FROM marriage" +
      " WHERE relationship = 'child');";
    
    String q2 = 
      "CREATE TABLE q2" +        
      " (SELECT id_person, count(id_relative) as num_children" +
      "  FROM children" +
      "  GROUP BY id_person" +
      "  HAVING count(id_relative) = (SELECT count(id_relative)" +
      "                    FROM children as c" +
      "                  GROUP BY c.id_person" +
      "                  ORDER BY count(id_relative) DESC LIMIT 1));";
    
    String create_more = 
      "CREATE TABLE owning_more" +
      " (SELECT DISTINCT a.id_person" +
      "  FROM cars_owned as a, cars_owned as b" +
      "  WHERE a.id_person = b.id_person AND" +
      "       a.id_car <> b.id_car)";
    
    String create_count = 
      "CREATE TABLE owning_count" +
      " (SELECT marriage.id_person as id, count(id_relative) as num_children" +
      "  FROM owning_more, marriage" +
      "  WHERE owning_more.id_person = marriage.id_person AND" +
      "    marriage.relationship = 'child'" +
      "  GROUP BY marriage.id_person)";
    
    String q4 =
      "CREATE TABLE q4" +
      " (SELECT id, avg(num_children)" +
      "  FROM owning_count" +
      "  GROUP BY id);";
    
    try {
      stmt = conn.createStatement();
      switch (n) {
        case 2:  stmt.executeUpdate("DROP TABLE IF EXISTS children;");
            stmt.executeUpdate("DROP TABLE IF EXISTS q2;");
            stmt.executeUpdate(create_children);  
            stmt.executeUpdate(q2);
            printTable("q2");
            stmt.executeUpdate("DROP TABLE children;");
            stmt.executeUpdate("DROP TABLE q2;");
            break;
        case 4:  stmt.executeUpdate("DROP TABLE IF EXISTS owning_more;");
            stmt.executeUpdate("DROP TABLE IF EXISTS owning_count;");
            stmt.executeUpdate("DROP TABLE IF EXISTS q4;");
            stmt.executeUpdate(create_more);
            stmt.executeUpdate(create_count);
            stmt.executeUpdate(q4);
            printTable("q4");
            stmt.executeUpdate("DROP TABLE owning_more;");
            stmt.executeUpdate("DROP TABLE owning_count;");
            stmt.executeUpdate("DROP TABLE q4;");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  private String fileToString(String file) {

    String result = null;
    DataInputStream in = null;

    try {
      File f = new File(file);
      byte[] buffer = new byte[(int) f.length()];
      in = new DataInputStream(new FileInputStream(f));
      in.readFully(buffer);
      result = new String(buffer);
      result = result.replace("\n", " ").replace("\r", " ");
      
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return result;
  }
  
  public static void main(String[] args) {
    
    Main m = new Main();
    m.createConnection();
    m.createSchema();
    m.createConstraints();
    
    BufferedReader ir = new BufferedReader(new InputStreamReader (System.in));
    
      try {
          System.out.print("db> ");
          String line;
          
          while ((line = ir.readLine()) != null) {
            String[] parts = line.split("\\s+");
            
            if (parts[0].equals("ADD")) {
              m.add(parts[1], parts[2]);
            } else if (parts[0].equals("PRINT")) {
              m.printTable(parts[1]);
            } else if (parts[0].equals("SQL")) {              
              StringBuilder sb = new StringBuilder("");
              String last;
              sb.append(line + ' ');
              last = parts[parts.length-1];
              while (! last.equals("SQL#")) {
                line = ir.readLine();
                parts = line.split("\\s+");
                sb.append(line + ' ');
                last = parts[parts.length-1];
              }
              String q = sb.toString();
              m.query(q.substring(3,q.length()-5));
            } else if (parts[0].equals("REPORT")) {
              if (parts[1].equals("cars"))
                m.reportCars();
              else if (parts[1].equals("descendants"))
                m.reportDescendants();
            } else if (parts[0].equals("INTERACTIVE")) {
              m.doQueries(Integer.parseInt(parts[1]));
            }

            System.out.print("db> ");
          }
      } catch (IOException e) {
          e.printStackTrace();
      }
      System.out.println("\nGoodbye.");
  }
}

//ADD ex4.data.persons.csv persons
//ADD ex4.data.marriage.csv marriage
//ADD ex4.data.cars.csv cars
//ADD ex4.data.car.owned.by.people.csv cars_owned

/* TODO
 * Add richest person.
 */


/*
// Test constraint #1
insert persons
values(66000, NULL, NULL, NULL, 22, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

// Test constraint #1
update persons
set educationnum = 22
where id = 3;

// Test constraint #2
insert persons
values(67000, 11, NULL ,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

insert marriage
values(67000, 1, 'child');

// Test constraint #2
update persons
set age = 10
where id = 13;




*/
