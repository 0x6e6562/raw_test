package net.lshift.raw_test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RawTest {

  private App app;

  @Before
  public void setUpConnection() throws Exception {


    String driver = System.getProperty("db.driver", "org.h2.Driver");
    String url = System.getProperty("db.url", "jdbc:h2:mem:test");

    Class.forName(driver);
    Connection con = DriverManager.getConnection(url);

    app = new App(con);
  }

  @After
  public void closeConnection() throws Exception {
    app.close();
  }


  @Test
  public void testRaw() throws Exception{

    int rows = 50000;

    long start = System.currentTimeMillis();
    app.insertRows(rows);
    long end = System.currentTimeMillis();

    System.err.println(String.format("Time to insert %s rows: %s", rows, (end-start)));

    UUID uuid = UUID.randomUUID();
    long time = System.currentTimeMillis();
    app.insertRow(uuid, time);

    assertEquals(rows + 1, app.countRows());

    start = System.currentTimeMillis();
    long fromDB = app.readRow(uuid);
    end = System.currentTimeMillis();

    assertEquals(time, fromDB);

    System.err.println(String.format("Time to query %s rows: %s", rows, (end-start)));
  }
}
