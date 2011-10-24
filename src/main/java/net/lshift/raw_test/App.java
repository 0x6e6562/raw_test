package net.lshift.raw_test;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.UUID;

public class App implements Closeable{

  Connection con;

  public App(Connection c) throws Exception {
    this.con = c;

    Statement stmt = con.createStatement();

    try {
      stmt.execute("drop table test_table");
    }
    catch (Exception e) {
      // ignore
    }

    stmt.execute("create table test_table (id raw(16), value number, primary key(id))");
    stmt.close();
  }

  public void insertRow(UUID uuid, long value) throws Exception{
    PreparedStatement stmt = con.prepareStatement("insert into test_table values (?,?)");
    stmt.setBytes(1, pack(uuid));
    stmt.setLong(2, value);
    stmt.execute();
    stmt.close();
    con.commit();
  }

  public int countRows() throws Exception {
    int rows = 0;
    PreparedStatement stmt = con.prepareStatement("select count(*) from test_table");
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      rows = rs.getInt(1);
    }
    return rows;
  }

  public void insertRows(int rows) throws Exception {
    PreparedStatement stmt = con.prepareStatement("insert into test_table values (?,?)");

    for (int i = 0; i < rows; i++) {
      stmt.setBytes(1, pack(UUID.randomUUID()));
      stmt.setLong(2, System.currentTimeMillis());
      stmt.execute();
    }

    stmt.close();
    con.commit();
  }

  public long readRow(UUID uuid) throws Exception{
    long returnValue = Long.MIN_VALUE;
    PreparedStatement stmt = con.prepareStatement("select value from test_table where id = ?");
    stmt.setBytes(1, pack(uuid));
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      returnValue = rs.getLong(1);
    }
    stmt.close();
    return returnValue;
  }

  public byte[] pack(UUID uuid) {
    byte[] bytes = new byte[16];
    System.arraycopy( fromLong(uuid.getMostSignificantBits()), 0, bytes, 0, 8 );
    System.arraycopy( fromLong(uuid.getLeastSignificantBits()), 0, bytes, 8, 8 );
    return bytes;
  }

  public UUID unpack(Object value) {
    byte[] msb = new byte[8];
    byte[] lsb = new byte[8];
    System.arraycopy( value, 0, msb, 0, 8 );
    System.arraycopy( value, 8, lsb, 0, 8 );
    return new UUID( asLong(msb), asLong(lsb) );
  }

  public static long asLong(byte[] bytes) {
    if ( bytes == null ) {
      return 0;
    }
    if ( bytes.length != 8 ) {
      throw new IllegalArgumentException( "Expecting 8 byte values to construct a long" );
    }
    long value = 0;
    for (int i=0; i<8; i++) {
      value = (value << 8) | (bytes[i] & 0xff);
    }
    return value;
  }

  public static byte[] fromLong(long longValue) {
    byte[] bytes = new byte[8];
    bytes[0] = (byte) ( longValue >> 56 );
    bytes[1] = (byte) ( ( longValue << 8 ) >> 56 );
    bytes[2] = (byte) ( ( longValue << 16 ) >> 56 );
    bytes[3] = (byte) ( ( longValue << 24 ) >> 56 );
    bytes[4] = (byte) ( ( longValue << 32 ) >> 56 );
    bytes[5] = (byte) ( ( longValue << 40 ) >> 56 );
    bytes[6] = (byte) ( ( longValue << 48 ) >> 56 );
    bytes[7] = (byte) ( ( longValue << 56 ) >> 56 );
    return bytes;
  }

  @Override
  public void close() throws IOException {
    if (null != con) {
      try {
        con.close();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }
}
