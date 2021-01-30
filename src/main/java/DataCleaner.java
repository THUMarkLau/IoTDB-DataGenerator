import org.apache.iotdb.session.*;


public class DataCleaner {
  private static final Session session = new Session("127.0.0.1", 6667, "root", "root");

  public static void main(String[] args) throws Exception {
    session.open(false);
    session.deleteStorageGroup("root.test");
    session.executeNonQueryStatement("flush");
    session.close();
  }
}
