import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.*;

public class DataQuerier {
  private static final Session session = new Session("127.0.0.1", 6667, "root", "root");
  private static final String[] sqls = new String[] {"select count(s0), count(s999) from root.test.device group by ([0, 100000), 100ms)"};
  public static void main(String[] args) throws IoTDBConnectionException {
    session.open(false);
    query();
  }

  static void query() {
    for(String sql : sqls) {
      try {
        long startTime = System.currentTimeMillis();

        SessionDataSet dataset = session.executeQueryStatement(sql);
        dataset.setFetchSize(1024);
        while(dataset.hasNext()) {
          dataset.next();
        }
        long lastingTime = System.currentTimeMillis() - startTime;
        System.out.println("SQL:" + sql + ": " + lastingTime + " ms");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
