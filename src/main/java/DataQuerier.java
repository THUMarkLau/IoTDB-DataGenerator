import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.*;

public class DataQuerier {
  private static final Session session = new Session("127.0.0.1", 6667, "root", "root");
  private static final String[] sqls = new String[] {"select count(s0), count(s999) from root.test.device group by ([0, 100000), 100ms)"};
  public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
    session.open(false);

    query();
  }

  static void query() {
    for(String sql : sqls) {
      long totalTime = 0;
      for (int i = 0; i < 100; i++) {
        try {
          session.executeNonQueryStatement("clear cache");
          long startTime = System.currentTimeMillis();
          SessionDataSet dataset = session.executeQueryStatement(sql);
          dataset.setFetchSize(1024);
          while (dataset.hasNext()) {
            dataset.next();
          }
          long lastingTime = System.currentTimeMillis() - startTime;
          totalTime += lastingTime;
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.out.println("SQL:" + sql + ": " + totalTime / 100l + " ms");
      }
    }
  }
}
