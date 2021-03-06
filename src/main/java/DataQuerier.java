import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.*;

import java.util.*;

public class DataQuerier {
  private static final Session session = new Session("127.0.0.1", 6667, "root", "root");
  static String GROUP_BY_ARGS = " group by ([0, 100000), 200ms)";
  public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
    session.open(false);
    int dataCount = 100;
    if (args.length > 0) {
      dataCount = Integer.valueOf(args[0]);
    }
    Set<Integer> idxSet = new HashSet<>();
    Random r = new Random();
    while(idxSet.size() < dataCount) {
      int value = r.nextInt() % 1000;
      value = value < 0 ? -value : value;
      idxSet.add(value);
    }
    List<Integer> measurementIdx = new ArrayList<>();
    for(Integer idx : idxSet) {
      measurementIdx.add(idx);
    }
    Collections.sort(measurementIdx);
    StringBuilder sql = new StringBuilder();
    sql.append("Select ");
    for(int i = 0; i < measurementIdx.size(); ++i) {
      sql.append("avg(s" + measurementIdx.get(i) + ")");
      if (i != measurementIdx.size() - 1) {
        sql.append(", ");
      } else {
        sql.append(" ");
      }
    }
    sql.append("from root.test.device");
    sql.append(GROUP_BY_ARGS);
    System.out.println("Executing sql: " + sql.toString());
    session.executeNonQueryStatement("clear cache");
    long startTime = System.currentTimeMillis();
    SessionDataSet dataSet = session.executeQueryStatement(sql.toString());
    while(dataSet.hasNext()) {
      dataSet.next();
    }
    long lastTime = System.currentTimeMillis() - startTime;
    float estimatedCost = 0;
    for(int i = 0; i < measurementIdx.size() - 1; ++i) {
      if (measurementIdx.get(i+1) - measurementIdx.get(i) > 1) {
        estimatedCost += CostEstimator.getSeekCost((long)(measurementIdx.get(i+1) - measurementIdx.get(i) - 1) * 80645l);
      }
    }
    System.out.println("Estimated seek time: " + (estimatedCost * 10.0f) + " ms");
    System.out.println("Estimated read time: " + 1.1 * (dataCount * 10) + " ms");
    System.out.println("Estimated total time: " + (estimatedCost * 10.0f + 6 + 1.1 * dataCount * 10) + "ms");
    System.out.println("Real query time: " + lastTime + " ms");
  }
}
