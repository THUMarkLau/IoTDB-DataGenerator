import org.apache.iotdb.tsfile.*;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.*;

public class TsFileReadTime {
  static String filePath = "/data/iotdb/sequence/root.test/0/1612026727214-1-0.tsfile";
  public static void main(String[] args) throws Exception{
    int dataCount = 100;
    if (args.length > 0) {
      dataCount = Integer.valueOf(args[0]);
    }
    try(TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      Set<Integer> idxSet = new HashSet<>();
      Random r = new Random();
      while (idxSet.size() < dataCount) {
        int value = r.nextInt() % 1000;
        value = value < 0 ? -value : value;
        idxSet.add(value);
      }
      List<Integer> measurementIdx = new ArrayList<>();
      for(Integer idx : idxSet) {
        measurementIdx.add(idx);
      }
      Collections.sort(measurementIdx);
      float estimatedCost = 0;
      for(int i = 0; i < measurementIdx.size() - 1; ++i) {
        if (measurementIdx.get(i+1) - measurementIdx.get(i) > 1) {
          estimatedCost += CostEstimator.getSeekCost((long)(measurementIdx.get(i+1) - measurementIdx.get(i) - 1) * 80643l);
        }
      }
      System.out.println(measurementIdx.toString());
      for(Integer idx : measurementIdx) {
        paths.add(new Path("root.test.device", "s" + idx));
      }
      // IExpression timeFilter = new GlobalTimeExpression(TimeFilter.ltEq(10000l));
      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
      long startTime = System.currentTimeMillis();
      while(dataSet.hasNext()) {
        dataSet.next();
      }
      long lastTime = System.currentTimeMillis() - startTime;
      System.out.println("Estimation seek time: " + estimatedCost + "ms");
      System.out.println("Real query time: " + lastTime + " ms");
    }
  }
}
