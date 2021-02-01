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

import java.util.ArrayList;

public class TsFileReadTime {
  static String filePath = "/data/iotdb/sequence/root.test/0/....tsfile";
  public static void main(String[] args) throws Exception{
    try(TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("root.test.device", "s0"));
      paths.add(new Path("root.test.device", "s999"));
      IExpression timeFilter = new GlobalTimeExpression(TimeFilter.ltEq(10000l));
      QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
      QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
      long startTime = System.currentTimeMillis();
      while(dataSet.hasNext()) {
        dataSet.next();
      }
      long lastTime = System.currentTimeMillis() - startTime;
      System.out.println("Query time: " + lastTime + " ms");
    }
  }
}
