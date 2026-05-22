package task9;

import util.OutputWriter;
import util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.desc;

/**
 * Task 9 — Group customers based on order count, average order value,
 *           and purchase frequency.
 *
 * For each customer (Customer_Trx_ID):
 *   - OrderCount      = number of distinct orders
 *   - AvgOrderValue   = average total value (Price + Freight_Value) per order
 *   - PurchaseFrequency = average days between consecutive orders
 *                         (calculated as span / (orderCount - 1), or 0 if only 1 order)
 *
 * Customers are then segmented into groups:
 *   - "High-Value Frequent"   : AvgOrderValue >= overall median AND OrderCount >= 3
 *   - "High-Value Infrequent" : AvgOrderValue >= overall median AND OrderCount < 3
 *   - "Low-Value Frequent"    : AvgOrderValue <  overall median AND OrderCount >= 3
 *   - "Low-Value Infrequent"  : AvgOrderValue <  overall median AND OrderCount < 3
 *
 * Usage: Task9App <dataDir> <outputFile>
 */
public final class Task9App {
  private Task9App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task9App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task9")) {
      Dataset<Row> orders = readCsv(spark, dataDir + "/Orders.csv");
      Dataset<Row> orderItems = readCsv(spark, dataDir + "/Order_Items.csv");

      // ----- Step 1: Compute total value per order -----
      // Each order may have multiple items; sum Price + Freight_Value per Order_ID.
      Dataset<Row> orderValues = orderItems
          .withColumn("ItemValue", col("Price").plus(col("Freight_Value")))
          .groupBy("Order_ID")
          .agg(
              round(sum("ItemValue"), 2).alias("OrderValue")
          );

      // ----- Step 2: Join orders with their values and keep customer info -----
      Dataset<Row> ordersWithValue = orders
          .join(orderValues, "Order_ID")
          .select(
              col("Customer_Trx_ID"),
              col("Order_ID"),
              col("Order_Purchase_Timestamp").cast("timestamp").alias("PurchaseDate"),
              col("OrderValue")
          );

      // ----- Step 3: Aggregate per customer -----
      // OrderCount, AvgOrderValue, first and last purchase dates for frequency calc
      Dataset<Row> customerStats = ordersWithValue
          .groupBy("Customer_Trx_ID")
          .agg(
              count("Order_ID").alias("OrderCount"),
              round(avg("OrderValue"), 2).alias("AvgOrderValue"),
              min("PurchaseDate").alias("FirstPurchase"),
              max("PurchaseDate").alias("LastPurchase")
          );

      // ----- Step 4: Calculate purchase frequency -----
      // PurchaseFrequency = total span in days / (OrderCount - 1)
      // If customer has only 1 order, frequency is 0.
      Dataset<Row> withFrequency = customerStats
          .withColumn("SpanDays",
              datediff(col("LastPurchase"), col("FirstPurchase")))
          .withColumn("PurchaseFreqDays",
              when(col("OrderCount").gt(1),
                  round(col("SpanDays").cast("double").divide(col("OrderCount").minus(1)), 1))
              .otherwise(lit(0.0)))
          .drop("FirstPurchase", "LastPurchase", "SpanDays");

      // ----- Step 5: Determine segmentation thresholds -----
      // Use median AvgOrderValue and OrderCount >= 3 as thresholds.
      double medianOrderValue = withFrequency
          .stat()
          .approxQuantile("AvgOrderValue", new double[]{0.5}, 0.01)[0];

      long freqThreshold = 3; // customers with 3+ orders are "frequent"

      // ----- Step 6: Assign customer segment -----
      Dataset<Row> segmented = withFrequency
          .withColumn("Segment",
              when(col("AvgOrderValue").geq(medianOrderValue)
                      .and(col("OrderCount").geq(freqThreshold)),
                  lit("High-Value Frequent"))
              .when(col("AvgOrderValue").geq(medianOrderValue)
                      .and(col("OrderCount").lt(freqThreshold)),
                  lit("High-Value Infrequent"))
              .when(col("AvgOrderValue").lt(medianOrderValue)
                      .and(col("OrderCount").geq(freqThreshold)),
                  lit("Low-Value Frequent"))
              .otherwise(lit("Low-Value Infrequent"))
          );

      // ----- Step 7: Summarise segment distribution -----
      Dataset<Row> segmentSummary = segmented
          .groupBy("Segment")
          .agg(
              count("*").alias("CustomerCount"),
              round(avg("OrderCount"), 2).alias("AvgOrders"),
              round(avg("AvgOrderValue"), 2).alias("AvgValue"),
              round(avg("PurchaseFreqDays"), 1).alias("AvgFreqDays")
          )
          .orderBy(desc("CustomerCount"));

      // ----- Output -----
      List<String> lines = new ArrayList<>();
      lines.add("=== Task 9: Customer segmentation ===");
      lines.add("");
      lines.add(String.format("Median order value threshold: %.2f EUR", medianOrderValue));
      lines.add(String.format("Frequency threshold: %d+ orders", freqThreshold));
      lines.add("");

      // Segment summary table
      lines.add("--- Segment Summary ---");
      lines.add(String.format("%-25s %-15s %-12s %-15s %s",
          "Segment", "CustomerCount", "AvgOrders", "AvgValue(EUR)", "AvgFreqDays"));
      lines.add("-".repeat(80));

      List<Row> summaryRows = segmentSummary.collectAsList();
      for (Row row : summaryRows) {
        String segment = row.getString(0);
        long custCount = row.getLong(1);
        double avgOrders = ((Number) row.get(2)).doubleValue();
        double avgValue = ((Number) row.get(3)).doubleValue();
        double avgFreq = ((Number) row.get(4)).doubleValue();
        lines.add(String.format("%-25s %-15d %-12.2f %-15.2f %.1f",
            segment, custCount, avgOrders, avgValue, avgFreq));
      }

      lines.add("");
      lines.add("--- Sample customers per segment (top 5 each) ---");
      lines.add(String.format("%-36s %-12s %-15s %-15s %s",
          "Customer_Trx_ID", "OrderCount", "AvgValue(EUR)", "FreqDays", "Segment"));
      lines.add("-".repeat(100));

      // Show top 5 customers from each segment
      for (Row summaryRow : summaryRows) {
        String seg = summaryRow.getString(0);
        Dataset<Row> sample = segmented
            .filter(col("Segment").equalTo(seg))
            .orderBy(desc("OrderCount"), desc("AvgOrderValue"))
            .limit(5);

        List<Row> sampleRows = sample.collectAsList();
        for (Row r : sampleRows) {
          String custId = r.isNullAt(0) ? "(unknown)" : r.getString(0);
          long orderCount = r.getLong(1);
          double avgVal = ((Number) r.get(2)).doubleValue();
          double freqDays = ((Number) r.get(3)).doubleValue();
          String segName = r.getString(4);
          lines.add(String.format("%-36s %-12d %-15.2f %-15.1f %s",
              custId, orderCount, avgVal, freqDays, segName));
        }
      }

      for (String line : lines) {
        System.out.println(line);
      }
      OutputWriter.writeLines(outputFile, lines);
      System.out.println("Output written to: " + outputFile);
    }
  }

  private static Dataset<Row> readCsv(SparkSession spark, String path) {
    return spark.read()
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(path);
  }
}
