package SalesAnalysis;

import SalesAnalysis.dto.CategorySales;
import SalesAnalysis.entities.OrderItem;
import SalesAnalysis.entities.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class DataBatchJob {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Get Data Sources
        DataSource<OrderItem> orderItems = env.readCsvFile("/Users/svc/Documents/Intellij/Flink_Sales_Batch/datasets/order_items.csv")
                .ignoreFirstLine()
                .pojoType(OrderItem.class, "OrderItemId", "OrderId", "ProductId", "Quantity", "PricePerUnit");

        DataSource<Product> products = env.readCsvFile("/Users/svc/Documents/Intellij/Flink_Sales_Batch/datasets/products.csv")
                                          .ignoreFirstLine()
                                          .pojoType(Product.class, "ProductId", "Name", "Description", "Price", "Category");

        // Join Data sources on ProductID
        DataSet<Tuple6<String, String, Float, Integer, Float, String>> joined_data = orderItems
                .join(products).where("ProductId").equalTo("ProductId")
                .with((JoinFunction<OrderItem, Product, Tuple6<String, String, Float, Integer, Float, String>>)  (o, p) ->
                        new Tuple6<>(
                            p.ProductId.toString(),
                            p.Name,
                            o.PricePerUnit,
                            o.Quantity,
                            o.PricePerUnit * o.Quantity,
                            p.Category
                        ))
                .returns(TypeInformation.of(new TypeHint<Tuple6<String, String, Float, Integer, Float, String>>() {}));

        // Group by Category & Sort By Total Sales
        DataSet<CategorySales> categorySales = joined_data
                .map((MapFunction<Tuple6<String, String, Float, Integer, Float, String>, CategorySales>) record ->
                    new CategorySales(record.f5, record.f4, 1))
                .returns(CategorySales.class)
                .groupBy("Category")
                .reduce((ReduceFunction<CategorySales>) (record1, record2) ->
                    new CategorySales(
                            record1.getCategory(),
                            record1.getTotalSales() + record2.getTotalSales(),
                            record1.getCount() + record2.getCount()))
                .sortPartition("TotalSales", Order.DESCENDING);


        // Write to Sink
        // Approach #1: Convert DTO class to Tuple, NOT as Efficient
//        DataSet<Tuple3<String, Float, Integer>> conversion = categorySales
//                .map((MapFunction<CategorySales, Tuple3<String, Float, Integer>>) record ->
//                    new Tuple3<>(record.getCategory(), record.getTotalSales(), record.getCount()))
//                .returns(new TypeHint<Tuple3<String, Float, Integer>>() {});
//
//        conversion.writeAsCsv("/Users/svc/Documents/Intellij/Flink_Sales_Batch/output/category-sales-tuple-output.csv","\n", ",",
//                FileSystem.WriteMode.OVERWRITE);


        // Approach #2: Map Function to iterate through records then convert to Tuple, NOT as efficient for large data
        categorySales.output(new OutputFormat<CategorySales>() {
            private transient BufferedWriter writer;

            @Override
            public void configure(Configuration configuration) {

            }

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                String path = "/Users/svc/Documents/Intellij/Flink_Sales_Batch/output/category-sales-output.json";
                File outputfile = new File(path);
                this.writer = new BufferedWriter(new FileWriter(outputfile, true));
            }

            @Override
            public void writeRecord(CategorySales categorySales) throws IOException {
                writer.write(categorySales.getCategory() + "," + categorySales.getTotalSales()+ "," + categorySales.getCount());
                writer.newLine();
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }
        });

        env.execute("Flink Sales Batch Job");
    }
}
