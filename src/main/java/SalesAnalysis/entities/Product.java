package SalesAnalysis.entities;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Product {
    public Integer ProductId;
    public String Name;
    public String Description;
    public String Category;
    public Float Price;
}
