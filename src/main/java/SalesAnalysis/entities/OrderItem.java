package SalesAnalysis.entities;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class OrderItem {
    public Integer OrderItemId;
    public Integer OrderId;
    public Integer ProductId;
    public Integer Quantity;
    public Float PricePerUnit;
}
