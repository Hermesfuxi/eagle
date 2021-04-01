package bigdata.hermesfuxi.eagle.etl.bean;

public class ItemEventCount {
    private String eventId;
    private String categoryId;
    private String productId;
    private Long count;
    private Long start;
    private Long end;

    public ItemEventCount() {
    }

    public ItemEventCount(String eventId, String categoryId, String productId, Long count, Long start, Long end) {
        this.eventId = eventId;
        this.categoryId = categoryId;
        this.productId = productId;
        this.count = count;
        this.start = start;
        this.end = end;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "ItemEventCount{" +
                "eventId='" + eventId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", productId='" + productId + '\'' +
                ", count=" + count +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
