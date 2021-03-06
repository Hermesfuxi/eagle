package bigdata.hermesfuxi.eagle.rules.pojo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author hermesfuxi
 * desc 规则参数中的原子条件封装实体
 */
public class AtomicRuleParam implements Serializable {

    // 事件的类型要求
    private String eventId;

    // 事件的属性要求
    private HashMap<String,String> properties;

    // 规则要求的阈值
    private int cnts;

    // 要求的事件发生时间段起始
    private long rangeStart;

    // 要求的事件发生时间段结束
    private long rangeEnd;

    // 用于记录查询服务所返回的查询值
    private int realCnts;

    private Long originStart;

    private Long originEnd;

    private String countQuerySql;

    private String seqQuerySql;

    // 用于记录初始 range
    public void setOriginStart(long originStart){
        this.originStart = originStart;
        this.rangeStart = originStart;
    }

    public void setOriginEnd(long originEnd){
        this.originEnd = originEnd;
        this.rangeEnd = originEnd;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, String> properties) {
        this.properties = properties;
    }

    public int getCnts() {
        return cnts;
    }

    public void setCnts(int cnts) {
        this.cnts = cnts;
    }

    public long getRangeStart() {
        return rangeStart;
    }

    public void setRangeStart(long rangeStart) {
        this.rangeStart = rangeStart;
    }

    public long getRangeEnd() {
        return rangeEnd;
    }

    public void setRangeEnd(long rangeEnd) {
        this.rangeEnd = rangeEnd;
    }

    public int getRealCnts() {
        return realCnts;
    }

    public void setRealCnts(int realCnts) {
        this.realCnts = realCnts;
    }

    public Long getOriginStart() {
        return originStart;
    }

    public void setOriginStart(Long originStart) {
        this.originStart = originStart;
    }

    public Long getOriginEnd() {
        return originEnd;
    }

    public void setOriginEnd(Long originEnd) {
        this.originEnd = originEnd;
    }

    public String getCountQuerySql() {
        return countQuerySql;
    }

    public void setCountQuerySql(String countQuerySql) {
        this.countQuerySql = countQuerySql;
    }

    public String getSeqQuerySql() {
        return seqQuerySql;
    }

    public void setSeqQuerySql(String seqQuerySql) {
        this.seqQuerySql = seqQuerySql;
    }
}
