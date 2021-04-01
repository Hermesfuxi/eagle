package bigdata.hermesfuxi.eagle.rule.buffer;

/**
 * @author 涛哥
 * desc 封装从缓存中查询到的结果的实体
 */
public class BufferResult {

    // 缓存结果所对应的key
    private String bufferKey;

    // 缓存结果中的value
    private Integer bufferValue;

    // 缓存数据的时间窗口起始
    private Long bufferRangeStart;

    // 缓存数据的时间窗口结束
    private Long bufferRangeEnd;

    // 缓存结果的有效性等级
    private BufferAvailableLevel bufferAvailableLevel;

    // 调整后的后续查询窗口起始点
    private Long outSideQueryStart;


    public String getBufferKey() {
        return bufferKey;
    }

    public void setBufferKey(String bufferKey) {
        this.bufferKey = bufferKey;
    }

    public Integer getBufferValue() {
        return bufferValue;
    }

    public void setBufferValue(Integer bufferValue) {
        this.bufferValue = bufferValue;
    }

    public Long getBufferRangeStart() {
        return bufferRangeStart;
    }

    public void setBufferRangeStart(Long bufferRangeStart) {
        this.bufferRangeStart = bufferRangeStart;
    }

    public Long getBufferRangeEnd() {
        return bufferRangeEnd;
    }

    public void setBufferRangeEnd(Long bufferRangeEnd) {
        this.bufferRangeEnd = bufferRangeEnd;
    }

    public BufferAvailableLevel getBufferAvailableLevel() {
        return bufferAvailableLevel;
    }

    public void setBufferAvailableLevel(BufferAvailableLevel bufferAvailableLevel) {
        this.bufferAvailableLevel = bufferAvailableLevel;
    }

    public Long getOutSideQueryStart() {
        return outSideQueryStart;
    }

    public void setOutSideQueryStart(Long outSideQueryStart) {
        this.outSideQueryStart = outSideQueryStart;
    }

    @Override
    public String toString() {
        return "BufferResult{" +
                "bufferKey='" + bufferKey + '\'' +
                ", bufferValue=" + bufferValue +
                ", bufferRangeStart=" + bufferRangeStart +
                ", bufferRangeEnd=" + bufferRangeEnd +
                ", bufferAvailableLevel=" + bufferAvailableLevel +
                ", outSideQueryStart=" + outSideQueryStart +
                '}';
    }
}
