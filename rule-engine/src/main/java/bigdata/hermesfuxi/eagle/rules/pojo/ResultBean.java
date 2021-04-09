package bigdata.hermesfuxi.eagle.rules.pojo;

public class ResultBean {
    private String ruleId;
    private String deviceId;
    private long timeStamp;

    public ResultBean() {
    }

    public ResultBean(String ruleId, String deviceId, long timeStamp) {
        this.ruleId = ruleId;
        this.deviceId = deviceId;
        this.timeStamp = timeStamp;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
