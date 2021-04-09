package bigdata.hermesfuxi.eagle.rules.pojo;

import java.util.List;

public class RuleCanalBean {
    private List<RuleTableRecord> data;
    private String type;

    public List<RuleTableRecord> getData() {
        return data;
    }

    public void setData(List<RuleTableRecord> data) {
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
