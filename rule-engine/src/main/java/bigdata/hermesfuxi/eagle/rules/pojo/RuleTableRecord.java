package bigdata.hermesfuxi.eagle.rules.pojo;

public class RuleTableRecord {
    private int id;
    private String rule_name;
    private String rule_code;
    private int rule_status;
    private String rule_type;
    private String rule_version;
    private String cnt_sqls;
    private String seq_sqls;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getRule_name() {
        return rule_name;
    }

    public void setRule_name(String rule_name) {
        this.rule_name = rule_name;
    }

    public String getRule_code() {
        return rule_code;
    }

    public void setRule_code(String rule_code) {
        this.rule_code = rule_code;
    }

    public int getRule_status() {
        return rule_status;
    }

    public void setRule_status(int rule_status) {
        this.rule_status = rule_status;
    }

    public String getRule_type() {
        return rule_type;
    }

    public void setRule_type(String rule_type) {
        this.rule_type = rule_type;
    }

    public String getRule_versioin() {
        return rule_version;
    }

    public void setRule_versioin(String rule_version) {
        this.rule_version = rule_version;
    }

    public String getCnt_sqls() {
        return cnt_sqls;
    }

    public void setCnt_sqls(String cnt_sqls) {
        this.cnt_sqls = cnt_sqls;
    }

    public String getSeq_sqls() {
        return seq_sqls;
    }

    public void setSeq_sqls(String seq_sqls) {
        this.seq_sqls = seq_sqls;
    }
}
