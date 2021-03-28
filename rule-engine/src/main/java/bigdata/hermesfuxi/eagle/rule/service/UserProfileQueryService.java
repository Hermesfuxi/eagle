package bigdata.hermesfuxi.eagle.rule.service;

import bigdata.hermesfuxi.eagle.rule.pojo.RuleParam;

import java.io.IOException;

/**
 * @author hermesfuxi
 * @desc 用户画像数据查询服务接口
 */
public interface UserProfileQueryService {

    public boolean judgeProfileCondition(String deviceId, RuleParam ruleParam) throws IOException;

}
