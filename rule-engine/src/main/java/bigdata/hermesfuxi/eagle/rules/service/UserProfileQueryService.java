package bigdata.hermesfuxi.eagle.rules.service;

import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;

import java.io.IOException;

/**
 * @author hermesfuxi
 * @desc 用户画像数据查询服务接口
 */
public interface UserProfileQueryService {

    public boolean judgeProfileCondition(String deviceId, RuleParam ruleParam) throws IOException;

}
