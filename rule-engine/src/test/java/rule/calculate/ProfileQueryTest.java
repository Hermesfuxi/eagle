package rule.calculate;

import bigdata.hermesfuxi.eagle.rules.pojo.RuleParam;
import bigdata.hermesfuxi.eagle.rules.service.UserProfileQueryServiceHbaseImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author hermesfuxi
 * desc 画像条件查询服务模块测试类
 */
public class ProfileQueryTest {

    @Test
    public void testQueryProfile() throws IOException {
        // 构造一个查询服务
        UserProfileQueryServiceHbaseImpl impl = new UserProfileQueryServiceHbaseImpl();

        // 构造参数
        HashMap<String, String> userProfileParams = new HashMap<>();
        userProfileParams.put("tag1","v9");
        userProfileParams.put("tag2","v3");

        RuleParam ruleParam = new RuleParam();
        ruleParam.setUserProfileParams(userProfileParams);

        boolean a = impl.judgeProfileCondition("645", ruleParam);
        System.out.println(a);


        // 构造参数
        userProfileParams.put("tag1","v7");
        userProfileParams.put("tag2","v3");
        ruleParam.setUserProfileParams(userProfileParams);

        boolean b = impl.judgeProfileCondition("999", ruleParam);
        System.out.println(b);

    }

}
