package bigdata.hermesfuxi.eagle.etl.utils;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

public class IDUtils {

    /** 16进制的字符数组 */
    private final static String[] hexDigits = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };

    public static final Map<String, String> hexDigitsMap = new HashMap<String, String>(){
        {
            put("a", "1");
            put("b", "2");
            put("c", "3");
            put("d", "4");
            put("e", "5");
            put("f", "6");
            put("g", "7");
        }
    };

    public static Long getMD5(String input) throws Exception {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] bytes = md5.digest(input.getBytes(StandardCharsets.UTF_8));
        String md5Str = new BigInteger(1, bytes).toString(16).substring(8, 24);
        StringBuilder strBuilder = new StringBuilder();
        for (byte b : md5Str.getBytes()) {
            if(hexDigitsMap.containsKey(String.valueOf(b))){
                strBuilder.append(hexDigitsMap.get(String.valueOf(b)));
            }else {
                strBuilder.append(b);
            }
        }
        return Long.parseLong(strBuilder.toString());
    }
}
