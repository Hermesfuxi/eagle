import org.apache.commons.lang3.RandomUtils;

public class RandomTest {
    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            // 不包括10
            System.out.println(RandomUtils.nextInt(1, 10));
        }
    }
}
