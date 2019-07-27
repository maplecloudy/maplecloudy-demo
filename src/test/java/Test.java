import java.security.Timestamp;
import java.util.TimeZone;

public class Test {
  public static void main(String[] args) {
    long current = System.currentTimeMillis();// 当前时间毫秒数
    long zero = current / (1000 * 3600 * 24) * (1000 * 3600 * 24)
        - TimeZone.getDefault().getRawOffset();// 今天零点零分零秒的毫秒数
    long twelve = zero + 24 * 60 * 60 * 1000 - 1;// 今天23点59分59秒的毫秒数
    System.out.println(current);// 当前时间
    System.out.println(zero);// 今天零点零分零秒
    System.out.println(twelve);// 今天23点59分59秒
    
  }
}
