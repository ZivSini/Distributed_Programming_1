import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Region;

public class Utils {
    public static String amiID = "ami-06e402a0205298790";
    public static InstanceType instanceType = InstanceType.T2_MEDIUM;
    public static int maxWorkers = 10;
    public static String[] colors = {
      null,
            "color:darkRed;",
            "color:red;",
            "color:black;",
            "color:lightGreen;",
            "color:darkGreen;",
    };
}
