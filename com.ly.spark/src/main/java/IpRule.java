import java.io.Serializable;

public class IpRule implements Serializable {
    private Long startIP;
    private Long endIP;
    private String province;
    public IpRule(Long startIP,Long endIP,String Province){
        this.startIP=startIP;
        this.endIP=endIP;
        this.province=Province;
    }
    public Long getStartIP(){
        return startIP;
    }
    public Long getEndIP(){
        return endIP;
    }
    public String getProvince(){
         return province;
    }
}
