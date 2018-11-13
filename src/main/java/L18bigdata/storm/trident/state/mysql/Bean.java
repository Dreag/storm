package L18bigdata.storm.trident.state.mysql;


public class Bean {

    private String tel;
    private long sum;
    private long txid;
    private String time;

    public Bean(){
    }
    
    public Bean(String tel, long sum, long txid, String time) {
        super();
        this.tel = tel;
        this.sum = sum;
        this.txid = txid;
        this.time = time;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "Bean [tel=" + tel + ", sum=" + sum + ", txid=" + txid + ", time=" + time + "]";
    }

}