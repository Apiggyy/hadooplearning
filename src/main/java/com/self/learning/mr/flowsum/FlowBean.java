package com.self.learning.mr.flowsum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private String msisdn;
    private long upFlow;
    private long downFlow;
    private long totalFlow;

    public FlowBean() {
    }

    public FlowBean(String msisdn, long upFlow, long downFlow) {
        this.msisdn = msisdn;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = this.downFlow + this.upFlow;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(msisdn);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(totalFlow);
    }

    public void readFields(DataInput in) throws IOException {
        msisdn = in.readUTF();
        upFlow = in.readLong();
        downFlow = in.readLong();
        totalFlow = in.readLong();
    }

    @Override
    public String toString() {
        return msisdn + "\t" + upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

    public int compareTo(FlowBean o) {
        return totalFlow > o.getTotalFlow() ? -1 : 0;
    }

}
