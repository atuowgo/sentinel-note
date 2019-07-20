package com.alibaba.csp.sentinel.demo.extend;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.context.ContextUtil;
import com.alibaba.csp.sentinel.slots.block.BlockException;

public class ExtendTest {

    public static void main(String[] args) throws BlockException, InterruptedException {
        Entry node1C = SphU.entry("nodeC");
        Entry node1A = SphU.entry("nodeA");
        Entry node1CC = SphU.entry("nodeC");
        Entry node1D = SphU.entry("nodeD");
        if (node1D != null){
            node1D.exit();
        }
        if (node1CC != null){
            node1CC.exit();
        }
        if (node1A != null){
            node1A.exit();
        }
        if (node1C != null){
            node1C.exit();
        }

        ContextUtil.enter("entrance1", "appA");
        Entry node2A = SphU.entry("nodeA");
        Entry node2B = SphU.entry("nodeB");
        if (node2B != null){
            node2B.exit();
        }
        if (node2A != null) {
            node2A.exit();
        }
        ContextUtil.exit();

        ContextUtil.enter("entrance2", "appA");
        Entry node3A = SphU.entry("nodeA");
        if (node3A != null) {
            node3A.exit();
        }
        ContextUtil.exit();

        Thread.sleep(100000);
    }

}
