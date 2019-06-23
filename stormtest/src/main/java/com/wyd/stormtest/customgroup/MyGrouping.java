package com.wyd.stormtest.customgroup;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class MyGrouping implements CustomStreamGrouping {

    //接收目标任务的id集合
    private List<Integer> targetTasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> subTaskIds = new ArrayList<Integer>();
        for(int i = 0; i < targetTasks.size() /2; i++){
            subTaskIds.add(targetTasks.get(i));
        }
        return subTaskIds;
    }
}
