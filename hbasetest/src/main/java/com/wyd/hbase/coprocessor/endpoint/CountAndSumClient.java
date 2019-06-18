package com.wyd.hbase.coprocessor.endpoint;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class CountAndSumClient {

    public static class CountAndSumResult {
        public long count;
        public double sum;

    }

    private Connection connection;

    public CountAndSumClient(Connection connection) {
        this.connection = connection;
    }

    public CountAndSumResult call(String tableName, String family, String column, String
            startRow, String endRow) throws Throwable {
        Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
        final CountAndSumProtocol.CountAndSumRequest request = CountAndSumProtocol.CountAndSumRequest
                .newBuilder()
                .setFamily(family)
                .setColumn(column)
                .build();

        byte[] startKey = (null != startRow) ? Bytes.toBytes(startRow) : null;
        byte[] endKey = (null != endRow) ? Bytes.toBytes(endRow) : null;
        // coprocessorService方法的第二、三个参数是定位region的，是不是范围查询，在startKey和endKey之间的region上的数据都会参与计算
        Map<byte[], CountAndSumResult> map = table.coprocessorService(CountAndSumProtocol.RowCountAndSumService.class,
                startKey, endKey, new Batch.Call<CountAndSumProtocol.RowCountAndSumService,
                        CountAndSumResult>() {
                    @Override
                    public CountAndSumResult call(CountAndSumProtocol.RowCountAndSumService service) throws IOException {
                        BlockingRpcCallback<CountAndSumProtocol.CountAndSumResponse> rpcCallback = new BlockingRpcCallback<>();
                        service.getCountAndSum(null, request, rpcCallback);
                        CountAndSumProtocol.CountAndSumResponse response = rpcCallback.get();
                        //直接返回response也行。
                        CountAndSumResult responseInfo = new CountAndSumResult();
                        responseInfo.count = response.getCount();
                        responseInfo.sum = response.getSum();
                        return responseInfo;
                    }
                });

        CountAndSumResult result = new CountAndSumResult();
        for (CountAndSumResult ri : map.values()) {
            result.count += ri.count;
            result.sum += ri.sum;
        }

        return result;
    }

}
