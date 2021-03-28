package bigdata.hermesfuxi.eagle.etl.functions;

import bigdata.hermesfuxi.eagle.etl.bean.DataLogBean;
import bigdata.hermesfuxi.eagle.etl.utils.DBConnectUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<DataLogBean, Connection, Void> {
    public static final Logger logger = LoggerFactory.getLogger(MySQLTwoPhaseCommitSink.class);

    /**
     * 事务数据是保存在state状态里，checkpoint后可回复，所以需要序列化
     */
    public MySQLTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        System.out.println("beginTransaction run ...");
        return DBConnectUtils.getConnection();
    }

    @Override
    protected void invoke(Connection transaction, DataLogBean bean, Context context) throws Exception {
        System.out.println("invoke run ...");
        PreparedStatement ps = transaction.prepareStatement("insert into `data_log_bean` " +
                "(carrier,deviceId,deviceType,eventId,isNew,latitude,longitude,geoHashCode,country,province,city,district,netType," +
                "osName,osVersion,releaseChannel,resolution,sessionId,`timestamp`, id_str) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ps.setString(1, bean.getCarrier());
        ps.setString(2, bean.getDeviceId());
        ps.setString(3, bean.getDeviceType());
        ps.setString(4, bean.getEventId());
        ps.setInt(5, bean.getIsNew());
        ps.setDouble(6, bean.getLatitude());
        ps.setDouble(7, bean.getLongitude());
        ps.setString(8, bean.getGeoHashCode());
        ps.setString(9, bean.getCountry());
        ps.setString(10, bean.getProvince());
        ps.setString(11, bean.getCity());
        ps.setString(12, bean.getDistrict());
        ps.setString(13, bean.getNetType());
        ps.setString(14, bean.getOsName());
        ps.setString(15, bean.getOsVersion());
        ps.setString(16, bean.getReleaseChannel());
        ps.setString(17, bean.getResolution());
        ps.setString(18, bean.getSessionId());
        ps.setLong(19, bean.getTimestamp());
        ps.setString(20, bean.getId());
        String sqlStr = ps.toString().substring(ps.toString().indexOf(":") + 2);
        System.out.println(sqlStr);
        //执行insert语句
        ps.execute();
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
        System.out.println("preCommit run ...");
    }

    @Override
    protected void commit(Connection transaction) {
        System.out.println("commit run ...");
        try {
            transaction.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    protected void abort(Connection transaction) {
        System.out.println("abort run ...");
        try {
            transaction.rollback();
        } catch (SQLException throwables) {
            logger.error(throwables.getMessage());
        }
    }
}
