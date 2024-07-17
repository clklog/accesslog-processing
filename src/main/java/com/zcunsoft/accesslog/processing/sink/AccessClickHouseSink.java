package com.zcunsoft.accesslog.processing.sink;

import com.zcunsoft.accesslog.processing.bean.AccessLog;
import com.zcunsoft.accesslog.processing.utils.ClickHouseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

public class AccessClickHouseSink extends RichSinkFunction<List<AccessLog>> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    Connection conn = null;

    PreparedStatement preparedStatement;

    public AccessClickHouseSink() {

    }

    @Override
    public void open(Configuration config) throws SQLException {
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String clickhouseHost = parameters.get("clickhouse.host");
        String clickhouseDb = parameters.get("clickhouse.database");
        String clickhouseUsername = parameters.get("clickhouse.username");
        String clickhousePwd = parameters.get("clickhouse.password");
        conn = ClickHouseUtil.getConn(clickhouseHost, clickhouseDb, clickhouseUsername, clickhousePwd);

        String accessSql = "insert into " + parameters.get("clickhouse.nginx_access_table") + " (country,upstream_uri,upstream_addr,uri,request_method,http_host,http_user_agent," +
                "stat_hour,manufacturer,remote_user,upstream_status,request_time,province,browser,model,browser_version," +
                "brand,remote_addr,stat_date,stat_min,time_local,http_version,city,body_sent_bytes,http_referrer," +
                "server_name,upstream_response_time,status,application_code,create_time) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        preparedStatement = conn.prepareStatement(accessSql);

    }

    @Override
    public void invoke(List<AccessLog> accessLogList, Context context) throws Exception {
        if (conn != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (AccessLog accessLog : accessLogList) {
                if (StringUtils.isNotBlank(accessLog.getUri())) {
                    preparedStatement.setString(1, accessLog.getCountry());
                    preparedStatement.setString(2, StringUtils.defaultString(accessLog.getUpstreamUri()));
                    preparedStatement.setString(3, StringUtils.defaultString(accessLog.getUpstreamAddr()));
                    preparedStatement.setString(4, accessLog.getUri());
                    preparedStatement.setString(5, accessLog.getRequestMethod());
                    preparedStatement.setString(6, StringUtils.defaultString(accessLog.getHttpHost()));
                    preparedStatement.setString(7, accessLog.getHttpUserAgent());
                    preparedStatement.setString(8, accessLog.getStatHour());
                    preparedStatement.setString(9, StringUtils.defaultString(accessLog.getManufacturer()));
                    preparedStatement.setString(10, StringUtils.defaultString(accessLog.getRemoteUser()));
                    preparedStatement.setString(11, StringUtils.defaultString(accessLog.getUpstreamStatus()));
                    preparedStatement.setFloat(12, 1000 * accessLog.getRequestTime());
                    preparedStatement.setString(13, accessLog.getProvince());
                    preparedStatement.setString(14, StringUtils.defaultString(accessLog.getBrowser()));
                    preparedStatement.setString(15, StringUtils.defaultString(accessLog.getModel()));
                    preparedStatement.setString(16, StringUtils.defaultString(accessLog.getBrowserVersion()));
                    preparedStatement.setString(17, StringUtils.defaultString(accessLog.getBrand()));
                    preparedStatement.setString(18, accessLog.getRemoteAddr());
                    preparedStatement.setDate(19, accessLog.getStatDate());
                    preparedStatement.setString(20, accessLog.getStatMin());
                    preparedStatement.setTimestamp(21, accessLog.getTime());
                    preparedStatement.setString(22, StringUtils.defaultString(accessLog.getHttpVersion()));
                    preparedStatement.setString(23, accessLog.getCity());
                    preparedStatement.setString(24, StringUtils.defaultString(accessLog.getBodySentBytes()));
                    preparedStatement.setString(25, StringUtils.defaultString(accessLog.getHttpReferrer()));
                    preparedStatement.setString(26, accessLog.getServerName());
                    preparedStatement.setString(27, StringUtils.defaultString(accessLog.getUpstreamResponseTime()));
                    preparedStatement.setString(28, accessLog.getStatus());
                    preparedStatement.setString(29, accessLog.getApplicationCode());
                    preparedStatement.setString(30, sdf.format(new Timestamp(System.currentTimeMillis())));
                    preparedStatement.addBatch();
                }
            }
            long now = System.currentTimeMillis();
            int[] ints = preparedStatement.executeBatch();
            logger.info("accesslog sink " + ints.length + " records token " + (System.currentTimeMillis() - now));
        } else {
            logger.error("conn is null ");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
