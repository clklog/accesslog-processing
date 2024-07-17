package com.zcunsoft.accesslog.processing.function;

import com.zcunsoft.accesslog.processing.bean.AccessLog;
import com.zcunsoft.accesslog.processing.bean.Region;
import com.zcunsoft.accesslog.processing.utils.ExtractUtil;
import com.zcunsoft.accesslog.processing.utils.IPUtil;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import nl.basjes.parse.useragent.AbstractUserAgentAnalyzer;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class AccessLogMapper extends RichMapFunction<String, AccessLog> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ObjectMapper objectMapper;

    private AbstractUserAgentAnalyzer userAgentAnalyzer;

    private transient Jedis jedis;

    private IPUtil ipUtil;

    private Grok grok = null;

    //private transient JedisPool jedisPool;

    public AccessLogMapper() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public AccessLog map(String line) {
        AccessLog accessLog = new AccessLog();

        try {
            JsonNode jsonNode = objectMapper.readTree(line);
            if ("stdout".equalsIgnoreCase(jsonNode.get("stream").asText())) {
                String log = jsonNode.get("log").asText();
                accessLog = ExtractUtil.extractToAccessLog(log, grok, userAgentAnalyzer);
                if (StringUtils.isNotBlank(accessLog.getRemoteAddr())) {
                    Region region = null;
                    String regionInfo = jedis.hget("ClientIpRegionHash", accessLog.getRemoteAddr());
                    if (StringUtils.isNotBlank(regionInfo)) {
                        region = new Region();
                        String[] regionArr = regionInfo.split(",", -1);
                        if (regionArr.length == 4) {
                            region.setCountry(regionArr[1]);
                            region.setProvince(regionArr[2]);
                            region.setCity(regionArr[3]);
                        }
                    } else {
                        region = ipUtil.analysisRegionFromIp(accessLog.getRemoteAddr());
                        String sbRegion = region.getClientIp() + "," + region.getCountry() + "," + region.getProvince() + "," + region.getCity();
                        jedis.hset("ClientIpRegionHash", region.getClientIp(), sbRegion);
                    }
                    accessLog.setCountry(region.getCountry());
                    accessLog.setProvince(region.getProvince());
                    accessLog.setCity(region.getCity());
                }
            }
        } catch (Exception ex) {
            logger.error("AccessLogMapper err ", ex);
        }

        return accessLog;
    }

    @Override
    public void open(Configuration configuration) {
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String redisHost = parameters.get("redis.host");
        int redisPort = parameters.getInt("redis.port");
        String redisPwd = parameters.get("redis.password");
        if ("".equalsIgnoreCase(redisPwd)) {
            redisPwd = null;
        }

        jedis = new Jedis(redisHost, redisPort);
        if (StringUtils.isNotBlank(redisPwd)) {
            jedis.auth(redisPwd);
        }
        logger.info("open redis ok");
        ipUtil = new IPUtil(parameters.get("processing-file-location"));
        ipUtil.loadIpFile();

//        int maxTotal = parameters.getInt("redis.pool.max-active", 16);
//        int maxIdle = parameters.getInt("redis.pool.max-idle", 16);
//        int minIdle = parameters.getInt("redis.pool.min-idle", 16);
//        JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxIdle(maxIdle);
//        config.setMaxTotal(maxTotal);
//        config.setMinIdle(minIdle);
//        jedisPool = new JedisPool(config, redisHost, redisPort, 3000, redisPwd);
//        jedis = jedisPool.getResource();

        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();

        grok = grokCompiler.compile(parameters.get("accesslog-grok"));

        userAgentAnalyzer = UserAgentAnalyzer.newBuilder().hideMatcherLoadStats().withCache(10000)
                .build();
    }

    @Override
    public void close() {
        try {
            jedis.close();
        } catch (Exception ex) {
            logger.error("close jedis error " + ex.getMessage());
        }

//        try {
//            jedisPool.close();
//        } catch (Exception ex) {
//            logger.error("close jedisPool error " + ex.getMessage());
//        }
    }
}
