package com.zcunsoft.accesslog.processing.utils;

import com.zcunsoft.accesslog.processing.bean.AccessLog;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.Match;
import nl.basjes.parse.useragent.AbstractUserAgentAnalyzer;
import nl.basjes.parse.useragent.AgentField;
import nl.basjes.parse.useragent.UserAgent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

public class ExtractUtil {
    private static final Logger logger = LoggerFactory.getLogger(ExtractUtil.class);


    private static final ThreadLocal<DateFormat> formatter = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        }
    };

    private static final ThreadLocal<DateFormat> HH = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("HH");
        }
    };

    private static final ThreadLocal<DateFormat> HHmm = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("HH:mm");
        }
    };

    private static final ThreadLocal<DecimalFormat> decimalFormat =
            new ThreadLocal<DecimalFormat>() {
                @Override
                protected DecimalFormat initialValue() {
                    return new DecimalFormat("0.##");
                }
            };

    public static AccessLog extractToAccessLog(String log, Grok grok, AbstractUserAgentAnalyzer userAgentAnalyzer) throws ParseException {
        AccessLog accessLog = new AccessLog();

        Match gm = grok.match(log);
        Map<String, Object> capture = gm.capture();

        if (capture.containsKey("upstream-uri")) {
            accessLog.setUpstreamUri(capture.get("upstream-uri").toString());
        }
        if (capture.containsKey("upstream_addr")) {
            accessLog.setUpstreamAddr(capture.get("upstream_addr").toString());
        }
        if (capture.containsKey("uri")) {
            String uri = capture.get("uri").toString();
            int index = uri.indexOf("?");
            if (index != -1) {
                uri = uri.substring(0, index);
            }
            accessLog.setUri(uri);
        }
        if (capture.containsKey("request_method")) {
            accessLog.setRequestMethod(capture.get("request_method").toString());
        }
        if (capture.containsKey("host")) {
            accessLog.setHttpHost(capture.get("host").toString());
        }
        if (capture.containsKey("http_user_agent")) {
            accessLog.setHttpUserAgent(capture.get("http_user_agent").toString());
        }
        if (capture.containsKey("remote_user")) {
            accessLog.setRemoteUser(capture.get("remote_user").toString());
        }
        if (capture.containsKey("upstream_status")) {
            accessLog.setUpstreamStatus(capture.get("upstream_status").toString());
        }
        if (capture.containsKey("request_time")) {
            accessLog.setRequestTime(Float.parseFloat(capture.get("request_time").toString()));
        }
        if (capture.containsKey("http_version")) {
            accessLog.setHttpVersion(capture.get("http_version").toString());
        }
        if (capture.containsKey("body_sent_bytes")) {
            accessLog.setBodySentBytes(capture.get("body_sent_bytes").toString());
        }
        if (capture.containsKey("http_referrer")) {
            accessLog.setHttpReferrer(capture.get("http_referrer").toString());
        }
        if (capture.containsKey("upstream_response_time")) {
            String upStreamRespTime = capture.get("upstream_response_time").toString();
            if (!upStreamRespTime.equalsIgnoreCase("-")) {
                try {
                    float fRespTime = Float.parseFloat(upStreamRespTime) * 1000;
                    upStreamRespTime = decimalFormat.get().format(fRespTime);
                } catch (Exception ex) {
                    logger.error("parse upstream_response_time error " + log);
                }
            }
            accessLog.setUpstreamResponseTime(upStreamRespTime);
        }
        if (capture.containsKey("status")) {
            accessLog.setStatus(capture.get("status").toString());
        }
        if (capture.containsKey("remote_addr")) {
            accessLog.setRemoteAddr(capture.get("remote_addr").toString());
        }
        if (capture.containsKey("time")) {
            Date date = formatter.get().parse(capture.get("time").toString());
            accessLog.setStatDate(new java.sql.Date(date.getTime()));
            accessLog.setStatHour(HH.get().format(date));
            accessLog.setStatMin(HHmm.get().format(date));
            accessLog.setTime(new Timestamp(date.getTime()));
        }
        accessLog.setServerName("hqq");
        accessLog.setApplicationCode("hqq");

        if (StringUtils.isNotBlank(accessLog.getHttpUserAgent())) {
            UserAgent userAgent = userAgentAnalyzer.parse(accessLog.getHttpUserAgent());

            String browser = "";
            AgentField browserField = userAgent.get(UserAgent.AGENT_NAME);
            if (!browserField.isDefaultValue()) {
                browser = browserField.getValue();
            }
            accessLog.setBrowser(browser);

            String browserVersion = "";
            AgentField browserVersionField = userAgent.get(UserAgent.AGENT_NAME_VERSION);
            if (!browserVersionField.isDefaultValue()) {
                browserVersion = browserVersionField.getValue();
            }
            accessLog.setBrowserVersion(browserVersion);

            String model = "";
            AgentField deviceName = userAgent.get(UserAgent.DEVICE_NAME);
            if (!deviceName.isDefaultValue()) {
                model = deviceName.getValue();
            }
            accessLog.setModel(model);

            String brand = "";
            AgentField deviceBrand = userAgent.get(UserAgent.DEVICE_BRAND);
            if (!deviceBrand.isDefaultValue()) {
                brand = deviceBrand.getValue();
            }
            accessLog.setBrand(brand);
            accessLog.setManufacturer(brand);
        }

        return accessLog;
    }
}
