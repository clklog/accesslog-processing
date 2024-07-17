package com.zcunsoft.accesslog.processing;

import com.zcunsoft.accesslog.processing.bean.AccessLog;
import com.zcunsoft.accesslog.processing.utils.ExtractUtil;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import nl.basjes.parse.useragent.AbstractUserAgentAnalyzer;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class AppTest {
    private Object getFieldValueByName(String fieldName, Object o) {
        try {
            String firstLetter = fieldName.substring(0, 1).toUpperCase();
            String getter = "get" + firstLetter + fieldName.substring(1);
            Method method = o.getClass().getMethod(getter, new Class[]{});
            Object value = method.invoke(o, new Object[]{});
            if (value != null) {
                value = value.toString();
            }
            return value;
        } catch (Exception e) {

            return null;
        }
    }

    private List<Object> getFieldsInfo(Object o) {
        Field[] fields = o.getClass().getDeclaredFields();
        String[] fieldNames = new String[fields.length];
        List<Object> list = new ArrayList();
        for (int i = 0; i < fields.length; i++) {
            list.add(getFieldValueByName(fields[i].getName(), o));
        }
        return list;
    }

    @TestFactory
    Collection<DynamicTest> dynamicTestExtractToAccesslog() throws ParseException {
        AbstractUserAgentAnalyzer userAgentAnalyzer = UserAgentAnalyzer.newBuilder().hideMatcherLoadStats().withCache(10000)
                .build();

        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();

        Grok grok = grokCompiler.compile("%{IPORHOST:remote_addr} - \\[%{DATA:x_forward_for}\\] - %{DATA:remote_user} \\[%{HTTPDATE:time}\\] \\\"%{WORD:request_method} %{DATA:uri} HTTP/%{NUMBER:http_version}\\\" %{NUMBER:status} %{NUMBER:body_sent_bytes} \\\"%{DATA:http_referrer}\\\" \\\"%{DATA:http_user_agent}\\\" %{NUMBER:request_length} %{NUMBER:request_time} \\[%{DATA:proxy_upstream_name}\\] %{DATA:upstream_addr} %{DATA:upstream_response_length} %{DATA:upstream_response_time} %{DATA:upstream_status} %{DATA:req_id} %{DATA:host} \\[\\]");

        AccessLog actualLog = ExtractUtil.extractToAccessLog("39.10.49.152 - [39.10.49.152] - - [18/Jan/2024:09:10:49 +0800] \"POST /pub/goods/comments/unreadcount?time=123 HTTP/2.0\" 200 64 \"https://app.accesslog.com/\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Kuang/2.2.0\" 47 0.004 [accesslog-backend-80] 10.245.0.182:8181 75 0.004 200 a9ade1763ef2ddf3e54465a6b2a5b179 accesslog.com []", grok, userAgentAnalyzer);
        Object[] actualArr = getFieldsInfo(actualLog).toArray();

        AccessLog expectedLog = new AccessLog();
        expectedLog.setUpstreamAddr("10.245.0.182:8181");
        expectedLog.setUri("/pub/goods/comments/unreadcount");
        expectedLog.setRequestMethod("POST");
        expectedLog.setHttpHost("accesslog.com");
        expectedLog.setHttpUserAgent("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Kuang/2.2.0");
        expectedLog.setRemoteUser("-");
        expectedLog.setUpstreamStatus("200");
        expectedLog.setRequestTime(0.004f);
        expectedLog.setHttpVersion("2.0");
        expectedLog.setBodySentBytes("64");
        expectedLog.setHttpReferrer("https://app.accesslog.com/");
        expectedLog.setUpstreamResponseTime("4");
        expectedLog.setStatus("200");
        expectedLog.setRemoteAddr("39.10.49.152");
        expectedLog.setStatDate(java.sql.Date.valueOf("2024-01-18"));
        expectedLog.setStatHour("09");
        expectedLog.setStatMin("09:10");
        expectedLog.setTime(Timestamp.valueOf("2024-01-18 09:10:49"));
        expectedLog.setServerName("hqq");
        expectedLog.setApplicationCode("hqq");
        expectedLog.setBrowser("Kuang");
        expectedLog.setBrowserVersion("Kuang 2.2.0");
        expectedLog.setModel("Apple iPhone");
        expectedLog.setBrand("Apple");
        expectedLog.setManufacturer("Apple");
        Object[] expectedArr = getFieldsInfo(expectedLog).toArray();

        List<DynamicTest> dynamicTestList = new ArrayList<>();
        dynamicTestList.add(dynamicTest("test1 extractToLogBean dynamic test", () -> Assertions.assertArrayEquals(expectedArr, actualArr, "ok")));

        return dynamicTestList;
    }
}
