package com.zcunsoft.accesslog.processing.bean;

import lombok.Data;

@Data
public class Region {
    private String clientIp = "";

    private String country = "";

    private String province = "";

    private String city = "";
}
