package com.ytjj.qmyx.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pv {
    Long browsetime;
    String channel;
    Long createtime;
    String cur_date;
    String daytime;
    String frompage;
    int hourtime;
    String id;
    String ip;
    int isnewuser;
    String param;
    String pathcode;
    String platform;
    String seq_id;
    String topage;
    String uuid;
    String model;
}
