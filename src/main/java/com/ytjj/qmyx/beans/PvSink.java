package com.ytjj.qmyx.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PvSink {
    String platform;
    String channel;
    int cnt;
    Timestamp sinktime;
}
