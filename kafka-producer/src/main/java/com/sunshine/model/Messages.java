package com.sunshine.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
public class Messages {

    private Long id;

    private String msg;

    private Date sendTime;

}