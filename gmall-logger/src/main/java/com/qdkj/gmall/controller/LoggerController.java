package com.qdkj.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class LoggerController {

    /**
     * spring自带的功能，加了kafka的依赖，不需要再编写kafka发送的属性；需要在properties中配置下kafka的地址
      */
    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String jsonLog){

      log.info(jsonLog);

      kafkaTemplate.send("ods_base_log", jsonLog);

      return "success";
    }

}
