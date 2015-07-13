/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sindicetech.mixedemotions.etl.delete;

import com.sindicetech.mixedemotions.etl.DwApiBean;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;

public class MyRouteConfig extends RouteBuilder {

    @Override
    public void configure() {
//        from("restlet:/persons?restletMethod=POST")
//                .setBody(simple("insert into person(firstName, lastName) values('${header.firstName}','${header.lastName}')"))
//                .to("jdbc:dataSource")
//                .setBody(simple("select * from person where id in (select max(id) from person)"))
//                .to("jdbc:dataSource");
//
//        from("restlet:/persons/{personId}?restletMethods=GET,PUT,DELETE")
//                .choice()
//                    .when(simple("${header.CamelHttpMethod} == 'GET'"))
//                        .setBody(simple("select * from person where id = ${header.personId}"))
//                    .when(simple("${header.CamelHttpMethod} == 'PUT'"))
//                        .setBody(simple("update person set firstName='${header.firstName}', lastName='${header.lastName}' where id = ${header.personId}"))
//                    .when(simple("${header.CamelHttpMethod} == 'DELETE'"))
//                        .setBody(simple("delete from person where id = ${header.personId}"))
//                    .otherwise()
//                        .stop()
//                .end()
//                .to("jdbc:dataSource");
//
//        from("restlet:/persons?restletMethod=GET")
//                .setBody(simple("select * from person"))
//                .to("jdbc:dataSource");


//      from("timer://foo?fixedRate=true&period=1000")
//          .process(new Processor() {
//            @Override
//            public void process(Exchange exchange) throws Exception {
//              exchange.getIn().setHeader(Exchange.HTTP_METHOD, "GET");
//              exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/xml");
//              exchange.getIn().setHeader(RestletConstants.RESTLET_LOGIN, "xxxx");
//              exchange.getIn().setHeader(RestletConstants.RESTLET_PASSWORD, "xxxx");
//            }
//          })
//          .to("restcustom")
//          .to("log:com.jakub?level=INFO");

      ProducerTemplate producer = getContext().createProducerTemplate();
      ConsumerTemplate consumer = getContext().createConsumerTemplate();
      MyBean myBean = new MyBean(consumer, producer);

//      from("timer://bar?fixedRate=true&period=1000")
//          .to("restlet:http://www.dw.com/api/list/mediacenter/2?restletMethod=GET")
//          .to("log:com.RESTLETXXX?level=INFO");

      DwApiBean dwApiBean = new DwApiBean(producer);

      from("timer://foo?fixedRate=true&period=5000")
          .bean(dwApiBean, "process");

      from("direct:asdf")
          .to("log:com.jakub123?level=INFO")
      .to("file:target/items");
    }
}
