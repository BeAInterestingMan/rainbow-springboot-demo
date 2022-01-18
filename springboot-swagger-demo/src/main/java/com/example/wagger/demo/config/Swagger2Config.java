package com.example.wagger.demo.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;
/**
 *  @Description Swagger配置类
 *  @author liuhu
 *  @Date 2022/1/18 19:13
 */
@Configuration
@EnableSwagger2
@ConditionalOnProperty(matchIfMissing = true,value = "api.enable",havingValue = "true")
public class Swagger2Config {

    @Bean(value = "indexApi")
    public Docket indexApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("前台API分组")
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.example.swagger.user"))
                .paths(PathSelectors.any())
                .build();
    }


    @Bean(value = "adminApi")
    public Docket adminApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .groupName("后台API分组")
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.example.swagger.admin"))
                .paths(PathSelectors.any())
                .build();
    }


    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("swagger-bootstrap-ui RESTful APIs")
                .description("swagger-bootstrap-ui")
                .termsOfServiceUrl("http://localhost:8999/")
                .contact(new Contact("xx","","1649471814@qq.com"))
                .version("1.0")
                .build();
    }
}
