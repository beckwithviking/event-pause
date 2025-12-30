package com.example.app.config;

import com.example.app.topology.PauseConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "app")
public class PauseFlowProperties {

    private List<PauseConfig> flows = new ArrayList<>();

    public List<PauseConfig> getFlows() {
        return flows;
    }

    public void setFlows(List<PauseConfig> flows) {
        this.flows = flows;
    }
}
