package cn.kjky.collector.arms;

import cn.kjky.collector.config.CollectorConfig;
import com.aliyuncs.CommonRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class ArmsRpcApiTest {
    @Test
    void separatesApiRoutingRegionFromResourceRegionAndAddsSiteHeaders() {
        CollectorConfig.ArmsConfig config = new CollectorConfig.ArmsConfig();
        config.protocol = "HTTP";
        config.apiRegionId = "bjdc-1";
        config.regionId = "zj-3";
        config.organizationId = "426";
        config.resourceGroupId = "rs-test";
        config.product = "ARMS";
        config.version = "2019-08-08";

        CommonRequest request = ArmsRpcApi.buildRequest(config,
                "arms-api.res.sgmc.sgcc.com.cn", "ListTraceApps", Collections.emptyMap());

        assertThat(request.getSysRegionId()).isEqualTo("bjdc-1");
        assertThat(request.getSysQueryParameters()).containsEntry("RegionId", "zj-3");
        assertThat(request.getSysHeadParameters())
                .containsEntry("x-acs-organizationid", "426")
                .containsEntry("x-acs-resourcegroupid", "rs-test");
    }

    @Test
    void omitsOptionalSiteHeadersWhenTheyAreBlank() {
        CollectorConfig.ArmsConfig config = new CollectorConfig.ArmsConfig();
        config.protocol = "HTTP";
        config.apiRegionId = "bjdc-1";
        config.regionId = "zj-3";
        config.organizationId = " ";
        config.resourceGroupId = "__REQUIRED_ON_SITE__";
        config.product = "ARMS";
        config.version = "2019-08-08";

        CommonRequest request = ArmsRpcApi.buildRequest(config,
                "arms-api.res.sgmc.sgcc.com.cn", "ListTraceApps", Collections.emptyMap());

        assertThat(request.getSysHeadParameters())
                .doesNotContainKeys("x-acs-organizationid", "x-acs-resourcegroupid");
    }
}
