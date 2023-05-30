package com.jiruffe.fluxes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class StandaloneTests {

    @Test
    public void testBasic() {
        System.out.println(Flux.builder()
                .from("my_bucket")
                .measurement("my_measurement")
                .range("-1y", "-1m")
                .pivot(Arrays.asList("_time", "my_tag_1", "my_tag_2"), Arrays.asList("_field"), "_value")
                .group()
                .sort("_time")
                .limit(10, 100)
                .prettified()
                .build()
                .toString());
    }

}
