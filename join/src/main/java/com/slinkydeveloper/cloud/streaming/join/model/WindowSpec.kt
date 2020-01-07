package com.slinkydeveloper.cloud.streaming.join.model

import java.time.Duration

data class WindowSpec(
    val length: Duration,
    var hop: Duration? = null
)
