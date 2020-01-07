package com.slinkydeveloper.cloud.streaming.inbound.model

data class InboundStreamSpec(val name: String, val useMetaAsKey: String?, val useCeTime: Boolean = false)
