package com.slinkydeveloper.cloud.streaming.join.model

data class JoinSpec(
    val left: String,
    val right: String,
    val leftJoinLatest: Boolean = false,
    val rightJoinLatest: Boolean = false,
    val success: String,
    val window: WindowSpec
)


