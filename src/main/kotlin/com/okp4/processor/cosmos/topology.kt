package com.okp4.processor.cosmos

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Branched.*
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Simple Kafka Stream Processor that consumes a message on a topic and returns a new message on another.
 */
fun topology(props: Properties): Topology {
    val logger = LoggerFactory.getLogger("com.okp4.processor.cosmos.topology")
    val topicIn = requireNotNull(props.getProperty("topic.in")) {
        "Option 'topic.in' was not specified."
    }
    val topicOut = requireNotNull(props.getProperty("topic.out")) {
        "Option 'topic.out' was not specified."
    }
    val topicError: String? = props.getProperty("topic.error")

    return StreamsBuilder()
        .apply {
            stream(topicIn, Consumed.with(Serdes.String(), Serdes.String()).withName("input"))
                .peek({ _, _ -> logger.info("Received a message") }, Named.`as`("log"))
                .mapValues({ v ->
                    Pair(
                        v,
                        runCatching {
                            require(!v.isNullOrBlank()) { "Message cannot be blank" }

                            "Hello $v!"
                        }
                    )
                }, Named.`as`("map-value"))
                    .split().branch(
                        { _, v -> v.second.isFailure },
                        withConsumer { ks ->
                            ks.peek(
                                { k, v ->
                                    v.second.onFailure {
                                        logger.warn("Deserialization failed for block with key <$k>: ${it.message}", it)
                                    }
                                },
                                Named.`as`("log-failure")
                            )
                                .mapValues { v -> v.first }
                                .apply {
                                    if (!topicError.isNullOrEmpty()) {
                                        logger.info("Failed block will be sent to the topic $topicError")
                                        to(
                                            topicError, Produced.with(Serdes.String(), Serdes.String()).withName("error")
                                        )
                                    }
                                }
                        }
                    )
                    .defaultBranch(
                        withConsumer { ks ->
                            ks.mapValues(
                                { v -> v.second.getOrThrow() }, Named.`as`("unwrap-value")
                                ).to(
                                    topicOut, Produced.with(Serdes.String(), Serdes.String()).withName("output")
                                )
                            }
                        )
                }.build()
        }
        