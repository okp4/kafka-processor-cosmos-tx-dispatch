package com.okp4.processor.cosmos

import com.google.protobuf.util.JsonFormat
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import cosmos.tx.v1beta1.TxOuterClass
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject

@ApplicationScoped
enum class FilteredTxType(val code: Int) {
    ERROR(-1),
    UNFILTERED(-2),
}

/**
 * Simple Kafka Stream Processor that consumes a block on a topic and returns his transactions on another.
 */
@ApplicationScoped
class TopologyProducer {
    fun TxOuterClass.TxRaw.toTx(): TxOuterClass.Tx {
        return TxOuterClass.Tx.newBuilder()
            .addAllSignatures(this.signaturesList)
            .setBody(TxOuterClass.TxBody.parseFrom(this.bodyBytes))
            .setAuthInfo(TxOuterClass.AuthInfo.parseFrom(this.authInfoBytes))
            .build()
    }

    val jsonPathConf: Configuration = Configuration.builder()
        .options(Option.SUPPRESS_EXCEPTIONS)
        .options(Option.AS_PATH_LIST)
        .build()

    @field:ConfigProperty(name = "topic.in", defaultValue = "topic.in")
    lateinit var topicIn: String

    @field:ConfigProperty(name = "topic.dlq", defaultValue = "topic.dlq")
    lateinit var topicDLQ: String

    @field:ConfigProperty(name = "topic.error", defaultValue = "")
    var topicError: String? = null

    @Inject
    lateinit var txsDispatch: TxsDispatch

    @Produces
    fun buildTopology(): Topology {
        val logger: Logger = LoggerFactory.getLogger("com.okp4.processor.cosmos.topology")

        val txDispatchRules = txsDispatch.getTxDispatchList()

        val formatter =
            JsonFormat.printer()
                .usingTypeRegistry(ProtoTypeRegistry.protoTypeRegistry)
                .omittingInsignificantWhitespace()

        return StreamsBuilder().apply {
            stream(topicIn, Consumed.with(Serdes.String(), Serdes.ByteArray()).withName("input"))
                .mapValues(
                    { v ->
                        Pair(
                            v,
                            kotlin.runCatching {
                                TxOuterClass.TxRaw.parseFrom(v).toTx()
                            }
                        )
                    },
                    Named.`as`("tx-deserialization")
                )
                .mapValues({ v ->
                    getEvaluatedTxList(v, txDispatchRules, formatter, logger)
                }, Named.`as`("evaluate-tx"))
                .flatMapValues(
                    { it ->
                        it
                    },
                    Named.`as`("flat-evaluated-tx-list")
                )
                .split()
                // If deserialization failed → topic Error
                .branch(
                    { _, v -> v.first == FilteredTxType.ERROR.code },
                    Branched.withConsumer { ks ->
                        ks.mapValues({ v ->
                            v.second
                        }, Named.`as`("extract-error-pair"))
                            .peek(
                                { k, v ->
                                    v.second.onFailure {
                                        logger.warn("Deserialization failed for tx with key <$k>: ${it.message}", it)
                                    }
                                },
                                Named.`as`("log-deserialization-failure")
                            )
                            .mapValues({ pair -> pair.first }, Named.`as`("extract-original-bytearray"))
                            .apply {
                                if (!topicError.isNullOrEmpty()) {
                                    logger.info("Failed tx will be sent to the topic $topicError")
                                    to(
                                        topicError,
                                        Produced.with(Serdes.String(), Serdes.ByteArray()).withName("error")
                                    )
                                }
                            }
                    }
                )
                .apply {
                    txDispatchRules.rules?.forEachIndexed { ruleIndex, rule ->
                        branch(
                            { _, v ->
                                v.first == ruleIndex
                            },
                            Branched.withConsumer { ks ->
                                ks.mapValues(
                                    { v ->
                                        v.second
                                    },
                                    Named.`as`("extract-pair-rule-$ruleIndex-${rule?.name}")
                                )
                                    .mapValues(
                                        { v ->
                                            v.second.getOrThrow()
                                        },
                                        Named.`as`("extract-tx-rule-$ruleIndex-${rule?.name}")
                                    ).peek(
                                        { k, tx -> logger.info("→ tx with key <$k> (${tx.body.messagesList.count()} messages): ${rule?.outputTopic}") },
                                        Named.`as`("log-tx-rule-$ruleIndex-${rule?.name}")
                                    ).mapValues(
                                        { tx ->
                                            tx.toByteArray()
                                        },
                                        Named.`as`("convert-unfiltered-txs-to-bytearray-rule-$ruleIndex-${rule?.name}")
                                    ).to(
                                        rule?.outputTopic,
                                        Produced.with(Serdes.String(), Serdes.ByteArray())
                                            .withName("output-topic-rule-$ruleIndex-${rule?.name}")
                                    )
                            }
                        )
                    }
                }
                .defaultBranch(
                    // Default to dead letter queue
                    Branched.withConsumer { ks ->
                        ks.mapValues(
                            { v ->
                                v.second
                            },
                            Named.`as`("extract-pair-dlq")
                        )
                            .mapValues(
                                { v ->
                                    v.second.getOrThrow()
                                },
                                Named.`as`("extract-tx")
                            ).peek(
                                { k, tx -> logger.info("→ tx with key <$k> (${tx.body.messagesList.count()} messages): unfiltered") },
                                Named.`as`("log-tx-unfiltered")
                            ).mapValues(
                                { tx ->
                                    tx.toByteArray()
                                },
                                Named.`as`("convert-unfiltered-txs-to-bytearray")
                            ).to(
                                topicDLQ,
                                Produced.with(Serdes.String(), Serdes.ByteArray()).withName("dlq")
                            )
                    }
                )
        }.build()
    }

    fun getEvaluatedTxList(
        pair: Pair<ByteArray, Result<TxOuterClass.Tx>>,
        txDispatchRules: TxDispatchRules,
        formatter: JsonFormat.Printer,
        logger: Logger
    ): List<Pair<Int, Pair<ByteArray, Result<TxOuterClass.Tx>>>> {
        return mutableListOf<Pair<Int, Pair<ByteArray, Result<TxOuterClass.Tx>>>>().apply {
            pair.second.onSuccess { tx ->
                formatter.print(tx).let { txJson ->
                    txDispatchRules.rules?.forEachIndexed { ruleKey, rule ->
                        runCatching {
                            with(JsonPath.using(jsonPathConf).parse(txJson)) {
                                if (this@with.read<List<String>>(rule?.predicate).isNotEmpty()) {
                                    this@apply.add(Pair(ruleKey, pair))
                                }
                            }
                        }.onFailure {
                            logger.warn("JsonPath rule <${rule?.name}> error: ${it.message}")
                        }
                    }
                }
                this@apply.takeIf { it.isEmpty() }?.add(Pair(FilteredTxType.UNFILTERED.code, pair))
            }.onFailure {
                this@apply.add(Pair(FilteredTxType.ERROR.code, pair))
            }
        }
    }
}
