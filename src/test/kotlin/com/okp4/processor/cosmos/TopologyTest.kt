package com.okp4.processor.cosmos

import com.google.protobuf.ByteString
import com.google.protobuf.ByteString.copyFrom
import cosmos.bank.v1beta1.Tx
import cosmos.base.v1beta1.CoinOuterClass
import cosmos.tx.v1beta1.TxOuterClass
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.data.forAll
import io.kotest.data.headers
import io.kotest.data.row
import io.kotest.data.table
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import java.util.Base64.getDecoder

fun String.b64ToByteString(): ByteString = copyFrom(getDecoder().decode(this))

val tx1: ByteArray = TxOuterClass.Tx.newBuilder()
    .addSignatures("2utl1VHdSC3pyHCNgeNmgGImnEChQcd9sWEgi4Uc4lwOhWhrqYy8WkJ8xNkVzjF/WVg3ayVWZp8ipVzO1kUK9g==".b64ToByteString())
    .setAuthInfo(
        TxOuterClass.AuthInfo.newBuilder()
            .mergeFrom("Ck4KRgofL2Nvc21vcy5jcnlwdG8uc2VjcDI1NmsxLlB1YktleRIjCiECf1JPoIG8+pMDKtmH2vtOg5+xvfNxoDXV0iD++Ha5a/0SBAoCCAESBBDAmgw=".b64ToByteString())
            .build()
    ).setBody(
        TxOuterClass.TxBody.newBuilder()
            .addMessages(
                com.google.protobuf.Any.pack(
                    Tx.MsgSend.newBuilder()
                        .addAmount(CoinOuterClass.Coin.getDefaultInstance())
                        .setFromAddress("testFrom")
                        .setToAddress("tesTo")
                        .build()
                )
            )
    ).build().toByteArray()
val tx2: ByteArray = TxOuterClass.Tx.newBuilder()
    .addSignatures("2utl1VHdSC3pyHCNgeNmgGImnEChQcd9sWEgi4Uc4lwOhWhrqYy8WkJ8xNkVzjF/WVg3ayVWZp8ipVzO1kUK9g==".b64ToByteString())
    .setAuthInfo(
        TxOuterClass.AuthInfo.newBuilder()
            .mergeFrom("Ck4KRgofL2Nvc21vcy5jcnlwdG8uc2VjcDI1NmsxLlB1YktleRIjCiECf1JPoIG8+pMDKtmH2vtOg5+xvfNxoDXV0iD++Ha5a/0SBAoCCAESBBDAmgw=".b64ToByteString())
            .build()
    ).setBody(
        TxOuterClass.TxBody.newBuilder()
            .addMessages(
                com.google.protobuf.Any.pack(
                    Tx.MsgSend.newBuilder()
                        .addAmount(CoinOuterClass.Coin.getDefaultInstance())
                        .setToAddress("okp41wwr8ye24766rmjjh7eva0rc2p7cnwa8py6s6fc")
                        .setFromAddress("okp41rhd8744u4vqvcjuvyfm8fea4k9mefe3k57qz27")
                        .build()
                )
            )
            .build()
    ).build().toByteArray()
val tx3: ByteArray = TxOuterClass.Tx.newBuilder()
    .addSignatures("2utl1VHdSC3pyHCNgeNmgGImnEChQcd9sWEgi4Uc4lwOhWhrqYy8WkJ8xNkVzjF/WVg3ayVWZp8ipVzO1kUK9g==".b64ToByteString())
    .setAuthInfo(
        TxOuterClass.AuthInfo.getDefaultInstance()
    ).setBody(
        TxOuterClass.TxBody.getDefaultInstance()
    ).build().toByteArray()
val txError: ByteArray = "test".toByteArray()

class TopologyTest : BehaviorSpec({
    val stringSerde = Serdes.StringSerde()
    val byteArraySerde = Serdes.ByteArraySerde()

    table(
        headers("case", "tx", "output-topic", "description"),
        row(1, tx1, listOf("topic-1"), "Transaction matches one rule"),
        row(2, tx2, listOf("topic-1", "topic-2"), "Transaction matches two rules"),
        row(3, tx3, listOf("dlq"), "Transaction matches no rules"),
        row(4, txError, listOf("error"), "Faulty transaction goes to error topic")
    ).forAll { case, tx, outputTopic, description ->
        given("A topology for case <$case>: $description") {
            val config = mutableMapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "simple",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "dummy:1234",
                "topic.in" to "in",
                "topic.dlq" to "dlq",
                "topic.error" to "error",
                "rules.path" to (this::class.java.classLoader.getResource("rules_example.yaml")?.path ?: "")
            ).toProperties()

            val topology = TopologyProducer().apply {
                topicError = config.getProperty("topic.error")
                topicIn = config.getProperty("topic.in")
                topicDLQ = config.getProperty("topic.dlq")
                txsDispatch = TxsDispatch().apply {
                    configDocPath = config.getProperty("rules.path")
                }
            }.buildTopology()
            val testDriver = TopologyTestDriver(topology, config)
            val inputTopic = testDriver.createInputTopic("in", stringSerde.serializer(), byteArraySerde.serializer())
            val outputTopics = mapOf(
                "dlq" to testDriver.createOutputTopic("dlq", stringSerde.deserializer(), byteArraySerde.deserializer()),
                "error" to testDriver.createOutputTopic(
                    "error",
                    stringSerde.deserializer(),
                    byteArraySerde.deserializer()
                ),
                "topic-1" to testDriver.createOutputTopic(
                    "topic-1",
                    stringSerde.deserializer(),
                    byteArraySerde.deserializer()
                ),
                "topic-2" to testDriver.createOutputTopic(
                    "topic-2",
                    stringSerde.deserializer(),
                    byteArraySerde.deserializer()
                ),
                "topic-3" to testDriver.createOutputTopic(
                    "topic-3",
                    stringSerde.deserializer(),
                    byteArraySerde.deserializer()
                )
            )

            `when`("sending the transaction to the input topic ($inputTopic)") {
                inputTopic.pipeInput("", tx)

                outputTopic.forEach {
                    then("the transaction is sent to <$it> topic") {
                        val result = outputTopics[it]?.readValue()

                        result shouldNotBe null
                        result shouldBe tx
                    }
                }
            }
        }
    }
})
