package com.okp4.processor.cosmos

import com.google.protobuf.ByteString
import com.google.protobuf.ByteString.copyFrom
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

val tx1: ByteArray = TxOuterClass.TxRaw.newBuilder()
    .addSignatures("2utl1VHdSC3pyHCNgeNmgGImnEChQcd9sWEgi4Uc4lwOhWhrqYy8WkJ8xNkVzjF/WVg3ayVWZp8ipVzO1kUK9g==".b64ToByteString())
    .setAuthInfoBytes("Ck4KRgofL2Nvc21vcy5jcnlwdG8uc2VjcDI1NmsxLlB1YktleRIjCiECf1JPoIG8+pMDKtmH2vtOg5+xvfNxoDXV0iD++Ha5a/0SBAoCCAESBBDAmgw=".b64ToByteString())
    .setBodyBytes(
        "CoUBChwvY29zbW9zLmJhbmsudjFiZXRhMS5Nc2dTZW5kEmUKK29rcDQxcmhkODc0NHU0dnF2Y2p1dnlmbThmZWE0azltZWZlM2s1N3F6MjcSK29rcDQxOTY4NzdkajRjcnB4bWphMnd3MmhqMnZneTQ1djZ1c3Bremt0OGwaCQoEa25vdxIBMw==".b64ToByteString()
    ).build().toByteArray()
val tx2: ByteArray = TxOuterClass.Tx.newBuilder()
    .addSignatures("2utl1VHdSC3pyHCNgeNmgGImnEChQcd9sWEgi4Uc4lwOhWhrqYy8WkJ8xNkVzjF/WVg3ayVWZp8ipVzO1kUK9g==".b64ToByteString())
    .setAuthInfo(
        TxOuterClass.AuthInfo.newBuilder()
            .mergeFrom("Ck4KRgofL2Nvc21vcy5jcnlwdG8uc2VjcDI1NmsxLlB1YktleRIjCiECf1JPoIG8+pMDKtmH2vtOg5+xvfNxoDXV0iD++Ha5a/0SBAoCCAESBBDAmgw=".b64ToByteString())
            .build()
    ).setBody(
        TxOuterClass.TxBody.newBuilder().addMessages(
            com.google.protobuf.Any.newBuilder().setTypeUrl("/cosmos.bank.v1beta1.MsgTest")
                .setValue(ByteString.copyFromUtf8("test message")).build()
        ).build()
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
        row(1, tx1, "topic-1", "filter tx to topic-1"),
        row(2, tx2, "topic-2", "filter tx to topic-2"),
        row(3, tx3, "dlq", "filter tx to dlq"),
        row(4, txError, "error", "filter tx to error topic"),
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

            val topology = topology(config).also { println(it.describe()) }
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
                ),
            )

            `when`("sending the transaction to the input topic ($inputTopic)") {
                inputTopic.pipeInput("", tx)

                then("the transaction is sent to <$outputTopic> topic") {
                    val result = outputTopics[outputTopic]?.readValue()
                    result shouldNotBe null
                    result shouldBe tx
                }
            }
        }
    }
})
