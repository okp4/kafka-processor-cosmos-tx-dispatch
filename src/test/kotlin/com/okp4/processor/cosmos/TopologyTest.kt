package com.okp4.processor.cosmos

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

class TopologyTest : BehaviorSpec({
    val stringSerde = Serdes.StringSerde()
    val config = mapOf(
        StreamsConfig.APPLICATION_ID_CONFIG to "simple",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "dummy:1234",
        "topic.in" to "in",
        "topic.out" to "out",
        "topic.error" to "error"
    ).toProperties()

    given("A topology") {
        val topology = topology(config)
        val testDriver = TopologyTestDriver(topology, config)
        val inputTopic = testDriver.createInputTopic("in", stringSerde.serializer(), stringSerde.serializer())
        val outputTopic = testDriver.createOutputTopic("out", stringSerde.deserializer(), stringSerde.deserializer())
        val errorTopic = testDriver.createOutputTopic("error", stringSerde.deserializer(), stringSerde.deserializer())

        table(
            headers("case", "message", "isError", "expected"),
            row("simple message", "John Doe", false, "Hello John Doe!"),
            row("empty message", "  ", true, "  "),
        ).forAll { case, message, isError, expected ->
            When("sending the message <$message> to the input topic ($inputTopic) ($case)") {
                inputTopic.pipeInput("", message)

                val topic = if (isError) errorTopic else outputTopic

                then("message is received from the output topic ($topic)") {
                    val result = topic.readKeyValue()

                    result shouldNotBe null
                    result.value shouldBe expected
                }
            }
        }
    }
})
