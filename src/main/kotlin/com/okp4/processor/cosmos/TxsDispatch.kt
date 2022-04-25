package com.okp4.processor.cosmos

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.nio.file.Files
import java.nio.file.Path
import java.text.ParseException

data class DispatchRule(
    @JsonProperty("topic")
    val outputTopic: String,
    val predicate: String,
    val name: String,
)

data class TxDispatchRules(
    val rules: List<DispatchRule>
)

class TxsDispatch(private val configDocPath: String) {
    private var mapper: ObjectMapper = ObjectMapper(YAMLFactory())

    init {
        mapper.registerKotlinModule()
    }
    
    fun getTxDispatchList(): TxDispatchRules {
        return try {
            Files.newBufferedReader(Path.of(configDocPath)).use {
                mapper.readValue(it, TxDispatchRules::class.java)
            }
        } catch (exception: MissingKotlinParameterException) {
            throw ParseException("Could not parse YAML file: ${exception.message}", 0)
        }
    }
}