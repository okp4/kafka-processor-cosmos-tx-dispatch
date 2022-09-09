package com.okp4.processor.cosmos

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.MissingKotlinParameterException
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.nio.file.Files
import java.nio.file.Path
import java.text.ParseException
import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
data class DispatchRule(
    @field:JsonProperty("topic")
    val outputTopic: String? = null,

    @field:JsonProperty("predicate")
    val predicate: String? = null,

    @field:JsonProperty("name")
    val name: String? = null,
)

@ApplicationScoped
data class TxDispatchRules(
    @field:JsonProperty("rules")
    val rules: List<DispatchRule?>? = null,
)

@ApplicationScoped
class TxsDispatch() {
    private var mapper: ObjectMapper = ObjectMapper(YAMLFactory())

    @field:ConfigProperty(name = "rules.path", defaultValue = "rules.path")
    lateinit var configDocPath: String

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
