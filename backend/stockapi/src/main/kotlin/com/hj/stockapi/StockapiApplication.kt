package com.hj.stockapi

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class StockapiApplication

fun main(args: Array<String>) {
	runApplication<StockapiApplication>(*args)
}
