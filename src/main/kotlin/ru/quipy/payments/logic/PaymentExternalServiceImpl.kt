package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newCoroutineContext
import okhttp3.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.*
import java.util.concurrent.*
import kotlin.math.pow
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toDurationUnit
import kotlin.time.toJavaDuration


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val window = Semaphore(parallelRequests)
    private val rateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1000, TimeUnit.MILLISECONDS)

    private val client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(Duration.ofSeconds(10)).build()


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (!window.tryAcquire(requestAverageProcessingTime.toMillis(), TimeUnit.MILLISECONDS)) {
            logger.warn("[$accountName] Deadline exceeded, cancelling request, deadline: $deadline")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
            }
            window.release()
            return
        }

        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .timeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        while (!rateLimiter.tick()) {
            Thread.sleep(3)
        }

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenAcceptAsync { response ->
                try {
                    val responseBody = mapper.readValue(response.body(), ExternalSysResponse::class.java)
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${responseBody.result}, message: ${responseBody.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(responseBody.result, now(), transactionId, reason = responseBody.message)
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                } finally {
                    window.release()
                }
            }
            .exceptionally { e ->
                try {
                    when (e.cause) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId)
                            {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                } finally {
                    window.release()
                }
                null
            }

    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()