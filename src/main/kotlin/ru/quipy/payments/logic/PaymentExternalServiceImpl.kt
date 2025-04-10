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
import java.util.*
import java.util.concurrent.*
import kotlin.math.pow
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toDurationUnit
import kotlin.time.toJavaDuration


class RateLimitInterceptor(private val rateLimiter: FixedWindowRateLimiter) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        rateLimiter.tickBlocking()
        return chain.proceed(chain.request())
    }
}

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val window = OngoingWindow(parallelRequests)
    private val rateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1000, TimeUnit.MILLISECONDS)

    @OptIn(ExperimentalCoroutinesApi::class)
    private val dispatcher = Dispatchers.IO.limitedParallelism(1100 * 10)
    private val scope = CoroutineScope(dispatcher + SupervisorJob() + CoroutineExceptionHandler {_, ex ->
        logger.warn("Exception: " + ex.message)
    })

    private val client = OkHttpClient.Builder()
        .callTimeout(requestAverageProcessingTime.toMillis() * 3, TimeUnit.MILLISECONDS)
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .dispatcher(
            Dispatcher().apply {
                maxRequests = 1100 * 10
                maxRequestsPerHost = 1100 * 10
            })
        .connectionPool(
            ConnectionPool(
                maxIdleConnections = 1100 * 10,
                keepAliveDuration = 10,
                timeUnit = TimeUnit.MINUTES,
            )
        )
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .build()


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        var job = scope.launch {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

            if (deadline - now() < 0) {
                logger.error("[$accountName] Payment deadline reached for txId: $transactionId, payment: $paymentId")

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "deadline reached")
                }

                return@launch
            }

            window.acquire()

            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                val request = Request.Builder()
                    .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    .post(emptyBody)
                    .build()

                var responseBody: ExternalSysResponse? = null
                var attempt = 0
                var success = false

                while (attempt < 3 && !success) {
                    if (!rateLimiter.tick()) {
                        delay(1)
                        continue
                    }

                    if (deadline - now() < 0) {
                        logger.error("[$accountName] Payment deadline reached for txId: $transactionId, payment: $paymentId")

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "deadline reached")
                        }

                        return@launch
                    }

                    client.newCall(request).execute().use { response ->
                        val responseStr = response.body?.string()

                        responseBody = try {
                            mapper.readValue(responseStr, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        success = responseBody!!.result

                        if (!success) {
                            attempt++
                            if (attempt < 3) {
                                val delay = (100L * 2.0.pow(attempt)).toLong()
                                logger.warn("[$accountName] Повторная попытка $attempt для $paymentId (код: ${response.code}), задержка: $delay мс")
                                Thread.sleep(delay)
                            }
                        }
                    }
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${responseBody?.result}, message: ${responseBody?.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(responseBody?.result ?: false, now(), transactionId, reason = responseBody?.message)
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                window.release()
            }
        }
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()