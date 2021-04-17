package com.wutsi.storage.aws

import com.amazonaws.services.s3.AmazonS3
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator

open class S3HealthIndicator(
    private val s3: AmazonS3,
    private val bucket: String
) : HealthIndicator {

    override fun health(): Health {
        val start = System.currentTimeMillis()
        try {
            val location = s3.getBucketLocation(bucket)
            return Health.up()
                .withDetail("bucket", this.bucket)
                .withDetail("location", location)
                .withDetail("latency", System.currentTimeMillis() - start)
                .build()
        } catch (ex: Exception) {
            return Health.down()
                .withDetail("bucket", this.bucket)
                .withDetail("latency", System.currentTimeMillis() - start)
                .withException(ex)
                .build()
        }
    }
}
