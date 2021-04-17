package com.wutsi.storage.aws

import com.amazonaws.services.s3.AmazonS3
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.doThrow
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.springframework.boot.actuate.health.Status
import kotlin.test.assertEquals

internal class S3HealthIndicatorTest {
    private lateinit var s3: AmazonS3
    private lateinit var health: S3HealthIndicator

    @BeforeEach
    fun setUp() {
        s3 = mock()
        health = S3HealthIndicator(s3, "foo")
    }

    @Test
    fun up() {
        doReturn("foo").whenever(s3).getBucketLocation(ArgumentMatchers.anyString())

        assertEquals(Status.UP, health.health().status)
    }

    @Test
    fun down() {
        doThrow(RuntimeException::class).whenever(s3).getBucketLocation(ArgumentMatchers.anyString())

        assertEquals(Status.DOWN, health.health().status)
    }
}
