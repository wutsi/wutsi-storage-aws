package com.wutsi.storage.aws

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.doThrow
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.whenever
import com.wutsi.storage.StorageVisitor
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.URL
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

internal class S3StorageServiceTest {
    private lateinit var s3: AmazonS3
    private lateinit var storage: S3StorageService

    @BeforeEach
    fun setUp() {
        s3 = mock()
        storage = S3StorageService(s3, "test")
    }

    @Test
    fun contains() {
        assertTrue(storage.contains(URL("https://s3.amazonaws.com/test/document/1/2/text.txt")))
        assertFalse(storage.contains(URL("https://www.google.com/1/2/text.txt")))
    }

    @Test
    fun store() {
        val content = ByteArrayInputStream("hello".toByteArray())
        val result = storage.store("document/test.txt", content, "text/plain", 11111, "utf-16")

        assertNotNull(result)
        assertEquals(URL("https://s3.amazonaws.com/test/document/test.txt"), result)

        val request: ArgumentCaptor<PutObjectRequest> = ArgumentCaptor.forClass(PutObjectRequest::class.java)
        verify(s3).putObject(request.capture())
        assertEquals(request.value.bucketName, "test")
        assertEquals(request.value.metadata.cacheControl, "max-age=11111, must-revalidate")
        assertEquals(request.value.metadata.contentType, "text/plain")
        assertEquals(request.value.metadata.contentEncoding, "utf-16")
    }

    @Test
    fun storeNoContentType() {
        val content = ByteArrayInputStream("hello".toByteArray())
        val result = storage.store("document/test.txt", content, null, 31536000)

        assertNotNull(result)
        assertEquals(URL("https://s3.amazonaws.com/test/document/test.txt"), result)

        val request: ArgumentCaptor<PutObjectRequest> = ArgumentCaptor.forClass(PutObjectRequest::class.java)
        verify(s3).putObject(request.capture())
        assertNull(request.value.metadata.contentType)
    }

    @Test
    fun storeNoTTL() {
        val content = ByteArrayInputStream("hello".toByteArray())
        val result = storage.store("document/test.txt", content, "text/plain", null)

        assertNotNull(result)
        assertEquals(URL("https://s3.amazonaws.com/test/document/test.txt"), result)

        val request: ArgumentCaptor<PutObjectRequest> = ArgumentCaptor.forClass(PutObjectRequest::class.java)
        verify(s3).putObject(request.capture())
        assertNull(request.value.metadata.cacheControl)
    }

    @Test
    fun storeWithError() {
        doThrow(RuntimeException::class).whenever(s3).putObject(ArgumentMatchers.any())

        val content = ByteArrayInputStream("hello".toByteArray())
        assertThrows<IOException> {
            storage.store("document/test.txt", content, "text/plain")
        }
    }

    @Test
    fun get() {
        val url = "https://s3.amazonaws.com/test/100/document/203920392/toto.txt"
        val os = ByteArrayOutputStream()

        val obj: S3Object = mock()
        val content: S3ObjectInputStream = mock()
        doReturn(-1).whenever(content).read(ArgumentMatchers.any())
        doReturn(content).whenever(obj).objectContent
        doReturn(obj).whenever(s3).getObject(ArgumentMatchers.any())

        storage.get(URL(url), os)

        val request: ArgumentCaptor<GetObjectRequest> = ArgumentCaptor.forClass(GetObjectRequest::class.java)
        verify(s3).getObject(request.capture())
        assertEquals(request.value.bucketName, "test")
        assertEquals(request.value.key, "100/document/203920392/toto.txt")
    }

    @Test
    fun getWithError() {
        Mockito.`when`(s3.getObject(ArgumentMatchers.any())).thenThrow(RuntimeException::class.java)

        val url = "https://s3.amazonaws.com/test/100/document/203920392/toto.txt"
        val os = ByteArrayOutputStream()

        assertThrows<IOException> { storage.get(URL(url), os) }
    }

    @Test
    fun visit() {
        val listings = Mockito.mock(ObjectListing::class.java)
        doReturn(
            listOf(
                createObjectSummary("a/file-a1.txt"),
                createObjectSummary("a/file-a2.txt"),
                createObjectSummary("a/b/file-ab1.txt"),
                createObjectSummary("a/b/c/file-abc1.txt")
            )
        ).whenever(listings).objectSummaries
        doReturn(listings).whenever(s3).listObjects(ArgumentMatchers.any(ListObjectsRequest::class.java))

        val urls = mutableListOf<URL>()
        val visitor = createStorageVisitor(urls)
        val baseUrl = "https://s3.amazonaws.com/test"

        storage.visit("a", visitor)
        assertEquals(4, urls.size)
        assertTrue(urls.contains(URL("$baseUrl/a/file-a1.txt")))
        assertTrue(urls.contains(URL("$baseUrl/a/file-a2.txt")))
        assertTrue(urls.contains(URL("$baseUrl/a/b/file-ab1.txt")))
        assertTrue(urls.contains(URL("$baseUrl/a/b/c/file-abc1.txt")))
    }

    private fun createStorageVisitor(urls: MutableList<URL>) = object : StorageVisitor {
        override fun visit(url: URL) {
            urls.add(url)
        }
    }

    private fun createObjectSummary(key: String): S3ObjectSummary {
        val obj = S3ObjectSummary()
        obj.key = key
        return obj
    }
}
