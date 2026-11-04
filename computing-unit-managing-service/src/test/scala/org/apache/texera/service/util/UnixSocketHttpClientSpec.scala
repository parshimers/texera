/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.service.util

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.concurrent.{CompletableFuture, CountDownLatch}

class UnixSocketHttpClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var directory: Path = _
  private def socket: Path = directory.resolve("test.socket")
  private def client(timeout: Duration = Duration.ofSeconds(5)) =
    new UnixSocketHttpClient(socket, timeout)

  override def beforeEach(): Unit = directory = Files.createTempDirectory("slurm-socket")

  override def afterEach(): Unit = {
    Files.deleteIfExists(socket)
    Files.deleteIfExists(directory)
  }

  private def readRequest(channel: SocketChannel): String = {
    val received = new StringBuilder
    val buffer = ByteBuffer.allocate(4096)
    def complete: Boolean = {
      val text = received.toString
      val headerEnd = text.indexOf("\r\n\r\n")
      headerEnd >= 0 && {
        val length = "(?i)content-length: (\\d+)".r
          .findFirstMatchIn(text)
          .map(_.group(1).toInt)
          .getOrElse(0)
        text.getBytes(UTF_8).length >= headerEnd + 4 + length
      }
    }
    while (!complete && channel.read(buffer) >= 0) {
      buffer.flip()
      received.append(UTF_8.decode(buffer))
      buffer.clear()
    }
    received.toString
  }

  /** Answers one connection with raw bytes, leaving the connection open if asked to. */
  private def serveOnce(
      response: Seq[String],
      closeAfter: Boolean = true
  ): (ServerSocketChannel, CompletableFuture[String]) = {
    val server = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    server.bind(UnixDomainSocketAddress.of(socket))
    val request = new CompletableFuture[String]()
    val thread = new Thread(() => {
      val channel = server.accept()
      try {
        request.complete(readRequest(channel))
        response.foreach { part =>
          channel.write(ByteBuffer.wrap(part.getBytes(UTF_8)))
          Thread.sleep(5)
        }
        if (!closeAfter) new CountDownLatch(1).await()
      } catch {
        case _: InterruptedException =>
      } finally channel.close()
    })
    thread.setDaemon(true)
    thread.start()
    (server, request)
  }

  private def withServer[T](response: Seq[String], closeAfter: Boolean = true)(
      body: CompletableFuture[String] => T
  ): T = {
    val (server, request) = serveOnce(response, closeAfter)
    try body(request)
    finally server.close()
  }

  "send" should "write an HTTP/1.1 request and read the status and body" in {
    val payload = """{"job_id":7}"""
    withServer(
      Seq(s"HTTP/1.1 200 OK\r\nContent-Length: ${payload.length}\r\n\r\n$payload")
    ) { request =>
      val response = client().send("POST", "/slurm/v0.0.42/job/submit", Some("""{"job":{}}"""))
      response shouldBe UnixSocketHttpResponse(200, payload)

      val sent = request.get()
      sent should startWith("POST /slurm/v0.0.42/job/submit HTTP/1.1\r\n")
      sent should include("Content-Type: application/json\r\n")
      sent should include("Content-Length: 10\r\n")
      sent should endWith("\r\n\r\n{\"job\":{}}")
    }
  }

  it should "count Content-Length in bytes, not characters" in {
    val payload = """{"name":"cœur-计算"}"""
    withServer(
      Seq(s"HTTP/1.1 200 OK\r\nContent-Length: ${payload.getBytes(UTF_8).length}\r\n\r\n$payload")
    ) { request =>
      client().send("POST", "/p", Some(payload)).body shouldBe payload
      request.get() should include(s"Content-Length: ${payload.getBytes(UTF_8).length}\r\n")
    }
  }

  // slurmrestd only closes the connection itself after an error, so a successful answer has to
  // end at its Content-Length rather than at end of stream.
  it should "finish at Content-Length even when the server keeps the connection open" in {
    withServer(Seq("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"), closeAfter = false) { _ =>
      client().send("GET", "/p", None) shouldBe UnixSocketHttpResponse(200, "{}")
    }
  }

  it should "reassemble a response delivered in pieces" in {
    withServer(Seq("HTTP/1.1 404 NOT", " FOUND\r\nContent-Le", "ngth: 5\r\n\r\nab", "cde")) { _ =>
      client().send("GET", "/p", None) shouldBe UnixSocketHttpResponse(404, "abcde")
    }
  }

  it should "decode a chunked body" in {
    withServer(
      Seq(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n",
        "2\r\nde\r\n0\r\n\r\n"
      ),
      closeAfter = false
    ) { _ =>
      client().send("GET", "/p", None) shouldBe UnixSocketHttpResponse(200, "abcde")
    }
  }

  it should "read to end of stream when the response states no length" in {
    withServer(Seq("HTTP/1.1 200 OK\r\n\r\nall of it")) { _ =>
      client().send("GET", "/p", None).body shouldBe "all of it"
    }
  }

  it should "fail on a connection closed before the body arrived" in {
    withServer(Seq("HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\nshort")) { _ =>
      an[IOException] should be thrownBy client().send("GET", "/p", None)
    }
  }

  it should "fail on a connection closed before any response" in {
    withServer(Seq.empty) { _ =>
      an[IOException] should be thrownBy client().send("GET", "/p", None)
    }
  }

  it should "fail on a response that is not HTTP" in {
    withServer(Seq("not http at all\r\n\r\n")) { _ =>
      an[IOException] should be thrownBy client().send("GET", "/p", None)
    }
  }

  it should "fail on a malformed chunk size" in {
    withServer(Seq("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nabc\r\n")) { _ =>
      an[IOException] should be thrownBy client().send("GET", "/p", None)
    }
  }

  it should "fail once the timeout passes without a complete response" in {
    withServer(Seq("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{"), closeAfter = false) { _ =>
      val started = System.nanoTime()
      an[IOException] should be thrownBy client(Duration.ofMillis(200)).send("GET", "/p", None)
      Duration.ofNanos(System.nanoTime() - started).toMillis should be < 5000L
    }
  }

  it should "fail when nothing is listening on the socket" in {
    an[IOException] should be thrownBy client().send("GET", "/p", None)
  }
}
