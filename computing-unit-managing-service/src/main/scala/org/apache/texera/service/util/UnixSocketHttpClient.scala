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

import java.io.{ByteArrayOutputStream, IOException}
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector, SocketChannel}
import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_8}
import java.nio.file.Path
import java.time.Duration

final case class UnixSocketHttpResponse(status: Int, body: String)

/**
  * One HTTP/1.1 exchange per call over a unix domain socket.
  *
  * Hand-written because nothing on this service's classpath speaks HTTP over a JDK unix channel:
  * sttp's and the JDK's clients are TCP-only, and OkHttp needs a java.net.Socket, which a unix
  * SocketChannel cannot provide.
  *
  * Non-blocking because a blocking channel has no read timeout, and a stuck slurmrestd must not
  * pin a request thread that is holding a database transaction open.
  */
class UnixSocketHttpClient(socketPath: Path, timeout: Duration) {

  /** @throws IOException when the socket cannot be reached, or no complete response arrives. */
  def send(method: String, path: String, body: Option[String]): UnixSocketHttpResponse = {
    val deadline = System.nanoTime() + timeout.toNanos
    val channel = SocketChannel.open(StandardProtocolFamily.UNIX)
    try {
      channel.connect(UnixDomainSocketAddress.of(socketPath))
      channel.configureBlocking(false)
      val selector = Selector.open()
      try {
        write(channel, selector, encodeRequest(method, path, body), deadline)
        read(channel, selector, deadline)
      } finally selector.close()
    } finally channel.close()
  }

  private def encodeRequest(method: String, path: String, body: Option[String]): ByteBuffer = {
    val payload = body.map(_.getBytes(UTF_8)).getOrElse(Array.emptyByteArray)
    val contentHeaders = body.fold("") { _ =>
      s"Content-Type: application/json\r\nContent-Length: ${payload.length}\r\n"
    }
    val head = s"$method $path HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n" +
      s"Connection: close\r\n$contentHeaders\r\n"
    ByteBuffer.wrap(head.getBytes(ISO_8859_1) ++ payload)
  }

  private def awaitReady(selector: Selector, deadline: Long): Unit = {
    val remainingMillis = Duration.ofNanos(deadline - System.nanoTime()).toMillis
    if (remainingMillis <= 0) {
      throw new IOException(s"No complete response from $socketPath within $timeout")
    }
    selector.select(remainingMillis)
    selector.selectedKeys().clear()
  }

  private def write(
      channel: SocketChannel,
      selector: Selector,
      request: ByteBuffer,
      deadline: Long
  ): Unit = {
    val key = channel.register(selector, SelectionKey.OP_WRITE)
    while (request.hasRemaining) {
      if (channel.write(request) == 0) awaitReady(selector, deadline)
    }
    key.cancel()
    selector.selectNow()
  }

  private def read(
      channel: SocketChannel,
      selector: Selector,
      deadline: Long
  ): UnixSocketHttpResponse = {
    channel.register(selector, SelectionKey.OP_READ)
    val received = new ByteArrayOutputStream()
    val buffer = ByteBuffer.allocate(16 * 1024)
    var response = Option.empty[UnixSocketHttpResponse]
    while (response.isEmpty) {
      buffer.clear()
      val count = channel.read(buffer)
      if (count == 0) {
        awaitReady(selector, deadline)
      } else {
        if (count > 0) received.write(buffer.array(), 0, count)
        response = UnixSocketHttpClient.parseResponse(received.toByteArray, count < 0)
      }
    }
    response.get
  }
}

object UnixSocketHttpClient {
  private val StatusLine = "HTTP/1\\.[01] (\\d{3}).*".r
  private val HeaderEnd = "\r\n\r\n".getBytes(ISO_8859_1)

  /**
    * The response in `bytes`, or None if more is still to come.
    *
    * @throws IOException if it is malformed, or `atEnd` says what is missing will never arrive.
    */
  private def parseResponse(bytes: Array[Byte], atEnd: Boolean): Option[UnixSocketHttpResponse] = {
    def truncated = throw new IOException("Connection closed before the response was complete")

    val headerEnd = bytes.indexOfSlice(HeaderEnd)
    if (headerEnd < 0) {
      return if (atEnd) truncated else None
    }
    val lines = new String(bytes, 0, headerEnd, ISO_8859_1).split("\r\n").toList
    val status = lines.head match {
      case StatusLine(code) => code.toInt
      case _                => throw new IOException("Response is not HTTP")
    }
    val headers = lines.tail.flatMap { line =>
      line.split(":", 2) match {
        case Array(name, value) => Some(name.trim.toLowerCase -> value.trim)
        case _                  => None
      }
    }.toMap

    val rest = bytes.drop(headerEnd + HeaderEnd.length)
    val body: Option[Array[Byte]] =
      if (headers.get("transfer-encoding").exists(_.toLowerCase.contains("chunked"))) {
        decodeChunks(rest)
      } else {
        headers.get("content-length") match {
          case Some(length) =>
            val expected =
              length.toIntOption.getOrElse(throw new IOException("Malformed Content-Length"))
            Option.when(rest.length >= expected)(rest.take(expected))
          case None => Option.when(atEnd)(rest)
        }
      }
    if (body.isEmpty && atEnd) truncated
    body.map(content => UnixSocketHttpResponse(status, new String(content, UTF_8)))
  }

  /** The decoded body, or None if the terminating chunk has not arrived yet. */
  private def decodeChunks(bytes: Array[Byte]): Option[Array[Byte]] = {
    val crlf = "\r\n".getBytes(ISO_8859_1)
    val decoded = new ByteArrayOutputStream()
    var position = 0
    while (true) {
      val lineEnd = bytes.indexOfSlice(crlf, position)
      if (lineEnd < 0) return None
      val sizeField = new String(bytes, position, lineEnd - position, ISO_8859_1).split(";")(0).trim
      val size =
        try Integer.parseInt(sizeField, 16)
        catch {
          case _: NumberFormatException => throw new IOException("Malformed chunk size")
        }
      if (size == 0) return Some(decoded.toByteArray)
      val chunkStart = lineEnd + crlf.length
      if (bytes.length < chunkStart + size + crlf.length) return None
      decoded.write(bytes, chunkStart, size)
      position = chunkStart + size + crlf.length
    }
    None
  }
}
