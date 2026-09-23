package com.twitchat.irc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.SocketException
import java.util.concurrent.ConcurrentHashMap
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory

private const val TWITCH_IRC_HOST = "irc.chat.twitch.tv"
private const val TWITCH_IRC_TLS_PORT = 6697

/**
 * Minimal tmi.js-like Twitch IRC client for Kotlin/JVM.
 */
class TwitchIrcClient(
    private val username: String,
    private var oauthToken: String,
    private val initialChannels: Set<String>,
    private val host: String = TWITCH_IRC_HOST,
    private val port: Int = TWITCH_IRC_TLS_PORT,
    private val maxReconnectDelayMillis: Long = 30_000,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val joinedChannels = ConcurrentHashMap.newKeySet<String>().apply {
        initialChannels.forEach { add(normalizeChannel(it)) }
    }

    private val _rawMessages = MutableSharedFlow<IrcMessage>(extraBufferCapacity = 256)
    val rawMessages: SharedFlow<IrcMessage> = _rawMessages.asSharedFlow()

    private val _events = MutableSharedFlow<TwitchEvent>(extraBufferCapacity = 256)
    val events: SharedFlow<TwitchEvent> = _events.asSharedFlow()

    private var reconnectAttempts = 0
    private var running = false

    @Volatile
    private var socket: SSLSocket? = null

    @Volatile
    private var writer: BufferedWriter? = null

    private var readJob: Job? = null

    fun connect() {
        if (running) return
        running = true
        reconnectAttempts = 0
        scope.launch { connectLoop() }
    }

    suspend fun disconnect() {
        running = false
        closeConnection()
        readJob?.cancel()
    }

    suspend fun refreshToken(newOauthToken: String) {
        oauthToken = newOauthToken
        reconnectAttempts = 0
        closeConnection()
    }

    suspend fun join(channel: String) {
        val normalized = normalizeChannel(channel)
        joinedChannels.add(normalized)
        sendRaw("JOIN #$normalized")
    }

    suspend fun part(channel: String) {
        val normalized = normalizeChannel(channel)
        joinedChannels.remove(normalized)
        sendRaw("PART #$normalized")
    }

    suspend fun sendMessage(channel: String, message: String) {
        sendPrivmsg(normalizeChannel(channel), message)
    }

    suspend fun sendReply(channel: String, message: String, replyParentMsgId: String) {
        val normalized = normalizeChannel(channel)
        sendRaw("@reply-parent-msg-id=$replyParentMsgId PRIVMSG #$normalized :$message")
    }

    suspend fun timeout(channel: String, targetUser: String, durationSeconds: Int, reason: String? = null) {
        val payload = if (reason.isNullOrBlank()) {
            "/timeout $targetUser $durationSeconds"
        } else {
            "/timeout $targetUser $durationSeconds $reason"
        }
        sendPrivmsg(normalizeChannel(channel), payload)
    }

    suspend fun ban(channel: String, targetUser: String, reason: String? = null) {
        val payload = if (reason.isNullOrBlank()) {
            "/ban $targetUser"
        } else {
            "/ban $targetUser $reason"
        }
        sendPrivmsg(normalizeChannel(channel), payload)
    }

    suspend fun deleteMessage(channel: String, messageId: String) {
        sendPrivmsg(normalizeChannel(channel), "/delete $messageId")
    }

    private suspend fun sendPrivmsg(channel: String, text: String) {
        sendRaw("PRIVMSG #$channel :$text")
    }

    private suspend fun connectLoop() {
        while (running && scope.isActive) {
            try {
                openConnection()
                reconnectAttempts = 0
                readLoop()
            } catch (cancel: CancellationException) {
                throw cancel
            } catch (_: SocketException) {
                // disconnected
            } catch (_: Exception) {
                // disconnected
            } finally {
                closeConnection()
            }

            if (running) {
                reconnectAttempts += 1
                delay(reconnectDelayMillis(reconnectAttempts))
            }
        }
    }

    private fun reconnectDelayMillis(attempt: Int): Long {
        val base = 1_000L
        val exp = (1L shl attempt.coerceAtMost(8))
        return (base * exp).coerceAtMost(maxReconnectDelayMillis)
    }

    private suspend fun openConnection() {
        val sslSocket = SSLSocketFactory.getDefault().createSocket(host, port) as SSLSocket
        sslSocket.startHandshake()

        val bufferedWriter = BufferedWriter(OutputStreamWriter(sslSocket.getOutputStream(), Charsets.UTF_8))
        val bufferedReader = BufferedReader(InputStreamReader(sslSocket.getInputStream(), Charsets.UTF_8))

        socket = sslSocket
        writer = bufferedWriter

        sendRaw("PASS oauth:$oauthToken")
        sendRaw("NICK $username")
        sendRaw("CAP REQ :twitch.tv/tags twitch.tv/commands twitch.tv/membership")

        joinedChannels.forEach { sendRaw("JOIN #$it") }

        readJob = scope.launch {
            while (running && isActive) {
                val line = bufferedReader.readLine() ?: break
                val parsed = IrcParser.parse(line)
                _rawMessages.emit(parsed)
                handleServerLine(parsed)
            }
        }

        readJob?.join()
    }

    private suspend fun readLoop() {
        readJob?.join()
    }

    private suspend fun handleServerLine(message: IrcMessage) {
        if (message.command == "PING") {
            val payload = message.trailing ?: message.params.firstOrNull().orEmpty()
            sendRaw("PONG :$payload")
            _events.emit(TwitchEvent.Ping(payload))
            return
        }

        val event = TwitchEvent.from(message)
        _events.emit(event)

        if (message.command == "RECONNECT") {
            closeConnection()
        }
    }

    private suspend fun sendRaw(line: String) {
        val activeWriter = writer ?: return
        activeWriter.write(line)
        activeWriter.write("\r\n")
        activeWriter.flush()
    }

    private fun closeConnection() {
        try {
            writer?.close()
        } catch (_: Exception) {
        }
        writer = null

        try {
            socket?.close()
        } catch (_: Exception) {
        }
        socket = null
    }

    private fun normalizeChannel(channel: String): String = channel.removePrefix("#").lowercase()
}

data class IrcMessage(
    val raw: String,
    val tags: Map<String, String>,
    val prefix: String?,
    val command: String,
    val params: List<String>,
    val trailing: String?,
)

object IrcParser {
    fun parse(raw: String): IrcMessage {
        var cursor = raw

        val tags = if (cursor.startsWith("@")) {
            val split = cursor.substringBefore(' ')
            cursor = cursor.substringAfter(' ', "")
            parseTags(split.removePrefix("@"))
        } else {
            emptyMap()
        }

        val prefix = if (cursor.startsWith(":")) {
            val p = cursor.substringBefore(' ').removePrefix(":")
            cursor = cursor.substringAfter(' ', "")
            p
        } else {
            null
        }

        val command = cursor.substringBefore(' ').ifEmpty { cursor }
        cursor = cursor.substringAfter(' ', "")

        val params = mutableListOf<String>()
        var trailing: String? = null

        while (cursor.isNotEmpty()) {
            if (cursor.startsWith(":")) {
                trailing = cursor.removePrefix(":")
                break
            }
            val token = cursor.substringBefore(' ')
            params.add(token)
            cursor = cursor.substringAfter(' ', "")
        }

        return IrcMessage(raw = raw, tags = tags, prefix = prefix, command = command, params = params, trailing = trailing)
    }

    private fun parseTags(rawTags: String): Map<String, String> {
        if (rawTags.isBlank()) return emptyMap()
        return rawTags
            .split(';')
            .associate { entry ->
                val key = entry.substringBefore('=')
                val value = entry.substringAfter('=', "")
                key to unescapeTagValue(value)
            }
    }

    private fun unescapeTagValue(value: String): String {
        return value
            .replace("\\:", ";")
            .replace("\\s", " ")
            .replace("\\\\", "\\")
            .replace("\\r", "\r")
            .replace("\\n", "\n")
    }
}

sealed class TwitchEvent {
    abstract val message: IrcMessage

    data class Message(
        override val message: IrcMessage,
        val channel: String,
        val text: String,
        val login: String?,
        val userId: String?,
        val badgesRaw: String?,
        val emotesRaw: String?,
        val replyParentMsgId: String?,
        val gifsRaw: String?,
        val gifUrl: String?,
    ) : TwitchEvent()

    data class UserNotice(
        override val message: IrcMessage,
        val channel: String,
        val msgId: String?,
        val msgCategory: String?,
        val text: String,
    ) : TwitchEvent()

    data class ClearChat(
        override val message: IrcMessage,
        val channel: String,
        val targetLogin: String?,
        val targetUserId: String?,
        val durationSeconds: Int?,
    ) : TwitchEvent()

    data class ClearMsg(
        override val message: IrcMessage,
        val channel: String,
        val targetMsgId: String?,
        val login: String?,
        val text: String,
    ) : TwitchEvent()

    data class Join(
        override val message: IrcMessage,
        val channel: String,
        val login: String,
    ) : TwitchEvent()

    data class Part(
        override val message: IrcMessage,
        val channel: String,
        val login: String,
    ) : TwitchEvent()

    data class Ping(
        val payload: String,
    ) : TwitchEvent() {
        override val message: IrcMessage = IrcMessage("PING :$payload", emptyMap(), null, "PING", emptyList(), payload)
    }

    data class Raw(override val message: IrcMessage) : TwitchEvent()

    companion object {
        fun from(msg: IrcMessage): TwitchEvent {
            return when (msg.command) {
                "PRIVMSG" -> {
                    val channel = msg.params.firstOrNull().orEmpty().removePrefix("#")
                    val gifsRaw = msg.tags["gifs"]
                    val gifUrl = gifsRaw
                        ?.split("|")
                        ?.firstOrNull { it.startsWith("url=") }
                        ?.removePrefix("url=")
                    Message(
                        message = msg,
                        channel = channel,
                        text = msg.trailing.orEmpty(),
                        login = msg.tags["login"],
                        userId = msg.tags["user-id"],
                        badgesRaw = msg.tags["badges"],
                        emotesRaw = msg.tags["emotes"],
                        replyParentMsgId = msg.tags["reply-parent-msg-id"],
                        gifsRaw = gifsRaw,
                        gifUrl = gifUrl,
                    )
                }

                "USERNOTICE" -> UserNotice(
                    message = msg,
                    channel = msg.params.firstOrNull().orEmpty().removePrefix("#"),
                    msgId = msg.tags["msg-id"],
                    msgCategory = msg.tags["msg-param-category"],
                    text = msg.trailing.orEmpty(),
                )

                "CLEARCHAT" -> ClearChat(
                    message = msg,
                    channel = msg.params.firstOrNull().orEmpty().removePrefix("#"),
                    targetLogin = msg.trailing,
                    targetUserId = msg.tags["target-user-id"],
                    durationSeconds = msg.tags["ban-duration"]?.toIntOrNull(),
                )

                "CLEARMSG" -> ClearMsg(
                    message = msg,
                    channel = msg.params.firstOrNull().orEmpty().removePrefix("#"),
                    targetMsgId = msg.tags["target-msg-id"],
                    login = msg.tags["login"],
                    text = msg.trailing.orEmpty(),
                )

                "JOIN" -> Join(
                    message = msg,
                    channel = (msg.trailing ?: msg.params.firstOrNull().orEmpty()).removePrefix("#"),
                    login = msg.prefix?.substringBefore('!').orEmpty(),
                )

                "PART" -> Part(
                    message = msg,
                    channel = (msg.trailing ?: msg.params.firstOrNull().orEmpty()).removePrefix("#"),
                    login = msg.prefix?.substringBefore('!').orEmpty(),
                )

                else -> Raw(msg)
            }
        }
    }
}

/**
 * Example run:
 * TWITCH_LOGIN=bot_login TWITCH_OAUTH=oauth_without_prefix TWITCH_CHANNELS=durss,kappa ./gradlew run
 */
suspend fun runExample() {
    val login = requireEnv("TWITCH_LOGIN")
    val oauth = requireEnv("TWITCH_OAUTH")
    val channels = System.getenv("TWITCH_CHANNELS")
        ?.split(',')
        ?.map { it.trim() }
        ?.filter { it.isNotBlank() }
        ?.toSet()
        .orEmpty()

    val client = TwitchIrcClient(
        username = login,
        oauthToken = oauth.removePrefix("oauth:"),
        initialChannels = channels,
    )

    client.connect()

    // Wait for first PRIVMSG and print core tags.
    val firstMessage = client.events
        .filter { it is TwitchEvent.Message }
        .first() as TwitchEvent.Message

    println("Message from=${firstMessage.login} channel=${firstMessage.channel} text=${firstMessage.text}")
    println("gifs=${firstMessage.gifsRaw} gifUrl=${firstMessage.gifUrl}")

    client.disconnect()
}

private fun requireEnv(name: String): String =
    System.getenv(name) ?: error("Missing required env var: $name")

fun main() {
    kotlinx.coroutines.runBlocking {
        runExample()
    }
}
