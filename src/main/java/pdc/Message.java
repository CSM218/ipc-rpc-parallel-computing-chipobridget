package pdc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * Implements a custom length-prefixed binary wire format using DataOutputStream.
 *
 * Wire Format:
 * [Total Length (4 bytes)] [Magic len (2) + Magic] [Version (4)]
 * [Type len (2) + messageType] [Id len (2) + studentId]
 * [Timestamp (8)] [Payload len (4) + payload]
 */
public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public String payload;

    public static final String PROTOCOL_MAGIC = "CSM218";
    public static final int PROTOCOL_VERSION = 1;

    public Message() {
        this.magic = PROTOCOL_MAGIC;
        this.version = PROTOCOL_VERSION;
        this.timestamp = System.currentTimeMillis();
        this.payload = "";
    }

    public Message(String messageType, String studentId) {
        this();
        this.messageType = messageType;
        this.studentId = studentId;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses custom length-prefixed binary encoding with DataOutputStream.
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            byte[] magicBytes = (magic != null ? magic : PROTOCOL_MAGIC).getBytes(StandardCharsets.UTF_8);
            dos.writeShort(magicBytes.length);
            dos.write(magicBytes);

            dos.writeInt(version);

            byte[] typeBytes = (messageType != null ? messageType : "").getBytes(StandardCharsets.UTF_8);
            dos.writeShort(typeBytes.length);
            dos.write(typeBytes);

            byte[] idBytes = (studentId != null ? studentId : "").getBytes(StandardCharsets.UTF_8);
            dos.writeShort(idBytes.length);
            dos.write(idBytes);

            dos.writeLong(timestamp);

            byte[] payloadBytes = (payload != null ? payload : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(payloadBytes.length);
            dos.write(payloadBytes);

            dos.flush();
            byte[] body = baos.toByteArray();

            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            DataOutputStream frameDos = new DataOutputStream(frame);
            frameDos.writeInt(body.length);
            frameDos.write(body);
            frameDos.flush();

            return frame.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream (including 4-byte length prefix).
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 4) {
            return null;
        }
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Message msg = new Message();

            int totalLength = dis.readInt();

            int magicLen = dis.readShort();
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

            msg.version = dis.readInt();

            int typeLen = dis.readShort();
            byte[] typeBytes = new byte[typeLen];
            dis.readFully(typeBytes);
            msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);

            int idLen = dis.readShort();
            byte[] idBytes = new byte[idLen];
            dis.readFully(idBytes);
            msg.studentId = new String(idBytes, StandardCharsets.UTF_8);

            msg.timestamp = dis.readLong();

            int payloadLen = dis.readInt();
            if (payloadLen > 0) {
                byte[] payloadBytes = new byte[payloadLen];
                dis.readFully(payloadBytes);
                msg.payload = new String(payloadBytes, StandardCharsets.UTF_8);
            } else {
                msg.payload = "";
            }

            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    /**
     * Reads a single framed message from an input stream.
     */
    public static Message readFromStream(DataInputStream dis) throws IOException {
        int totalLength = dis.readInt();
        if (totalLength <= 0 || totalLength > 10_000_000) {
            throw new IOException("Invalid message length: " + totalLength);
        }
        byte[] data = new byte[totalLength + 4];
        ByteBuffer.wrap(data).putInt(totalLength);
        dis.readFully(data, 4, totalLength);
        return unpack(data);
    }

    /**
     * Writes this message to an output stream.
     */
    public void writeToStream(DataOutputStream dos) throws IOException {
        byte[] packed = pack();
        dos.write(packed);
        dos.flush();
    }

    /**
     * Serializes message to JSON string for logging and interop.
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"magic\":\"").append(escapeJson(magic)).append("\",");
        sb.append("\"version\":").append(version).append(",");
        sb.append("\"messageType\":\"").append(escapeJson(messageType)).append("\",");
        sb.append("\"studentId\":\"").append(escapeJson(studentId)).append("\",");
        sb.append("\"timestamp\":").append(timestamp).append(",");
        sb.append("\"payload\":\"").append(escapeJson(payload)).append("\"");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Parses a JSON string into a Message object.
     */
    public static Message parse(String json) {
        if (json == null || json.isEmpty()) return null;
        Message msg = new Message();
        msg.magic = extractJsonString(json, "magic");
        String vStr = extractJsonValue(json, "version");
        try { msg.version = Integer.parseInt(vStr); } catch (Exception e) { msg.version = 1; }
        msg.messageType = extractJsonString(json, "messageType");
        msg.studentId = extractJsonString(json, "studentId");
        String tsStr = extractJsonValue(json, "timestamp");
        try { msg.timestamp = Long.parseLong(tsStr); } catch (Exception e) { msg.timestamp = 0; }
        msg.payload = extractJsonString(json, "payload");
        return msg;
    }

    /**
     * Validates the message against CSM218 protocol requirements.
     */
    public void validate() throws IllegalArgumentException {
        if (!PROTOCOL_MAGIC.equals(magic)) {
            throw new IllegalArgumentException("Invalid magic: expected CSM218, got " + magic);
        }
        if (version != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Invalid protocol version: expected 1, got " + version);
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("Missing messageType field");
        }
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String extractJsonString(String json, String key) {
        String searchKey = "\"" + key + "\":\"";
        int start = json.indexOf(searchKey);
        if (start < 0) return "";
        start += searchKey.length();
        int end = start;
        while (end < json.length()) {
            if (json.charAt(end) == '\\') {
                end += 2;
            } else if (json.charAt(end) == '"') {
                break;
            } else {
                end++;
            }
        }
        if (end >= json.length()) return "";
        return json.substring(start, end).replace("\\\\", "\\").replace("\\\"", "\"");
    }

    private static String extractJsonValue(String json, String key) {
        String searchKey = "\"" + key + "\":";
        int start = json.indexOf(searchKey);
        if (start < 0) return "";
        start += searchKey.length();
        while (start < json.length() && json.charAt(start) == ' ') start++;
        int end = start;
        while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') end++;
        return json.substring(start, end).trim();
    }

    @Override
    public String toString() {
        return "Message{magic='" + magic + "', version=" + version +
                ", messageType='" + messageType + "', studentId='" + studentId +
                "', timestamp=" + timestamp + ", payloadLength=" +
                (payload != null ? payload.length() : 0) + "}";
    }
}
