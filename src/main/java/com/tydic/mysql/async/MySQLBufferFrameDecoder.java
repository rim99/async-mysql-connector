package com.tydic.mysql.async;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteOrder;

/**
 * Created by shihailong on 2017/9/21.
 */
public class MySQLBufferFrameDecoder extends LengthFieldBasedFrameDecoder {

    public MySQLBufferFrameDecoder() {
        super(Integer.MAX_VALUE, 0, 3, 1, 0);
    }

    @Override
    protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
        if (length == 3) {
            return buf.getUnsignedMediumLE(offset);
        } else {
            throw new DecoderException(
                    "unsupported lengthFieldLength: " + length + " (expected: 3)");
        }
    }
}
