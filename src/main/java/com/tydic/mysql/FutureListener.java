package com.tydic.mysql;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Created by shihailong on 2017/9/22.
 */
public interface FutureListener<T> extends GenericFutureListener<Future<T>> {
}
