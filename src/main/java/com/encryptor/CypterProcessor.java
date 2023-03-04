package com.encryptor;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.collections4.queue.CircularFifoQueue;

public class CypterProcessor<E> extends CircularFifoQueue<E> {


  private static final long serialVersionUID = -2144921221422328958L;

  private final ReentrantLock lock;

  private final Condition notFull;

  private final Condition notEmpty;

  public CypterProcessor() {
    super(2);
    lock = new ReentrantLock(true);
    notEmpty = lock.newCondition();
    notFull = lock.newCondition();
  }

  public void addElement(final E element) throws InterruptedException {

    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (isAtFullCapacity()) {
        notFull.await();
      }

      super.add(element);
      notEmpty.signal();
    } finally {
      lock.unlock();
    }

  }



  public E take() throws InterruptedException {
    final ReentrantLock lock = this.lock;
    E result = null;
    lock.lockInterruptibly();
    try {
      while (size() == 0) {
        notEmpty.await();
      }
      result = this.poll();
      notFull.signal();
    } finally {
      lock.unlock();
    }

    return result;
  }


}
