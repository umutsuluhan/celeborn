package rdma_comms;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledDirectByteBuf;
import java.nio.ByteBuffer;

public class CustomRDMAByteBuf extends UnpooledDirectByteBuf {
  private final Runnable releaseHook;

  public CustomRDMAByteBuf(ByteBufAllocator alloc, ByteBuffer buffer, int maxCapacity, Runnable releaseHook) {
    super(alloc, buffer, maxCapacity);
    this.releaseHook = releaseHook;
  }

  @Override
  protected void deallocate() {
    super.deallocate();
    if (releaseHook != null) {
      releaseHook.run();
    }
  }
}
